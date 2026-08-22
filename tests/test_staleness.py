"""The staleness sweep (spec §8, §14) — fixtures-first, end to end.

Driven through `staleness.sweep_staleness`, which is the composition an operator
surface or a scheduled driver will call: deliverable rows in, synthetic
`semantic_signal` findings out, minted (or deliberately not) by the SHIPPED
policy artifact through the same evaluation + minting machinery every real event
travels. Nothing here asserts that the sweep minted something the plugin decided
on: the assertions are about which POLICY fired, which is the §1 boundary the
design exists to hold.

The acceptance criteria under test, in §8's words and §14's:

- "staleness findings cite the exact source artifact versions and recorded
  deliverable provenance they compared" — both sides, in the minted body;
- "findings mint (deduplicated) staleness items through the same policy
  machinery; a finding whose deliverable is already covered by an open minted
  item does not double-file" — re-sweeping an unchanged stale state mints
  nothing more;
- a FURTHER revision is a new finding and a fresh mint, because the deliverable
  is now stale against different facts and the open item's citation is wrong;
- an up-to-date deliverable produces nothing at all;
- an unavailable governance never reads as "not stale" OR as "stale": the
  deliverables citing it are skipped and reported, and the ones citing healthy
  artifacts are unaffected;
- screenshot classes are ROUTED, not captured — §8 leaves capture to the asset
  plugin, so the class-qualified signal is the whole of this plugin's part;
- the same inputs produce the same finding ids in the same order, whatever the
  clock or the store did.

Everything that touches a store runs over BOTH store sets (the `sweep_stores`
fixture) for `test_watch.py`'s reason: the in-memory stores are what the
fixtures-first flow drives, so proving a convergence property only against
Postgres would prove it for half the paths that rely on it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from conftest import (
    COMPLETION_FIXTURES_DIR,
    MINTED_ITEM_REFS,
    POLICY_FIXTURES_DIR,
    TENANT,
)

from snowline_marketing.artifact_versions import (
    ArtifactVersionError,
    InMemoryArtifactVersions,
    VersionFailure,
)
from snowline_marketing.cursors import InMemoryCursorStore
from snowline_marketing.deliverables import (
    DeliverableProvenanceLedger,
    InMemoryDeliverables,
    SourceVersion,
)
from snowline_marketing.engine import StallReason
from snowline_marketing.events import EventEnvelope, EventType, parse_envelope
from snowline_marketing.ledger import (
    DeliveryLedger,
    DeliveryOutcome,
    InMemoryDeliveryLedger,
)
from snowline_marketing.minting import MintDisposition
from snowline_marketing.policy_cache import InMemoryPolicyCache
from snowline_marketing.policy_source import (
    InMemoryPolicyProvider,
    PolicyResolutionError,
    ResolutionFailure,
)
from snowline_marketing.quarantine import InMemoryCompletionQuarantine
from snowline_marketing.rendering import PROVENANCE_HEADING
from snowline_marketing.sources import FixturesEventSource
from snowline_marketing.staleness import (
    STALE_SIGNAL,
    SkippedDeliverable,
    StalenessFinding,
    SyntheticFindingSource,
    class_signal,
    finding_event_id,
    sweep_staleness,
)
from snowline_marketing.watch import run_intake_and_watch
from snowline_marketing.work_sink import InMemoryWorkItemSink

TURTLESEDGE_BODY = (POLICY_FIXTURES_DIR / "turtlesedge.json").read_text()
VERSION_ID = "gv-7f3a91c4"

# The scope findings are raised on — the tenant's marketing scope, which is what
# a caller configures (the sweep is TOLD, never guesses; see staleness.py).
SWEEP_SCOPE = "turtlesedge/marketing"

ITEM = MINTED_ITEM_REFS["provenance"]

# The three source artifacts the seeded deliverables cite, at the versions they
# recorded. The listing reflects two; the screenshot set reflects a third, so a
# governance failure on one can be shown NOT to touch the other.
MESSAGING = "b964d217"
LISTING_DOC = "9f21ac04"
SHOTS = "7c55e1b9"
RECORDED: dict[str, tuple[str, str | None]] = {
    MESSAGING: ("av-3c81f9d2", "v1.4"),
    LISTING_DOC: ("av-77b0e315", None),
    SHOTS: ("av-51d0aa73", "v1.4"),
}

PRODUCED_AT = datetime(2026, 7, 28, 9, 14, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 22, 7, 0, tzinfo=timezone.utc)

LISTING_KEY = (TENANT, ITEM, "app_store", "store_listing")
SCREENSHOT_KEY = (TENANT, ITEM, "app_store", "screenshot_set")


class Stores:
    """The two stores one sweep drives, kept together so the param fixture hands
    out a matched set (one delivery ledger, one deliverable ledger) rather than
    two independent choices."""

    def __init__(self, ledger, deliverables) -> None:
        self.ledger = ledger
        self.deliverables = deliverables


@pytest.fixture(params=["postgres", "memory"])
def sweep_stores(request) -> Stores:
    """One matched store set per param: the real Postgres-backed stores (riding
    `migrated_db`, so the whole param skips cleanly when Postgres is
    unreachable) or the in-memory ones the fixtures-first flow drives."""
    if request.param == "postgres":
        request.getfixturevalue("migrated_db")
        return Stores(DeliveryLedger(), DeliverableProvenanceLedger())
    return Stores(InMemoryDeliveryLedger(), InMemoryDeliverables())


def governance(revised: dict[str, tuple[str, str | None]] | None = None):
    """Governance holding exactly what the seeded deliverables recorded, except
    where a test revises an artifact — which is the event that makes every
    deliverable citing it stale."""
    versions = InMemoryArtifactVersions()
    for artifact_id, (version_id, milestone) in {
        **RECORDED,
        **(revised or {}),
    }.items():
        versions.put(artifact_id, version_id, milestone=milestone)
    return versions


class DownGovernance:
    """Governance that cannot answer for named artifacts and answers normally
    for the rest — the partial outage the sweep must not round off in either
    direction."""

    def __init__(self, inner, *, down: set[str]) -> None:
        self.inner = inner
        self.down = down

    def resolve(self, artifact_id: str):
        if artifact_id in self.down:
            return ArtifactVersionError(
                artifact_id=artifact_id,
                failure=VersionFailure.unavailable,
                detail="governance unreachable",
            )
        return self.inner.resolve(artifact_id)


class DownPolicies:
    """A policy provider having a bad afternoon (`test_engine.DownProvider`)."""

    def resolve(self, tenant: str):
        return PolicyResolutionError(
            tenant=tenant,
            failure=ResolutionFailure.unavailable,
            detail="governance unreachable",
        )


def provider_for(body: str = TURTLESEDGE_BODY) -> InMemoryPolicyProvider:
    provider = InMemoryPolicyProvider()
    provider.put(TENANT, VERSION_ID, body)
    return provider


def seed_listing(stores: Stores, **overrides):
    """The App Store listing deliverable, as a completion would have recorded
    it (spec §8's watch) — seeded through the store's own verb, so the row the
    sweep reads is the row production holds."""
    values = {
        "tenant": TENANT,
        "item_ref": ITEM,
        "channel": "app_store",
        "deliverable_class": "store_listing",
        "source_versions": (
            SourceVersion(MESSAGING, *_recorded(MESSAGING)),
            SourceVersion(LISTING_DOC, *_recorded(LISTING_DOC)),
        ),
        "produced_at": PRODUCED_AT,
        "event_id": "pm-evt-0000501",
        "external_url": "https://apps.apple.com/app/turtletracks/id6470000000",
    }
    values.update(overrides)
    return stores.deliverables.upsert(**values)


def seed_screenshots(stores: Stores, **overrides):
    """The screenshot set deliverable — a DIFFERENT class citing a DIFFERENT
    artifact, which is what makes the routing and isolation cases separable."""
    values = {
        "tenant": TENANT,
        "item_ref": ITEM,
        "channel": "app_store",
        "deliverable_class": "screenshot_set",
        "source_versions": (SourceVersion(SHOTS, *_recorded(SHOTS)),),
        "produced_at": PRODUCED_AT,
        "event_id": "pm-evt-0000501",
    }
    values.update(overrides)
    return stores.deliverables.upsert(**values)


def _recorded(artifact_id: str) -> tuple[str, str | None]:
    return RECORDED[artifact_id]


def seed_minted(ledger, item_ref: str, *, policy_id: str = "listing-regeneration"):
    """A `created` delivery-ledger row naming `item_ref` — what a mint leaves
    behind (spec §7), written through the ledger's own verbs, so the watch's
    join is against the row production would hold (`test_watch.seed_minted`)."""
    key = f"{TENANT}:{policy_id}:{item_ref}"
    ledger.record(
        tenant=TENANT,
        dedup_key=key,
        policy_id=policy_id,
        event_id=f"pm-evt-mint-{item_ref}",
        event_type="milestone_released",
        outcome=DeliveryOutcome.matched,
        policy_version_id=VERSION_ID,
    )
    stored = f"p:{key}"
    ledger.claim(TENANT, stored, detail="mint in flight")
    ledger.confirm_created(TENANT, stored, item_ref=item_ref, detail="minted")
    return stored


def sweep(
    stores: Stores,
    versions=None,
    *,
    provider=None,
    sink=None,
    cache=None,
    clock=None,
):
    """One full sweep: read, resolve, compare, synthesize, evaluate, mint."""
    return sweep_staleness(
        TENANT,
        scope=SWEEP_SCOPE,
        versions=versions if versions is not None else governance(),
        provider=provider if provider is not None else provider_for(),
        sink=sink if sink is not None else InMemoryWorkItemSink(),
        deliverables=stores.deliverables,
        ledger=stores.ledger,
        cache=cache if cache is not None else InMemoryPolicyCache(),
        clock=clock if clock is not None else (lambda: OBSERVED_AT),
    )


def by_policy(report) -> dict[str, object]:
    return {outcome.policy_id: outcome for outcome in report.mint.outcomes}


# --- the comparison ----------------------------------------------------------


def test_an_up_to_date_deliverable_produces_nothing(sweep_stores):
    seed_listing(sweep_stores)
    seed_screenshots(sweep_stores)
    report = sweep(sweep_stores)
    assert report.ok
    assert len(report.examined) == 2
    assert report.findings == ()
    assert report.skipped == ()
    assert report.mint.outcomes == ()
    # Every artifact was still asked about — "nothing is stale" is a CONCLUSION
    # the sweep reached, not a question it skipped.
    assert {v.artifact_id for v in report.resolved} == {MESSAGING, LISTING_DOC, SHOTS}


def test_a_stale_deliverable_becomes_one_finding_citing_both_sides(sweep_stores):
    seed_listing(sweep_stores)
    seed_screenshots(sweep_stores)
    report = sweep(sweep_stores, governance({MESSAGING: ("av-newer", "v1.5")}))
    # Only the deliverable that CITES the revised artifact is stale.
    assert [finding.identity for finding in report.findings] == [LISTING_KEY]
    finding = report.findings[0]
    # Both sides of the whole compared set, not only the difference (§14).
    assert {
        (c.artifact_id, c.recorded_version_id, c.current_version_id)
        for c in finding.comparisons
    } == {
        (MESSAGING, "av-3c81f9d2", "av-newer"),
        (LISTING_DOC, "av-77b0e315", "av-77b0e315"),
    }
    assert [c.artifact_id for c in finding.stale] == [MESSAGING]
    details = finding.details
    assert details["recorded_versions"] == (
        f"{LISTING_DOC}=av-77b0e315, {MESSAGING}=av-3c81f9d2"
    )
    assert details["current_versions"] == (
        f"{LISTING_DOC}=av-77b0e315, {MESSAGING}=av-newer"
    )
    assert details["comparison"] == (
        f"{MESSAGING}: recorded av-3c81f9d2 → current av-newer"
    )
    # The recorded deliverable provenance it compared (§14's second half).
    assert details["deliverable_event_id"] == "pm-evt-0000501"
    # The completion's own time, offset-carrying (a store may hand it back in
    # any timezone; the INSTANT is the fact).
    assert datetime.fromisoformat(details["deliverable_produced_at"]) == PRODUCED_AT
    assert details["deliverable_external_url"].endswith("id6470000000")
    # The milestone stamps refine the story and never trigger anything.
    assert details["recorded_milestones"] == f"{MESSAGING}=v1.4"
    assert details["current_milestones"] == f"{MESSAGING}=v1.5"
    assert details["milestone_moved_artifacts"] == MESSAGING


def test_a_milestone_move_alone_is_not_staleness(sweep_stores):
    # Snowline#141's stamp gives the release boundary, but version inequality is
    # v1's only trigger (spec §8) — so the sweep works unchanged against
    # artifacts nobody has stamped, and a re-stamp of the SAME version mints
    # nothing.
    seed_screenshots(sweep_stores)
    report = sweep(sweep_stores, governance({SHOTS: ("av-51d0aa73", "v1.5")}))
    assert report.findings == ()
    assert report.mint.outcomes == ()


def test_a_deliverable_is_compared_whole_or_not_at_all(sweep_stores):
    # The listing cites one revised artifact AND one unresolvable one. A finding
    # synthesized from a partial current-version set would carry an id derived
    # from facts still unknown, and the sweep after recovery would mint a second
    # item for the same staleness.
    seed_listing(sweep_stores)
    report = sweep(
        sweep_stores,
        DownGovernance(
            governance({MESSAGING: ("av-newer", "v1.5")}), down={LISTING_DOC}
        ),
    )
    assert report.findings == ()
    assert [skipped.identity for skipped in report.skipped] == [LISTING_KEY]
    assert [error.artifact_id for error in report.skipped[0].unresolved] == [
        LISTING_DOC
    ]
    assert report.mint.outcomes == ()


# --- minting through the policy machinery ------------------------------------


def test_a_stale_deliverable_mints_one_item_citing_the_versions(sweep_stores):
    sink = InMemoryWorkItemSink()
    seed_listing(sweep_stores)
    report = sweep(
        sweep_stores, governance({MESSAGING: ("av-newer", "v1.5")}), sink=sink
    )
    assert report.intake.ok
    assert len(report.minted) == 1
    minted = report.minted[0]
    # The TENANT's policy decided everything about the item — the plugin only
    # said "this deliverable is stale" (spec §1).
    assert minted.policy_id == "review-sweep-on-stale-deliverable"
    assert minted.disposition is MintDisposition.created

    (request,) = sink.requests
    assert request.scope == "turtlesedge/marketing"
    assert request.initiative == "monthly-loop"
    assert request.title == (
        "Refresh the store_listing on app_store — its sources moved"
    )
    # §14: the exact versions compared, on both sides, in the body.
    assert "av-3c81f9d2" in request.body and "av-newer" in request.body
    assert f"{MESSAGING}: recorded av-3c81f9d2 → current av-newer" in request.body
    # The recorded deliverable provenance it compared against.
    assert "pm-evt-0000501" in request.body
    # And §7's provenance block, appended whatever the template said — the
    # finding's own synthetic event id is the delivery's audit trail.
    assert PROVENANCE_HEADING in request.body
    assert report.findings[0].event_id in request.body
    assert request.event_id == report.findings[0].event_id

    # The ledger row the finding claimed: a real delivery, keyed on the
    # deterministic id, naming the minted item.
    row = sweep_stores.ledger.get(TENANT, minted.record.dedup_key)
    assert row.outcome is DeliveryOutcome.created
    assert row.created_item_ref == minted.item_ref
    assert row.event_id == report.findings[0].event_id
    assert row.policy_version_id == VERSION_ID


def test_re_sweeping_an_unchanged_stale_state_does_not_double_file(sweep_stores):
    # Spec §8: "a finding whose deliverable is already covered by an open minted
    # item does not double-file". Nothing staleness-specific enforces it — the
    # deterministic event id renders the same dedup key and the delivery ledger
    # does the rest.
    sink = InMemoryWorkItemSink()
    seed_listing(sweep_stores)
    revised = governance({MESSAGING: ("av-newer", "v1.5")})
    first = sweep(sweep_stores, revised, sink=sink)
    second = sweep(
        sweep_stores,
        revised,
        sink=sink,
        clock=lambda: OBSERVED_AT + timedelta(days=1),
    )
    assert len(first.minted) == 1
    # The finding is still SYNTHESIZED — the sweep re-observes the staleness
    # every pass; what changes is that the delivery owes nothing, so no
    # consequence reaches the minting pass at all.
    assert [f.event_id for f in second.findings] == [f.event_id for f in first.findings]
    assert second.mint.outcomes == ()
    # One item in PM across two sweeps a day apart, and the row still names it.
    assert len(sink.requests) == 1
    row = sweep_stores.ledger.get(TENANT, first.minted[0].record.dedup_key)
    assert row.outcome is DeliveryOutcome.created
    assert row.created_item_ref == first.minted[0].item_ref


def test_a_further_revision_is_a_new_finding_and_a_fresh_mint(sweep_stores):
    # The deliverable is now stale against DIFFERENT facts, and the open item
    # cites versions that are themselves out of date — collapsing the two would
    # leave an operator working from a citation the sweep knows is wrong.
    sink = InMemoryWorkItemSink()
    seed_listing(sweep_stores)
    first = sweep(
        sweep_stores, governance({MESSAGING: ("av-newer", "v1.5")}), sink=sink
    )
    second = sweep(
        sweep_stores, governance({MESSAGING: ("av-newest", "v1.6")}), sink=sink
    )
    assert len(first.minted) == 1 and len(second.minted) == 1
    assert first.findings[0].event_id != second.findings[0].event_id
    assert len(sink.requests) == 2
    assert "av-newest" in sink.requests[1].body


def test_the_class_qualified_signal_routes_screenshots(sweep_stores):
    # §8 leaves CAPTURE to the asset plugin; the marketing plugin only tracks
    # staleness and mints review work. Routing is therefore the whole of this
    # plugin's part, and it is a tenant policy's decision — reached through a
    # signal derived mechanically from the tenant's own deliverable class.
    sink = InMemoryWorkItemSink()
    seed_screenshots(sweep_stores)
    report = sweep(sweep_stores, governance({SHOTS: ("av-reshot", "v1.5")}), sink=sink)
    finding = report.findings[0]
    assert finding.signals == (STALE_SIGNAL, class_signal("screenshot_set"))
    minted = by_policy(report)
    # Both the general staleness policy and the screenshot-specific one select
    # this finding — the documented consequence of carrying the bare signal
    # alongside the qualified one (staleness.py), and the same behaviour any two
    # overlapping policy entries already have.
    assert set(minted) == {
        "review-sweep-on-stale-deliverable",
        "screenshot-review-on-stale-screenshots",
    }
    screenshots = minted["screenshot-review-on-stale-screenshots"]
    assert screenshots.disposition is MintDisposition.created
    request = [r for r in sink.requests if r.policy_id.startswith("screenshot")][0]
    assert request.title == f"Recapture the screenshot set for {ITEM}"
    assert "av-51d0aa73" in request.body and "av-reshot" in request.body


def test_a_stale_deliverable_of_another_tenant_is_never_swept(sweep_stores):
    # The sweep reads ONE tenant's rows and stamps every envelope with that
    # tenant — isolation is structural, not a predicate (§3/§14).
    seed_listing(sweep_stores)
    seed_listing(sweep_stores, tenant="snowlinedev", item_ref="sd-item-1")
    report = sweep(sweep_stores, governance({MESSAGING: ("av-newer", "v1.5")}))
    assert [f.deliverable.tenant for f in report.findings] == [TENANT]
    assert [f.identity for f in report.findings] == [LISTING_KEY]


# --- governance failure -------------------------------------------------------


def test_an_unavailable_governance_stalls_only_what_cites_it(sweep_stores):
    # Never "not stale" (drift hidden) and never "stale" (work minted against
    # evidence nobody has): the deliverables citing the unreachable artifact are
    # skipped and REPORTED, and the ones citing healthy artifacts still mint.
    sink = InMemoryWorkItemSink()
    seed_listing(sweep_stores)
    seed_screenshots(sweep_stores)
    report = sweep(
        sweep_stores,
        DownGovernance(governance({SHOTS: ("av-reshot", "v1.5")}), down={MESSAGING}),
        sink=sink,
    )
    assert [skipped.identity for skipped in report.skipped] == [LISTING_KEY]
    assert [f.identity for f in report.findings] == [SCREENSHOT_KEY]
    assert {o.artifact_id for o in report.unresolved} == {MESSAGING}
    # Visible: a sweep that could not judge everything is not `ok`, even though
    # nothing crashed and the pass itself ran clean.
    assert report.intake.ok
    assert not report.ok
    assert "unavailable" in report.skipped[0].detail
    # The healthy half minted, unaffected.
    assert len(sink.requests) == 2


def test_a_missing_artifact_is_reported_distinctly(sweep_stores):
    # Governance ANSWERED: there is no such artifact. Still nothing to compare —
    # but a different operator fix, and retrying will not help.
    seed_listing(sweep_stores, source_versions=(SourceVersion("ghost", "av-1"),))
    report = sweep(sweep_stores)
    assert report.findings == ()
    assert report.skipped[0].unresolved[0].failure is VersionFailure.not_found


def test_a_policy_stall_mints_nothing_and_says_why(sweep_stores):
    # The tenant's rules could not be read: the pass stops exactly as it does
    # for a real event, nothing is minted, and nothing is lost — the findings
    # are recomputed from the ledger next sweep.
    sink = InMemoryWorkItemSink()
    seed_listing(sweep_stores)
    report = sweep(
        sweep_stores,
        governance({MESSAGING: ("av-newer", "v1.5")}),
        provider=DownPolicies(),
        sink=sink,
    )
    assert len(report.findings) == 1
    assert report.stall is not None
    assert report.stall.reason is StallReason.policy_unavailable
    assert not report.intake.ok
    assert sink.requests == []
    assert not report.ok


# --- determinism --------------------------------------------------------------


def test_the_same_inputs_produce_the_same_findings_in_the_same_order():
    # Two stores seeded in OPPOSITE order: the sweep sorts by natural key, so
    # neither the store's listing order nor the insertion order can reach the
    # synthetic stream.
    forward = Stores(InMemoryDeliveryLedger(), InMemoryDeliverables())
    backward = Stores(InMemoryDeliveryLedger(), InMemoryDeliverables())
    seed_listing(forward)
    seed_screenshots(forward)
    seed_screenshots(backward)
    seed_listing(backward)
    revised = governance({MESSAGING: ("av-newer", None), SHOTS: ("av-reshot", None)})
    first = sweep(forward, revised)
    second = sweep(backward, revised, clock=lambda: OBSERVED_AT + timedelta(hours=3))
    assert [f.identity for f in first.findings] == [SCREENSHOT_KEY, LISTING_KEY]
    assert [f.event_id for f in first.findings] == [f.event_id for f in second.findings]


def test_a_finding_id_is_a_function_of_its_facts_and_nothing_else():
    def id_for(**overrides) -> str:
        values = {
            "tenant": TENANT,
            "item_ref": ITEM,
            "channel": "app_store",
            "deliverable_class": "store_listing",
            "current_versions": [(MESSAGING, "av-newer"), (LISTING_DOC, "av-77b0e315")],
        }
        values.update(overrides)
        return finding_event_id(**values)

    baseline = id_for()
    # Stable across calls (and across processes: sha256, never Python's salted
    # hash — a dedup key that changed on restart would re-mint everything).
    assert baseline == id_for()
    # Order of the current versions is not a fact.
    assert baseline == id_for(
        current_versions=[(LISTING_DOC, "av-77b0e315"), (MESSAGING, "av-newer")]
    )
    # Every component of the deliverable's identity separates findings...
    assert baseline != id_for(tenant="snowlinedev")
    assert baseline != id_for(item_ref="mkt-item-0002")
    assert baseline != id_for(channel="website")
    assert baseline != id_for(deliverable_class="screenshot_set")
    # ...and so does a further revision of any cited artifact.
    assert baseline != id_for(
        current_versions=[(MESSAGING, "av-newest"), (LISTING_DOC, "av-77b0e315")]
    )
    assert baseline.startswith("mkt-stale-")


def test_the_clock_is_not_part_of_a_finding_id(sweep_stores):
    seed_listing(sweep_stores)
    revised = governance({MESSAGING: ("av-newer", None)})
    early = sweep(sweep_stores, revised, clock=lambda: OBSERVED_AT)
    late = sweep(sweep_stores, revised, clock=lambda: OBSERVED_AT + timedelta(days=30))
    assert early.findings[0].event_id == late.findings[0].event_id
    # ...but the envelope still says when the sweep looked.
    assert late.findings[0].envelope.occurred_at == OBSERVED_AT + timedelta(days=30)


# --- the synthetic stream -----------------------------------------------------


def test_synthesized_envelopes_are_legal_envelopes():
    # They travel the intake loop exactly as an outbox event does, so they must
    # survive `parse_envelope` — the loop's own front door.
    store = Stores(InMemoryDeliveryLedger(), InMemoryDeliverables())
    seed_listing(store)
    seed_screenshots(store)
    report = sweep(
        store,
        governance({MESSAGING: ("av-newer", None), SHOTS: ("av-reshot", None)}),
    )
    source = SyntheticFindingSource(TENANT, report.findings)
    assert source.source_key == f"staleness:{TENANT}"
    raw = list(source.read())
    assert [event.position for event in raw] == ["000000", "000001"]
    # Positions are lexicographically monotone (the `EventSource` contract) and
    # `after` filtering rides them.
    assert [e.position for e in source.read(after="000000")] == ["000001"]
    for event, finding in zip(raw, report.findings):
        parsed = parse_envelope(event.body, ref=event.ref, position=event.position)
        assert isinstance(parsed, EventEnvelope), parsed
        assert parsed.event_type is EventType.semantic_signal
        assert parsed.event_id == finding.event_id
        assert parsed.tenant == TENANT
        assert parsed.payload.scope == SWEEP_SCOPE
        assert parsed.subject.id == ITEM
        assert STALE_SIGNAL in parsed.payload.signals
        # The comparison facts survive the round trip as text a template quotes.
        assert parsed.payload.details["comparison"] == finding.details["comparison"]


def test_the_comparison_answers_with_a_type(sweep_stores):
    # Three answers, and the TYPE says which, so no caller has to re-derive
    # "was this stale?" from a report field that could disagree.
    seed_listing(sweep_stores)
    seed_screenshots(sweep_stores)
    report = sweep(
        sweep_stores,
        DownGovernance(governance({SHOTS: ("av-reshot", None)}), down={LISTING_DOC}),
    )
    assert all(isinstance(f, StalenessFinding) for f in report.findings)
    assert all(isinstance(s, SkippedDeliverable) for s in report.skipped)
    assert len(report.findings) == 1 and len(report.skipped) == 1


# --- the watch and the sweep, composed ----------------------------------------


def test_what_the_watch_recorded_is_what_the_sweep_compares(sweep_stores):
    """The two halves of §8, end to end: a completion records deliverable rows,
    and the sweep finds the one whose source artifact has since moved."""
    seed_minted(sweep_stores.ledger, ITEM)
    watched = run_intake_and_watch(
        FixturesEventSource(COMPLETION_FIXTURES_DIR),
        tenant=TENANT,
        provider=provider_for(),
        sink=InMemoryWorkItemSink(),
        cursor_store=InMemoryCursorStore(),
        ledger=sweep_stores.ledger,
        deliverables=sweep_stores.deliverables,
        quarantine=InMemoryCompletionQuarantine(),
        cache=InMemoryPolicyCache(),
        on_malformed=lambda malformed: None,
    )
    assert watched.intake.ok
    # The capture's listing cites b964d217 + 9f21ac04; its screenshot set cites
    # 9f21ac04 alone.
    sink = InMemoryWorkItemSink()
    report = sweep(
        sweep_stores, governance({MESSAGING: ("av-newer", "v1.5")}), sink=sink
    )
    assert [f.identity for f in report.findings] == [LISTING_KEY]
    assert report.findings[0].details["deliverable_event_id"] == "pm-evt-0000501"
    assert len(sink.requests) == 1
    assert "av-3c81f9d2" in sink.requests[0].body

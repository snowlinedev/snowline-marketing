"""The provenance watch (spec §8, §14) — fixtures-first, end to end.

Driven over the SHIPPED completion capture and the SHIPPED policy artifact
through `run_intake_and_watch`, which is the same composition a scheduled driver
will call: the watch runs PER EVENT inside the intake handler, BEFORE the ack.
The `created` delivery-ledger rows the watch joins against are seeded through
the ledger's own verbs — record, claim, confirm — because that is exactly what a
mint leaves behind; one test drives the REAL minting composition first and
completes the item it actually minted, so the join is proved against a genuinely
minted ref and not only against a hand-written one.

The acceptance criteria under test, in §14's words and §8's:

- a provenance-full completion records deliverable rows citing the exact source
  artifact versions it declared;
- "a provenance-less completion is visible in quarantine within one sweep" — and
  stays ONE open row across repeated deliveries;
- a malformed declaration quarantines with a reason naming the defect, never as
  though nothing had been declared;
- completions of items this plugin never minted are not its business and are
  passed through untouched;
- provenance attached after the fact records the deliverables and closes the
  row; dismissal closes it and records nothing;
- a crash before the ack re-delivers and converges — one row either way.

Everything runs over BOTH store sets (the `watch_stores` fixture) for
`test_ledger.py`'s reason: the in-memory stores are what the fixtures-first flow
drives, so proving a convergence property only against Postgres would prove it
for half the paths that rely on it.
"""

from __future__ import annotations

import json

import pytest
from conftest import (
    COMPLETION_FIXTURES_DIR,
    EVENT_FIXTURES_DIR,
    MINTED_ITEM_REFS,
    NEVER_MINTED_ITEM_REF,
    POLICY_FIXTURES_DIR,
    SCOPE,
    TENANT,
    make_envelope,
)

from snowline_marketing.cursors import InMemoryCursorStore
from snowline_marketing.deliverables import (
    DeliverableProvenanceLedger,
    InMemoryDeliverables,
)
from snowline_marketing.events import EventEnvelope, EventType, parse_envelope
from snowline_marketing.ledger import (
    DeliveryLedger,
    DeliveryOutcome,
    InMemoryDeliveryLedger,
)
from snowline_marketing.minting import MintDisposition, run_intake_and_mint
from snowline_marketing.policy_cache import InMemoryPolicyCache
from snowline_marketing.policy_source import InMemoryPolicyProvider
from snowline_marketing.provenance import (
    PROVENANCE_DETAILS_KEY,
    DeliverableProvenance,
    parse_provenance,
)
from snowline_marketing.quarantine import (
    CompletionQuarantine,
    InMemoryCompletionQuarantine,
    QuarantineReason,
    QuarantineStatus,
)
from snowline_marketing.sources import FixturesEventSource
from snowline_marketing.watch import (
    WatchDisposition,
    dismiss_quarantined,
    resolve_quarantined,
    run_intake_and_watch,
    watch_completion,
)
from snowline_marketing.work_sink import InMemoryWorkItemSink, SinkUnavailable

TURTLESEDGE_BODY = (POLICY_FIXTURES_DIR / "turtlesedge.json").read_text()
VERSION_ID = "gv-7f3a91c4"

# The shipped completion capture, in stream order. The middle two are named
# here because tests read their envelopes directly; the malformed and
# never-minted files are reached only through the drive, by event id.
WITH_PROVENANCE = "0010-completion-with-provenance.json"
WITHOUT_PROVENANCE = "0020-completion-without-provenance.json"


class Stores:
    """The three stores one watch pass drives, kept together so the param
    fixture hands out a matched set (one delivery ledger, one deliverable
    ledger, one quarantine) rather than three independent choices."""

    def __init__(self, ledger, deliverables, quarantine) -> None:
        self.ledger = ledger
        self.deliverables = deliverables
        self.quarantine = quarantine


@pytest.fixture(params=["postgres", "memory"])
def watch_stores(request) -> Stores:
    """One matched store set per param: the real Postgres-backed stores (riding
    `migrated_db`, so the whole param skips cleanly when Postgres is unreachable)
    or the in-memory ones the fixtures-first flow drives."""
    if request.param == "postgres":
        request.getfixturevalue("migrated_db")
        return Stores(
            DeliveryLedger(), DeliverableProvenanceLedger(), CompletionQuarantine()
        )
    return Stores(
        InMemoryDeliveryLedger(), InMemoryDeliverables(), InMemoryCompletionQuarantine()
    )


def provider_for(body: str = TURTLESEDGE_BODY) -> InMemoryPolicyProvider:
    provider = InMemoryPolicyProvider()
    provider.put(TENANT, VERSION_ID, body)
    return provider


def seed_minted(
    ledger, item_ref: str, *, policy_id: str = "listing-regeneration"
) -> str:
    """A `created` delivery-ledger row naming `item_ref` — what a mint leaves
    behind (spec §7), written through the ledger's own verbs so the row the
    watch joins against is the row production would hold. Returns the stored
    key."""
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


def seed_the_capture(stores: Stores) -> None:
    """Seed a `created` row for every item the completion capture names as
    marketing-minted. The never-minted one is deliberately left unseeded — it is
    the capture's control case."""
    for item_ref in MINTED_ITEM_REFS.values():
        seed_minted(stores.ledger, item_ref)


def drive(stores: Stores, *, sink=None, cursor_store=None, quarantine=None):
    """One full pass over the shipped completion capture: intake, watch,
    evaluate, mint.

    A fresh cursor store by default, so a second call re-delivers the WHOLE
    capture — the at-least-once behaviour §14's convergence criteria are about,
    forced rather than waited for."""
    return run_intake_and_watch(
        FixturesEventSource(COMPLETION_FIXTURES_DIR),
        tenant=TENANT,
        provider=provider_for(),
        sink=sink if sink is not None else InMemoryWorkItemSink(),
        cursor_store=cursor_store or InMemoryCursorStore(),
        ledger=stores.ledger,
        deliverables=stores.deliverables,
        quarantine=quarantine if quarantine is not None else stores.quarantine,
        cache=InMemoryPolicyCache(),
        on_malformed=lambda malformed: None,
    )


def fixture_envelope(name: str, directory=COMPLETION_FIXTURES_DIR) -> EventEnvelope:
    parsed = parse_envelope((directory / name).read_bytes())
    assert isinstance(parsed, EventEnvelope), getattr(parsed, "detail", "")
    return parsed


def declaration(name: str = WITH_PROVENANCE) -> DeliverableProvenance:
    parsed = parse_provenance(fixture_envelope(name))
    assert isinstance(parsed, DeliverableProvenance), parsed
    return parsed


def by_event(report) -> dict[str, object]:
    return {outcome.event_id: outcome for outcome in report.outcomes}


# --- §14: a provenance-full completion records exactly what it declared --------


def test_a_declared_completion_records_its_deliverables(watch_stores):
    seed_the_capture(watch_stores)
    result = drive(watch_stores)
    assert result.intake.ok
    outcome = by_event(result.watch)["pm-evt-0000501"]
    assert outcome.disposition is WatchDisposition.recorded
    # The join that made this the watch's business, carried on the outcome so an
    # operator can get from a deliverable back to the policy that minted it.
    assert outcome.minted_by.outcome is DeliveryOutcome.created

    rows = watch_stores.deliverables.list_for_item(
        TENANT, MINTED_ITEM_REFS["provenance"]
    )
    assert [row.deliverable_class for row in rows] == [
        "screenshot_set",
        "store_listing",
    ]
    listing = rows[1]
    # The EXACT version ids, with their milestone stamps — this is what §8's
    # sweep compares, so a row that rounded them off would make every later
    # finding unciteable.
    assert [
        (v.artifact_id, v.version_id, v.milestone) for v in listing.source_versions
    ] == [("9f21ac04", "av-77b0e315", None), ("b964d217", "av-3c81f9d2", "v1.4")]
    assert listing.external_url.endswith("id6470000000")
    assert listing.event_id == "pm-evt-0000501"
    # `produced_at` is the completion's own time, never a producer-declared one.
    assert listing.produced_at == fixture_envelope(WITH_PROVENANCE).occurred_at
    # Nothing quarantined for a completion that declared what it produced.
    assert watch_stores.quarantine.get(TENANT, "pm-evt-0000501") is None


def test_a_re_delivered_declaration_converges_on_the_same_rows(watch_stores):
    seed_the_capture(watch_stores)
    first = drive(watch_stores)
    second = drive(watch_stores)
    item = MINTED_ITEM_REFS["provenance"]
    assert len(watch_stores.deliverables.list_for_item(TENANT, item)) == 2
    assert first.watch.counts[WatchDisposition.recorded] == 1
    assert second.watch.counts[WatchDisposition.recorded] == 1
    for row in watch_stores.deliverables.list_for_item(TENANT, item):
        # Convergence, not a second deliverable: the row was re-declared, which
        # `updated_at` records and `created_at` deliberately does not.
        assert row.updated_at is not None


# --- §14: "a provenance-less completion is visible in quarantine" -------------


def test_a_provenance_less_completion_is_visible_in_quarantine_within_one_sweep(
    watch_stores,
):
    seed_the_capture(watch_stores)
    result = drive(watch_stores)
    outcome = by_event(result.watch)["pm-evt-0000502"]
    assert outcome.disposition is WatchDisposition.quarantined_missing
    assert outcome.needs_operator

    (row,) = [
        row
        for row in watch_stores.quarantine.list_open(TENANT)
        if row.item_ref == MINTED_ITEM_REFS["missing"]
    ]
    assert row.reason is QuarantineReason.provenance_missing
    assert row.status is QuarantineStatus.open
    assert PROVENANCE_DETAILS_KEY in row.detail
    # The row names the delivery that minted the item, so the operator can see
    # which policy asked for the work in the first place.
    assert MINTED_ITEM_REFS["missing"] in row.detail
    # The completion is kept WHOLE — the resolve verb reads it.
    assert json.loads(row.raw_event)["event_id"] == "pm-evt-0000502"
    # The pass itself did not fail: no completion gate, no friction (spec §8).
    assert result.intake.ok
    assert not result.ok  # ...but a human has to resolve the row


def test_repeated_deliveries_leave_one_open_quarantine_row(watch_stores):
    seed_the_capture(watch_stores)
    drive(watch_stores)
    drive(watch_stores)
    drive(watch_stores)
    assert [row.event_id for row in watch_stores.quarantine.list_open(TENANT)] == [
        "pm-evt-0000502",
        "pm-evt-0000503",
    ]


def test_a_malformed_declaration_names_the_defect(watch_stores):
    # Never filed as "absent": a producer bug reported as an operator's omission
    # sends someone looking for a human who did nothing wrong.
    seed_the_capture(watch_stores)
    result = drive(watch_stores)
    outcome = by_event(result.watch)["pm-evt-0000503"]
    assert outcome.disposition is WatchDisposition.quarantined_malformed
    row = watch_stores.quarantine.get(TENANT, "pm-evt-0000503")
    assert row.reason is QuarantineReason.provenance_malformed
    assert "deliverable_class" in row.detail
    assert "source_artifact_versions" in row.detail
    # Nothing was recorded for it — a broken declaration is not a partial one.
    assert (
        watch_stores.deliverables.list_for_item(TENANT, MINTED_ITEM_REFS["malformed"])
        == []
    )


# --- completions this plugin never minted are not its business ----------------


def test_a_completion_of_a_never_minted_item_is_passed_through_untouched(watch_stores):
    seed_the_capture(watch_stores)
    result = drive(watch_stores)
    outcome = by_event(result.watch)["pm-evt-0000504"]
    assert outcome.disposition is WatchDisposition.not_marketing_minted
    assert not outcome.needs_operator
    assert outcome.quarantine is None
    assert watch_stores.deliverables.list_for_item(TENANT, NEVER_MINTED_ITEM_REF) == []
    assert watch_stores.quarantine.get(TENANT, "pm-evt-0000504") is None
    # Untouched by the WATCH, not ignored by the plugin: the same completion
    # still matched a policy and minted marketing follow-through, which is what
    # ordinary roadmap work completing is supposed to do.
    assert result.mint.counts[MintDisposition.created] == 1


def test_no_completion_in_the_main_capture_is_the_watchs_business(watch_stores):
    # The shipped v1 stream drives every other event type past the watch. With
    # no `created` row naming any of their subjects, not one of them writes
    # anything — the silence the module docstring insists on.
    result = run_intake_and_watch(
        FixturesEventSource(EVENT_FIXTURES_DIR),
        tenant=TENANT,
        provider=provider_for(),
        sink=InMemoryWorkItemSink(),
        cursor_store=InMemoryCursorStore(),
        ledger=watch_stores.ledger,
        deliverables=watch_stores.deliverables,
        quarantine=watch_stores.quarantine,
        cache=InMemoryPolicyCache(),
        on_malformed=lambda malformed: None,
    )
    assert set(result.watch.counts) <= {
        WatchDisposition.not_a_completion,
        WatchDisposition.not_marketing_minted,
        # The capture's cross-tenant envelope: the ENGINE quarantines that
        # delivery (§14); the watch writes nothing under a tenant it does not
        # own, which is a different refusal for a different reason.
        WatchDisposition.foreign_tenant,
    }
    assert result.watch.counts[WatchDisposition.foreign_tenant] == 1
    assert result.watch.quarantined == ()
    assert watch_stores.quarantine.list_open(TENANT) == []


def test_the_watch_joins_against_an_item_the_real_minting_pass_created(watch_stores):
    # The seeded rows above are what a mint leaves behind; this proves it against
    # a ref the REAL composition actually minted, so the join cannot be an
    # agreement between two hand-written strings.
    sink = InMemoryWorkItemSink()
    minted = run_intake_and_mint(
        FixturesEventSource(EVENT_FIXTURES_DIR),
        tenant=TENANT,
        provider=provider_for(),
        sink=sink,
        cursor_store=InMemoryCursorStore(),
        ledger=watch_stores.ledger,
        cache=InMemoryPolicyCache(),
        on_malformed=lambda malformed: None,
    )
    item_ref = minted.mint.minted[0].item_ref
    assert item_ref

    completion = parse_envelope(
        make_envelope(
            EventType.item_completed,
            event_id="pm-evt-0000900",
            subject={"kind": "work_item", "id": item_ref},
            payload={"scope": SCOPE, "details": {}},
        )
    )
    outcome = watch_completion(
        completion,
        tenant=TENANT,
        ledger=watch_stores.ledger,
        deliverables=watch_stores.deliverables,
        quarantine=watch_stores.quarantine,
    )
    assert outcome.disposition is WatchDisposition.quarantined_missing
    assert outcome.minted_by.created_item_ref == item_ref


def test_a_foreign_tenant_completion_writes_nothing(watch_stores):
    # Isolation held at the watch as well as at the engine: a boundary held in
    # exactly one place is one refactor away from not being held at all.
    seed_minted(watch_stores.ledger, "mkt-item-0001")
    theirs = parse_envelope(
        make_envelope(
            EventType.item_completed,
            tenant="snowlinedev",
            event_id="pm-evt-0000901",
            subject={"kind": "work_item", "id": "mkt-item-0001"},
        )
    )
    outcome = watch_completion(
        theirs,
        tenant=TENANT,
        ledger=watch_stores.ledger,
        deliverables=watch_stores.deliverables,
        quarantine=watch_stores.quarantine,
    )
    assert outcome.disposition is WatchDisposition.foreign_tenant
    assert watch_stores.quarantine.get("snowlinedev", "pm-evt-0000901") is None
    assert watch_stores.quarantine.get(TENANT, "pm-evt-0000901") is None
    assert watch_stores.deliverables.list_for_item(TENANT, "mkt-item-0001") == []


# --- §8's "resolvable by attaching provenance after the fact" -----------------


def test_resolving_records_the_deliverables_and_closes_the_row(watch_stores):
    seed_the_capture(watch_stores)
    drive(watch_stores)
    resolution = resolve_quarantined(
        TENANT,
        "pm-evt-0000502",
        provenance=declaration(),
        deliverables=watch_stores.deliverables,
        quarantine=watch_stores.quarantine,
        note="attached by the operator from the shipped listing",
    )
    assert resolution.applied
    assert resolution.record.status is QuarantineStatus.resolved
    assert "app_store/store_listing" in resolution.record.resolution_detail
    assert "operator" in resolution.record.resolution_detail

    rows = watch_stores.deliverables.list_for_item(TENANT, MINTED_ITEM_REFS["missing"])
    assert [row.deliverable_class for row in rows] == [
        "screenshot_set",
        "store_listing",
    ]
    # Stamped with when the WORK completed, not when someone got round to filing
    # it: the row's own `occurred_at` supplies `produced_at`.
    assert rows[0].produced_at == fixture_envelope(WITHOUT_PROVENANCE).occurred_at
    assert rows[0].event_id == "pm-evt-0000502"
    # Out of the queue, and a re-delivery cannot put it back.
    assert [row.event_id for row in watch_stores.quarantine.list_open(TENANT)] == [
        "pm-evt-0000503"
    ]
    drive(watch_stores)
    assert watch_stores.quarantine.get(TENANT, "pm-evt-0000502").status is (
        QuarantineStatus.resolved
    )


def test_dismissing_closes_the_row_and_records_no_deliverable(watch_stores):
    seed_the_capture(watch_stores)
    drive(watch_stores)
    transition = dismiss_quarantined(
        TENANT,
        "pm-evt-0000502",
        quarantine=watch_stores.quarantine,
        detail="completed as no longer needed — nothing was produced",
    )
    assert transition.applied
    assert transition.record.status is QuarantineStatus.dismissed
    assert (
        watch_stores.deliverables.list_for_item(TENANT, MINTED_ITEM_REFS["missing"])
        == []
    )
    assert [row.event_id for row in watch_stores.quarantine.list_open(TENANT)] == [
        "pm-evt-0000503"
    ]


def test_resolving_a_closed_or_missing_row_writes_nothing(watch_stores):
    seed_the_capture(watch_stores)
    drive(watch_stores)
    watch_stores.quarantine.dismiss(TENANT, "pm-evt-0000502", detail="nothing produced")
    refused = resolve_quarantined(
        TENANT,
        "pm-evt-0000502",
        provenance=declaration(),
        deliverables=watch_stores.deliverables,
        quarantine=watch_stores.quarantine,
    )
    assert not refused.applied
    assert refused.deliverables == ()
    assert "already dismissed" in refused.detail
    # A closed row must never be a channel for writing deliverables nobody
    # reviewed.
    assert (
        watch_stores.deliverables.list_for_item(TENANT, MINTED_ITEM_REFS["missing"])
        == []
    )

    absent = resolve_quarantined(
        TENANT,
        "pm-evt-never-filed",
        provenance=declaration(),
        deliverables=watch_stores.deliverables,
        quarantine=watch_stores.quarantine,
    )
    assert not absent.applied
    assert absent.record is None


def test_a_later_delivery_carrying_provenance_closes_the_row_it_left(watch_stores):
    # The same completion, re-emitted by a producer that fixed its payload. The
    # deliverables are written FIRST and the row closes itself — §8's "resolvable
    # by attaching provenance", happening without an operator.
    seed_the_capture(watch_stores)
    kwargs = dict(
        tenant=TENANT,
        ledger=watch_stores.ledger,
        deliverables=watch_stores.deliverables,
        quarantine=watch_stores.quarantine,
    )
    first = watch_completion(fixture_envelope(WITHOUT_PROVENANCE), **kwargs)
    assert first.disposition is WatchDisposition.quarantined_missing

    fixed = parse_envelope(
        make_envelope(
            EventType.item_completed,
            event_id="pm-evt-0000502",
            subject={"kind": "work_item", "id": MINTED_ITEM_REFS["missing"]},
            payload={
                "scope": SCOPE,
                "details": {
                    PROVENANCE_DETAILS_KEY: declaration().model_dump(mode="json")
                },
            },
        )
    )
    second = watch_completion(fixed, **kwargs)
    assert second.disposition is WatchDisposition.recorded
    row = watch_stores.quarantine.get(TENANT, "pm-evt-0000502")
    assert row.status is QuarantineStatus.resolved
    assert "closed itself" in row.resolution_detail
    assert (
        len(
            watch_stores.deliverables.list_for_item(TENANT, MINTED_ITEM_REFS["missing"])
        )
        == 2
    )


# --- the crash window ---------------------------------------------------------


class CrashesAfterFiling:
    """The crash window, made deterministic: the quarantine row lands and the
    process dies before the event is acked.

    A proxy rather than a monkeypatch so every OTHER call still goes to the real
    store — the row this leaves behind is exactly the row a real crash leaves
    behind, which is what the re-delivery assertion then acts on."""

    def __init__(self, inner) -> None:
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def record(self, **kwargs):
        self._inner.record(**kwargs)
        raise RuntimeError("process died after filing, before the ack")


def test_a_crash_after_the_write_re_delivers_and_converges(watch_stores):
    seed_the_capture(watch_stores)
    cursor_store = InMemoryCursorStore()
    crashed = drive(
        watch_stores,
        cursor_store=cursor_store,
        quarantine=CrashesAfterFiling(watch_stores.quarantine),
    )
    # The pass stopped ON the provenance-less completion, so its position is
    # un-acked and it re-delivers; the completion before it was fully handled.
    assert not crashed.intake.ok
    assert crashed.intake.failure.event_id == "pm-evt-0000502"
    assert crashed.intake.acked_position == WITH_PROVENANCE
    assert watch_stores.quarantine.get(TENANT, "pm-evt-0000502").is_open

    healed = drive(watch_stores, cursor_store=cursor_store)
    assert healed.intake.ok
    # ONE row, not two: the filing converges exactly as the delivery ledger's
    # insert does.
    assert [row.event_id for row in watch_stores.quarantine.list_open(TENANT)] == [
        "pm-evt-0000502",
        "pm-evt-0000503",
    ]
    # And the deliverables the acked event recorded were not written twice.
    assert (
        len(
            watch_stores.deliverables.list_for_item(
                TENANT, MINTED_ITEM_REFS["provenance"]
            )
        )
        == 2
    )


def test_a_watch_store_failure_stops_the_pass_rather_than_dropping_the_record(
    watch_stores,
):
    # Spec §8 gives the completion no gate, but an observation that was never
    # recorded must not be acked either: the store raises, the intake loop stops
    # the pass with the position un-acked (`intake.py`), and the next pass
    # observes it. The pass stops on the FIRST marketing-minted completion it
    # reaches — every write path touches the quarantine, including the recorded
    # one (which attempts the guarded self-resolve).
    class Broken:
        def __getattr__(self, name):
            raise RuntimeError("quarantine store unreachable")

    seed_the_capture(watch_stores)
    result = drive(watch_stores, quarantine=Broken())
    assert not result.intake.ok
    assert "quarantine store unreachable" in result.intake.failure.error
    assert result.intake.failure.event_id == "pm-evt-0000501"
    assert result.intake.acked_position is None
    assert not result.intake.failure.while_acking
    # Nothing acked means nothing lost: the next pass re-delivers from the same
    # cursor and records everything.
    assert not result.intake.delivered


def test_pm_being_down_does_not_delay_the_provenance_record(watch_stores):
    # Watch BEFORE mint: the mint half stops the pass when PM is unavailable, and
    # a completion that already happened must not have its provenance recorded
    # late because an unrelated mint could not reach PM.
    seed_the_capture(watch_stores)
    down = InMemoryWorkItemSink(lambda request: SinkUnavailable(detail="PM restarting"))
    result = drive(watch_stores, sink=down)
    assert not result.intake.ok
    assert result.unavailable is not None
    # Everything before the stopping event was still observed, including the
    # deliverables and the quarantine rows.
    assert (
        len(
            watch_stores.deliverables.list_for_item(
                TENANT, MINTED_ITEM_REFS["provenance"]
            )
        )
        == 2
    )
    assert len(watch_stores.quarantine.list_open(TENANT)) == 2


# --- DB-only: what §11 will read ---------------------------------------------


def test_the_recorded_rows_are_readable_as_operator_surfaces(migrated_db):
    stores = Stores(
        DeliveryLedger(), DeliverableProvenanceLedger(), CompletionQuarantine()
    )
    seed_the_capture(stores)
    drive(stores)
    assert len(stores.deliverables.list_for_tenant(TENANT)) == 2
    assert [row.reason for row in stores.quarantine.list_open(TENANT)] == [
        QuarantineReason.provenance_missing,
        QuarantineReason.provenance_malformed,
    ]
    # The per-item audit read spans open and closed rows.
    stores.quarantine.resolve(TENANT, "pm-evt-0000502", detail="attached")
    assert [
        row.status
        for row in stores.quarantine.list_for_item(TENANT, MINTED_ITEM_REFS["missing"])
    ] == [QuarantineStatus.resolved]

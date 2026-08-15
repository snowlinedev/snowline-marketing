"""The minting pass (spec §7, §14) — fixtures-first, end to end.

Driven over the SHIPPED capture and the SHIPPED policy artifact through
`run_intake_and_mint`, which is the same composition a scheduled driver will
call: intake pass, then mint what it owed. The sink is
`InMemoryWorkItemSink` — PM does not run, and the properties under test do not
need it to, because every one of them is a property of the LEDGER's convergence:

- §14's headline, restated for minting: duplicate delivery of the same event
  creates exactly ONE item, across repeated passes.
- The crash window is closed. A mint that succeeded with its confirmation lost
  must not re-mint on re-delivery, and must not vanish either — it surfaces.
- A dry-run match closes and stops re-owing; an approval-gated match neither
  mints, nor spams, nor is lost.
- A permanent refusal dead-letters with its reason; a transient one re-owes and
  mints on recovery; an ambiguous one holds its claim.

The convergence tests run over BOTH ledger stores (the `minting_ledger`
fixture), for the reason `test_ledger.py` gives: the in-memory store is what a
dry-run and the fixtures-first flow drive, so proving the property only against
Postgres would prove it for half the code paths that rely on it.
"""

from __future__ import annotations

import json

import pytest
from conftest import EVENT_FIXTURES_DIR, POLICY_FIXTURES_DIR, TENANT

from snowline_marketing.cursors import InMemoryCursorStore
from snowline_marketing.engine import EvaluationResult, evaluate, resolve_policy_set
from snowline_marketing.events import EventEnvelope, parse_envelope
from snowline_marketing.ledger import (
    DeliveryLedger,
    DeliveryOutcome,
    InMemoryDeliveryLedger,
)
from snowline_marketing.minting import (
    MintDisposition,
    mint_consequence,
    mint_pass,
    run_intake_and_mint,
)
from snowline_marketing.policy_cache import InMemoryPolicyCache
from snowline_marketing.policy_source import InMemoryPolicyProvider
from snowline_marketing.rendering import PROVENANCE_HEADING
from snowline_marketing.sources import FixturesEventSource
from snowline_marketing.work_sink import (
    InMemoryWorkItemSink,
    SinkCreated,
    SinkIndeterminate,
    SinkRejected,
    SinkUnavailable,
)

TURTLESEDGE_BODY = (POLICY_FIXTURES_DIR / "turtlesedge.json").read_text()
VERSION_ID = "gv-7f3a91c4"

COMPLETED_FIXTURE = "0010-item-completed.json"
RELEASE_FIXTURE = "0100-milestone-released.json"

# What the shipped artifact does with the shipped capture, by mode: five active
# matches mint, two approval-gated matches wait, one dry-run match closes.
EXPECTED_MINTS = 5
EXPECTED_GATED = 2
EXPECTED_DRY_RUN = 1


@pytest.fixture(params=["postgres", "memory"])
def minting_ledger(request):
    """One ledger per param — the real store (riding `migrated_db`, so the
    param skips cleanly with no Postgres) and the in-memory one. Both satisfy
    `ledger.MintingLedger`, which is the whole point of the protocol: the
    engine writes the rows and the mint transitions them, through one store."""
    if request.param == "postgres":
        request.getfixturevalue("migrated_db")
        return DeliveryLedger()
    return InMemoryDeliveryLedger()


def provider_for(body: str = TURTLESEDGE_BODY) -> InMemoryPolicyProvider:
    provider = InMemoryPolicyProvider()
    provider.put(TENANT, VERSION_ID, body)
    return provider


def drive(ledger, sink, *, body: str = TURTLESEDGE_BODY, cursor_store=None):
    """One full pass over the shipped capture: intake, evaluate, mint.

    A fresh cursor store by default, so the second call re-delivers the WHOLE
    capture — which is the at-least-once behaviour §14's duplicate-delivery
    criterion is about, forced rather than waited for. The policy cache is the
    in-memory one on both params: what is under test is the ledger, and a cache
    row would only add a table to clean up."""
    return run_intake_and_mint(
        FixturesEventSource(EVENT_FIXTURES_DIR),
        tenant=TENANT,
        provider=provider_for(body),
        sink=sink,
        cursor_store=cursor_store or InMemoryCursorStore(),
        ledger=ledger,
        cache=InMemoryPolicyCache(),
        on_malformed=lambda malformed: None,
    )


def fixture_envelope(name: str) -> EventEnvelope:
    parsed = parse_envelope((EVENT_FIXTURES_DIR / name).read_bytes())
    assert isinstance(parsed, EventEnvelope), getattr(parsed, "detail", "")
    return parsed


def one_consequence(ledger, name: str = COMPLETED_FIXTURE, *, body=TURTLESEDGE_BODY):
    """Evaluate one fixture and hand back the single consequence it owes —
    through the real engine, so the ledger row under test is the one production
    would have written."""
    envelope = fixture_envelope(name)
    resolution = resolve_policy_set(
        TENANT, provider=provider_for(body), cache=InMemoryPolicyCache()
    )
    result = evaluate(envelope, resolution, ledger=ledger)
    assert isinstance(result, EvaluationResult)
    (consequence,) = result.consequences
    return envelope, resolution, consequence


def re_deliver(envelope, resolution, ledger) -> EvaluationResult:
    result = evaluate(envelope, resolution, ledger=ledger)
    assert isinstance(result, EvaluationResult)
    return result


class LosesConfirmations:
    """The crash window, made deterministic: the claim lands, PM mints, and the
    process dies before the confirmation is written.

    A proxy rather than a monkeypatch so every OTHER call still goes to the real
    store — the row this leaves behind is exactly the row a real crash leaves
    behind, which is what the re-delivery assertions then act on."""

    def __init__(self, inner) -> None:
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def confirm_created(self, *args, **kwargs):
        raise RuntimeError("process died before the confirmation landed")


# --- §14: exactly one item per delivery, across repeated passes ---------------


def test_the_capture_mints_each_matched_delivery_exactly_once(minting_ledger):
    sink = InMemoryWorkItemSink()
    first = drive(minting_ledger, sink)
    assert first.intake.ok
    assert first.stall is None
    counts = first.mint.counts
    assert counts[MintDisposition.created] == EXPECTED_MINTS
    assert counts[MintDisposition.awaiting_approval] == EXPECTED_GATED
    assert counts[MintDisposition.dry_run_closed] == EXPECTED_DRY_RUN
    assert len(sink.requests) == EXPECTED_MINTS

    # The whole capture again, from position zero — at-least-once delivery,
    # forced.
    second = drive(minting_ledger, sink)
    assert len(sink.requests) == EXPECTED_MINTS, "a re-delivery minted a second item"
    assert len(set(sink.dedup_keys)) == len(sink.dedup_keys)
    assert MintDisposition.created not in second.mint.counts
    # Every created row still points at the item the first pass minted.
    for outcome in first.mint.minted:
        row = minting_ledger.get(TENANT, outcome.dedup_key)
        assert row.outcome is DeliveryOutcome.created
        assert row.created_item_ref == outcome.item_ref


def test_a_dry_run_row_closes_and_stops_re_owing(minting_ledger):
    sink = InMemoryWorkItemSink()
    first = drive(minting_ledger, sink)
    (closed,) = [
        outcome
        for outcome in first.mint.outcomes
        if outcome.disposition is MintDisposition.dry_run_closed
    ]
    assert closed.policy_id == "monthly-metrics-snapshot"
    row = minting_ledger.get(TENANT, closed.dedup_key)
    assert row.outcome is DeliveryOutcome.dry_run
    assert row.created_item_ref is None

    # Second pass over the same capture: the row is SETTLED, so the engine owes
    # nothing for it — the flagged open question, closed. Left at `matched` it
    # would re-owe a mint its own mode forbids, forever.
    second = drive(minting_ledger, sink)
    assert not [
        outcome
        for outcome in second.mint.outcomes
        if outcome.policy_id == "monthly-metrics-snapshot"
    ]
    assert minting_ledger.get(TENANT, closed.dedup_key).outcome is (
        DeliveryOutcome.dry_run
    )


def test_approval_required_rows_neither_mint_nor_spam_nor_vanish(minting_ledger):
    sink = InMemoryWorkItemSink()
    first = drive(minting_ledger, sink)
    gated = [
        outcome
        for outcome in first.mint.outcomes
        if outcome.disposition is MintDisposition.awaiting_approval
    ]
    assert {outcome.policy_id for outcome in gated} == {
        "launch-plan-on-build-phase-completed",
        "app-store-listing-publish",
    }
    rows = [minting_ledger.get(TENANT, outcome.dedup_key) for outcome in gated]
    for row in rows:
        assert row.outcome is DeliveryOutcome.awaiting_approval
        assert row.created_item_ref is None
        assert "approval" in row.detail
    # Nothing was minted for them.
    assert {request.policy_id for request in sink.requests}.isdisjoint(
        {outcome.policy_id for outcome in gated}
    )

    second = drive(minting_ledger, sink)
    # NOT LOST: the work is still owed, so the consequence is re-offered and
    # re-declined — visible in the report, absent from the sink.
    assert second.mint.counts[MintDisposition.awaiting_approval] == EXPECTED_GATED
    assert len(sink.requests) == EXPECTED_MINTS
    # NOT SPAMMED: the guarded transition refuses an already-marked row, so the
    # row is untouched by the second pass.
    for row, outcome in zip(rows, gated, strict=True):
        assert minting_ledger.get(TENANT, outcome.dedup_key) == row


# --- the crash window --------------------------------------------------------


def test_a_lost_confirmation_neither_re_mints_nor_disappears(minting_ledger):
    envelope, resolution, consequence = one_consequence(minting_ledger)
    sink = InMemoryWorkItemSink()
    with pytest.raises(RuntimeError):
        mint_consequence(
            consequence, sink=sink, ledger=LosesConfirmations(minting_ledger)
        )
    # PM DID mint: the request was submitted before the crash.
    assert len(sink.requests) == 1
    row = minting_ledger.get(TENANT, consequence.dedup_key)
    assert row.outcome is DeliveryOutcome.claimed
    assert row.created_item_ref is None

    # Re-delivery: the engine refuses to re-own a claimed row. No consequence
    # (so nothing can re-mint), a `failed` DELIVERY (so nothing is silent), and
    # the row untouched (so §11 can reconcile it against PM).
    result = re_deliver(envelope, resolution, minting_ledger)
    assert result.outcomes == (DeliveryOutcome.failed,)
    assert result.consequences == ()
    (delivery,) = result.deliveries
    assert "claimed" in delivery.detail
    assert "spec §11" in delivery.detail
    assert delivery.record.outcome is DeliveryOutcome.claimed

    # And the minting pass over what that delivery owed mints nothing at all.
    report = mint_pass(result.consequences, sink=sink, ledger=minting_ledger)
    assert report.outcomes == ()
    assert len(sink.requests) == 1


def test_two_passes_racing_one_row_mint_once(minting_ledger):
    # A second pass reaching the same row WHILE the first holds its claim: the
    # inner mint runs from inside the sink call, which is the moment the window
    # is actually open. The claim is a compare-and-set, so the inner pass is
    # refused and reports a reconciliation case rather than minting.
    envelope, resolution, consequence = one_consequence(minting_ledger)
    inner: list = []
    sink = InMemoryWorkItemSink()

    def responder(request):
        if not inner:
            inner.append(
                mint_consequence(consequence, sink=sink, ledger=minting_ledger)
            )
        return SinkCreated(item_ref="pm-item-42")

    sink = InMemoryWorkItemSink(responder)
    outer = mint_consequence(consequence, sink=sink, ledger=minting_ledger)
    (contended,) = inner
    assert contended.disposition is MintDisposition.reconciliation_needed
    assert contended.needs_operator
    assert outer.disposition is MintDisposition.created
    assert outer.item_ref == "pm-item-42"
    assert len(sink.requests) == 1


def test_a_repeat_mint_of_a_created_row_is_a_no_op(minting_ledger):
    _envelope, _resolution, consequence = one_consequence(minting_ledger)
    sink = InMemoryWorkItemSink()
    first = mint_consequence(consequence, sink=sink, ledger=minting_ledger)
    second = mint_consequence(consequence, sink=sink, ledger=minting_ledger)
    assert first.disposition is MintDisposition.created
    assert second.disposition is MintDisposition.already_minted
    assert second.item_ref == first.item_ref
    assert not second.needs_operator
    assert len(sink.requests) == 1


# --- the sink's four answers -------------------------------------------------


def test_a_permanent_rejection_dead_letters_with_its_reason(minting_ledger):
    _envelope, _resolution, consequence = one_consequence(minting_ledger)
    sink = InMemoryWorkItemSink(
        lambda request: SinkRejected(reason="no such scope 'turtlesedge/marketing'")
    )
    outcome = mint_consequence(consequence, sink=sink, ledger=minting_ledger)
    assert outcome.disposition is MintDisposition.failed
    assert outcome.needs_operator
    row = minting_ledger.get(TENANT, consequence.dedup_key)
    assert row.outcome is DeliveryOutcome.failed
    assert "no such scope" in row.detail
    assert row.created_item_ref is None


def test_a_transient_failure_re_owes_and_mints_on_recovery(minting_ledger):
    envelope, resolution, consequence = one_consequence(minting_ledger)
    down = InMemoryWorkItemSink(lambda request: SinkUnavailable(detail="PM restarting"))
    deferred = mint_consequence(consequence, sink=down, ledger=minting_ledger)
    assert deferred.disposition is MintDisposition.deferred
    # Not an operator's problem: re-delivery IS the retry loop.
    assert not deferred.needs_operator
    row = minting_ledger.get(TENANT, consequence.dedup_key)
    assert row.outcome is DeliveryOutcome.matched
    assert "re-owes" in row.detail

    # The next pass re-delivers the event, the engine re-owes the consequence,
    # and a recovered PM mints it.
    result = re_deliver(envelope, resolution, minting_ledger)
    assert result.outcomes == (DeliveryOutcome.matched,)
    up = InMemoryWorkItemSink()
    report = mint_pass(result.consequences, sink=up, ledger=minting_ledger)
    (outcome,) = report.outcomes
    assert outcome.disposition is MintDisposition.created
    assert minting_ledger.get(TENANT, consequence.dedup_key).created_item_ref
    assert len(down.requests) == 1 and len(up.requests) == 1


def test_an_ambiguous_answer_holds_the_claim(minting_ledger):
    # The case that must not re-owe: PM may have minted, so releasing the claim
    # would duplicate. Held, visible, reconcilable — never guessed at.
    envelope, resolution, consequence = one_consequence(minting_ledger)
    sink = InMemoryWorkItemSink(
        lambda request: SinkIndeterminate(detail="read timeout after sending")
    )
    outcome = mint_consequence(consequence, sink=sink, ledger=minting_ledger)
    assert outcome.disposition is MintDisposition.reconciliation_needed
    assert outcome.needs_operator
    assert "claim HELD" in outcome.detail
    row = minting_ledger.get(TENANT, consequence.dedup_key)
    assert row.outcome is DeliveryOutcome.claimed

    result = re_deliver(envelope, resolution, minting_ledger)
    assert result.consequences == ()
    assert result.outcomes == (DeliveryOutcome.failed,)


# --- rendering failures are per delivery -------------------------------------


BROKEN_TEMPLATE_BODY = json.dumps(
    {
        "schema_version": 1,
        "tenant": TENANT,
        "policies": [
            {
                "policy_id": "broken-title",
                "event_types": ["milestone_released"],
                "consequence": "announcement_preparation",
                "destination": {"scope": "turtlesedge/marketing"},
                # `details.release_notes` is a key this producer does not send —
                # renderable for some events, not for this one, which is why no
                # parse-time check could have caught it.
                "title_template": "Announce {details.release_notes}",
                "body_template": "Body.",
                "dedup_key_template": "{tenant}:{policy_id}:{event_id}",
            },
            {
                "policy_id": "sound-policy",
                "event_types": ["milestone_released"],
                "consequence": "listing_regeneration",
                "destination": {"scope": "turtlesedge/marketing"},
                "title_template": "Regenerate the listing for {milestone}",
                "body_template": "Body.",
                "dedup_key_template": "{tenant}:{policy_id}:{event_id}",
            },
        ],
    }
)


def test_an_unrenderable_template_fails_one_delivery_and_not_the_pass(minting_ledger):
    sink = InMemoryWorkItemSink()
    result = drive(minting_ledger, sink, body=BROKEN_TEMPLATE_BODY)
    assert result.intake.ok  # the pass completed; one tenant's typo stops nothing
    by_policy = {outcome.policy_id: outcome for outcome in result.mint.outcomes}
    broken = by_policy["broken-title"]
    assert broken.disposition is MintDisposition.failed
    assert "details.release_notes" in broken.detail
    row = minting_ledger.get(TENANT, broken.dedup_key)
    assert row.outcome is DeliveryOutcome.failed
    assert "title_template" in row.detail
    # The sound policy on the SAME event minted normally.
    assert by_policy["sound-policy"].disposition is MintDisposition.created
    assert [request.policy_id for request in sink.requests] == ["sound-policy"]


def test_a_dead_lettered_row_is_not_retried_by_re_delivery(minting_ledger):
    # Rendering is deterministic, so the failure would repeat on every
    # re-delivery: the row is terminal, and §11's replay (after the artifact is
    # revised) is the way back — not the accident of another delivery.
    sink = InMemoryWorkItemSink()
    first = drive(minting_ledger, sink, body=BROKEN_TEMPLATE_BODY)
    second = drive(minting_ledger, sink, body=BROKEN_TEMPLATE_BODY)
    assert MintDisposition.failed not in second.mint.counts
    assert not [
        outcome
        for outcome in second.mint.outcomes
        if outcome.policy_id == "broken-title"
    ]
    broken_key = next(
        outcome.dedup_key
        for outcome in first.mint.outcomes
        if outcome.policy_id == "broken-title"
    )
    assert minting_ledger.get(TENANT, broken_key).outcome is DeliveryOutcome.failed


# --- what lands in PM --------------------------------------------------------


def test_every_minted_body_carries_the_provenance_block(minting_ledger):
    sink = InMemoryWorkItemSink()
    drive(minting_ledger, sink)
    for request in sink.requests:
        assert PROVENANCE_HEADING in request.body
        assert f"delivery ledger key: {request.dedup_key}" in request.body
        assert f"evaluated policy artifact version: {VERSION_ID}" in request.body
        assert f"matched policy: {request.policy_id}" in request.body
        assert "musher dispatch requested:" in request.body
    listing = next(
        request
        for request in sink.requests
        if request.policy_id == "listing-regeneration-on-release"
    )
    # §7's external refs: the reconciled release URL from the originating event.
    assert (
        "external ref (release): "
        "https://github.com/turtlesedge/turtletracks/releases/tag/v1.4.0"
    ) in listing.body
    assert listing.scope == "turtlesedge/marketing"
    assert listing.initiative == "app-store"
    assert listing.phase == "release"


def test_the_dispatch_opt_in_reaches_the_payload_and_the_body(minting_ledger):
    # The plugin sets a flag; PM's watcher routes it. Nothing here calls musher,
    # and the intent is recorded twice over — once for PM, once for the human.
    sink = InMemoryWorkItemSink()
    drive(minting_ledger, sink)
    announcement = next(
        request
        for request in sink.requests
        if request.policy_id == "announcement-preparation-on-release"
    )
    assert announcement.musher_dispatch is True
    assert announcement.payload()["musher_dispatch"] is True
    assert "musher dispatch requested: yes" in announcement.body
    # The policy's ownership template rendered, even though PM's create surface
    # has no assignee parameter to put it in.
    assert announcement.owner == "turtlesedge-marketing"


def test_nothing_is_minted_for_a_tenant_whose_events_do_not_match(minting_ledger):
    # §14: unmatched events audit as `ignored` and create no work — which at
    # this layer means the engine hands minting nothing at all.
    sink = InMemoryWorkItemSink()
    empty = json.dumps({"schema_version": 1, "tenant": TENANT, "policies": []})
    result = drive(minting_ledger, sink, body=empty)
    assert result.mint.outcomes == ()
    assert sink.requests == []


# --- DB-only: what §11 will read ---------------------------------------------


def test_the_recorded_states_are_readable_as_operator_queues(migrated_db):
    ledger = DeliveryLedger()
    sink = InMemoryWorkItemSink()
    drive(ledger, sink)
    assert [
        row.policy_id
        for row in ledger.list_by_outcome(TENANT, DeliveryOutcome.awaiting_approval)
    ] == ["app-store-listing-publish", "launch-plan-on-build-phase-completed"]
    assert (
        len(ledger.list_by_outcome(TENANT, DeliveryOutcome.created)) == EXPECTED_MINTS
    )
    assert len(ledger.list_by_outcome(TENANT, DeliveryOutcome.dry_run)) == (
        EXPECTED_DRY_RUN
    )
    # Nothing stuck and nothing dead-lettered on a clean run.
    assert ledger.list_by_outcome(TENANT, DeliveryOutcome.claimed) == []
    assert ledger.list_by_outcome(TENANT, DeliveryOutcome.failed) == []


def test_a_stalled_pass_surfaces_the_stall_and_mints_nothing(minting_ledger):
    # Governance down: the pass stops with the event un-acked and NOTHING is
    # evaluated, so there is nothing owed and nothing to mint. The driver hears
    # both facts separately — "the pass failed" and "why" — because backing off
    # from an outage is a different response from fixing a broken artifact.
    class Down:
        def resolve(self, tenant):
            from snowline_marketing.policy_source import (
                PolicyResolutionError,
                ResolutionFailure,
            )

            return PolicyResolutionError(
                tenant=tenant,
                failure=ResolutionFailure.unavailable,
                detail="governance unreachable",
            )

    sink = InMemoryWorkItemSink()
    result = run_intake_and_mint(
        FixturesEventSource(EVENT_FIXTURES_DIR),
        tenant=TENANT,
        provider=Down(),
        sink=sink,
        cursor_store=InMemoryCursorStore(),
        ledger=minting_ledger,
        cache=InMemoryPolicyCache(),
        on_malformed=lambda malformed: None,
    )
    assert not result.intake.ok
    assert result.stall is not None
    assert result.mint.outcomes == ()
    assert sink.requests == []
    assert not result.ok

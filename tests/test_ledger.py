"""The delivery ledger (spec §4).

What is pinned here is the write shape, because everything above it depends on
exactly these properties: the first delivery of a key claims it, every later one
finds it TAKEN AND UNCHANGED (so a row the minting layer already advanced to
`created` survives a re-delivery), the same key under two tenants is two rows,
the store — never the caller — namespaces every key ("p:"/"e:", which is what
makes the engine's reserved event-level shapes unforgeable), and the
constraints that keep an audit row answerable are the database's rather than
every future writer's.

The contract tests run as ONE conformance suite over both stores (the
`ledger_store` fixture): `DeliveryLedger` rides `migrated_db` and skips cleanly
when Postgres is unreachable; `InMemoryDeliveryLedger` — spec §11's dry-run
store — needs no database at all, which is its whole point. Running the same
tests over both is what proves the dry-run's dedup behavior is the SAME as
production's, not a lookalike. Cases only the real store can express (CHECK
constraints, direct SQL, the read surface) stay DB-only below.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from conftest import TENANT

from snowline_marketing.db import session_scope
from snowline_marketing.ledger import (
    DeliveryLedger,
    DeliveryOutcome,
    InMemoryDeliveryLedger,
    LedgerTransition,
)
from snowline_marketing.models import DeliveryLedgerEntry as DeliveryLedgerRow

VERSION_ID = "gv-7f3a91c4"
KEY = f"{TENANT}:messaging-refresh:pm-evt-0000101"


def _matched(ledger, **overrides):
    values = {
        "tenant": TENANT,
        "dedup_key": KEY,
        "policy_id": "messaging-refresh",
        "event_id": "pm-evt-0000101",
        "event_type": "item_completed",
        "outcome": DeliveryOutcome.matched,
        "policy_version_id": VERSION_ID,
        "detail": "matched in mode 'active'",
    }
    values.update(overrides)
    return ledger.record(**values)


@pytest.fixture(params=["postgres", "memory"])
def ledger_store(request):
    """One delivery ledger per param: the real Postgres-backed store (riding
    `migrated_db`, so the whole param skips cleanly when Postgres is
    unreachable) or the in-memory dry-run store. The shared contract tests
    take this fixture so both implementations answer the same questions with
    the same code — the two stores cannot drift apart on the public surface a
    dry-run's honesty depends on."""
    if request.param == "postgres":
        request.getfixturevalue("migrated_db")
        return DeliveryLedger()
    return InMemoryDeliveryLedger()


# --- the store contract, run over BOTH stores ---------------------------------


def test_a_fresh_key_is_claimed(ledger_store):
    write = _matched(ledger_store)
    assert write.inserted
    # The record reports the STORED key: the caller's rendered key under the
    # store's policy-level namespace.
    assert write.record.dedup_key == f"p:{KEY}"
    assert write.record.outcome is DeliveryOutcome.matched
    assert write.record.policy_version_id == VERSION_ID
    assert write.record.created_item_ref is None
    assert write.record.created_at is not None


def test_a_repeat_delivery_finds_the_key_taken(ledger_store):
    first = _matched(ledger_store)
    second = _matched(ledger_store)
    assert first.inserted
    assert not second.inserted
    # The SAME row comes back — spec §4's "re-delivery returns the existing
    # result". `inserted` is the entitlement to act, and exactly one delivery
    # ever gets it.
    assert second.record == first.record


def test_created_at_marks_the_first_convergence(ledger_store):
    # Unlike the policy cache's `fetched_at`, this timestamp must NOT advance: it
    # is what makes a re-delivery visible as a no-op rather than as fresh work.
    first = _matched(ledger_store)
    second = _matched(ledger_store)
    assert second.record.created_at == first.record.created_at


def test_the_same_key_under_two_tenants_is_two_rows(ledger_store):
    # Tenant is a COLUMN of the key, not merely a rendered substring: a tenant
    # may author a dedup template that omits {tenant}, and uniqueness on the
    # rendered string alone would let one org read back another's row.
    bare_key = "messaging-refresh:pm-evt-0000101"
    mine = ledger_store.record(
        tenant=TENANT,
        dedup_key=bare_key,
        policy_id="messaging-refresh",
        event_id="pm-evt-0000101",
        event_type="item_completed",
        outcome=DeliveryOutcome.matched,
        policy_version_id=VERSION_ID,
    )
    theirs = ledger_store.record(
        tenant="snowlinedev",
        dedup_key=bare_key,
        policy_id="messaging-refresh",
        event_id="pm-evt-0000101",
        event_type="item_completed",
        outcome=DeliveryOutcome.matched,
        policy_version_id="gv-other",
    )
    assert mine.inserted and theirs.inserted
    assert ledger_store.get(TENANT, f"p:{bare_key}").policy_version_id == VERSION_ID
    assert (
        ledger_store.get("snowlinedev", f"p:{bare_key}").policy_version_id == "gv-other"
    )


def test_get_misses_return_none(ledger_store):
    assert ledger_store.get(TENANT, "never-written") is None


def test_the_store_namespaces_policy_and_event_keys(ledger_store):
    # The namespace is the STORE's, derived from whether the write names a
    # policy — a caller renders keys but cannot choose which namespace they
    # land in.
    policy = _matched(ledger_store)
    event = ledger_store.record(
        tenant=TENANT,
        dedup_key=f"{TENANT}:pm-evt-0000200:ignored",
        event_id="pm-evt-0000200",
        event_type="item_reopened",
        outcome=DeliveryOutcome.ignored,
        detail="no entry selects this event",
    )
    assert policy.record.dedup_key.startswith("p:")
    assert event.record.dedup_key == f"e:{TENANT}:pm-evt-0000200:ignored"


def test_a_forged_event_shape_cannot_reach_an_event_level_row(ledger_store):
    # The unforgeability the namespace exists for: a tenant-authored template
    # rendering the engine's reserved shape VERBATIM — even one that renders
    # the "e:" prefix itself — is a policy-level write, lands under "p:", and
    # neither collides with nor reads back the real event-level row.
    reserved = f"{TENANT}:pm-evt-0000300:ignored"
    event = ledger_store.record(
        tenant=TENANT,
        dedup_key=reserved,
        event_id="pm-evt-0000300",
        event_type="item_reopened",
        outcome=DeliveryOutcome.ignored,
        detail="the real event-level row",
    )
    for forged in (reserved, f"e:{reserved}"):
        write = _matched(ledger_store, dedup_key=forged, event_id="pm-evt-0000300")
        assert write.inserted  # a fresh row, not a read-back of the reserved one
        assert write.record.dedup_key == f"p:{forged}"
        assert write.record.outcome is DeliveryOutcome.matched
    assert event.record.dedup_key == f"e:{reserved}"


def test_record_refuses_an_event_level_outcome_naming_a_policy(ledger_store):
    # The store-side half of the CHECK constraint tests below: loud and typed,
    # before the database gets a chance to answer with a driver-shaped
    # IntegrityError (and the in-memory store has no CHECK to fall back on).
    with pytest.raises(ValueError, match="must not name a policy"):
        ledger_store.record(
            tenant=TENANT,
            dedup_key=f"{TENANT}:pm-evt-0000400:ignored",
            policy_id="some-policy",
            event_id="pm-evt-0000400",
            event_type="item_reopened",
            outcome=DeliveryOutcome.ignored,
        )


def test_record_refuses_a_policy_level_outcome_without_a_policy(ledger_store):
    with pytest.raises(ValueError, match="must name the policy"):
        ledger_store.record(
            tenant=TENANT,
            dedup_key=f"{TENANT}:no-policy:pm-evt-0000401",
            event_id="pm-evt-0000401",
            event_type="item_completed",
            outcome=DeliveryOutcome.matched,
            policy_version_id=VERSION_ID,
        )


def test_event_level_rows_may_omit_policy_and_version(ledger_store):
    # The other side of those two constraints: `ignored` and `quarantined` are
    # facts about an EVENT, and inventing a policy id for them would put a
    # rule's name on a decision it never made.
    write = ledger_store.record(
        tenant=TENANT,
        dedup_key=f"{TENANT}:pm-evt-0000103:ignored",
        event_id="pm-evt-0000103",
        event_type="item_reopened",
        outcome=DeliveryOutcome.ignored,
        detail="tenant has no policy-set artifact",
    )
    assert write.inserted
    assert write.record.policy_id is None
    assert write.record.policy_version_id is None


# --- the transition contract, run over BOTH stores ---------------------------
#
# Every one of these is a GUARDED compare-and-set (`ledger._TRANSITIONS`), and
# the guard is what the minting layer's whole convergence story rests on: one
# claim per row means one mint per delivery, and a transition that refuses tells
# the caller — by the row it hands back — which of several very different
# situations it is in.


STORED_KEY = f"p:{KEY}"


def _claimed(ledger) -> LedgerTransition:
    _matched(ledger)
    transition = ledger.claim(TENANT, STORED_KEY, detail="mint in flight")
    assert transition.applied
    return transition


def test_a_claim_takes_a_matched_row_and_stamps_it(ledger_store):
    write = _matched(ledger_store)
    transition = ledger_store.claim(TENANT, STORED_KEY, detail="mint in flight")
    assert transition.applied
    assert transition.record.outcome is DeliveryOutcome.claimed
    assert transition.record.detail == "mint in flight"
    # `created_at` marks the delivery's first convergence and must not move;
    # `updated_at` is what makes a held claim measurable at all (§11).
    assert transition.record.created_at == write.record.created_at
    assert write.record.updated_at is None
    assert transition.record.updated_at is not None


def test_only_one_caller_can_claim_a_row(ledger_store):
    _claimed(ledger_store)
    second = ledger_store.claim(TENANT, STORED_KEY, detail="another pass")
    assert not second.applied
    # The refusing row comes back, because "who holds it" is the fact the
    # caller needs: `claimed` means reconcile, `created` means nothing to do.
    assert second.record.outcome is DeliveryOutcome.claimed
    assert second.record.detail == "mint in flight"


def test_confirming_a_claim_writes_the_outcome_and_the_ref_together(ledger_store):
    _claimed(ledger_store)
    transition = ledger_store.confirm_created(
        TENANT, STORED_KEY, item_ref="pm-item-42", detail="minted"
    )
    assert transition.applied
    assert transition.record.outcome is DeliveryOutcome.created
    assert transition.record.created_item_ref == "pm-item-42"


def test_a_created_row_cannot_be_claimed_again(ledger_store):
    _claimed(ledger_store)
    ledger_store.confirm_created(TENANT, STORED_KEY, item_ref="pm-item-42")
    refused = ledger_store.claim(TENANT, STORED_KEY)
    assert not refused.applied
    assert refused.record.outcome is DeliveryOutcome.created
    assert refused.record.created_item_ref == "pm-item-42"


def test_confirming_without_a_claim_is_refused(ledger_store):
    _matched(ledger_store)
    transition = ledger_store.confirm_created(TENANT, STORED_KEY, item_ref="pm-item-42")
    assert not transition.applied
    assert transition.record.outcome is DeliveryOutcome.matched
    assert transition.record.created_item_ref is None


def test_confirming_with_no_ref_is_refused_before_the_store_is_touched(ledger_store):
    _claimed(ledger_store)
    with pytest.raises(ValueError, match="requires the minted item's ref"):
        ledger_store.confirm_created(TENANT, STORED_KEY, item_ref="  ")
    assert ledger_store.get(TENANT, STORED_KEY).outcome is DeliveryOutcome.claimed


def test_releasing_a_claim_puts_the_work_back_on_the_delivery(ledger_store):
    _claimed(ledger_store)
    transition = ledger_store.release_claim(TENANT, STORED_KEY, detail="PM down")
    assert transition.applied
    # Back to `matched` with no ref, which is exactly the shape the engine
    # re-owes — re-delivery is the retry loop, no timer required.
    assert transition.record.outcome is DeliveryOutcome.matched
    assert transition.record.created_item_ref is None
    assert ledger_store.claim(TENANT, STORED_KEY).applied


def test_a_permanent_failure_closes_the_claim_with_its_reason(ledger_store):
    _claimed(ledger_store)
    transition = ledger_store.mark_failed(
        TENANT, STORED_KEY, detail="PM rejected: no such scope"
    )
    assert transition.applied
    assert transition.record.outcome is DeliveryOutcome.failed
    assert "no such scope" in transition.record.detail


def test_marking_a_row_awaiting_approval_is_idempotent(ledger_store):
    # The property that keeps "waiting" from becoming "spamming": the
    # consequence is re-offered on every re-delivery, and only the first mark
    # writes.
    _matched(ledger_store)
    first = ledger_store.mark_awaiting_approval(TENANT, STORED_KEY, detail="gated")
    assert first.applied
    assert first.record.outcome is DeliveryOutcome.awaiting_approval
    second = ledger_store.mark_awaiting_approval(TENANT, STORED_KEY, detail="gated")
    assert not second.applied
    assert second.record.outcome is DeliveryOutcome.awaiting_approval
    assert second.record.updated_at == first.record.updated_at


def test_closing_a_dry_run_row_is_terminal(ledger_store):
    _matched(ledger_store)
    transition = ledger_store.close_dry_run(TENANT, STORED_KEY, detail="dry run")
    assert transition.applied
    assert transition.record.outcome is DeliveryOutcome.dry_run
    assert transition.record.created_item_ref is None
    # Terminal: nothing may take it back out, which is what stops the row
    # re-owing a mint the policy's own mode forbids.
    assert not ledger_store.claim(TENANT, STORED_KEY).applied
    assert not ledger_store.mark_awaiting_approval(
        TENANT, STORED_KEY, detail="x"
    ).applied
    assert ledger_store.get(TENANT, STORED_KEY).outcome is DeliveryOutcome.dry_run


def test_a_transition_on_a_row_that_does_not_exist_reports_neither(ledger_store):
    transition = ledger_store.claim(TENANT, "p:never-recorded")
    assert not transition.applied
    assert transition.record is None


def test_transitions_are_isolated_per_tenant(ledger_store):
    _matched(ledger_store)
    assert not ledger_store.claim("snowlinedev", STORED_KEY).applied
    assert ledger_store.get(TENANT, STORED_KEY).outcome is DeliveryOutcome.matched


# --- DB-only: direct SQL, the read surface, the CHECK constraints -------------


def test_a_repeat_delivery_never_overwrites_the_row(migrated_db):
    # The reason the conflict path is DO NOTHING and not DO UPDATE: by the time
    # an event re-delivers, the earlier delivery may already have MINTED, and an
    # upsert would erase the link to real work in the roadmap.
    ledger = DeliveryLedger()
    _matched(ledger)
    with session_scope() as session:
        session.execute(
            sa.update(DeliveryLedgerRow)
            .where(
                DeliveryLedgerRow.tenant == TENANT,
                DeliveryLedgerRow.dedup_key == f"p:{KEY}",
            )
            .values(
                outcome=DeliveryOutcome.created.value, created_item_ref="pm-item-42"
            )
        )

    repeat = _matched(ledger, detail="a later pass, saying something else")
    assert not repeat.inserted
    assert repeat.record.outcome is DeliveryOutcome.created
    assert repeat.record.created_item_ref == "pm-item-42"
    assert repeat.record.detail == "matched in mode 'active'"


def test_for_event_answers_what_happened_to_one_event(migrated_db):
    # The operator's first question, and one the dedup key cannot answer — a
    # custom template need not contain the event id at all.
    ledger = DeliveryLedger()
    for policy_id in ("policy-a", "policy-b"):
        ledger.record(
            tenant=TENANT,
            dedup_key=f"{TENANT}:{policy_id}:pm-evt-0000110",
            policy_id=policy_id,
            event_id="pm-evt-0000110",
            event_type="milestone_released",
            outcome=DeliveryOutcome.matched,
            policy_version_id=VERSION_ID,
        )
    ledger.record(
        tenant=TENANT,
        dedup_key=f"{TENANT}:pm-evt-0000999:ignored",
        event_id="pm-evt-0000999",
        event_type="item_reopened",
        outcome=DeliveryOutcome.ignored,
        policy_version_id=VERSION_ID,
    )
    rows = ledger.for_event(TENANT, "pm-evt-0000110")
    assert {r.policy_id for r in rows} == {"policy-a", "policy-b"}
    assert ledger.for_event(TENANT, "nothing-like-this") == []
    # Isolation holds on the read side too.
    assert ledger.for_event("snowlinedev", "pm-evt-0000110") == []


def test_list_for_tenant_is_newest_first_and_isolated(migrated_db):
    ledger = DeliveryLedger()
    for index in range(3):
        ledger.record(
            tenant=TENANT,
            dedup_key=f"{TENANT}:policy:{index}",
            policy_id="policy",
            event_id=f"pm-evt-{index}",
            event_type="item_completed",
            outcome=DeliveryOutcome.matched,
            policy_version_id=VERSION_ID,
        )
    ledger.record(
        tenant="snowlinedev",
        dedup_key="snowlinedev:policy:0",
        policy_id="policy",
        event_id="pm-evt-0",
        event_type="item_completed",
        outcome=DeliveryOutcome.matched,
        policy_version_id="gv-other",
    )
    rows = ledger.list_for_tenant(TENANT)
    assert len(rows) == 3
    assert [r.created_at for r in rows] == sorted(
        (r.created_at for r in rows), reverse=True
    )
    assert len(ledger.list_for_tenant(TENANT, limit=2)) == 2
    assert len(ledger.list_for_tenant("snowlinedev")) == 1
    assert ledger.list_for_tenant("nobody") == []


def _rejects(**values) -> None:
    """A direct write that must not be storable. The invariants below are the
    DATABASE's, not every future writer's — the point is that a row which cannot
    be acted on cannot exist, however it is written."""
    base = {
        "tenant": TENANT,
        "event_id": "pm-evt-0000101",
        "event_type": "item_completed",
    }
    base.update(values)
    with session_scope() as session:
        try:
            session.execute(sa.insert(DeliveryLedgerRow).values(**base))
        except sa.exc.IntegrityError:
            return
        raise AssertionError(f"delivery_ledger accepted a row it must reject: {base}")


def test_an_unknown_outcome_cannot_be_stored(migrated_db):
    # §11's dashboard FILTERS on this column: a typo'd outcome would not error,
    # it would quietly drop rows out of the audit view.
    _rejects(
        dedup_key="bad-outcome",
        policy_id="p",
        outcome="matched-ish",
        policy_version_id=VERSION_ID,
    )


def test_a_policy_level_row_must_name_its_policy(migrated_db):
    _rejects(
        dedup_key="no-policy-id",
        outcome=DeliveryOutcome.matched.value,
        policy_version_id=VERSION_ID,
    )


def test_a_policy_level_row_must_name_the_evaluated_version(migrated_db):
    # Spec §6's contract requirement, enforced rather than trusted: a matched row
    # that cannot say WHICH policy version decided it is not an audit row.
    _rejects(
        dedup_key="no-version",
        policy_id="p",
        outcome=DeliveryOutcome.matched.value,
    )


def test_a_created_row_must_point_at_the_item_it_created(migrated_db):
    # A `created` row with nothing to point at claims work exists that nobody
    # can find (spec §7 fills this column).
    _rejects(
        dedup_key="created-without-ref",
        policy_id="p",
        outcome=DeliveryOutcome.created.value,
        policy_version_id=VERSION_ID,
    )


def test_an_event_level_row_cannot_name_a_policy(migrated_db):
    # The other direction of ck_delivery_ledger_policy_id: an ignored row
    # claiming a policy puts a rule's name on a decision it never made.
    _rejects(
        dedup_key="ignored-with-policy",
        policy_id="p",
        outcome=DeliveryOutcome.ignored.value,
    )
    _rejects(
        dedup_key="quarantined-with-policy",
        policy_id="p",
        outcome=DeliveryOutcome.quarantined.value,
        detail="a quarantine that claims a rule decided it",
    )


def test_a_non_created_row_cannot_carry_an_item_ref(migrated_db):
    # The other direction of ck_delivery_ledger_created_item_ref: an item ref
    # on a row whose outcome never says `created` is work the audit trail
    # cannot account for.
    _rejects(
        dedup_key="ref-without-created",
        policy_id="p",
        outcome=DeliveryOutcome.matched.value,
        policy_version_id=VERSION_ID,
        created_item_ref="pm-item-orphan",
    )


def test_a_quarantined_row_must_explain_itself(migrated_db):
    # Same invariant `policy_cache` holds for a quarantined version: a rejection
    # with no reason is an operator staring at a refusal with nothing to fix.
    _rejects(
        dedup_key="quarantined-silently", outcome=DeliveryOutcome.quarantined.value
    )


def test_failed_is_a_storable_outcome_for_later(migrated_db):
    # §11 (retry / dead-letter) is a later item, but the vocabulary is fixed
    # NOW: the value, its CHECK and the dashboard's filter set must not need a
    # migration the day that item starts.
    write = DeliveryLedger().record(
        tenant=TENANT,
        dedup_key=f"{TENANT}:policy:failed",
        policy_id="policy",
        event_id="pm-evt-0000101",
        event_type="item_completed",
        outcome=DeliveryOutcome.failed,
        policy_version_id=VERSION_ID,
        detail="minting raised; §11 owns the retry",
    )
    assert write.record.outcome is DeliveryOutcome.failed


# --- in-memory-only: no SQL to advance a row with -----------------------------


def test_in_memory_never_overwrites_the_row():
    # The same "DO NOTHING, never DO UPDATE" contract the DB test above pins
    # via a direct SQL update — this store has no SQL, so a direct dict write
    # plays the same role: a matched row advanced to `created` must survive a
    # re-delivery within the same run, the same way it does for the real
    # store.
    import dataclasses

    ledger = InMemoryDeliveryLedger()
    first = _matched(ledger)
    key = (TENANT, first.record.dedup_key)
    ledger._rows[key] = dataclasses.replace(
        first.record, outcome=DeliveryOutcome.created, created_item_ref="pm-item-42"
    )
    repeat = _matched(ledger, detail="a later delivery, saying something else")
    assert not repeat.inserted
    assert repeat.record.outcome is DeliveryOutcome.created
    assert repeat.record.created_item_ref == "pm-item-42"


def test_two_passes_racing_one_claim_resolve_in_the_database(migrated_db):
    # The claim is a compare-and-set, not a check-then-write: two ledger
    # handles (two processes, in production) see the same row and exactly one
    # is entitled to mint. Same guarantee `inserted` gives on the insert.
    _matched(DeliveryLedger())
    first = DeliveryLedger().claim(TENANT, f"p:{KEY}", detail="pass A")
    second = DeliveryLedger().claim(TENANT, f"p:{KEY}", detail="pass B")
    assert first.applied
    assert not second.applied
    assert second.record.detail == "pass A"


def test_list_by_outcome_serves_the_three_operator_queues(migrated_db):
    # §11 reads three queues off one column: dead-letter (`failed`),
    # reconciliation (`claimed`), approval (`awaiting_approval`). The states are
    # this item's to RECORD; the verbs are §11/§12's.
    ledger = DeliveryLedger()
    for index, transition in enumerate(("claimed", "failed", "awaiting_approval")):
        key = f"{TENANT}:policy:{index}"
        _matched(ledger, dedup_key=key, event_id=f"pm-evt-{index}")
        stored = f"p:{key}"
        ledger.claim(TENANT, stored, detail="claimed")
        if transition == "failed":
            ledger.mark_failed(TENANT, stored, detail="PM rejected it")
        elif transition == "awaiting_approval":
            ledger.release_claim(TENANT, stored, detail="back to matched")
            ledger.mark_awaiting_approval(TENANT, stored, detail="gated")
    assert [
        r.dedup_key for r in ledger.list_by_outcome(TENANT, DeliveryOutcome.claimed)
    ] == [f"p:{TENANT}:policy:0"]
    assert [
        r.detail for r in ledger.list_by_outcome(TENANT, DeliveryOutcome.failed)
    ] == ["PM rejected it"]
    assert len(ledger.list_by_outcome(TENANT, DeliveryOutcome.awaiting_approval)) == 1
    assert ledger.list_by_outcome(TENANT, DeliveryOutcome.created) == []
    # Isolation holds on the read side, like every other listing here.
    assert ledger.list_by_outcome("snowlinedev", DeliveryOutcome.claimed) == []


def test_the_new_outcomes_are_storable_and_the_old_check_still_bites(migrated_db):
    # The migration's CHECK swap, from both sides: the minting vocabulary must
    # be storable, and a typo must still be refused (§11's dashboard FILTERS on
    # this column, so a bad value would drop rows out of the audit view rather
    # than error).
    ledger = DeliveryLedger()
    for index, outcome in enumerate(
        (
            DeliveryOutcome.claimed,
            DeliveryOutcome.awaiting_approval,
            DeliveryOutcome.dry_run,
        )
    ):
        key = f"{TENANT}:vocabulary:{index}"
        _matched(ledger, dedup_key=key, event_id=f"pm-evt-v{index}")
        stored = f"p:{key}"
        if outcome is DeliveryOutcome.claimed:
            ledger.claim(TENANT, stored)
        elif outcome is DeliveryOutcome.awaiting_approval:
            ledger.mark_awaiting_approval(TENANT, stored, detail="gated")
        else:
            ledger.close_dry_run(TENANT, stored, detail="dry run")
        assert ledger.get(TENANT, stored).outcome is outcome
    _rejects(
        dedup_key="claimed-ish",
        policy_id="p",
        outcome="claimed_maybe",
        policy_version_id=VERSION_ID,
    )

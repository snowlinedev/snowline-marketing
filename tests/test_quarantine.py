"""The completion quarantine (spec §4/§8).

What is pinned here is the queue's honesty: a provenance-less completion is
filed once however often it re-delivers (§14), an operator's decision on a row
can never be silently reopened by the stream, and the two ways to close a row —
provenance attached, or judged to have produced nothing — stay tellable apart
forever.

The contract tests run as ONE conformance suite over both stores (the
`quarantine_store` fixture), for `test_ledger.py`'s reason: the in-memory store
is what the fixtures-first flow drives, and §14's "visible in quarantine within
one sweep" is a criterion checked there. Cases only the real store can express
(the CHECK constraints, direct SQL, the item-keyed read) stay DB-only below.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa
from conftest import TENANT

from snowline_marketing.db import session_scope
from snowline_marketing.models import CompletionQuarantineEntry as QuarantineRow
from snowline_marketing.quarantine import (
    CompletionQuarantine,
    InMemoryCompletionQuarantine,
    QuarantineReason,
    QuarantineStatus,
)

EVENT = "pm-evt-0000502"
ITEM = "mkt-item-0002"
OCCURRED_AT = datetime(2026, 7, 28, 11, 2, 30, tzinfo=timezone.utc)
RAW = '{"event_id": "pm-evt-0000502", "event_type": "item_completed"}'


def _filed(store, **overrides):
    values = {
        "tenant": TENANT,
        "event_id": EVENT,
        "item_ref": ITEM,
        "reason": QuarantineReason.provenance_missing,
        "detail": "completion carries no 'deliverable_provenance' declaration",
        "raw_event": RAW,
        "occurred_at": OCCURRED_AT,
    }
    values.update(overrides)
    return store.record(**values)


@pytest.fixture(params=["postgres", "memory"])
def quarantine_store(request):
    """One completion quarantine per param: the real Postgres-backed store
    (riding `migrated_db`, so the whole param skips cleanly when Postgres is
    unreachable) or the in-memory one the fixtures-first flow drives."""
    if request.param == "postgres":
        request.getfixturevalue("migrated_db")
        return CompletionQuarantine()
    return InMemoryCompletionQuarantine()


# --- the store contract, run over BOTH stores ---------------------------------


def test_a_provenance_less_completion_is_filed_open_with_its_reason(quarantine_store):
    write = _filed(quarantine_store)
    assert write.inserted
    row = write.record
    assert row.status is QuarantineStatus.open
    assert row.is_open
    assert row.reason is QuarantineReason.provenance_missing
    assert row.item_ref == ITEM
    assert row.raw_event == RAW
    # The completion's own time, as distinct from when we recorded it: "the item
    # completed three weeks ago and nothing was ever recorded" is the sentence
    # this queue exists to make sayable.
    assert row.occurred_at == OCCURRED_AT
    assert row.created_at is not None
    assert row.updated_at is None
    assert row.resolution_detail is None
    assert quarantine_store.get(TENANT, EVENT) == row


def test_repeated_deliveries_converge_to_one_open_row(quarantine_store):
    # §14, for quarantine: at-least-once delivery must not accumulate a row per
    # pass. First-filing-wins, exactly like the delivery ledger's insert.
    first = _filed(quarantine_store)
    second = _filed(quarantine_store, detail="a later delivery, saying it again")
    assert first.inserted
    assert not second.inserted
    assert second.record == first.record
    assert quarantine_store.list_open(TENANT) == [first.record]


def test_a_malformed_declaration_files_a_different_reason(quarantine_store):
    # Two reasons because they send an operator to two different places: nobody
    # declared anything (attach provenance) vs a declaration was attempted and
    # is broken (fix the producer).
    write = _filed(
        quarantine_store,
        event_id="pm-evt-0000503",
        reason=QuarantineReason.provenance_malformed,
        detail="deliverables.0.deliverable_class: Field required",
    )
    assert write.record.reason is QuarantineReason.provenance_malformed
    assert "deliverable_class" in write.record.detail


def test_resolving_closes_the_row_with_the_operators_sentence(quarantine_store):
    _filed(quarantine_store)
    transition = quarantine_store.resolve(
        TENANT, EVENT, detail="provenance attached after the fact: app_store/whats_new"
    )
    assert transition.applied
    assert transition.record.status is QuarantineStatus.resolved
    assert "app_store/whats_new" in transition.record.resolution_detail
    assert transition.record.updated_at is not None
    # Out of the queue, still readable by key.
    assert quarantine_store.list_open(TENANT) == []
    assert quarantine_store.get(TENANT, EVENT).status is QuarantineStatus.resolved


def test_dismissing_closes_the_row_as_a_different_answer(quarantine_store):
    # "We recorded what it produced" and "it produced nothing worth recording"
    # are different audit facts; one closed state would make the second
    # unsayable.
    _filed(quarantine_store)
    transition = quarantine_store.dismiss(
        TENANT, EVENT, detail="item completed as no longer needed — no deliverable"
    )
    assert transition.applied
    assert transition.record.status is QuarantineStatus.dismissed
    assert quarantine_store.list_open(TENANT) == []


def test_a_closed_row_refuses_every_further_verb(quarantine_store):
    # Guarded compare-and-set, like every transition in this codebase: two
    # operators racing one row resolve in the store, and exactly one is
    # entitled to act.
    _filed(quarantine_store)
    quarantine_store.resolve(TENANT, EVENT, detail="attached")
    again = quarantine_store.resolve(TENANT, EVENT, detail="attached twice")
    assert not again.applied
    # The refusing ROW comes back, because "which way was it closed" is the fact
    # the caller needs.
    assert again.record.status is QuarantineStatus.resolved
    assert again.record.resolution_detail == "attached"
    dismissed = quarantine_store.dismiss(TENANT, EVENT, detail="changed my mind")
    assert not dismissed.applied
    assert dismissed.record.status is QuarantineStatus.resolved


def test_a_re_delivery_cannot_reopen_a_closed_decision(quarantine_store):
    # The reason the filing is DO NOTHING and not an upsert: by the time a
    # completion re-delivers, an operator may already have resolved it, and an
    # upsert would silently reopen a decision someone made.
    _filed(quarantine_store)
    quarantine_store.dismiss(TENANT, EVENT, detail="no deliverable")
    write = _filed(quarantine_store)
    assert not write.inserted
    assert write.record.status is QuarantineStatus.dismissed
    assert quarantine_store.list_open(TENANT) == []


def test_a_verb_on_a_row_that_does_not_exist_reports_neither(quarantine_store):
    transition = quarantine_store.resolve(TENANT, "never-filed", detail="x")
    assert not transition.applied
    assert transition.record is None
    assert quarantine_store.get(TENANT, "never-filed") is None


def test_rows_and_verbs_are_isolated_per_tenant(quarantine_store):
    _filed(quarantine_store)
    _filed(quarantine_store, tenant="snowlinedev", item_ref="their-item")
    assert not quarantine_store.resolve("nobody", EVENT, detail="x").applied
    assert quarantine_store.get(TENANT, EVENT).item_ref == ITEM
    assert quarantine_store.get("snowlinedev", EVENT).item_ref == "their-item"
    assert len(quarantine_store.list_open(TENANT)) == 1
    assert quarantine_store.list_open("nobody") == []


def test_the_open_queue_is_oldest_first_and_bounded(quarantine_store):
    # §11's queue is worked from the front, and "how long has this been
    # unrecorded?" is what makes it a queue at all.
    for index in range(3):
        _filed(
            quarantine_store,
            event_id=f"pm-evt-000060{index}",
            item_ref=f"mkt-item-000{index}",
        )
    rows = quarantine_store.list_open(TENANT)
    assert [row.event_id for row in rows] == [
        "pm-evt-0000600",
        "pm-evt-0000601",
        "pm-evt-0000602",
    ]
    assert [row.created_at for row in rows] == sorted(row.created_at for row in rows)
    assert len(quarantine_store.list_open(TENANT, limit=2)) == 2
    assert quarantine_store.list_open(TENANT, limit=0) == []


# --- DB-only: the CHECK constraints and the item-keyed read -------------------


def _rejects(**values) -> None:
    """A direct write that must not be storable — the invariants are the
    DATABASE's, not every future writer's."""
    base = {
        "tenant": TENANT,
        "event_id": EVENT,
        "item_ref": ITEM,
        "reason": QuarantineReason.provenance_missing.value,
        "detail": "no declaration",
        "raw_event": RAW,
        "status": QuarantineStatus.open.value,
        "occurred_at": OCCURRED_AT,
    }
    base.update(values)
    with session_scope() as session:
        try:
            session.execute(sa.insert(QuarantineRow).values(**base))
        except sa.exc.IntegrityError:
            return
        raise AssertionError(
            f"completion_quarantine accepted a row it must reject: {base}"
        )


def test_an_unknown_reason_or_status_cannot_be_stored(migrated_db):
    # §11's dashboard FILTERS on both columns: a typo'd value would not error, it
    # would quietly drop rows out of the queue.
    _rejects(reason="provenance_missingish")
    _rejects(status="closed")


def test_an_open_row_cannot_carry_a_resolution_and_a_closed_one_must(migrated_db):
    # Both directions: an open row carrying a resolution is a row someone closed
    # without saying so, and a closed row with none is a decision with no author.
    _rejects(resolution_detail="closed without saying so")
    _rejects(status=QuarantineStatus.resolved.value)
    _rejects(status=QuarantineStatus.dismissed.value)


def test_list_for_item_includes_the_rows_that_were_closed(migrated_db):
    # "Was this item ever unrecorded, and who closed it?" — the audit question
    # the item-keyed alternative would have answered by construction, restored
    # as a read (see `models.CompletionQuarantineEntry`).
    store = CompletionQuarantine()
    _filed(store)
    _filed(store, event_id="pm-evt-0000700")
    store.resolve(TENANT, EVENT, detail="attached")
    rows = store.list_for_item(TENANT, ITEM)
    assert [row.status for row in rows] == [
        QuarantineStatus.resolved,
        QuarantineStatus.open,
    ]
    assert store.list_for_item(TENANT, "some-other-item") == []
    assert store.list_for_item("snowlinedev", ITEM) == []


def test_two_operators_racing_one_row_resolve_in_the_database(migrated_db):
    # The guard is a compare-and-set, not a check-then-write: two store handles
    # (two processes, in production) see the same open row and exactly one closes
    # it.
    _filed(CompletionQuarantine())
    first = CompletionQuarantine().resolve(TENANT, EVENT, detail="operator A")
    second = CompletionQuarantine().dismiss(TENANT, EVENT, detail="operator B")
    assert first.applied
    assert not second.applied
    assert second.record.resolution_detail == "operator A"


# --- in-memory-only / vocabulary ---------------------------------------------


def test_the_in_memory_clock_is_injectable():
    frozen = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = frozen + timedelta(hours=1)
    clock = iter((frozen, later))
    store = InMemoryCompletionQuarantine(clock=lambda: next(clock))
    write = _filed(store)
    assert write.record.created_at == frozen
    transition = store.resolve(TENANT, EVENT, detail="attached")
    assert transition.record.created_at == frozen
    assert transition.record.updated_at == later


def test_the_vocabularies_are_declared_once():
    # The import-time pins' other half, visible in the suite: the app enums and
    # the schema CHECKs are both built from models.QUARANTINE_*_VALUES.
    from snowline_marketing import models

    assert {reason.value for reason in QuarantineReason} == (
        models.QUARANTINE_REASON_VALUES
    )
    assert {status.value for status in QuarantineStatus} == (
        models.QUARANTINE_STATUS_VALUES
    )

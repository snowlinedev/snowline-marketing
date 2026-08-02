"""The delivery ledger (spec §4).

DB-backed, so these ride `migrated_db` and skip cleanly when Postgres is
unreachable — the ledger's whole job is a uniqueness guarantee held by the
database, and an in-memory substitute would test the wrong thing entirely.

What is pinned here is the write shape, because everything above it depends on
exactly these properties: the first delivery of a key claims it, every later one
finds it TAKEN AND UNCHANGED (so a row the minting layer already advanced to
`created` survives a re-delivery), the same key under two tenants is two rows,
and the constraints that keep an audit row answerable are the database's rather
than every future writer's.
"""

from __future__ import annotations

import sqlalchemy as sa
from conftest import TENANT

from snowline_marketing.db import session_scope
from snowline_marketing.ledger import DeliveryLedger, DeliveryOutcome
from snowline_marketing.models import DeliveryLedgerEntry as DeliveryLedgerRow

VERSION_ID = "gv-7f3a91c4"
KEY = f"{TENANT}:messaging-refresh:pm-evt-0000101"


def _matched(ledger: DeliveryLedger, **overrides):
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


def test_a_fresh_key_is_claimed(migrated_db):
    write = _matched(DeliveryLedger())
    assert write.inserted
    assert write.record.dedup_key == KEY
    assert write.record.outcome is DeliveryOutcome.matched
    assert write.record.policy_version_id == VERSION_ID
    assert write.record.created_item_ref is None
    assert write.record.created_at is not None


def test_a_repeat_delivery_finds_the_key_taken(migrated_db):
    ledger = DeliveryLedger()
    first = _matched(ledger)
    second = _matched(ledger)
    assert first.inserted
    assert not second.inserted
    # The SAME row comes back — spec §4's "re-delivery returns the existing
    # result". `inserted` is the entitlement to act, and exactly one delivery
    # ever gets it.
    assert second.record == first.record


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
                DeliveryLedgerRow.tenant == TENANT, DeliveryLedgerRow.dedup_key == KEY
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


def test_created_at_marks_the_first_convergence(migrated_db):
    # Unlike the policy cache's `fetched_at`, this timestamp must NOT advance: it
    # is what makes a re-delivery visible as a no-op rather than as fresh work.
    ledger = DeliveryLedger()
    first = _matched(ledger)
    second = _matched(ledger)
    assert second.record.created_at == first.record.created_at


def test_the_same_key_under_two_tenants_is_two_rows(migrated_db):
    # Tenant is a COLUMN of the key, not merely a rendered substring: a tenant
    # may author a dedup template that omits {tenant}, and uniqueness on the
    # rendered string alone would let one org read back another's row.
    ledger = DeliveryLedger()
    bare_key = "messaging-refresh:pm-evt-0000101"
    mine = ledger.record(
        tenant=TENANT,
        dedup_key=bare_key,
        policy_id="messaging-refresh",
        event_id="pm-evt-0000101",
        event_type="item_completed",
        outcome=DeliveryOutcome.matched,
        policy_version_id=VERSION_ID,
    )
    theirs = ledger.record(
        tenant="snowlinedev",
        dedup_key=bare_key,
        policy_id="messaging-refresh",
        event_id="pm-evt-0000101",
        event_type="item_completed",
        outcome=DeliveryOutcome.matched,
        policy_version_id="gv-other",
    )
    assert mine.inserted and theirs.inserted
    assert ledger.get(TENANT, bare_key).policy_version_id == VERSION_ID
    assert ledger.get("snowlinedev", bare_key).policy_version_id == "gv-other"


def test_get_misses_return_none(migrated_db):
    assert DeliveryLedger().get(TENANT, "never-written") is None


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


def test_a_quarantined_row_must_explain_itself(migrated_db):
    # Same invariant `policy_cache` holds for a quarantined version: a rejection
    # with no reason is an operator staring at a refusal with nothing to fix.
    _rejects(
        dedup_key="quarantined-silently", outcome=DeliveryOutcome.quarantined.value
    )


def test_event_level_rows_may_omit_policy_and_version(migrated_db):
    # The other side of those two constraints: `ignored` and `quarantined` are
    # facts about an EVENT, and inventing a policy id for them would put a
    # rule's name on a decision it never made.
    ledger = DeliveryLedger()
    write = ledger.record(
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

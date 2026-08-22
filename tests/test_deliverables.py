"""The deliverable provenance ledger (spec §4/§8).

What is pinned here is the write shape, because §8's watch rests on exactly
these properties: a declaration lands as one row per deliverable instance, a
re-declaration CONVERGES onto that row instead of accumulating beside it, a
CORRECTED version set replaces the one it corrected (so the staleness sweep
never compares against a version this deliverable no longer reflects), and a
deliverable that could never be evaluated by the sweep cannot be stored at all.

The contract tests run as ONE conformance suite over both stores (the
`deliverable_store` fixture), for `test_ledger.py`'s reason: the in-memory store
is what the fixtures-first flow drives, so a convergence property proved only
against Postgres would be proved for half the paths that rely on it. Cases only
the real store can express (the foreign key, the cascade, the §11 listing) stay
DB-only below.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa
from conftest import TENANT

from snowline_marketing.db import session_scope
from snowline_marketing.deliverables import (
    DeliverableProvenanceLedger,
    InMemoryDeliverables,
    SourceVersion,
)
from snowline_marketing.models import DeliverableProvenanceEntry as DeliverableRow
from snowline_marketing.models import DeliverableSourceVersion as SourceVersionRow

ITEM = "mkt-item-0001"
PRODUCED_AT = datetime(2026, 7, 28, 9, 14, tzinfo=timezone.utc)
LISTING_VERSIONS = (
    SourceVersion(artifact_id="b964d217", version_id="av-3c81f9d2", milestone="v1.4"),
    SourceVersion(artifact_id="9f21ac04", version_id="av-77b0e315"),
)


def _listing(store, **overrides):
    values = {
        "tenant": TENANT,
        "item_ref": ITEM,
        "channel": "app_store",
        "deliverable_class": "store_listing",
        "source_versions": LISTING_VERSIONS,
        "produced_at": PRODUCED_AT,
        "event_id": "pm-evt-0000501",
        "external_url": "https://apps.apple.com/app/turtletracks/id6470000000",
    }
    values.update(overrides)
    return store.upsert(**values)


@pytest.fixture(params=["postgres", "memory"])
def deliverable_store(request):
    """One deliverable ledger per param: the real Postgres-backed store (riding
    `migrated_db`, so the whole param skips cleanly when Postgres is
    unreachable) or the in-memory one the fixtures-first flow drives. The shared
    contract tests take this fixture so both implementations answer the same
    questions with the same code."""
    if request.param == "postgres":
        request.getfixturevalue("migrated_db")
        return DeliverableProvenanceLedger()
    return InMemoryDeliverables()


# --- the store contract, run over BOTH stores ---------------------------------


def test_a_deliverable_records_everything_the_sweep_will_need(deliverable_store):
    write = _listing(deliverable_store)
    assert write.applied
    record = write.record
    assert record.identity == (TENANT, ITEM, "app_store", "store_listing")
    assert record.produced_at == PRODUCED_AT
    assert record.event_id == "pm-evt-0000501"
    assert record.external_url.endswith("id6470000000")
    # Ordered by artifact id on both stores, so a read-back compares equal to
    # what was written whatever order the payload listed them in.
    assert [
        (v.artifact_id, v.version_id, v.milestone) for v in record.source_versions
    ] == [("9f21ac04", "av-77b0e315", None), ("b964d217", "av-3c81f9d2", "v1.4")]
    assert record.created_at is not None
    assert record.updated_at is None
    assert deliverable_store.get(TENANT, ITEM, "app_store", "store_listing") == record


def test_a_re_declared_deliverable_converges_onto_one_row(deliverable_store):
    # The property the whole watch rests on: at-least-once re-delivery of a
    # completion leaves ONE row, identical both times. DO UPDATE, not DO
    # NOTHING — a deliverable row is a statement of fact, not a claim on work.
    first = _listing(deliverable_store)
    second = _listing(deliverable_store)
    # Equal `produced_at` re-APPLIES: a re-delivery of the same completion is
    # convergence, not a superseded write.
    assert first.applied and second.applied
    assert deliverable_store.list_for_item(TENANT, ITEM) == [second.record]
    assert second.record.source_versions == first.record.source_versions
    # `created_at` marks the first recording and must not move, so a re-delivery
    # reads as convergence rather than as a fresh deliverable; `updated_at` is
    # what says we heard about it again.
    assert second.record.created_at == first.record.created_at
    assert second.record.updated_at is not None


def test_a_corrected_declaration_replaces_the_version_it_corrected(deliverable_store):
    # Replace, never merge: a leftover version would make §8's sweep compare
    # against something this deliverable never reflected — a staleness finding
    # citing evidence nobody wrote.
    _listing(deliverable_store)
    corrected = _listing(
        deliverable_store,
        source_versions=(
            SourceVersion(
                artifact_id="b964d217", version_id="av-9911ffee", milestone="v1.5"
            ),
        ),
        event_id="pm-evt-0000600",
        external_url=None,
    ).record
    assert [v.version_id for v in corrected.source_versions] == ["av-9911ffee"]
    assert corrected.event_id == "pm-evt-0000600"
    # Nullable facts converge downward too — a correction that removes the URL
    # must not leave the old one standing.
    assert corrected.external_url is None
    assert deliverable_store.get(TENANT, ITEM, "app_store", "store_listing") == (
        corrected
    )


def test_one_completion_may_record_several_deliverables(deliverable_store):
    # §8's motivating case: one completion produced a listing update AND a
    # screenshot set. Same item, different class, two rows.
    _listing(deliverable_store)
    _listing(
        deliverable_store,
        deliverable_class="screenshot_set",
        source_versions=(
            SourceVersion(artifact_id="9f21ac04", version_id="av-77b0e315"),
        ),
        external_url=None,
    )
    rows = deliverable_store.list_for_item(TENANT, ITEM)
    assert [row.deliverable_class for row in rows] == [
        "screenshot_set",
        "store_listing",
    ]


def test_the_same_channel_and_class_under_two_items_are_two_rows(deliverable_store):
    # The producing ITEM is in the key: two marketing items may each own a store
    # listing deliverable, and neither may read back the other's.
    _listing(deliverable_store)
    other = _listing(
        deliverable_store, item_ref="mkt-item-0009", event_id="pm-evt-9"
    ).record
    assert other.item_ref == "mkt-item-0009"
    assert len(deliverable_store.list_for_item(TENANT, ITEM)) == 1
    assert len(deliverable_store.list_for_item(TENANT, "mkt-item-0009")) == 1


def test_rows_are_isolated_per_tenant(deliverable_store):
    _listing(deliverable_store)
    _listing(deliverable_store, tenant="snowlinedev", event_id="pm-evt-other")
    mine = deliverable_store.get(TENANT, ITEM, "app_store", "store_listing")
    assert mine.event_id == "pm-evt-0000501"
    assert (
        deliverable_store.get(
            "snowlinedev", ITEM, "app_store", "store_listing"
        ).event_id
        == "pm-evt-other"
    )
    assert deliverable_store.list_for_item("nobody", ITEM) == []


def test_get_misses_return_none(deliverable_store):
    assert deliverable_store.get(TENANT, ITEM, "app_store", "store_listing") is None


def test_a_deliverable_naming_no_source_version_is_refused(deliverable_store):
    # Refused BEFORE either store is touched, loudly and typed: a row the
    # staleness sweep can never evaluate is worse than no row, because it looks
    # like coverage.
    with pytest.raises(ValueError, match="at least one source artifact version"):
        _listing(deliverable_store, source_versions=())
    assert deliverable_store.list_for_item(TENANT, ITEM) == []


def test_an_older_declaration_is_refused_as_superseded(deliverable_store):
    # Newest completion wins, by `produced_at`: an outbox re-delivering an OLD
    # completion after a newer one recorded must converge as a no-op, not roll
    # the row's facts back.
    newer = _listing(deliverable_store)
    stale = _listing(
        deliverable_store,
        produced_at=PRODUCED_AT - timedelta(days=2),
        event_id="pm-evt-0000400",
        source_versions=(
            SourceVersion(artifact_id="b964d217", version_id="av-00stale"),
        ),
        external_url=None,
    )
    assert newer.applied
    assert not stale.applied
    # The refusing row comes back, so the caller can say superseded-by-what —
    # and the ledger keeps the newer completion's facts whole: row AND version
    # set (a superseded declaration must not swap the versions either).
    assert stale.record == newer.record
    standing = deliverable_store.get(TENANT, ITEM, "app_store", "store_listing")
    assert standing.event_id == "pm-evt-0000501"
    assert standing.produced_at == PRODUCED_AT
    assert [v.version_id for v in standing.source_versions] == [
        "av-77b0e315",
        "av-3c81f9d2",
    ]
    assert standing.external_url.endswith("id6470000000")


def test_a_newer_declaration_still_wins_after_a_refusal(deliverable_store):
    # The guard is per-write, not a latch: after a stale refusal, a genuinely
    # newer completion converges the row as ever.
    _listing(deliverable_store)
    _listing(
        deliverable_store,
        produced_at=PRODUCED_AT - timedelta(days=2),
        event_id="pm-evt-0000400",
    )
    newest = _listing(
        deliverable_store,
        produced_at=PRODUCED_AT + timedelta(days=1),
        event_id="pm-evt-0000700",
    )
    assert newest.applied
    assert (
        deliverable_store.get(TENANT, ITEM, "app_store", "store_listing").event_id
        == "pm-evt-0000700"
    )


def test_one_artifact_at_two_versions_is_refused(deliverable_store):
    # The same rule `provenance.py` enforces at the wire, held again where the
    # rows are actually written — §12's publish path will call the store without
    # going through a payload at all.
    with pytest.raises(ValueError, match="exactly one version"):
        _listing(
            deliverable_store,
            source_versions=(
                SourceVersion(artifact_id="b964d217", version_id="av-1"),
                SourceVersion(artifact_id="b964d217", version_id="av-2"),
            ),
        )
    assert deliverable_store.list_for_item(TENANT, ITEM) == []


def test_list_for_tenant_is_newest_first_and_isolated(deliverable_store):
    # §11's provenance listing AND §8's staleness sweep input — which is why it
    # runs over both stores now: the sweep is developed fixtures-first, so a
    # read only Postgres could answer would leave it with no fixtures-first
    # path (`InMemoryDeliverables.list_for_tenant`).
    _listing(deliverable_store)
    _listing(deliverable_store, item_ref="mkt-item-0009", event_id="pm-evt-9")
    _listing(deliverable_store, tenant="snowlinedev", event_id="pm-evt-other")
    rows = deliverable_store.list_for_tenant(TENANT)
    assert len(rows) == 2
    assert [row.created_at for row in rows] == sorted(
        (row.created_at for row in rows), reverse=True
    )
    assert len(deliverable_store.list_for_tenant(TENANT, limit=1)) == 1
    assert len(deliverable_store.list_for_tenant("snowlinedev")) == 1
    assert deliverable_store.list_for_tenant("nobody") == []
    # The listing carries the versions too — §11 reads it to show what each
    # deliverable reflects, §8 compares them, and a listing that dropped them
    # would need a second query per row.
    assert all(row.source_versions for row in rows)


def test_list_for_tenant_breaks_ties_by_the_natural_key():
    # The tie-break is what makes the order TOTAL — a paged listing over a
    # partial order can repeat or skip a row when several deliverables share a
    # timestamp. In-memory only because it takes a FROZEN clock to force the
    # tie: the real store's `func.now()` is per transaction, and each upsert is
    # its own.
    store = InMemoryDeliverables(clock=lambda: PRODUCED_AT)
    for channel in ("website", "app_store", "blog"):
        _listing(store, channel=channel)
    rows = store.list_for_tenant(TENANT)
    assert [row.channel for row in rows] == ["website", "blog", "app_store"]


# --- DB-only: the constraints and the §11 listing -----------------------------


def test_a_source_version_cannot_outlive_its_deliverable(migrated_db):
    # The foreign key: a source version with no deliverable is a fact about
    # nothing, and the invariant is the DATABASE's rather than every future
    # writer's.
    with session_scope() as session:
        with pytest.raises(sa.exc.IntegrityError):
            session.execute(
                sa.insert(SourceVersionRow).values(
                    tenant=TENANT,
                    item_ref="no-such-item",
                    channel="app_store",
                    deliverable_class="store_listing",
                    artifact_id="b964d217",
                    version_id="av-1",
                )
            )


def test_deleting_a_deliverable_takes_its_versions_with_it(migrated_db):
    ledger = DeliverableProvenanceLedger()
    _listing(ledger)
    with session_scope() as session:
        session.execute(
            sa.delete(DeliverableRow).where(DeliverableRow.tenant == TENANT)
        )
    with session_scope() as session:
        assert session.scalars(sa.select(SourceVersionRow)).all() == []


def test_listings_fetch_the_version_sets_in_one_query(migrated_db):
    # The association rows for a whole listing come back in ONE query (the
    # natural keys, IN-listed) and are stitched in memory — a listing must not
    # cost one version query per deliverable row.
    from snowline_marketing.db import get_engine

    ledger = DeliverableProvenanceLedger()
    for index in range(4):
        _listing(ledger, item_ref=f"mkt-item-100{index}", event_id=f"pm-evt-{index}")
    statements: list[str] = []

    def record_statement(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    sa.event.listen(get_engine(), "before_cursor_execute", record_statement)
    try:
        rows = ledger.list_for_tenant(TENANT)
    finally:
        sa.event.remove(get_engine(), "before_cursor_execute", record_statement)
    assert len(rows) == 4
    # Ordering stays deterministic and every row carries its versions.
    assert [row.item_ref for row in rows] == sorted(
        (row.item_ref for row in rows), reverse=True
    )
    assert all(row.source_versions for row in rows)
    selects = [s for s in statements if s.lstrip().upper().startswith("SELECT")]
    assert len(selects) == 2, selects


def test_the_association_rows_are_queryable_by_artifact(migrated_db):
    # The whole reason the versions are rows: §8's sweep asks "which deliverables
    # cite artifact X?", and this is that question as an indexed lookup rather
    # than a JSON containment scan. (The comparison itself is the sweep's item.)
    ledger = DeliverableProvenanceLedger()
    _listing(ledger)
    _listing(
        ledger,
        deliverable_class="screenshot_set",
        source_versions=(
            SourceVersion(artifact_id="9f21ac04", version_id="av-77b0e315"),
        ),
    )
    with session_scope() as session:
        citing = session.scalars(
            sa.select(SourceVersionRow.deliverable_class)
            .where(
                SourceVersionRow.tenant == TENANT,
                SourceVersionRow.artifact_id == "9f21ac04",
            )
            .order_by(SourceVersionRow.deliverable_class)
        ).all()
    assert citing == ["screenshot_set", "store_listing"]


# --- in-memory-only: the injectable clock -------------------------------------


def test_the_in_memory_clock_is_injectable():
    # The in-process analogue of the real store's server-side now(), for the
    # reason `InMemoryDeliveryLedger`'s is: a test proving `created_at` does not
    # move on convergence must be able to control both stamps.
    frozen = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = frozen + timedelta(days=1)
    clock = iter((frozen, later))
    store = InMemoryDeliverables(clock=lambda: next(clock))
    first = _listing(store).record
    second = _listing(store).record
    assert first.created_at == frozen
    assert second.created_at == frozen
    assert second.updated_at == later

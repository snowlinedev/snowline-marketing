"""Consumer cursor persistence (spec §4, "Cursor state").

The DB-backed tests use the `migrated_db` fixture, which skips cleanly when
Postgres is unreachable — the cursor's whole job is surviving a process, so
there is no honest in-memory substitute for testing it. `InMemoryCursorStore`
is tested here too, but as its own thing (the dry-run / loop-test store), not
as a stand-in for the real one.
"""

from __future__ import annotations

import sqlalchemy as sa

from snowline_marketing.cursors import DbCursorStore, InMemoryCursorStore
from snowline_marketing.db import session_scope
from snowline_marketing.models import ConsumerCursor

SOURCE = "fixtures:events"


def test_in_memory_store_round_trips():
    store = InMemoryCursorStore()
    assert store.read(SOURCE) is None
    store.ack(SOURCE, "0010-a.json", "pm-evt-1")
    assert store.read(SOURCE) == "0010-a.json"
    assert store.last_event_id(SOURCE) == "pm-evt-1"


def test_in_memory_store_keys_are_independent():
    store = InMemoryCursorStore({"other": "0999-z.json"})
    store.ack(SOURCE, "0010-a.json", "pm-evt-1")
    assert store.read("other") == "0999-z.json"


def test_unknown_source_reads_as_none(migrated_db):
    # No cursor means "never consumed", which the loop reads as "start at the
    # beginning" — not an error and not position zero.
    assert DbCursorStore().read("fixtures:never-consumed") is None


def test_cursor_round_trips_through_the_database(migrated_db):
    store = DbCursorStore()
    store.ack(SOURCE, "0010-item-completed.json", "pm-evt-0000101")

    # Read back through a FRESH store instance and session: the point of the
    # row is surviving the process that wrote it.
    assert DbCursorStore().read(SOURCE) == "0010-item-completed.json"
    with session_scope() as session:
        row = session.get(ConsumerCursor, SOURCE)
        assert row is not None
        assert row.position == "0010-item-completed.json"
        assert row.last_event_id == "pm-evt-0000101"
        assert row.created_at is not None
        assert row.updated_at is not None


def test_ack_upserts_rather_than_duplicating(migrated_db):
    store = DbCursorStore()
    store.ack(SOURCE, "0010-a.json", "pm-evt-1")
    store.ack(SOURCE, "0030-b.json", "pm-evt-3")
    assert store.read(SOURCE) == "0030-b.json"
    with session_scope() as session:
        count = session.execute(
            sa.text(
                "SELECT count(*) FROM consumer_cursors WHERE source_key = :k",
            ),
            {"k": SOURCE},
        ).scalar()
        assert count == 1


def test_ack_moves_updated_at(migrated_db):
    # `updated_at` is what an operator reads to see whether intake is still
    # moving; an INSERT ... ON CONFLICT that forgot to set it would freeze at
    # the first ack forever.
    store = DbCursorStore()
    store.ack(SOURCE, "0010-a.json", "pm-evt-1")
    with session_scope() as session:
        first = session.get(ConsumerCursor, SOURCE).updated_at
    store.ack(SOURCE, "0030-b.json", "pm-evt-3")
    with session_scope() as session:
        second = session.get(ConsumerCursor, SOURCE).updated_at
    # Distinct transactions, so `now()` (transaction start) genuinely advances.
    assert second > first


def test_ack_tolerates_an_event_without_an_id(migrated_db):
    # A malformed envelope can be acked past with no id at all (see
    # intake.run_intake) — the position is the resume key, the id is audit.
    store = DbCursorStore()
    store.ack("fixtures:no-ids", "0020-malformed.json", None)
    with session_scope() as session:
        row = session.get(ConsumerCursor, "fixtures:no-ids")
        assert row.position == "0020-malformed.json"
        assert row.last_event_id is None


def test_sources_do_not_share_a_cursor(migrated_db):
    DbCursorStore().ack("fixtures:a", "0010-a.json", "pm-evt-1")
    DbCursorStore().ack("fixtures:b", "0090-z.json", "pm-evt-9")
    assert DbCursorStore().read("fixtures:a") == "0010-a.json"
    assert DbCursorStore().read("fixtures:b") == "0090-z.json"

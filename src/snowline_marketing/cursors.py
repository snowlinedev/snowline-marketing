"""Consumer cursors — how far each source has been acknowledged (spec §4).

Two implementations behind one protocol:

- `DbCursorStore` is the real one: the cursor lives in marketing's OWN
  database (`consumer_cursors`), so a restart resumes where the last ack
  landed and the producer never has to remember anything about this consumer.
- `InMemoryCursorStore` is for callers with no database in play — the intake
  loop's own tests, and the §11 dry-run, which evaluates captured fixtures and
  must leave no trace at all (a dry-run that moved the real cursor would eat
  the events it was only supposed to preview).

An ack is one row, upserted per event, not batched at the end of a pass. That
is a write per event, which at intake volumes is cheap and buys the
at-least-once property outright: a crash mid-pass re-delivers only the events
after the last successful ack, rather than the whole pass. Re-delivery being
harmless is the delivery ledger's job (spec §4), not this module's.
"""

from __future__ import annotations

from typing import Protocol

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from snowline_marketing.db import session_scope
from snowline_marketing.models import ConsumerCursor


class CursorStore(Protocol):
    """Read/advance the consumer cursor for one source key."""

    def read(self, source_key: str) -> str | None:
        """The last acked position, or None when the source was never consumed
        (in which case the loop starts at the beginning of the stream)."""
        ...

    def ack(self, source_key: str, position: str, event_id: str | None) -> None:
        """Record `position` as acknowledged. Called AFTER the handler has
        succeeded, never before — the ordering is the whole guarantee."""
        ...


class InMemoryCursorStore:
    """A cursor that vanishes with the process. Used where persistence is
    wrong (dry-run) or irrelevant (loop tests that assert ordering and ack
    behaviour without needing Postgres to be up)."""

    def __init__(self, positions: dict[str, str] | None = None) -> None:
        self._positions: dict[str, str] = dict(positions or {})
        # Kept for symmetry with the DB row's audit column, so a test can
        # assert what the loop believed it was acking.
        self._event_ids: dict[str, str | None] = {}

    def read(self, source_key: str) -> str | None:
        return self._positions.get(source_key)

    def ack(self, source_key: str, position: str, event_id: str | None) -> None:
        self._positions[source_key] = position
        self._event_ids[source_key] = event_id

    def last_event_id(self, source_key: str) -> str | None:
        return self._event_ids.get(source_key)


class DbCursorStore:
    """The persisted cursor (`consumer_cursors`).

    `ack` is a single-statement Postgres upsert rather than a read-then-write:
    the first ack for a source and every later one take the same code path, and
    two loop passes overlapping (a supervisor restart racing the old process)
    cannot lose a row to a lost update — the last writer wins, which for a
    monotone position is the correct outcome."""

    def read(self, source_key: str) -> str | None:
        with session_scope() as session:
            row = session.get(ConsumerCursor, source_key)
            return row.position if row is not None else None

    def ack(self, source_key: str, position: str, event_id: str | None) -> None:
        statement = pg_insert(ConsumerCursor).values(
            source_key=source_key,
            position=position,
            last_event_id=event_id,
        )
        with session_scope() as session:
            session.execute(
                statement.on_conflict_do_update(
                    index_elements=[ConsumerCursor.source_key],
                    set_={
                        "position": statement.excluded.position,
                        "last_event_id": statement.excluded.last_event_id,
                        # Set explicitly: the model's `onupdate` fires on ORM
                        # UPDATEs, and this is an INSERT ... ON CONFLICT, which
                        # would otherwise leave updated_at at its insert value
                        # forever — the exact column an operator reads to see
                        # whether intake is still moving.
                        "updated_at": func.now(),
                    },
                )
            )

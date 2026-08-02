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
        # Same no-rewind guard as the DB store: the two implementations must
        # not differ in ack semantics, or a loop test passes in memory and
        # regresses against Postgres.
        current = self._positions.get(source_key)
        if current is not None and position <= current:
            return
        self._positions[source_key] = position
        self._event_ids[source_key] = event_id

    def last_event_id(self, source_key: str) -> str | None:
        return self._event_ids.get(source_key)


class DbCursorStore:
    """The persisted cursor (`consumer_cursors`).

    `ack` is a single-statement Postgres upsert rather than a read-then-write:
    the first ack for a source and every later one take the same code path.
    The conflict update is GUARDED — it applies only when the new position is
    greater than the stored one. Last-writer-wins would be wrong here: two
    loop passes can overlap (a supervisor restart racing the old process), and
    the stale process acking its in-flight event AFTER the new process has
    moved on would rewind the cursor and re-deliver the whole span between
    them. The delivery ledger (spec §4) makes that span converge rather than
    duplicate, so it is no longer a correctness hole — but it is still an
    arbitrary amount of the stream re-read and re-evaluated to reach the
    conclusion "already done". A stale ack is instead a silent no-op, which is
    what it should be.

    The `<` is pinned to `COLLATE "C"` (bytewise) on BOTH sides: the
    `EventSource` contract defines monotone as PYTHON string order — bytewise
    — and the database's default collation is not that (a linguistic ICU
    collation orders punctuation-insensitively, so it could judge a
    legitimate forward ack a rewind and silently wedge the cursor). One
    ordering, declared where it is compared."""

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
                        # Set explicitly — this upsert is the column's ONLY
                        # writer (the model deliberately declares no ORM
                        # `onupdate`), and updated_at is the exact column an
                        # operator reads to see whether intake is still moving.
                        "updated_at": func.now(),
                    },
                    # The rewind guard (see class docstring): a stale racing
                    # ack must not move the cursor backward. COLLATE "C" pins
                    # the comparison to byte order — the same ordering the
                    # sources and the in-memory store use — independent of the
                    # database's default collation.
                    where=ConsumerCursor.position.collate("C")
                    < statement.excluded.position.collate("C"),
                )
            )

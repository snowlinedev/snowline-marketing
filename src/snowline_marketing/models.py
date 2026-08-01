"""Marketing's persisted models.

`ConsumerCursor` (spec §4, "Cursor state") is the first table: one row per
event source, holding how far the intake loop has acknowledged. The rest of
§4 — delivery ledger, deliverable provenance ledger, quarantine, policy cache
— arrives with the policy-engine and provenance items; each is a separate
migration on this chain.

The cursor lives in MARKETING's own database, not in the source's. That is the
whole point of at-least-once consumption: the producer (PM's outbox) owns what
happened, this plugin owns how much of it it has processed, and neither can
corrupt the other's record. A crash between handling and acking re-delivers —
by design; the delivery ledger (§4) is what makes re-delivery idempotent.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ConsumerCursor(Base):
    """How far one event source has been consumed (spec §4)."""

    __tablename__ = "consumer_cursors"

    # The source's stable name (`EventSource.source_key`) — the natural
    # primary key: exactly one cursor per source, so a second consumer of the
    # same source is a deliberate second key, never an accidental duplicate
    # row racing the first.
    source_key: Mapped[str] = mapped_column(String(255), primary_key=True)

    # The last ACKED position — the source-defined token the next read
    # resumes strictly after. Text, not a bigint: positions are opaque to this
    # table (an outbox event id, a fixture file name), and typing them would
    # bake one source's shape into the schema every other source must fit.
    position: Mapped[str] = mapped_column(Text, nullable=False)

    # The event id at that position, when the event had one. NOT the resume
    # key — a malformed envelope can be acked past without ever having had an
    # id (see `intake.run_intake`). Kept because "which event was that?" is
    # the first question asked of a stuck cursor, and the id is what the
    # delivery ledger and quarantine rows key on.
    last_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    # timestamptz (timezone=True), NOT naive timestamp: a naive column makes
    # Postgres cast writes through the SESSION timezone, so on a non-UTC
    # server the stored wall time silently goes local while readers re-label
    # it UTC — and "when did this cursor last move?" is the operator's staleness
    # check on the intake loop itself.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # No ORM `onupdate`: the column's ONLY writer is DbCursorStore's
    # INSERT ... ON CONFLICT upsert, which bypasses ORM update hooks and sets
    # this explicitly. Declaring an onupdate here would document a code path
    # that does not exist; any future non-upsert writer must set the column
    # itself.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<ConsumerCursor {self.source_key!r} @ {self.position!r}>"

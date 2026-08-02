"""Marketing's persisted models.

`ConsumerCursor` (spec §4, "Cursor state") is the first table: one row per
event source, holding how far the intake loop has acknowledged.
`CachedPolicySet` (spec §4, "Policy cache") is the second: one row per resolved
governance artifact VERSION. The rest of §4 — delivery ledger, deliverable
provenance ledger, quarantine — arrives with the engine and provenance items;
each is a separate migration on this chain.

The cursor lives in MARKETING's own database, not in the source's. That is the
whole point of at-least-once consumption: the producer (PM's outbox) owns what
happened, this plugin owns how much of it it has processed, and neither can
corrupt the other's record. A crash between handling and acking re-delivers —
by design; the delivery ledger (§4) is what makes re-delivery idempotent.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, String, Text, func
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


class CachedPolicySet(Base):
    """One resolved policy artifact version (spec §4, "Policy cache").

    Keyed by the governance artifact VERSION id, not by tenant. A version id is
    an immutable content address: the row for a version can never need to say
    something different later, so there is no "current" pointer here and no
    invalidation to get wrong. Which version is current is governance's answer,
    resolved per sweep by `policy_source`; this table only remembers what each
    version CONTAINED and how it classified. The ledger records the same
    version id, so an audit row and a cache row join without a third table.
    """

    __tablename__ = "policy_cache"

    # The governance artifact version id.
    version_id: Mapped[str] = mapped_column(String(255), primary_key=True)

    # The tenant org scope this version governs (spec §6: one policy-set
    # artifact per tenant). Denormalized from the body on purpose: the
    # operator surface lists "quarantined policy versions, by tenant" (§11),
    # and a quarantined body may be unparseable — so the one column that
    # answers "whose?" cannot live inside the JSON.
    tenant: Mapped[str] = mapped_column(Text, nullable=False)

    # The artifact body VERBATIM, as text.
    #
    # Text, NOT JSONB, for two reasons that both bite exactly on the rows that
    # matter most. (1) A quarantined body may not be JSON at all — a policy
    # artifact revised to prose is the realistic accident, and JSONB would
    # reject the very row the quarantine surface exists to display, turning a
    # visible operator problem into a write error. (2) Even for valid JSON,
    # JSONB is lossy against the source: it reorders keys, drops duplicates and
    # renormalizes numbers, so a cached body could no longer be diffed
    # byte-for-byte against the artifact in governance — which is precisely the
    # question an operator asks when a policy did not do what they think they
    # wrote. Nothing here queries INTO the body (matching reads the parsed
    # model, not SQL), so JSONB's one advantage does not apply.
    body: Mapped[str] = mapped_column(Text, nullable=False)

    # 'valid' or 'quarantined' — the classification AS OF the fetch. This is
    # the audit/operator column (what §11's dashboard filters on); the engine
    # gets its model by re-parsing `body`. A short String with an app-side
    # enum, not a native Postgres ENUM type: adding a value to a PG enum is its
    # own ALTER TYPE migration and cannot be done inside a transaction on older
    # servers, which is a lot of ceremony for a two-value audit label.
    parse_outcome: Mapped[str] = mapped_column(String(16), nullable=False)

    # Why it quarantined: the `MalformedPolicyReason` value and the detail
    # naming the offending entry/field. Both NULL for a valid version — and the
    # CHECK below makes that an invariant rather than a convention, because a
    # quarantined row with no reason is an operator staring at a broken policy
    # with nothing to fix.
    quarantine_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    quarantine_detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    # timestamptz for the same reason as the cursor's timestamps: a naive
    # column stores local wall time that readers re-label UTC. "When did we
    # last see this version?" is a staleness question about the policy sweep
    # itself (spec §5: governance is polled, not evented).
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "(parse_outcome = 'valid' AND quarantine_reason IS NULL "
            "AND quarantine_detail IS NULL) "
            "OR (parse_outcome = 'quarantined' AND quarantine_reason IS "
            "NOT NULL AND quarantine_detail IS NOT NULL)",
            name="ck_policy_cache_quarantine_reason",
        ),
        # Not unique — a tenant accumulates one row per version it has ever
        # had, which IS the point (the ledger references old version ids
        # forever). The index serves the §11 listing, which is per tenant.
        Index("ix_policy_cache_tenant", "tenant"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"<CachedPolicySet {self.version_id!r} {self.tenant!r} "
            f"{self.parse_outcome}>"
        )

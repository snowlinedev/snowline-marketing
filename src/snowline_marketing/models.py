"""Marketing's persisted models.

`ConsumerCursor` (spec §4, "Cursor state") is the first table: one row per
event source, holding how far the intake loop has acknowledged.
`CachedPolicySet` (spec §4, "Policy cache") is the second: one row per resolved
governance artifact VERSION. `DeliveryLedgerEntry` (spec §4, "Delivery ledger")
is the third: one row per consumed event × matched policy, and the row whose
uniqueness makes at-least-once delivery converge. The rest of §4 — deliverable
provenance ledger, quarantine — arrives with the provenance items; each is a
separate migration on this chain.

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


class DeliveryLedgerEntry(Base):
    """One consumed event × matched policy (spec §4, "Delivery ledger").

    This table is the IDEMPOTENCY BACKBONE of the whole plugin. Intake is
    at-least-once and acks after handling (`intake.py`), so any crash between
    handling and acking re-delivers an event that was already evaluated. What
    makes that safe is not a lock and not a transaction spanning two services:
    it is the unique key below. A re-delivered event renders the same dedup key,
    the insert conflicts, and the delivery reports the EXISTING row's result —
    spec §4's "creation and ledger write are atomic or recoverably convergent;
    re-delivery returns the existing result", spelled as convergence.

    **The primary key is the logical key, not a surrogate.** Same choice as
    `ConsumerCursor.source_key` and `CachedPolicySet.version_id`: when the
    natural key IS the identity, there is no way to write two rows for one
    logical delivery — not by a racing pass, not by a future writer that forgot
    a WHERE clause. A surrogate id would need the same unique constraint
    anyway, plus a sequence, and would let the "one row per delivery" invariant
    live somewhere other than the row's identity.

    **Tenant is IN the key, not only in the rendered string.** Spec §4's logical
    key is `tenant + policy_id + event_id` and the default dedup template
    renders exactly that — but a tenant may author a template that omits
    `{tenant}` (`policies.py` validates the placeholder vocabulary, not the
    composition). Uniqueness on the rendered string alone would then let one
    tenant's delivery collide with another's: the second would read back the
    FIRST tenant's row, report its work as already done, and hand that row's
    policy id and item ref into another organization's audit trail. That is the
    cross-tenant leak §3/§14 forbid, arriving through a template typo. Tenant
    is therefore a column of the key, restoring the spec's own composition
    whatever the template says.

    **Rows for event-level outcomes carry no policy.** `ignored` (no policy set,
    or no entry matched) and `quarantined` (a cross-tenant delivery) are facts
    about the EVENT, not about a rule — there is no policy id to record and
    inventing one would put a rule's name on a decision it never made. They get
    a `NULL` `policy_id` and a dedup key in a distinct, reserved shape (see
    `engine.py`), so one unique key still covers every outcome and a
    re-delivered unmatched event converges to its single audit row instead of
    accumulating one per delivery. The CHECKs below make "policy-level outcomes
    name a policy" a database invariant rather than a convention.
    """

    __tablename__ = "delivery_ledger"

    # The tenant org scope this delivery belongs to — the isolation boundary,
    # and the first component of the key (see class docstring). String(255)
    # like `ConsumerCursor.source_key`: an org scope slug, not free text.
    tenant: Mapped[str] = mapped_column(String(255), primary_key=True)

    # The RENDERED dedup key: the policy's `dedup_key_template` filled from the
    # envelope for a matched row, or one of the engine's reserved event-level
    # shapes. Text, not a bounded String: the template is tenant-authored and
    # its values are envelope fields, so any width this table picked would be a
    # policy limit invented in the schema layer.
    dedup_key: Mapped[str] = mapped_column(Text, primary_key=True)

    # The matched policy's stable id — NULL exactly on the event-level outcomes
    # (see class docstring and `ck_delivery_ledger_policy_id`). Joins to the
    # entry inside the policy version named by `policy_version_id`.
    policy_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    # The originating event. Recorded even when it is already spelled inside
    # `dedup_key`: a custom template need not include it (the release-listing
    # policy keys on the milestone instead), and "what did this event do?" is
    # the first question asked of the ledger.
    event_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)

    # matched / ignored / created / deduplicated / quarantined / failed — spec
    # §4's enumeration, mirrored by `ledger.DeliveryOutcome`. A plain String
    # with an app-side enum plus a value CHECK, not a native PG ENUM (adding a
    # value to a PG enum is its own ALTER TYPE migration that cannot run inside
    # a transaction on older servers). The CHECK is worth its migration cost
    # here because §11's dashboard FILTERS on this column: a typo'd outcome
    # would not error, it would quietly drop rows out of the audit view.
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)

    # The governance artifact VERSION evaluated for this delivery — spec §6's
    # contract requirement ("records the evaluated version id on every ledger
    # row"), and the join back to `policy_cache` that lets an auditor read the
    # exact rule text that applied. NULL is permitted ONLY where no policy set
    # applied at all (see `ck_delivery_ledger_policy_version_id`): a tenant with
    # no policy artifact, or a cross-tenant delivery refused before any version
    # was in play. Every policy-level row must name one.
    policy_version_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # The PM item this delivery minted (spec §7). NULL until the minting layer
    # fills it; required once the outcome says `created`, because a `created`
    # row with nothing to point at is an audit trail claiming work exists that
    # nobody can find.
    created_item_ref: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Why, in operator words: which policy version had no entry for this event,
    # which foreign tenant the envelope claimed, which mode the match ran in.
    # Required on `quarantined` for the same reason `policy_cache` requires a
    # quarantine reason — a rejection with no explanation is an operator staring
    # at a refusal with nothing to fix.
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    # timestamptz, like every other timestamp here: a naive column would store
    # local wall time that readers re-label UTC, and "when was this delivery
    # first recorded?" is the ordering the §11 audit listing is built on. Never
    # updated — the conflict path is DO NOTHING, so this stays the moment the
    # delivery FIRST converged, which is what makes a re-delivery visible as a
    # no-op rather than as fresh work.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "outcome IN ('matched', 'ignored', 'created', 'deduplicated', "
            "'quarantined', 'failed')",
            name="ck_delivery_ledger_outcome",
        ),
        # Policy-level outcomes must name the rule that produced them; only the
        # event-level ones (no policy was involved) may leave it NULL.
        CheckConstraint(
            "policy_id IS NOT NULL OR outcome IN ('ignored', 'quarantined')",
            name="ck_delivery_ledger_policy_id",
        ),
        # The §6 contract requirement, enforced rather than trusted: a matched /
        # created / deduplicated / failed row that cannot say WHICH policy
        # version decided it is not an audit row.
        CheckConstraint(
            "policy_version_id IS NOT NULL OR outcome IN ('ignored', 'quarantined')",
            name="ck_delivery_ledger_policy_version_id",
        ),
        CheckConstraint(
            "outcome <> 'created' OR created_item_ref IS NOT NULL",
            name="ck_delivery_ledger_created_item_ref",
        ),
        CheckConstraint(
            "outcome <> 'quarantined' OR detail IS NOT NULL",
            name="ck_delivery_ledger_quarantine_detail",
        ),
        # The §11 audit listing: "this tenant's deliveries, newest first". The
        # primary key already indexes `tenant` as its leading column, so this
        # exists purely for the ORDERING — without it the dashboard sorts the
        # tenant's whole history in memory on every page load.
        Index("ix_delivery_ledger_tenant_created_at", "tenant", "created_at"),
        # "What happened to event X?" — the operator's other question, and the
        # one a support conversation starts from. Not answerable from the key:
        # a custom dedup template need not contain the event id at all.
        Index("ix_delivery_ledger_tenant_event_id", "tenant", "event_id"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"<DeliveryLedgerEntry {self.tenant!r} {self.dedup_key!r} {self.outcome}>"
        )

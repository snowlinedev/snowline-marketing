"""The delivery ledger — one row per consumed event × matched policy
(spec §4).

This is the store; `models.DeliveryLedgerEntry` is the schema and its docstring
carries the key design (natural composite key, tenant IN the key, event-level
rows with no policy). What lives here is the WRITE SHAPE, and it is the single
mechanism the plugin's at-least-once story rests on:

    INSERT ... ON CONFLICT DO NOTHING, then read the row back.

Every consequence of that choice is deliberate:

- **DO NOTHING, never DO UPDATE.** The conflicting row is the record of an
  earlier delivery that may already have MINTED something — outcome `created`,
  with a PM item ref on it (spec §7). An upsert would overwrite that with a
  fresh `matched` row and lose the link to real work in the roadmap. The
  existing row is the answer, not an obstacle: spec §4 says re-delivery
  "returns the existing result", and DO NOTHING is that sentence in SQL.

- **`RETURNING` decides who inserted; a read-back supplies the row.** Under
  `ON CONFLICT DO NOTHING`, `RETURNING` yields one row when the insert happened
  and NOTHING when it was skipped — the only reliable signal available here.
  (`rowcount` is not: psycopg reports -1 for this statement, so a
  `rowcount == 1` test would silently call every delivery a duplicate and a
  `rowcount == 0` test would silently call every delivery fresh. Either way the
  bug is invisible.) The row itself then comes from a read-back in the SAME
  transaction, because the conflict path has to read the existing row anyway
  (spec §4: re-delivery returns the existing result) and one code path through
  the most important write in the plugin is worth more than saving a round trip
  on the fresh path.

- **Idempotent under concurrency, not just under re-delivery.** Two passes
  racing on the same key (a supervisor restart overlapping the old process) both
  reach the read-back and both return the same row; exactly one of them sees
  `inserted=True`, and that one alone is entitled to produce a consequence. The
  ledger is where "did anyone already do this?" is answered, so it must be
  answered by the database, not by a flag in a process.

This module deliberately knows nothing about policies, envelopes or matching.
It stores what it is told, and the engine decides what to tell it — which is
what lets the ledger be tested against a real database with no policy artifact
in sight.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from snowline_marketing.db import session_scope
from snowline_marketing.models import DeliveryLedgerEntry as DeliveryLedgerRow


class DeliveryOutcome(enum.StrEnum):
    """What happened to one delivery — spec §4's enumeration, verbatim.

    The vocabulary is closed and mirrored by a database CHECK, because §11's
    dashboard filters on it and a value outside the set would not error, it
    would quietly drop rows out of the audit view.

    - `matched` — an entry selected the event and this delivery is the FIRST to
      record it. The row is the claim on the work; the consequence goes to the
      minting layer.
    - `ignored` — the event was evaluated against a policy set (or against the
      knowledge that the tenant has none) and no work is owed. Spec §14's
      "unmatched events audit as `ignored` and create no work": the row exists
      precisely so that "nothing happened" is a recorded decision rather than
      an absence someone has to trust.
    - `created` — the minting layer turned a `matched` row into a PM item and
      wrote its ref (spec §7). Owned by the next item; defined here because the
      transition targets THIS row.
    - `deduplicated` — this delivery found the key already taken. No new work,
      by design; the existing row's result stands.
    - `quarantined` — the delivery was refused rather than evaluated (today:
      a cross-tenant envelope, §14). Consumed, explained, never silently
      dropped.
    - `failed` — a delivery whose consequence could not be carried out. NOT
      produced by this item: retry, backoff and dead-lettering are spec §11,
      and the value is defined now so that the schema, the CHECK and the
      dashboard vocabulary do not need a migration when §11 lands.
    """

    matched = "matched"
    ignored = "ignored"
    created = "created"
    deduplicated = "deduplicated"
    quarantined = "quarantined"
    failed = "failed"


# The outcomes that describe an EVENT rather than a rule, and are therefore the
# only ones allowed to carry no policy id and no evaluated policy version (see
# the CHECK constraints in models.py). Kept here so the store's guard and the
# database's guard are the same list, read from one place.
EVENT_LEVEL_OUTCOMES = frozenset({DeliveryOutcome.ignored, DeliveryOutcome.quarantined})


@dataclass(frozen=True)
class LedgerRecord:
    """One ledger row, read back.

    A plain record rather than the ORM object, for the same reason
    `policy_cache.CachedPolicyVersion` is one: callers hold it outside the
    session that produced it, and an ORM instance would lazily emit SQL from
    inside the evaluation loop (or, worse, quietly hand back stale attributes
    after the session closed)."""

    tenant: str
    dedup_key: str
    event_id: str
    event_type: str
    outcome: DeliveryOutcome
    created_at: datetime
    policy_id: str | None = None
    policy_version_id: str | None = None
    created_item_ref: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class LedgerWrite:
    """The result of one `record` call: the row that now holds the key, and
    whether THIS call is the one that put it there.

    `inserted` is the entitlement to act. Exactly one delivery of a given key
    ever sees it True — across re-deliveries and across racing passes — so it
    is what the engine keys "produce a consequence" on. A caller that ignored
    it and minted on every delivery would mint duplicates on every crash-before-
    ack, which is the whole failure mode the ledger exists to prevent."""

    record: LedgerRecord
    inserted: bool


class DeliveryLedger:
    """The `delivery_ledger` table (spec §4)."""

    def record(
        self,
        *,
        tenant: str,
        dedup_key: str,
        event_id: str,
        event_type: str,
        outcome: DeliveryOutcome,
        policy_id: str | None = None,
        policy_version_id: str | None = None,
        detail: str | None = None,
    ) -> LedgerWrite:
        """Claim `dedup_key` for `tenant`, or report who holds it already.

        Never updates an existing row (see the module docstring): on conflict
        the stored row is returned untouched, `inserted=False`, and the caller
        learns that this delivery owes no new work. The insert and the read-back
        share one transaction, so the row handed back is the row that satisfies
        the key at the moment the claim was decided.

        Raises like any other store call if the database is unreachable — the
        never-raises contract belongs to the CLASSIFIERS (malformed input is an
        expected input class); an unreachable database is not an input, and
        swallowing it here would ack an event whose audit row was never
        written. The intake loop already turns the exception into a stopped
        pass with the position un-acked."""
        statement = (
            pg_insert(DeliveryLedgerRow)
            .values(
                tenant=tenant,
                dedup_key=dedup_key,
                policy_id=policy_id,
                event_id=event_id,
                event_type=event_type,
                outcome=outcome.value,
                policy_version_id=policy_version_id,
                detail=detail,
            )
            .on_conflict_do_nothing(
                index_elements=[DeliveryLedgerRow.tenant, DeliveryLedgerRow.dedup_key]
            )
            # One row back when this statement inserted, none when it conflicted
            # — see the module docstring on why `rowcount` cannot be used here.
            .returning(DeliveryLedgerRow.dedup_key)
        )
        read_back = select(DeliveryLedgerRow).where(
            DeliveryLedgerRow.tenant == tenant,
            DeliveryLedgerRow.dedup_key == dedup_key,
        )
        with session_scope() as session:
            inserted = session.execute(statement).first() is not None
            row = session.scalars(read_back).one()
            return LedgerWrite(record=_to_record(row), inserted=inserted)

    def get(self, tenant: str, dedup_key: str) -> LedgerRecord | None:
        """The row holding `dedup_key` for `tenant`, or None. Rides the primary
        key."""
        with session_scope() as session:
            row = session.get(DeliveryLedgerRow, (tenant, dedup_key))
            return _to_record(row) if row is not None else None

    def for_event(self, tenant: str, event_id: str) -> list[LedgerRecord]:
        """Every row this event produced, oldest first — "what happened to
        event X?", which is where an operator conversation starts and which the
        dedup key alone cannot answer (a custom template need not contain the
        event id). Rides `ix_delivery_ledger_tenant_event_id`."""
        statement = (
            select(DeliveryLedgerRow)
            .where(
                DeliveryLedgerRow.tenant == tenant,
                DeliveryLedgerRow.event_id == event_id,
            )
            .order_by(DeliveryLedgerRow.created_at, DeliveryLedgerRow.dedup_key)
        )
        with session_scope() as session:
            return [_to_record(row) for row in session.scalars(statement)]

    def list_for_tenant(
        self, tenant: str, *, limit: int | None = None
    ) -> list[LedgerRecord]:
        """One tenant's deliveries, newest first — §11's audit listing
        (received / ignored / matched / created / deduplicated / failed). Rides
        `ix_delivery_ledger_tenant_created_at`; `dedup_key` breaks ties so the
        order is total and a paged listing cannot repeat or skip a row when
        several deliveries share a timestamp."""
        statement = (
            select(DeliveryLedgerRow)
            .where(DeliveryLedgerRow.tenant == tenant)
            .order_by(
                DeliveryLedgerRow.created_at.desc(), DeliveryLedgerRow.dedup_key.desc()
            )
        )
        if limit is not None:
            statement = statement.limit(max(0, limit))
        with session_scope() as session:
            return [_to_record(row) for row in session.scalars(statement)]


def _to_record(row: DeliveryLedgerRow) -> LedgerRecord:
    return LedgerRecord(
        tenant=row.tenant,
        dedup_key=row.dedup_key,
        event_id=row.event_id,
        event_type=row.event_type,
        # The column is a plain String guarded by a CHECK (no native PG enum —
        # see models.py); a value outside the enum means someone wrote the table
        # around the constraint, and StrEnum's lookup raising here is the right,
        # loud answer.
        outcome=DeliveryOutcome(row.outcome),
        created_at=row.created_at,
        policy_id=row.policy_id,
        policy_version_id=row.policy_version_id,
        created_item_ref=row.created_item_ref,
        detail=row.detail,
    )

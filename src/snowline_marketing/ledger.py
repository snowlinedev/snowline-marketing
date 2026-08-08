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

- **The store owns the key namespace.** Every stored `dedup_key` is prefixed by
  `record` itself — "p:" for policy-level rows, "e:" for event-level rows,
  derived from whether the write names a policy (which the consistency guard in
  `record` makes equivalent to the outcome's level). Callers render keys; they
  cannot choose the namespace. That is what makes the engine's reserved
  event-level shapes UNFORGEABLE: a tenant-authored template that renders
  "e:whatever" is a policy-level write and stores as "p:e:whatever", so it can
  never collide with — or read back — a reserved event-level row. Reads take
  the STORED key, i.e. the namespaced string `LedgerRecord.dedup_key` reports.

This module deliberately knows nothing about policies, envelopes or matching.
It stores what it is told, and the engine decides what to tell it — which is
what lets the ledger be tested against a real database with no policy artifact
in sight.

`LedgerStore` is the protocol `engine.evaluate` actually depends on — the
`record` surface, and nothing else the engine ever calls. `InMemoryDeliveryLedger`
is its second implementation, held in the process rather than Postgres: the
store spec §11's dry-run drives, so a preview's dedup behavior against a
captured stream is provably the SAME as evaluating for real, not a
lookalike that happens to agree today. It mirrors the guard and the namespace
derivation above via the same helper `DeliveryLedger.record` calls, so the two
stores cannot drift apart on the one thing a dry-run's honesty depends on.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

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
    - `failed` — a delivery whose consequence could not be carried out. As a
      ROW outcome it is NOT produced by this item: retry, backoff and
      dead-lettering are spec §11, and the value is defined now so that the
      schema, the CHECK and the dashboard vocabulary do not need a migration
      when §11 lands. The engine does report `failed` on a DELIVERY — without
      writing a row — for a within-evaluation dedup-key collision
      (`engine.evaluate`), the operator-visible alternative to silently
      swallowing one policy's work as another's.
    """

    matched = "matched"
    ignored = "ignored"
    created = "created"
    deduplicated = "deduplicated"
    quarantined = "quarantined"
    failed = "failed"


# The outcomes that describe an EVENT rather than a rule, and are therefore the
# only ones allowed to carry no policy id and no evaluated policy version (see
# the CHECK constraints in models.py). Kept here so the store's guard in
# `record` and the database's CHECK are the same list, read from one place.
EVENT_LEVEL_OUTCOMES = frozenset({DeliveryOutcome.ignored, DeliveryOutcome.quarantined})

# The key namespaces (see the module docstring): prepended by `record`, never
# by callers, so an event-level shape cannot be forged from a policy template.
_POLICY_KEY_NAMESPACE = "p:"
_EVENT_KEY_NAMESPACE = "e:"


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


def _namespaced_key(
    dedup_key: str, *, outcome: DeliveryOutcome, policy_id: str | None
) -> str:
    """Validate the outcome<->policy_id invariant and return the STORED,
    namespaced key ("e:"/"p:", see the module docstring).

    Shared by `DeliveryLedger.record` and `InMemoryDeliveryLedger.record` so
    the guard and the namespace derivation — the two things that make a
    dry-run's dedup behavior identical to production's — exist in exactly one
    place and cannot drift between the two stores.

    Raises ValueError — before touching either store — when `outcome` and
    `policy_id` disagree about the delivery's level: `EVENT_LEVEL_OUTCOMES`
    may not name a policy, every other outcome must. The database CHECK
    enforces the same invariant for `DeliveryLedger`; guarding here too keeps
    the refusal loud and typed instead of a driver-shaped IntegrityError (and
    `InMemoryDeliveryLedger` has no CHECK to fall back on at all)."""
    if outcome in EVENT_LEVEL_OUTCOMES and policy_id is not None:
        raise ValueError(
            f"event-level outcome {outcome.value!r} must not name a policy "
            f"(got policy_id={policy_id!r}) — ignored/quarantined are facts "
            "about an event, not about a rule"
        )
    if outcome not in EVENT_LEVEL_OUTCOMES and policy_id is None:
        raise ValueError(
            f"policy-level outcome {outcome.value!r} must name the policy "
            "that produced it"
        )
    # The guard above just made "names a policy" equivalent to "is a
    # policy-level outcome", so the namespace derives from either; the
    # policy id is the one the type system already distinguishes.
    namespace = _EVENT_KEY_NAMESPACE if policy_id is None else _POLICY_KEY_NAMESPACE
    return namespace + dedup_key


class LedgerStore(Protocol):
    """What `engine.evaluate` needs from a delivery ledger — the `record`
    surface, and nothing else the engine ever calls.

    `DeliveryLedger` (Postgres) and `InMemoryDeliveryLedger` (spec §11's
    dry-run) both satisfy this without inheriting from it — the engine takes
    the protocol so a dry-run can point the same deterministic core at a store
    that leaves no trace, and `engine.py` never has to import, or know about,
    the dry-run at all."""

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
    ) -> LedgerWrite: ...


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

        The stored key is `dedup_key` under the store's own namespace — "e:"
        for an event-level write, "p:" for a policy-level one, derived from
        `policy_id`'s presence (see the module docstring). The returned
        record carries the stored, namespaced key, which is what every read
        takes.

        Never updates an existing row (see the module docstring): on conflict
        the stored row is returned untouched, `inserted=False`, and the caller
        learns that this delivery owes no new work. The insert and the read-back
        share one transaction, so the row handed back is the row that satisfies
        the key at the moment the claim was decided.

        Raises ValueError — before touching the database — when `outcome` and
        `policy_id` disagree about the delivery's level (see `_namespaced_key`).

        Raises like any other store call if the database is unreachable — the
        never-raises contract belongs to the CLASSIFIERS (malformed input is an
        expected input class); an unreachable database is not an input, and
        swallowing it here would ack an event whose audit row was never
        written. The intake loop already turns the exception into a stopped
        pass with the position un-acked."""
        dedup_key = _namespaced_key(dedup_key, outcome=outcome, policy_id=policy_id)
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
        """The row holding `dedup_key` for `tenant`, or None. Takes the STORED
        (namespaced) key — the one `LedgerRecord.dedup_key` reports — and rides
        the primary key."""
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


class InMemoryDeliveryLedger:
    """A delivery ledger held in the process — spec §11's dry-run ledger.

    Not a mock: this is the store a dry-run drives, the way `InMemoryCursorStore`
    is the store a dry-run points the intake loop's cursor at (`cursors.py`) —
    "a dry-run that moved the real cursor would eat the events it was only
    supposed to preview" applies to the ledger identically, and a real
    `matched` row would be a claim on work a dry-run must never actually own.

    Faithful, not merely a stand-in: first-insert-wins on `(tenant, dedup_key)`
    — a dict keyed on exactly that pair, so a second `record` call for a
    claimed key returns the EXISTING row untouched, `inserted=False`, same as
    `DeliveryLedger`'s `ON CONFLICT DO NOTHING` — the same "p:"/"e:" namespace
    derivation, and the same outcome<->policy_id guard, all three via the
    shared `_namespaced_key` helper `DeliveryLedger.record` itself calls. That
    is what makes a dry-run's dedup behavior against a captured stream provably
    the SAME as evaluating for real: within one dry-run, the same event
    delivered twice converges to one row exactly as it would in production.

    Satisfies `LedgerStore`, which is all `evaluate` requires, plus `get` — a
    trivial keyed lookup for tests to assert on what a dry-run recorded. The
    read surface grows when §11's dashboard actually reads it; until then the
    in-memory store implements exactly the `LedgerStore` protocol plus `get`.

    Not process-safe or thread-safe, unlike `DeliveryLedger`, whose uniqueness
    the DATABASE enforces under concurrency (module docstring: "idempotent
    under concurrency, not just under re-delivery"). A dry-run is a single
    caller driving a single capture through in one thread, which is the only
    claim this store needs to make good on."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], LedgerRecord] = {}

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
        """See `LedgerStore.record` / `DeliveryLedger.record`. Raises the same
        ValueError, on the same terms, before either store is touched."""
        stored_key = _namespaced_key(dedup_key, outcome=outcome, policy_id=policy_id)
        key = (tenant, stored_key)
        existing = self._rows.get(key)
        if existing is not None:
            # First-insert-wins, same as `ON CONFLICT DO NOTHING`: the
            # existing row is returned untouched, and the caller learns this
            # delivery owes no new work.
            return LedgerWrite(record=existing, inserted=False)
        record = LedgerRecord(
            tenant=tenant,
            dedup_key=stored_key,
            event_id=event_id,
            event_type=event_type,
            outcome=outcome,
            created_at=datetime.now(timezone.utc),
            policy_id=policy_id,
            policy_version_id=policy_version_id,
            created_item_ref=None,
            detail=detail,
        )
        self._rows[key] = record
        return LedgerWrite(record=record, inserted=True)

    def get(self, tenant: str, dedup_key: str) -> LedgerRecord | None:
        """See `DeliveryLedger.get`. Takes the STORED (namespaced) key. A
        trivial keyed lookup with no ordering promise."""
        return self._rows.get((tenant, dedup_key))

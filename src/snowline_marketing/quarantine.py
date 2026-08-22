"""Completion quarantine — provenance-less completions of marketing-minted
items (spec §4/§8).

Spec §8's watch-and-quarantine, second half: "a completion without one lands in
quarantine — visible, auditable, resolvable by attaching provenance after the
fact. No hard completion gate, no friction on the PM verb itself." This module
is the store. `models.CompletionQuarantineEntry` is the schema and its docstring
carries the key design (why the natural key is `tenant + event_id` rather than
the item, and why the raw event is kept whole).

**What this store does NOT hold, and why that is a decision rather than an
omission.** Spec §4's quarantine bullet names two populations in one sentence —
"malformed/unmapped events and provenance-missing completions". They share an
operator SURFACE (§11 lists both with reasons) and nothing else:

- A provenance-less completion is a VALID envelope. It has a tenant, an event
  id, and a marketing-minted item ref, and its verb is "attach provenance",
  which writes deliverable rows.
- A malformed event never parsed. It may have no tenant and no event id at all
  (`events.MalformedEnvelope.event_id` is best-effort None), so its identity is
  the source's `(source_key, position)` pair — `intake.run_intake` already says
  so, in the note about the crash window between report and ack — and its verb
  is "requeue these raw bytes", which writes nothing but replays a stream.

One table serving both would need a nullable tenant, a nullable event id, a
second key nobody uses on half the rows, and two disjoint verb sets: a union
pretending to be a thing. So this store is completions only, and the
malformed-event store lands with the operator surfaces that read it (§11),
against the `on_malformed` seam intake already exposes. Neither half changes the
other's shape.

**The write shape mirrors the delivery ledger, verb for verb**, because the same
two properties are wanted:

    INSERT ... ON CONFLICT DO NOTHING, then read the row back.

DO NOTHING and not DO UPDATE: by the time a completion re-delivers, the existing
row may already be RESOLVED (an operator attached provenance) or DISMISSED, and
an upsert would silently reopen a decision someone made — the same reasoning
that keeps `ledger.record` from overwriting a `created` row. `inserted` tells the
caller whether THIS delivery is the one that filed the observation, so a watch
can log once instead of once per pass. And every later change is a GUARDED
compare-and-set (`_TRANSITIONS`, applied through `_QuarantineTransitions`) —
one definition, both stores — so two operators (or an operator and a replay)
racing one row resolve in the database and exactly one sees `applied=True`.

The two verbs are §8's and §4's, and they are not the same answer:

- `resolve` — provenance was attached after the fact. The deliverable rows are
  written FIRST and the row is closed second (`watch.resolve_quarantined`), the
  same durable-fact-before-acknowledgement discipline that makes minting
  recoverable: a crash in between leaves an open row and a recorded deliverable,
  and re-running converges.
- `dismiss` — operator judgment that there was no deliverable to record (a
  marketing item completed as "no longer needed"). Nothing is written anywhere
  else; the row closes carrying the reason.

Both require a detail. A closed row that cannot say who closed it and why is an
audit trail that stops at the interesting part, and the database CHECK
(`ck_completion_quarantine_resolution_detail`) makes that an invariant rather
than a convention.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from snowline_marketing.db import session_scope
from snowline_marketing.models import QUARANTINE_REASON_VALUES, QUARANTINE_STATUS_VALUES
from snowline_marketing.models import CompletionQuarantineEntry as QuarantineRow


class QuarantineReason(enum.StrEnum):
    """Why a completion is in quarantine — the operator-visible verdict
    (spec §4).

    Two values, because they send an operator to two different places.
    `provenance_missing` is spec §8's ordinary case: the completion declared
    nothing, and the fix is to attach provenance (nobody did anything wrong; the
    PM verb deliberately has no gate). `provenance_malformed` is a declaration
    that was ATTEMPTED and could not be read — a producer or an agent wrote
    something broken, and the detail names the field. Folding them into one
    value would file a producer bug under "the operator forgot" and send someone
    looking for a human who did nothing.

    The finer classification stays in `provenance.ProvenanceReason` and reaches
    the row as `detail`; this enum is what §11's dashboard groups by."""

    provenance_missing = "provenance_missing"
    provenance_malformed = "provenance_malformed"


class QuarantineStatus(enum.StrEnum):
    """Where a quarantine row stands (spec §4's requeue/dismiss verbs, spelled
    for completions).

    - `open` — filed and unresolved. What §11's queue lists and what §14's "a
      provenance-less completion is visible in quarantine within one sweep"
      means.
    - `resolved` — provenance was attached after the fact and the deliverable
      ledger now holds it (spec §8). Terminal.
    - `dismissed` — an operator judged that there was no deliverable to record.
      Terminal, and deliberately NOT the same as resolved: "we recorded what it
      produced" and "it produced nothing worth recording" are different audit
      facts, and a single closed state would make the second unsayable."""

    open = "open"
    resolved = "resolved"
    dismissed = "dismissed"


# Import-time pins: these enums ARE `models.QUARANTINE_*_VALUES`, the single
# declarations the database CHECKs are built from. A value added to either side
# without the other would not error at runtime — it would quietly refuse
# storable rows or store unreadable ones — so the drift fails here, at import,
# where the suite cannot miss it. (Same posture as
# `ledger.DeliveryOutcome` vs `models.DELIVERY_OUTCOME_VALUES`.)
if {reason.value for reason in QuarantineReason} != QUARANTINE_REASON_VALUES:
    raise AssertionError(
        "quarantine.QuarantineReason must equal models.QUARANTINE_REASON_VALUES — "
        "the app enum and the schema CHECK are one vocabulary, declared once"
    )
if {status.value for status in QuarantineStatus} != QUARANTINE_STATUS_VALUES:
    raise AssertionError(
        "quarantine.QuarantineStatus must equal models.QUARANTINE_STATUS_VALUES — "
        "the app enum and the schema CHECK are one vocabulary, declared once"
    )

# The statuses a row is CLOSED in — nothing further is owed and §11's queue does
# not list them. Declared here because it is a fact about what a ROW means, so
# no caller has to carry its own copy of the list.
CLOSED_STATUSES = frozenset({QuarantineStatus.resolved, QuarantineStatus.dismissed})


def _utc_now() -> datetime:
    """`InMemoryCompletionQuarantine`'s default clock — the in-process stand-in
    for the timestamptz `func.now()` the real store's columns default to."""
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class QuarantineRecord:
    """One quarantine row, read back.

    A plain record rather than the ORM object, for the reason
    `ledger.LedgerRecord` is one: callers hold it outside the session that
    produced it."""

    tenant: str
    event_id: str
    item_ref: str
    reason: QuarantineReason
    detail: str
    raw_event: str
    status: QuarantineStatus
    occurred_at: datetime
    created_at: datetime
    resolution_detail: str | None = None
    updated_at: datetime | None = None

    @property
    def is_open(self) -> bool:
        return self.status is QuarantineStatus.open


@dataclass(frozen=True)
class QuarantineWrite:
    """The result of one `record` call: the row that now holds the key, and
    whether THIS call filed it.

    `inserted` is what the delivery ledger's is — the entitlement to treat this
    as news. Exactly one delivery of a completion ever sees it True, across
    re-deliveries and racing passes, which is what keeps a re-delivered
    provenance-less completion from being reported (or logged, or counted) as a
    second observation."""

    record: QuarantineRecord
    inserted: bool


@dataclass(frozen=True)
class QuarantineTransition:
    """The result of one guarded row transition.

    `applied` is to a transition what `QuarantineWrite.inserted` is to a filing:
    the entitlement to act on what the transition means. `record` is the row as
    it stands AFTER the attempt — the new state when it applied, and the state
    that REFUSED it otherwise, because a resolve refused by an already-resolved
    row is a no-op while one refused by a dismissed row is a disagreement an
    operator should see. None only if the row does not exist at all."""

    record: QuarantineRecord | None
    applied: bool


@dataclass(frozen=True)
class _Transition:
    """One legal row transition: what it sets, and what it is legal FROM.

    Declared as DATA rather than written twice, for `ledger._Transition`'s
    reason: the in-memory store is what the fixtures-first flow drives, and two
    stores whose guards were merely similar would let the suite prove a
    property production does not have."""

    name: str
    to_status: QuarantineStatus
    from_statuses: frozenset[QuarantineStatus]


_TRANSITIONS: dict[str, _Transition] = {
    # open -> resolved: provenance was attached after the fact and the
    # deliverable ledger now holds it (spec §8). Legal only from `open`, which
    # is what makes a second resolve a no-op rather than a rewrite of the first
    # operator's note.
    "resolve": _Transition(
        "resolve", QuarantineStatus.resolved, frozenset({QuarantineStatus.open})
    ),
    # open -> dismissed: operator judgment that there was no deliverable to
    # record. Never reachable from `resolved` — un-recording a deliverable is
    # not a quarantine verb, and pretending it is would let one row's history
    # contradict the ledger's.
    "dismiss": _Transition(
        "dismiss", QuarantineStatus.dismissed, frozenset({QuarantineStatus.open})
    ),
}


class _QuarantineTransitions:
    """The transition verbs, shared by both stores.

    Each verb is the same two lines — look up the declared `_Transition`, hand it
    to the store's own `_apply` — so the STORE implements one primitive (guarded
    compare-and-set) and the SEMANTICS live here, once. The same split
    `ledger._LedgerTransitions` makes, for the same reason."""

    def _apply(
        self,
        transition: _Transition,
        *,
        tenant: str,
        event_id: str,
        detail: str,
    ) -> QuarantineTransition:  # pragma: no cover - implemented by both stores
        raise NotImplementedError

    def resolve(
        self, tenant: str, event_id: str, *, detail: str
    ) -> QuarantineTransition:
        """Close an open row as RESOLVED: provenance was attached after the fact
        (spec §8).

        The store-level half of the verb. Writing the deliverable rows is the
        caller's, and it happens FIRST (`watch.resolve_quarantined`) — a crash
        between the two leaves an open row beside a recorded deliverable, which
        re-running converges, whereas closing first would lose the deliverable
        with the row closed over it.

        `detail` is required and is the operator's sentence on the row: what was
        attached, by whom. `applied=False` means the row was already closed;
        read `record.status` to tell resolved from dismissed."""
        return self._apply(
            _TRANSITIONS["resolve"], tenant=tenant, event_id=event_id, detail=detail
        )

    def dismiss(
        self, tenant: str, event_id: str, *, detail: str
    ) -> QuarantineTransition:
        """Close an open row as DISMISSED: an operator's judgment that there was
        no deliverable to record.

        Writes nothing to the deliverable ledger, by design — that is the whole
        difference from `resolve`, and it is why the two are separate verbs
        rather than one `close(status)`: a queue whose closures cannot be told
        apart cannot answer "how much did we actually record?". `detail` is
        required for the same reason `ledger.mark_failed`'s is."""
        return self._apply(
            _TRANSITIONS["dismiss"], tenant=tenant, event_id=event_id, detail=detail
        )


class QuarantineStore(Protocol):
    """What `watch.py` needs from a completion quarantine.

    `CompletionQuarantine` (Postgres) and `InMemoryCompletionQuarantine` (the
    fixtures-first / dry-run store) both satisfy it without inheriting from it,
    so the watch runs end to end with no database and the conformance suite
    proves the two stores answer the same questions the same way.

    `list_open` is IN the protocol — unlike the delivery ledger, whose listings
    are DB-only for §11's dashboard. Spec §14's acceptance criterion is that a
    provenance-less completion is VISIBLE in quarantine within one sweep, and a
    criterion the fixtures-first flow cannot check is a criterion that gets
    checked once, by hand, on a machine with Postgres."""

    def record(
        self,
        *,
        tenant: str,
        event_id: str,
        item_ref: str,
        reason: QuarantineReason,
        detail: str,
        raw_event: str,
        occurred_at: datetime,
    ) -> QuarantineWrite: ...

    def get(self, tenant: str, event_id: str) -> QuarantineRecord | None: ...

    def list_open(
        self, tenant: str, *, limit: int | None = None
    ) -> list[QuarantineRecord]: ...

    def resolve(
        self, tenant: str, event_id: str, *, detail: str
    ) -> QuarantineTransition: ...

    def dismiss(
        self, tenant: str, event_id: str, *, detail: str
    ) -> QuarantineTransition: ...


class CompletionQuarantine(_QuarantineTransitions):
    """The `completion_quarantine` table (spec §4)."""

    def record(
        self,
        *,
        tenant: str,
        event_id: str,
        item_ref: str,
        reason: QuarantineReason,
        detail: str,
        raw_event: str,
        occurred_at: datetime,
    ) -> QuarantineWrite:
        """File one provenance-less completion, or report the row already filed.

        Never updates an existing row (see the module docstring): on conflict the
        stored row comes back untouched with `inserted=False`, so a re-delivered
        completion converges to ONE row and cannot reopen a decision an operator
        already made. The insert and the read-back share one transaction, so the
        row handed back is the row that satisfies the key at the moment the
        filing was decided.

        `RETURNING` decides who inserted and a read-back supplies the row, for
        `ledger.record`'s reason exactly: under `ON CONFLICT DO NOTHING`,
        psycopg reports `rowcount` -1, so a rowcount test would silently call
        every filing fresh or every filing a duplicate.

        Raises like any other store call if the database is unreachable — the
        watch turns that into a stalled pass with the event un-acked, which is
        the honest outcome for an observation that was never recorded."""
        statement = (
            pg_insert(QuarantineRow)
            .values(
                tenant=tenant,
                event_id=event_id,
                item_ref=item_ref,
                reason=reason.value,
                detail=detail,
                raw_event=raw_event,
                status=QuarantineStatus.open.value,
                occurred_at=occurred_at,
            )
            .on_conflict_do_nothing(
                index_elements=[QuarantineRow.tenant, QuarantineRow.event_id]
            )
            .returning(QuarantineRow.event_id)
        )
        read_back = select(QuarantineRow).where(
            QuarantineRow.tenant == tenant, QuarantineRow.event_id == event_id
        )
        with session_scope() as session:
            inserted = session.execute(statement).first() is not None
            row = session.scalars(read_back).one()
            return QuarantineWrite(record=_to_record(row), inserted=inserted)

    def _apply(
        self,
        transition: _Transition,
        *,
        tenant: str,
        event_id: str,
        detail: str,
    ) -> QuarantineTransition:
        """One guarded compare-and-set (see `_QuarantineTransitions` for the
        verbs).

        The `WHERE` names the legal source statuses, so two closers racing one
        row resolve in the database: one UPDATE matches, the other matches
        nothing. The row is read back in the same transaction regardless,
        because the refusal path needs the row that refused."""
        statement = (
            update(QuarantineRow)
            .where(
                QuarantineRow.tenant == tenant,
                QuarantineRow.event_id == event_id,
                QuarantineRow.status.in_(
                    sorted(status.value for status in transition.from_statuses)
                ),
            )
            .values(
                status=transition.to_status.value,
                resolution_detail=detail,
                updated_at=func.now(),
            )
        )
        with session_scope() as session:
            applied = session.execute(statement).rowcount == 1
            row = session.get(QuarantineRow, (tenant, event_id))
            return QuarantineTransition(
                record=_to_record(row) if row is not None else None, applied=applied
            )

    def get(self, tenant: str, event_id: str) -> QuarantineRecord | None:
        """The row filed for this completion, or None. Rides the primary key."""
        with session_scope() as session:
            row = session.get(QuarantineRow, (tenant, event_id))
            return _to_record(row) if row is not None else None

    def list_open(
        self, tenant: str, *, limit: int | None = None
    ) -> list[QuarantineRecord]:
        """One tenant's OPEN rows, oldest first — §11's queue and §14's
        visibility criterion. Oldest first because the queue is worked from the
        front and because "how long has this been unrecorded?" is what makes it
        a queue. Rides `ix_completion_quarantine_tenant_status`; the event id
        breaks ties so the order is total."""
        return self._listing(
            QuarantineRow.tenant == tenant,
            QuarantineRow.status == QuarantineStatus.open.value,
            limit=limit,
        )

    def list_for_item(self, tenant: str, item_ref: str) -> list[QuarantineRecord]:
        """Every quarantine row this item has ever produced, oldest first —
        including closed ones, because "was this ever unrecorded, and who closed
        it?" is the audit question the item-keyed alternative would have answered
        by construction (see `models.CompletionQuarantineEntry`). Rides
        `ix_completion_quarantine_tenant_item_ref`."""
        return self._listing(
            QuarantineRow.tenant == tenant, QuarantineRow.item_ref == item_ref
        )

    def _listing(self, *criteria, limit: int | None = None) -> list[QuarantineRecord]:
        statement = (
            select(QuarantineRow)
            .where(*criteria)
            .order_by(QuarantineRow.created_at, QuarantineRow.event_id)
        )
        if limit is not None:
            statement = statement.limit(max(0, limit))
        with session_scope() as session:
            return [_to_record(row) for row in session.scalars(statement)]


def _to_record(row: QuarantineRow) -> QuarantineRecord:
    return QuarantineRecord(
        tenant=row.tenant,
        event_id=row.event_id,
        item_ref=row.item_ref,
        # The columns are plain Strings guarded by CHECKs (no native PG enums —
        # see models.py); a value outside the enum means someone wrote the table
        # around the constraint, and StrEnum's lookup raising here is the right,
        # loud answer.
        reason=QuarantineReason(row.reason),
        detail=row.detail,
        raw_event=row.raw_event,
        status=QuarantineStatus(row.status),
        occurred_at=row.occurred_at,
        created_at=row.created_at,
        resolution_detail=row.resolution_detail,
        updated_at=row.updated_at,
    )


class InMemoryCompletionQuarantine(_QuarantineTransitions):
    """A completion quarantine held in the process.

    Not a mock, for `InMemoryDeliveryLedger`'s reason: this is the store the
    fixtures-first flow (spec §5) drives, so §14's "visible in quarantine within
    one sweep" is provable without a database — and provable about the same
    code, since the guards and the first-filing-wins rule come from the shared
    `_TRANSITIONS` and the same `record` contract.

    `list_for_item` stays DB-only, like the delivery ledger's dashboard reads;
    `list_open` does not, because it is what the acceptance criterion is written
    in terms of (see `QuarantineStore`).

    Not thread-safe, unlike the real store, whose uniqueness the DATABASE
    enforces. `clock` is the injectable time source, the in-process analogue of
    the real store's server-side `func.now()`."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._rows: dict[tuple[str, str], QuarantineRecord] = {}
        self._clock = clock if clock is not None else _utc_now

    def record(
        self,
        *,
        tenant: str,
        event_id: str,
        item_ref: str,
        reason: QuarantineReason,
        detail: str,
        raw_event: str,
        occurred_at: datetime,
    ) -> QuarantineWrite:
        """See `QuarantineStore.record` / `CompletionQuarantine.record`."""
        key = (tenant, event_id)
        existing = self._rows.get(key)
        if existing is not None:
            # First-filing-wins, same as `ON CONFLICT DO NOTHING`: the existing
            # row comes back untouched, so a re-delivery cannot reopen a closed
            # decision or restate an open one.
            return QuarantineWrite(record=existing, inserted=False)
        record = QuarantineRecord(
            tenant=tenant,
            event_id=event_id,
            item_ref=item_ref,
            reason=reason,
            detail=detail,
            raw_event=raw_event,
            status=QuarantineStatus.open,
            occurred_at=occurred_at,
            created_at=self._clock(),
        )
        self._rows[key] = record
        return QuarantineWrite(record=record, inserted=True)

    def _apply(
        self,
        transition: _Transition,
        *,
        tenant: str,
        event_id: str,
        detail: str,
    ) -> QuarantineTransition:
        """See `CompletionQuarantine._apply`. The same guard, expressed against
        a dict: the transition applies only from the declared source statuses, so
        a second closer refusing an already-closed row behaves here exactly as
        the UPDATE's `WHERE` makes it behave in Postgres."""
        existing = self._rows.get((tenant, event_id))
        if existing is None:
            return QuarantineTransition(record=None, applied=False)
        if existing.status not in transition.from_statuses:
            return QuarantineTransition(record=existing, applied=False)
        updated = dataclasses.replace(
            existing,
            status=transition.to_status,
            resolution_detail=detail,
            updated_at=self._clock(),
        )
        self._rows[(tenant, event_id)] = updated
        return QuarantineTransition(record=updated, applied=True)

    def get(self, tenant: str, event_id: str) -> QuarantineRecord | None:
        """See `CompletionQuarantine.get`."""
        return self._rows.get((tenant, event_id))

    def list_open(
        self, tenant: str, *, limit: int | None = None
    ) -> list[QuarantineRecord]:
        """See `CompletionQuarantine.list_open` — same order (oldest first, event
        id breaking ties), so a test asserting on the sequence asserts the same
        thing on both stores."""
        rows = sorted(
            (
                record
                for record in self._rows.values()
                if record.tenant == tenant and record.is_open
            ),
            key=lambda record: (record.created_at, record.event_id),
        )
        return rows if limit is None else rows[: max(0, limit)]

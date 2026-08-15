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

**Rows also TRANSITION now (spec §7's minting layer).** `record` still writes
the row exactly once and never updates it; every later change to that row is a
GUARDED COMPARE-AND-SET declared in `_TRANSITIONS` and applied through
`_LedgerTransitions` — one definition, both stores. The guard is the point, not
a formality:

- The `WHERE` clause names the outcome(s) the transition is legal FROM, so a
  second minting pass racing the first cannot advance a row the first already
  advanced. Exactly one caller sees `applied=True`, the way exactly one caller
  sees `inserted=True` from `record` — which is what makes "two passes, one
  row, one mint" a property of the database rather than of a lock.
- Every transition is ONE statement. `confirm_created` therefore sets `outcome`
  and `created_item_ref` together, which is the only shape
  `ck_delivery_ledger_created_item_ref` permits anyway — the constraint and the
  honest write agree.
- `detail` is REPLACED, not appended. A row can cycle claim → release → claim
  indefinitely while a sink is down, and an appending note would grow a Text
  column without bound; the outcome column carries the durable fact and the
  detail carries the current explanation.
- `updated_at` moves on every transition and is NULL until the first one. That
  is what makes a held claim measurable ("how long has this been mid-mint?") —
  `created_at` deliberately never moves, so without this column §11 could see
  a stuck row but not how stuck it is.
"""

from __future__ import annotations

import dataclasses
import enum
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from sqlalchemy import func, select, update
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
    - `claimed` — a minting pass has taken the row and is calling PM's surface
      (spec §7). Written BEFORE the sink call, which is what makes it a durable
      single-writer guard rather than a status light: a claimed row whose
      confirmation never landed may or may not have minted, so re-delivery must
      NOT re-mint it — it surfaces for reconciliation (§11). Not terminal, and
      the only outcome an operator is ever expected to resolve by hand.
    - `created` — the minting layer turned a `claimed` row into a PM item and
      wrote its ref (spec §7), in the one UPDATE
      `ck_delivery_ledger_created_item_ref` permits. Terminal.
    - `awaiting_approval` — the entry matched in mode `approval_required`
      (`policies.PolicyMode`). The work IS owed and deliberately unminted until
      §12's operator verb releases it. NOT terminal and NOT a refusal: the row
      stays re-owable (see `RE_OWED_OUTCOMES`) so the consequence keeps being
      re-emitted on re-delivery, and re-marking an already-marked row is a
      guarded no-op, which is what keeps "waiting" from becoming "spamming".
    - `dry_run` — the entry matched in mode `dry_run`. TERMINAL, and terminal is
      the whole point: a dry-run match produced no work ON PURPOSE (§11:
      "report what would have been minted, mint nothing"), so leaving it at
      `matched` would re-owe a mint that must never happen, forever. Distinct
      from `created` (nothing exists), from `ignored` (a rule DID select this
      event) and from `failed` (nothing went wrong). Distinct, too, from
      `dryrun.py`'s preview, which writes no durable row at all: this outcome is
      what a LIVE pass records when a live policy is armed in dry-run mode.
    - `deduplicated` — this delivery found the key already taken. No new work,
      by design; the existing row's result stands. A DELIVERY-level answer:
      no row is ever written with it.
    - `quarantined` — the delivery was refused rather than evaluated (today:
      a cross-tenant envelope, §14). Consumed, explained, never silently
      dropped.
    - `failed` — the consequence could not be carried out and retrying will not
      help: PM permanently rejected the mint, or the entry's title/body template
      could not be rendered from this event. Terminal here and the input to
      §11's dead-letter/replay, which owns the operator verbs. The engine also
      reports `failed` on a DELIVERY — without writing a row — for a
      within-evaluation dedup-key collision and for a re-delivery that lands on
      a row needing reconciliation (`engine.evaluate`).
    """

    matched = "matched"
    ignored = "ignored"
    claimed = "claimed"
    created = "created"
    awaiting_approval = "awaiting_approval"
    dry_run = "dry_run"
    deduplicated = "deduplicated"
    quarantined = "quarantined"
    failed = "failed"


# The outcomes that describe an EVENT rather than a rule, and are therefore the
# only ones allowed to carry no policy id and no evaluated policy version (see
# the CHECK constraints in models.py). Kept here so the store's guard in
# `record` and the database's CHECK are the same list, read from one place.
EVENT_LEVEL_OUTCOMES = frozenset({DeliveryOutcome.ignored, DeliveryOutcome.quarantined})

# The row states that still OWE their work — a re-delivery must re-emit the
# consequence rather than report `deduplicated` (spec §4's recoverable
# convergence). Read by `engine.evaluate`; defined here because it is a fact
# about what a ROW means, and the engine must never carry its own copy of that
# list.
#
# `matched` is the crash window (the row was written, the mint never happened);
# `awaiting_approval` is the deliberate gate (the mint is owed and withheld).
# Everything else is excluded on purpose — `dry_run` and `created` are settled,
# and `claimed`/`failed` need an operator, not another attempt.
RE_OWED_OUTCOMES = frozenset(
    {DeliveryOutcome.matched, DeliveryOutcome.awaiting_approval}
)

# The row states a re-delivery must SURFACE rather than act on: minting them
# again risks a silent double-mint (`claimed` — the earlier attempt's fate is
# unknown) or silently retries work that was permanently refused (`failed` —
# §11's replay is the operator's verb for that, not an accidental re-delivery).
# The engine reports these deliveries as `failed` with a detail and emits no
# consequence.
RECONCILIATION_OUTCOMES = frozenset({DeliveryOutcome.claimed, DeliveryOutcome.failed})

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
    # When this row last TRANSITIONED (see the module docstring) — NULL for a
    # row that has only ever been recorded. `created_at` never moves, so this is
    # the only column that can answer "how long has this claim been held?",
    # which is the question §11's reconciliation surface is built on.
    updated_at: datetime | None = None


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


@dataclass(frozen=True)
class LedgerTransition:
    """The result of one guarded row transition (see the module docstring).

    `applied` is to a transition what `LedgerWrite.inserted` is to a claim: the
    entitlement to act on what the transition means. Exactly one caller ever
    sees it True for a given row-and-transition, across racing passes and across
    re-deliveries, because the guard is a compare-and-set in the database rather
    than a check-then-write in a process.

    `record` is the row as it stands AFTER the attempt — the new state when the
    transition applied, and the state that REFUSED it otherwise, which is the
    fact the caller needs (a claim refused because the row already says
    `created` is a no-op; one refused because it says `claimed` is a
    reconciliation case, and only the record can tell them apart). None only if
    the row does not exist at all, which callers should treat as a bug: minting
    only ever transitions rows the engine wrote."""

    record: LedgerRecord | None
    applied: bool


@dataclass(frozen=True)
class _Transition:
    """One legal row transition: what it sets, and what it is legal FROM.

    Declared as DATA rather than written twice, because `DeliveryLedger` and
    `InMemoryDeliveryLedger` must agree on the guard exactly — the in-memory
    store is what a dry-run and the fixtures-first tests drive, and a store
    whose transitions were merely similar would let the suite prove a
    convergence property production does not have."""

    name: str
    to_outcome: DeliveryOutcome
    # Applying from any other outcome is refused. Every source state listed here
    # has a NULL `created_item_ref` by construction (the CHECK ties a ref to
    # `created`), and the guard re-states that condition anyway: the in-memory
    # store has no CHECK behind it, and a claim that could be taken on a row
    # holding an item ref would be a licence to mint twice.
    from_outcomes: frozenset[DeliveryOutcome]
    sets_item_ref: bool = False


_TRANSITIONS: dict[str, _Transition] = {
    # matched -> claimed: the durable marker written BEFORE the sink call. This
    # is the single-writer guard the whole minting design rests on.
    "claim": _Transition(
        "claim", DeliveryOutcome.claimed, frozenset({DeliveryOutcome.matched})
    ),
    # claimed -> created + ref, in ONE statement (the only shape the CHECK
    # allows, and the only shape that cannot leave a `created` row pointing at
    # nothing).
    "confirm_created": _Transition(
        "confirm_created",
        DeliveryOutcome.created,
        frozenset({DeliveryOutcome.claimed}),
        sets_item_ref=True,
    ),
    # claimed -> matched: the sink was unreachable and PROVABLY never saw the
    # request, so the claim is given back and the delivery re-owes. Only ever
    # applied when the request cannot have been delivered — see
    # `work_sink.SinkIndeterminate` for the case where it must NOT be.
    "release_claim": _Transition(
        "release_claim", DeliveryOutcome.matched, frozenset({DeliveryOutcome.claimed})
    ),
    # claimed -> failed: permanent. PM refused the mint, or the entry's
    # templates could not be rendered for this event. §11 replays from here.
    "mark_failed": _Transition(
        "mark_failed", DeliveryOutcome.failed, frozenset({DeliveryOutcome.claimed})
    ),
    # matched -> awaiting_approval: gated, still owed. Guarded from `matched`
    # alone, which is what makes re-marking an already-marked row a no-op
    # instead of a write per pass.
    "mark_awaiting_approval": _Transition(
        "mark_awaiting_approval",
        DeliveryOutcome.awaiting_approval,
        frozenset({DeliveryOutcome.matched}),
    ),
    # matched -> dry_run: terminal closure of a match that must never mint.
    "close_dry_run": _Transition(
        "close_dry_run", DeliveryOutcome.dry_run, frozenset({DeliveryOutcome.matched})
    ),
}


class _LedgerTransitions:
    """The transition verbs, shared by both stores.

    Each verb is the same two lines — look up the declared `_Transition`, hand
    it to the store's own `_apply` — so the STORE implements one primitive
    (guarded compare-and-set) and the SEMANTICS live here, once. That is the
    same split `_namespaced_key` makes for `record`, for the same reason: the
    two stores may differ in how they write, never in what a write means.

    Every verb takes the STORED (namespaced) dedup key — the string
    `LedgerRecord.dedup_key` reports and `engine.PendingConsequence.dedup_key`
    carries — because that is the row's actual identity."""

    def _apply(
        self,
        transition: _Transition,
        *,
        tenant: str,
        dedup_key: str,
        detail: str | None,
        item_ref: str | None = None,
    ) -> LedgerTransition:  # pragma: no cover - implemented by both stores
        raise NotImplementedError

    def claim(
        self, tenant: str, dedup_key: str, *, detail: str | None = None
    ) -> LedgerTransition:
        """Take ownership of a `matched` row before minting it (spec §7).

        The durable marker that closes the crash window: written and committed
        BEFORE PM is called, so a mint whose confirmation is lost leaves a
        `claimed` row rather than a `matched` one — and a `claimed` row is
        never re-minted by a re-delivery, it is surfaced (see
        `RECONCILIATION_OUTCOMES`). `applied=False` means another pass holds
        the claim, or the work is already done; read `record.outcome` to tell
        which."""
        return self._apply(
            _TRANSITIONS["claim"], tenant=tenant, dedup_key=dedup_key, detail=detail
        )

    def confirm_created(
        self, tenant: str, dedup_key: str, *, item_ref: str, detail: str | None = None
    ) -> LedgerTransition:
        """Turn this pass's claim into the `created` row that names the minted
        PM item (spec §4/§7). One statement, outcome and ref together.

        Raises ValueError on a blank ref before touching the store, mirroring
        `_namespaced_key`'s posture: a `created` row with nothing to point at is
        an audit trail claiming work exists that nobody can find, and the
        database CHECK that says so must not be the first thing to notice."""
        if not item_ref or not item_ref.strip():
            raise ValueError(
                "confirm_created requires the minted item's ref — a 'created' "
                "row with nothing to point at claims work nobody can find"
            )
        return self._apply(
            _TRANSITIONS["confirm_created"],
            tenant=tenant,
            dedup_key=dedup_key,
            detail=detail,
            item_ref=item_ref.strip(),
        )

    def release_claim(
        self, tenant: str, dedup_key: str, *, detail: str | None = None
    ) -> LedgerTransition:
        """Give the claim back so the delivery re-owes its mint.

        For the transient failure that PROVABLY never reached PM. Re-delivery is
        the retry loop — there is no backoff timer here and none is wanted at
        this layer (spec §11 owns retry policy); the row simply goes back to
        `matched` and the next pass tries again."""
        return self._apply(
            _TRANSITIONS["release_claim"],
            tenant=tenant,
            dedup_key=dedup_key,
            detail=detail,
        )

    def mark_failed(
        self, tenant: str, dedup_key: str, *, detail: str
    ) -> LedgerTransition:
        """Close the claim as permanently failed, with the reason (§11's
        dead-letter input). `detail` is required: a failed row an operator
        cannot act on is a failure recorded twice over."""
        return self._apply(
            _TRANSITIONS["mark_failed"],
            tenant=tenant,
            dedup_key=dedup_key,
            detail=detail,
        )

    def mark_awaiting_approval(
        self, tenant: str, dedup_key: str, *, detail: str
    ) -> LedgerTransition:
        """Mark a matched row as gated on §12's approval verb — owed, unminted,
        and visible as its own state. Idempotent by the guard: a row already
        `awaiting_approval` refuses the transition, so a pass that keeps
        re-owing the consequence keeps declining to mint without writing
        anything."""
        return self._apply(
            _TRANSITIONS["mark_awaiting_approval"],
            tenant=tenant,
            dedup_key=dedup_key,
            detail=detail,
        )

    def close_dry_run(
        self, tenant: str, dedup_key: str, *, detail: str
    ) -> LedgerTransition:
        """Close a dry-run match: terminal, honest, and the reason it exists is
        that the alternative — leaving the row at `matched` — re-owes a mint the
        policy's own mode forbids, on every re-delivery, forever."""
        return self._apply(
            _TRANSITIONS["close_dry_run"],
            tenant=tenant,
            dedup_key=dedup_key,
            detail=detail,
        )


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


class MintingLedger(LedgerStore, Protocol):
    """What `minting.py` needs from a delivery ledger (spec §7).

    A SUPERSET of `LedgerStore`, and declared as one, because a minting driver
    hands the same store to the engine and to the minting pass: the ledger row
    the engine wrote is the row the mint transitions, and two stores would mean
    two ledgers. The engine keeps depending on the narrow protocol — it must
    stay unable to transition anything — while this one adds the guarded verbs
    (`_LedgerTransitions`) and the keyed read.

    Both `DeliveryLedger` and `InMemoryDeliveryLedger` satisfy it, which is what
    lets the whole minting layer be tested fixtures-first with no database and
    no PM in sight (spec §5)."""

    def get(self, tenant: str, dedup_key: str) -> LedgerRecord | None: ...

    def claim(
        self, tenant: str, dedup_key: str, *, detail: str | None = None
    ) -> LedgerTransition: ...

    def confirm_created(
        self, tenant: str, dedup_key: str, *, item_ref: str, detail: str | None = None
    ) -> LedgerTransition: ...

    def release_claim(
        self, tenant: str, dedup_key: str, *, detail: str | None = None
    ) -> LedgerTransition: ...

    def mark_failed(
        self, tenant: str, dedup_key: str, *, detail: str
    ) -> LedgerTransition: ...

    def mark_awaiting_approval(
        self, tenant: str, dedup_key: str, *, detail: str
    ) -> LedgerTransition: ...

    def close_dry_run(
        self, tenant: str, dedup_key: str, *, detail: str
    ) -> LedgerTransition: ...


class DeliveryLedger(_LedgerTransitions):
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

    def _apply(
        self,
        transition: _Transition,
        *,
        tenant: str,
        dedup_key: str,
        detail: str | None,
        item_ref: str | None = None,
    ) -> LedgerTransition:
        """One guarded compare-and-set (see `_LedgerTransitions` for the verbs).

        The `WHERE` names the legal source outcomes, so two passes racing the
        same row resolve in the database: one UPDATE matches, the other matches
        nothing. `rowcount` IS reliable here — unlike the `ON CONFLICT DO
        NOTHING` insert in `record`, whose -1 the module docstring explains —
        but the row is read back regardless, in the same transaction, because
        the refusal path needs the row that refused and one code path through a
        transition is worth more than a saved round trip.

        Raises like any other store call if the database is unreachable: a
        transition that did not land must not be reported as one that did."""
        values: dict[str, object] = {
            "outcome": transition.to_outcome.value,
            "detail": detail,
            "updated_at": func.now(),
        }
        if transition.sets_item_ref:
            values["created_item_ref"] = item_ref
        statement = (
            update(DeliveryLedgerRow)
            .where(
                DeliveryLedgerRow.tenant == tenant,
                DeliveryLedgerRow.dedup_key == dedup_key,
                DeliveryLedgerRow.outcome.in_(
                    sorted(outcome.value for outcome in transition.from_outcomes)
                ),
                DeliveryLedgerRow.created_item_ref.is_(None),
            )
            .values(**values)
        )
        with session_scope() as session:
            applied = session.execute(statement).rowcount == 1
            row = session.get(DeliveryLedgerRow, (tenant, dedup_key))
            return LedgerTransition(
                record=_to_record(row) if row is not None else None, applied=applied
            )

    def get(self, tenant: str, dedup_key: str) -> LedgerRecord | None:
        """The row holding `dedup_key` for `tenant`, or None. Takes the STORED
        (namespaced) key — the one `LedgerRecord.dedup_key` reports — and rides
        the primary key."""
        with session_scope() as session:
            row = session.get(DeliveryLedgerRow, (tenant, dedup_key))
            return _to_record(row) if row is not None else None

    def list_by_outcome(
        self, tenant: str, outcome: DeliveryOutcome, *, limit: int | None = None
    ) -> list[LedgerRecord]:
        """One tenant's rows in one state, oldest first — the read §11's three
        operator queues are all spelled with: `failed` is the dead-letter,
        `claimed` is the reconciliation list (a mint whose fate is unknown),
        `awaiting_approval` is §12's approval queue. Oldest first because every
        one of those queues is worked from the front, and because "how long has
        this been stuck?" is the question that makes them queues at all.

        The state-recording is this item's job and the verbs are §11/§12's; one
        read is what keeps the recorded states from being write-only."""
        statement = (
            select(DeliveryLedgerRow)
            .where(
                DeliveryLedgerRow.tenant == tenant,
                DeliveryLedgerRow.outcome == outcome.value,
            )
            .order_by(DeliveryLedgerRow.created_at, DeliveryLedgerRow.dedup_key)
        )
        if limit is not None:
            statement = statement.limit(max(0, limit))
        with session_scope() as session:
            return [_to_record(row) for row in session.scalars(statement)]

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
        updated_at=row.updated_at,
    )


class InMemoryDeliveryLedger(_LedgerTransitions):
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

    Satisfies `MintingLedger` — `LedgerStore` (all `evaluate` requires) plus
    `get` and the guarded transition verbs, so the minting pass (spec §7) runs
    against this store with no database in sight and the conformance suite
    proves its transitions behave like the real one's. The LISTING surface
    stays DB-only (`list_by_outcome`, `for_event`, `list_for_tenant`): those
    exist for §11's dashboard, which reads Postgres.

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

    def _apply(
        self,
        transition: _Transition,
        *,
        tenant: str,
        dedup_key: str,
        detail: str | None,
        item_ref: str | None = None,
    ) -> LedgerTransition:
        """See `DeliveryLedger._apply`. The same guard, expressed against a
        dict: the transition applies only from the declared source outcomes and
        only while no item ref has been written, so a second caller refusing to
        transition an already-transitioned row behaves here exactly as the
        UPDATE's `WHERE` makes it behave in Postgres.

        Single-threaded, like the rest of this store (see the class docstring):
        the real store's guarantee is the DATABASE's, and nothing in a dry-run
        or a fixtures test races itself."""
        existing = self._rows.get((tenant, dedup_key))
        if existing is None:
            return LedgerTransition(record=None, applied=False)
        if (
            existing.outcome not in transition.from_outcomes
            or existing.created_item_ref is not None
        ):
            return LedgerTransition(record=existing, applied=False)
        updated = dataclasses.replace(
            existing,
            outcome=transition.to_outcome,
            detail=detail,
            updated_at=datetime.now(timezone.utc),
            created_item_ref=(
                item_ref if transition.sets_item_ref else existing.created_item_ref
            ),
        )
        self._rows[(tenant, dedup_key)] = updated
        return LedgerTransition(record=updated, applied=True)

    def get(self, tenant: str, dedup_key: str) -> LedgerRecord | None:
        """See `DeliveryLedger.get`. Takes the STORED (namespaced) key. A
        trivial keyed lookup with no ordering promise."""
        return self._rows.get((tenant, dedup_key))

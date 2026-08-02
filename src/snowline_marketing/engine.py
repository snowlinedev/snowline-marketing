"""The evaluation engine — events in, ledger rows and consequences out
(spec §4/§6/§14).

This is the deterministic core the whole plugin is built around. One envelope,
one tenant's policy version, and the answer is fully determined: which entries
matched, what each delivery is owed, and what the audit trail says about it.
Nothing here calls PM, mints anything, or knows what a work item is — a match
produces a `PendingConsequence`, which is a typed description of work the
MINTING layer (spec §7) will carry out. Keeping the decision and the action
apart is what makes the decision testable against captured fixtures with no
gateway in sight (spec §5), and what makes §11's dry-run — "evaluate a policy
version against captured fixtures, report what would have been minted, mint
nothing" — a mode rather than a fork in the code.

**The two ways an event can end.** Every delivery either CONSUMES the event (it
is acked, and the ledger holds the reason — matched, deduplicated, ignored, or
quarantined) or STALLS (nothing is recorded, the event is not acked, and it
re-delivers on the next pass). The distinction is a TYPE, not a flag:
`evaluate` returns `EvaluationResult | EvaluationStalled`, so a caller cannot
forget to check and ack an event whose policies never ran. That is spec §6's
"never silently match-all or match-none", made unmissable:

- Resolution says `not_found` — the tenant has no policy artifact. This is an
  ANSWER, not a failure: the event audits as `ignored` and is consumed (§14).
- Resolution says `unavailable` / `malformed_response` — we do not KNOW the
  tenant's rules. Stall. Rounding this down to "no policies" would silently
  stop minting the moment governance hiccuped, and nothing would say so.
- The current policy version is QUARANTINED — we know the rules exist and we
  cannot read them. Stall, with a distinct reason so the operator surface says
  "fix your artifact" instead of "governance is down". Never fall back to a
  previous version: evaluating a version that is not current, while recording
  it on the ledger as though it were in force, is the same lie by a longer
  route.

**The stall is only visible if someone stops.** `EvaluationHandler` is the
adapter into `intake.run_intake`: it returns for consumed events (the loop
acks) and RAISES `EvaluationStalledError` for stalls (the loop stops the pass
with the position un-acked, records the failure, and returns). The event is
still in the source; the next pass re-delivers it; when the provider recovers
the same event evaluates normally. That is the whole recovery story, and it
works because it composes with the ledger's unique key: re-delivery is safe
precisely because a delivery that already converged reports `deduplicated`
rather than doing the work twice (spec §4, "recoverably convergent").

**Cross-tenant deliveries are consumed, not stalled.** An envelope whose tenant
is not the one being evaluated will never match legitimately no matter how many
times it comes back, so re-delivering it forever would wedge the stream on an
event that can never make progress. It gets a `quarantined` ledger row naming
the foreign tenant and the pass moves on — §14's "rejected with quarantine, not
silently dropped". The row is filed under the tenant whose pass rejected it, not
under the tenant the envelope claimed: writing rows under a foreign tenant's
name would be the cross-tenant attribution §3 forbids, and the operator who has
to act is the one who owns this stream.
"""

from __future__ import annotations

import enum
import string
from dataclasses import dataclass, field

from snowline_marketing.events import EventEnvelope
from snowline_marketing.ledger import (
    DeliveryLedger,
    DeliveryOutcome,
    LedgerRecord,
)
from snowline_marketing.matching import matching_entries
from snowline_marketing.policies import (
    ConsequenceType,
    MalformedPolicySet,
    PolicyDestination,
    PolicyEntry,
    PolicyMode,
    PolicySet,
)
from snowline_marketing.policy_cache import PolicyCache
from snowline_marketing.policy_source import (
    PolicyProvider,
    PolicyResolutionError,
    ResolutionFailure,
)

# The dedup keys for the two EVENT-LEVEL outcomes — deliveries that belong to
# an event rather than to a rule, and so cannot render a policy's template.
#
# They live in the same column and under the same unique key as policy keys, on
# purpose: one uniqueness mechanism covers every outcome, so a re-delivered
# unmatched event converges to its single audit row exactly the way a matched
# one does, instead of accumulating a row per delivery. The shapes are namespaced
# by a trailing literal that the default template cannot produce. A COLLISION
# with a policy key is possible only for a tenant who authored a template
# rendering the identical string — which for the default shape would need a
# policy whose id equals the event id and an event id literally "ignored" — and
# even then the consequence is a deduplicated delivery, never a cross-tenant
# read: `tenant` is a column of the key, not a rendered substring.
IGNORED_DEDUP_KEY_TEMPLATE = "{tenant}:{event_id}:ignored"
QUARANTINED_DEDUP_KEY_TEMPLATE = "{tenant}:{event_id}:quarantined"


class StallReason(enum.StrEnum):
    """Why evaluation refused to proceed — the operator-facing distinction
    between "the plugin cannot reach the rules" and "the rules are broken".

    ONE result type carries both (see `EvaluationStalled`), because the control
    flow is identical and must never diverge: both stall, both leave the event
    un-acked, both re-deliver. What differs is only the sentence the operator
    reads and the thing they go fix, which is what an enum is for. Contrast
    `policies.MalformedPolicySet` vs `PolicySet`, which ARE separate types
    precisely because a caller must not be able to use one where the other
    belongs."""

    # Governance could not be reached, or answered with something that is not a
    # policy artifact version. We learned nothing; try again next pass.
    policy_unavailable = "policy_unavailable"
    # The tenant's CURRENT policy version does not parse (spec §6). Retrying
    # will not help — an operator has to revise the artifact — but the event
    # still must not be consumed: policies that were supposed to fire have not,
    # and acking would lose that silently.
    policy_quarantined = "policy_quarantined"


@dataclass(frozen=True)
class EvaluationStalled:
    """Evaluation did not happen and the event must NOT be acked.

    Returned, not raised, so the engine keeps the same returns-a-result shape as
    every other seam in this codebase; `EvaluationHandler` is the one place that
    converts it into the exception `intake.run_intake` understands as "stop the
    pass"."""

    tenant: str
    reason: StallReason
    detail: str
    # The quarantined version's id, when there was one — what the operator
    # quotes when revising the artifact. None on the unavailable path, where we
    # never learned which version is current.
    version_id: str | None = None


@dataclass(frozen=True)
class NoPolicySet:
    """The tenant has no policy artifact — an evaluable ANSWER (spec §14: the
    event audits as `ignored` and creates no work).

    A distinct type from `EvaluatedPolicySet` rather than an empty `PolicySet`,
    because the two are different facts: an empty set is an artifact an operator
    wrote and reviewed that declares no rules yet (so the ledger names its
    version id), while this is the absence of an artifact (so there is no
    version to name). Collapsing them would put a NULL version on rows that
    should carry one, or invent one for rows that cannot."""

    tenant: str
    detail: str


@dataclass(frozen=True)
class EvaluatedPolicySet:
    """The tenant's current policy version, parsed and ready to evaluate.

    `version_id` is carried beside the set rather than derived from it: the body
    does not name its own governance version, and this id is the one the ledger
    is contractually required to record (spec §6)."""

    tenant: str
    version_id: str
    policy_set: PolicySet


# What resolution produced for one tenant: a set to evaluate, a definitive
# "no policies", or a stall.
PolicyResolution = EvaluatedPolicySet | NoPolicySet | EvaluationStalled


@dataclass(frozen=True)
class PendingConsequence:
    """Work a match is owed, described but not done (spec §7's input).

    Carries the whole `PolicyEntry` and the whole `EventEnvelope` rather than a
    flattened projection of "the fields minting needs". Two reasons, both
    learned from the schema: §7 renders TEMPLATES, so it needs the envelope's
    fields — including `payload.details`, whose keys this module cannot
    enumerate — and provenance (§7: "originating event + entity, matched policy
    + version, source scope/initiative/milestone, external refs, affected
    artifacts/channels") is a projection of exactly these two objects. A
    hand-copied subset would have to grow a field every time either schema
    does, and the day it fell behind, minted items would quietly lose
    provenance. Both objects are frozen, so passing them whole costs nothing
    and cannot be edited in flight.

    `dedup_key` is the ledger row this consequence belongs to: the minting layer
    writes the created item's ref back to `(tenant, dedup_key)`, turning
    `matched` into `created` (§4). It is the handle, and it is already claimed —
    a consequence exists only when THIS delivery won the insert."""

    tenant: str
    envelope: EventEnvelope
    entry: PolicyEntry
    # The governance artifact version this match was decided by — recorded on
    # the ledger row and repeated here so a consequence handed to §7 is
    # self-contained provenance.
    policy_version_id: str
    dedup_key: str

    @property
    def policy_id(self) -> str:
        return self.entry.policy_id

    @property
    def consequence(self) -> ConsequenceType:
        return self.entry.consequence

    @property
    def destination(self) -> PolicyDestination:
        return self.entry.destination

    @property
    def mode(self) -> PolicyMode:
        return self.entry.mode

    @property
    def mints(self) -> bool:
        """Whether this consequence may produce anything at all.

        FALSE for `dry_run` and only for `dry_run` — that is the mode whose
        entire meaning is "evaluate and report, mint nothing" (§11), so the
        prohibition is absolute and belongs here as a flag the minting layer
        cannot overlook. `approval_required` is NOT a negation but a GATE: the
        match is real, the work is owed, and an explicit operator verb releases
        it (§12's approval surface). Flattening the two into one boolean would
        make an approval-gated policy indistinguishable from a disarmed one, so
        `mode` is carried verbatim and §7 reads it for the gating."""
        return self.entry.mode is not PolicyMode.dry_run


@dataclass(frozen=True)
class Delivery:
    """One event × one policy (or one event, for the event-level outcomes).

    Two outcomes live here and they are not the same question. `outcome` is what
    THIS delivery decided — `matched` when it claimed the key, `deduplicated`
    when it found the key already taken, `ignored`/`quarantined` for the
    event-level rows on their first delivery. `record.outcome` is what the ROW
    says, which on a repeat delivery is whatever the earlier one left there —
    possibly `created`, with a PM item ref beside it. They coincide on a fresh
    delivery and diverge on every repeat, and both are worth having: the first
    answers "what did this pass do?", the second answers "what is the state of
    this work?" (spec §4: re-delivery returns the existing result).

    `consequence` is non-None exactly when `outcome is matched`. A repeat
    delivery produces none — that is the dedup, and it is why running the intake
    loop twice over the same capture mints once."""

    outcome: DeliveryOutcome
    record: LedgerRecord
    consequence: PendingConsequence | None = None


@dataclass(frozen=True)
class EvaluationResult:
    """One event, fully evaluated and CONSUMED — the intake loop may ack.

    Always carries at least one delivery: an event that matched nothing still
    produces its `ignored` row, because "nothing happened" has to be a recorded
    decision rather than an absence a reader has to trust (§14)."""

    tenant: str
    envelope: EventEnvelope
    # The governance version evaluated, or None where no policy set applied (the
    # tenant has no artifact, or the envelope was refused before any version was
    # in play) — the same nullability the ledger column carries, for the same
    # reason.
    policy_version_id: str | None
    deliveries: tuple[Delivery, ...] = field(default_factory=tuple)

    @property
    def consequences(self) -> tuple[PendingConsequence, ...]:
        """Everything this event newly owes, in policy declaration order — the
        minting layer's (§7) input, and what §11's dry-run reports."""
        return tuple(
            delivery.consequence
            for delivery in self.deliveries
            if delivery.consequence is not None
        )

    @property
    def outcomes(self) -> tuple[DeliveryOutcome, ...]:
        return tuple(delivery.outcome for delivery in self.deliveries)


# What one event's evaluation produced: a consumed result, or a stall that must
# leave the event un-acked. A union rather than a result with a `stalled` field,
# so a caller cannot ack by forgetting to look.
EvaluationOutcome = EvaluationResult | EvaluationStalled


class EvaluationStalledError(RuntimeError):
    """Raised by `EvaluationHandler` to stop an intake pass on a stall.

    The engine RETURNS stalls; this exception exists solely at the intake seam,
    because `run_intake`'s contract is that a handler which raises leaves the
    position un-acked and stops the pass (`intake.py`). It carries the typed
    stall so a driver above can tell "governance is down, back off" from "the
    artifact is broken, tell the operator" without parsing a message."""

    def __init__(self, stall: EvaluationStalled) -> None:
        super().__init__(
            f"evaluation stalled for tenant {stall.tenant!r} "
            f"({stall.reason.value}): {stall.detail}"
        )
        self.stall = stall


class DedupKeyUnrenderable(RuntimeError):
    """A matched entry's dedup template could not be rendered from the envelope.

    Unreachable by construction: `policies._validate_dedup_template` dry-renders
    every template at parse time and rejects any that references a field the
    entry's event types do not GUARANTEE, and `events.py` enforces those
    guarantees on every envelope. It exists because the failure it guards
    against is the worst one in the system — a template that renders a constant
    key (say, `None`) silently swallows every future delivery of that policy as
    a duplicate, and the work is never done and never reported missing. If the
    two validators ever disagree, the right answer is a loud stall on one event,
    not quiet loss on all of them."""


def resolve_policy_set(
    tenant: str,
    *,
    provider: PolicyProvider,
    cache: PolicyCache | None = None,
) -> PolicyResolution:
    """Resolve, cache and classify `tenant`'s current policy version.

    The one place the three seams meet: `provider.resolve` says which version is
    current (never raising — it returns typed failures), `cache.put` persists the
    body and OWNS the classification (so the row's verdict cannot disagree with
    the bytes it stored), and this function maps the result onto the three
    things evaluation can do about it.

    `cache.put` writes to the database and therefore may raise if the database is
    down. Deliberately not caught: a policy version we could not record is a
    version the ledger's `policy_version_id` would reference into a row that
    does not exist, and the audit trail's whole value is that it joins. The
    intake loop turns the exception into a stopped pass with the event un-acked,
    which is the same visible stall the typed path produces."""
    cache = cache if cache is not None else PolicyCache()
    resolved = provider.resolve(tenant)
    if isinstance(resolved, PolicyResolutionError):
        if resolved.failure is ResolutionFailure.not_found:
            # An ANSWER, not an absence of one (see policy_source.py): this
            # tenant has no policy artifact, so every event audits as ignored.
            return NoPolicySet(tenant=tenant, detail=resolved.detail)
        return EvaluationStalled(
            tenant=tenant,
            reason=StallReason.policy_unavailable,
            detail=f"{resolved.failure.value}: {resolved.detail}",
        )
    parsed = cache.put(resolved)
    if isinstance(parsed, MalformedPolicySet):
        return EvaluationStalled(
            tenant=tenant,
            reason=StallReason.policy_quarantined,
            detail=f"{parsed.reason.value}: {parsed.detail}",
            version_id=resolved.version_id,
        )
    return EvaluatedPolicySet(
        tenant=tenant, version_id=resolved.version_id, policy_set=parsed
    )


def render_dedup_key(entry: PolicyEntry, envelope: EventEnvelope) -> str:
    """Render `entry`'s dedup-key template against `envelope` (spec §4).

    Every value is a STRING, which is what `policies._validate_dedup_template`
    dry-rendered against at parse time; enum-valued fields are passed as their
    wire values so a key never depends on Python's repr of an enum member.

    A referenced field that is None raises rather than rendering "None" — see
    `DedupKeyUnrenderable` for why that trade is not close."""
    values: dict[str, str | None] = {
        "tenant": envelope.tenant,
        "policy_id": entry.policy_id,
        "event_id": envelope.event_id,
        "event_type": envelope.event_type.value,
        "entity_kind": envelope.subject.kind.value,
        "entity_id": envelope.subject.id,
        "scope": envelope.payload.scope,
        "consequence": entry.consequence.value,
        # The conditional half of the vocabulary: Optional on the payload, and
        # guaranteed present only for the event types that require them —
        # which parse-time validation already confirmed for every type this
        # entry selects.
        "initiative": envelope.payload.initiative,
        "phase": envelope.payload.phase,
        "milestone": envelope.payload.milestone,
    }
    referenced = {
        name
        for _, name, _, _ in string.Formatter().parse(entry.dedup_key_template)
        if name
    }
    missing = sorted(name for name in referenced if values.get(name) is None)
    if missing:
        raise DedupKeyUnrenderable(
            f"policy {entry.policy_id!r}: dedup_key_template "
            f"{entry.dedup_key_template!r} references {', '.join(missing)}, which "
            f"event {envelope.event_id!r} ({envelope.event_type.value}) does not "
            "carry — refusing to render a key that would swallow every later "
            "delivery as a duplicate"
        )
    return entry.dedup_key_template.format(**values)


def evaluate(
    envelope: EventEnvelope,
    resolution: PolicyResolution,
    *,
    ledger: DeliveryLedger | None = None,
) -> EvaluationOutcome:
    """Evaluate one envelope against one tenant's resolution — the core.

    The DECISION is a pure function of the envelope and the resolution: which
    entries match, in what order, and what each is owed. The ledger writes are
    where that decision becomes durable and idempotent — the insert is what
    decides whether a delivery is the first (`matched`, with a consequence) or a
    repeat (`deduplicated`, with none), so "have we already done this?" is
    answered by the database rather than by anything held in a process.

    `resolution` is passed in rather than resolved here so that one pass
    resolves once (see `EvaluationHandler`) and so that §11's dry-run can point
    the same code at a version that is NOT the tenant's current one."""
    if isinstance(resolution, EvaluationStalled):
        # Nothing is recorded and nothing is consumed. Passing the stall
        # straight through keeps every caller on one code path: they check the
        # returned type once, wherever the resolution came from.
        return resolution

    ledger = ledger if ledger is not None else DeliveryLedger()
    tenant = resolution.tenant
    version_id = (
        resolution.version_id if isinstance(resolution, EvaluatedPolicySet) else None
    )

    def event_level(
        outcome: DeliveryOutcome, template: str, detail: str
    ) -> EvaluationResult:
        """One row for the whole event — no policy, no consequence."""
        write = ledger.record(
            tenant=tenant,
            dedup_key=template.format(tenant=tenant, event_id=envelope.event_id),
            event_id=envelope.event_id,
            event_type=envelope.event_type.value,
            outcome=outcome,
            policy_version_id=version_id,
            detail=detail,
        )
        return EvaluationResult(
            tenant=tenant,
            envelope=envelope,
            policy_version_id=version_id,
            deliveries=(
                Delivery(
                    # A repeat delivery of an event-level row reports
                    # `deduplicated` for the same reason a repeat match does:
                    # the delivery-level question is "did this produce anything
                    # new?", and the answer is no. The ROW keeps saying
                    # `ignored`/`quarantined`, which is what it is.
                    outcome=outcome if write.inserted else DeliveryOutcome.deduplicated,
                    record=write.record,
                ),
            ),
        )

    if envelope.tenant != tenant:
        # §14's isolation check, and the reason it is a quarantine rather than a
        # non-match: a policy set cannot predicate on tenant (`policies.py`), so
        # an envelope from another org is not an uninteresting event — it is a
        # routing failure that will never match legitimately however often it
        # comes back. Consumed (it can never make progress), recorded, explained.
        # `version_id` is carried when there was one: "which version was in
        # force when we refused" is a real audit fact, and the column is
        # nullable for the case where no set existed at all.
        return event_level(
            DeliveryOutcome.quarantined,
            QUARANTINED_DEDUP_KEY_TEMPLATE,
            f"cross-tenant delivery: envelope declares tenant "
            f"{envelope.tenant!r}, evaluated for tenant {tenant!r} — rejected "
            "with quarantine, never routed across the isolation boundary",
        )

    if isinstance(resolution, NoPolicySet):
        return event_level(
            DeliveryOutcome.ignored, IGNORED_DEDUP_KEY_TEMPLATE, resolution.detail
        )

    if resolution.policy_set.tenant != envelope.tenant:
        # Defence in depth. `PolicyCache.put` cross-checks the body's declared
        # tenant against the tenant it was resolved FOR, so a set reaching here
        # under the wrong name should be impossible — but this is the isolation
        # boundary, and a boundary held in exactly one place is a boundary one
        # refactor away from not being held at all.
        return event_level(
            DeliveryOutcome.quarantined,
            QUARANTINED_DEDUP_KEY_TEMPLATE,
            f"policy version {resolution.version_id!r} declares tenant "
            f"{resolution.policy_set.tenant!r} but was evaluated for "
            f"{envelope.tenant!r} — refused",
        )

    entries = matching_entries(resolution.policy_set, envelope)
    if not entries:
        return event_level(
            DeliveryOutcome.ignored,
            IGNORED_DEDUP_KEY_TEMPLATE,
            f"no entry of policy version {resolution.version_id!r} selects "
            f"event type {envelope.event_type.value!r} with this payload",
        )

    deliveries: list[Delivery] = []
    for entry in entries:
        dedup_key = render_dedup_key(entry, envelope)
        write = ledger.record(
            tenant=tenant,
            dedup_key=dedup_key,
            policy_id=entry.policy_id,
            event_id=envelope.event_id,
            event_type=envelope.event_type.value,
            outcome=DeliveryOutcome.matched,
            policy_version_id=resolution.version_id,
            # The MODE is recorded on the row, not only on the consequence: an
            # auditor reading the ledger months later must be able to see why a
            # dry-run match never became a created item without re-resolving a
            # policy version that may since have been revised.
            detail=f"matched in mode {entry.mode.value!r}",
        )
        if not write.inserted:
            # The key was already claimed — by an earlier pass, by a re-delivery
            # after a crash, or by a racing process. No consequence: the earlier
            # delivery owns the work, and its row (possibly already `created`,
            # with an item ref) is the result this delivery returns. Spec §4.
            deliveries.append(
                Delivery(outcome=DeliveryOutcome.deduplicated, record=write.record)
            )
            continue
        deliveries.append(
            Delivery(
                outcome=DeliveryOutcome.matched,
                record=write.record,
                consequence=PendingConsequence(
                    tenant=tenant,
                    envelope=envelope,
                    entry=entry,
                    policy_version_id=resolution.version_id,
                    dedup_key=dedup_key,
                ),
            )
        )
    return EvaluationResult(
        tenant=tenant,
        envelope=envelope,
        policy_version_id=resolution.version_id,
        deliveries=tuple(deliveries),
    )


class EvaluationHandler:
    """The engine as an `intake.EventHandler` — one instance per intake pass.

    Plugs `evaluate` into `run_intake`: returns for a consumed event (the loop
    acks), raises `EvaluationStalledError` for a stall (the loop stops the pass,
    leaves the position un-acked, and records the failure). Together with the
    ledger's unique key that is the recoverable convergence spec §4 asks for —
    the event re-delivers, the deliveries that already converged report
    `deduplicated`, and the ones that never ran finally run.

    **One resolution per pass, resolved lazily.** The policy version is fetched
    on the FIRST event and reused for the rest: a resolution per event would be
    an HTTP round trip per event against governance once `GatewayPolicyProvider`
    is live, and it would let a mid-pass revision split one pass's ledger rows
    across two policy versions — an audit trail that cannot be read as "this
    version decided these events". Lazily, because a pass with nothing to
    consume must not call governance at all. The memo is per INSTANCE, which is
    what makes "per pass" true: build a new handler for each `run_intake` call
    (which is also what re-resolves after a stall — a stalled pass's handler is
    discarded, so the next pass asks governance again).

    The handler keeps every result, so a driver can report what a pass did and
    §11's dry-run can list what would have been minted without touching PM."""

    def __init__(
        self,
        tenant: str,
        *,
        provider: PolicyProvider,
        cache: PolicyCache | None = None,
        ledger: DeliveryLedger | None = None,
    ) -> None:
        self.tenant = tenant
        self._provider = provider
        self._cache = cache if cache is not None else PolicyCache()
        self._ledger = ledger if ledger is not None else DeliveryLedger()
        self._resolution: PolicyResolution | None = None
        self.results: list[EvaluationResult] = []
        # The stall this pass ended on, if any — the same object the exception
        # carries, kept so a driver that caught the pass's `IntakeResult`
        # (rather than the exception) can still say why.
        self.stall: EvaluationStalled | None = None

    @property
    def resolution(self) -> PolicyResolution:
        """This pass's policy resolution, resolved on first use (see the class
        docstring on why it is memoized and why that memo is per instance)."""
        if self._resolution is None:
            self._resolution = resolve_policy_set(
                self.tenant, provider=self._provider, cache=self._cache
            )
        return self._resolution

    def __call__(self, envelope: EventEnvelope) -> None:
        outcome = evaluate(envelope, self.resolution, ledger=self._ledger)
        if isinstance(outcome, EvaluationStalled):
            self.stall = outcome
            raise EvaluationStalledError(outcome)
        self.results.append(outcome)

    @property
    def consequences(self) -> tuple[PendingConsequence, ...]:
        """Everything this pass newly owes, in delivery order — what the minting
        layer (§7) consumes and what §11's dry-run reports."""
        return tuple(
            consequence
            for result in self.results
            for consequence in result.consequences
        )

    @property
    def deliveries(self) -> tuple[Delivery, ...]:
        return tuple(
            delivery for result in self.results for delivery in result.deliveries
        )

"""The provenance watch — completions in, deliverable rows or quarantine out
(spec §8).

Spec §8, settled 2026-07-18: "completing a marketing-minted item is watched via
the same PM event stream; a completion carrying a provenance payload (channel,
deliverable class, source artifact versions, URL) upserts the deliverable
ledger; a completion without one lands in quarantine — visible, auditable,
resolvable by attaching provenance after the fact. **No hard completion gate, no
friction on the PM verb itself.**"

This module is that watch. It decides; `deliverables.py` and `quarantine.py`
store; `provenance.py` says what a declaration looks like. Nothing here calls
PM — by the time an `item_completed` event arrives the verb has already
happened, and the watch's entire job is to RECORD what it produced.

**How a marketing-minted completion is recognized: the delivery ledger's
`created_item_ref`.** That column is written at mint time by the one transition
allowed to write it (`ledger.confirm_created`, §7), so it is the authoritative
statement that THIS plugin created THAT item. A completion whose subject id
matches a `created` row is a marketing-minted completion; one that matches
nothing is ordinary roadmap work and is passed through SILENTLY — no row, no
log, no quarantine. That silence is a requirement, not an economy: the marketing
plugin consumes the whole PM lifecycle stream (§5), so most completions it sees
belong to other people's work, and a plugin that filed observations about them
would be inventing a second backlog (§1) out of other teams' items.

The alternatives were considered and are worse. A naming convention on the item
would be §3's forbidden "infers the roadmap from GitHub naming conventions" in
another costume. A marker inside the item BODY (the §7 provenance block) is
producer-editable text that PM does not carry on the event at all. A PM lookup
per completion would make the watch depend on PM being reachable to decide
whether an event is its business — a read the plugin does not need, since it
already recorded the answer when it minted.

**A completion NEVER fails because of the watch.** Spec §8's "no hard completion
gate" is upheld structurally: nothing here can call a PM verb, and the only
things it writes are two local tables. What it does NOT do is drop the
observation when a store is down — a watch store failure RAISES, which
`intake.run_intake` turns into a stopped pass with the event un-acked, so the
completion re-delivers and is observed later. Same posture as
`engine.EvaluationStalledError` and `minting.MintingUnavailableError`, and the
same reason: an acked event is the loop's statement that the event is DONE, and
an observation that was never recorded is not done.

**Watch BEFORE mint, both before the ack.** `ProvenanceWatchHandler` extends
`minting.MintingEvaluationHandler` and runs the watch first, then the parent's
evaluate-and-mint. Order matters in exactly one direction: the mint half can
stop the pass when PM is unavailable (`MintingUnavailableError`), and a
completion that already happened should not have its provenance recorded late
because an unrelated mint could not reach PM. The reverse ordering has no
compensating benefit — the watch's join reads `created` rows written by EARLIER
events, never by the event in hand (an item cannot complete in the same event
that minted it), so nothing the mint does this pass changes what the watch would
see.

Everything is convergent under at-least-once re-delivery, which is what makes
the crash window harmless: the deliverable ledger upserts (a re-declared
deliverable lands on the same row), and the quarantine files first-writer-wins
(a re-delivered provenance-less completion converges to ONE open row, and cannot
reopen a decision an operator already made).
"""

from __future__ import annotations

import enum
import logging
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from snowline_marketing.cursors import CursorStore
from snowline_marketing.deliverables import (
    DeliverableProvenanceLedger,
    DeliverableRecord,
    DeliverableStore,
    SourceVersion,
)
from snowline_marketing.engine import EvaluationStalled, PolicyResolution
from snowline_marketing.events import EventEnvelope, EventType
from snowline_marketing.intake import IntakeResult, MalformedHandler
from snowline_marketing.ledger import (
    DeliveryLedger,
    LedgerRecord,
    MintingLedger,
)
from snowline_marketing.minting import (
    IntakeMintResult,
    MintingEvaluationHandler,
    MintOutcome,
    MintPassReport,
    drive_intake,
)
from snowline_marketing.policy_cache import PolicyCacheStore
from snowline_marketing.policy_source import PolicyProvider
from snowline_marketing.provenance import (
    DeclaredDeliverable,
    DeliverableProvenance,
    MissingProvenance,
    parse_provenance,
)
from snowline_marketing.quarantine import (
    CompletionQuarantine,
    QuarantineReason,
    QuarantineRecord,
    QuarantineStore,
    QuarantineTransition,
)
from snowline_marketing.sources import EventSource
from snowline_marketing.work_sink import WorkItemSink

log = logging.getLogger("snowline_marketing.watch")


class MintedItemLookup(Protocol):
    """What the watch needs to answer "did this plugin mint that item?" — the
    delivery ledger's `created_item_ref` join, and nothing else it ever calls.

    A narrow protocol for `ledger.LedgerStore`'s reason: `watch_completion` must
    be unable to write a delivery-ledger row, and taking the narrowest surface is
    how that is stated rather than promised."""

    def created_for_item(self, tenant: str, item_ref: str) -> list[LedgerRecord]: ...


class WatchLedger(MintingLedger, MintedItemLookup, Protocol):
    """What `ProvenanceWatchHandler` needs from a delivery ledger: the minting
    surface its parent already requires, plus the watch's join.

    Declared as the union rather than as a second parameter because there is only
    ever ONE ledger — the row the engine writes is the row the mint transitions
    and the row the watch joins against, and two stores would mean two ledgers.
    Both `DeliveryLedger` and `InMemoryDeliveryLedger` satisfy it."""


class WatchDisposition(enum.StrEnum):
    """What the watch did with one event.

    Distinct from anything the stores say, for `minting.MintDisposition`'s
    reason: this is the PASS's report, and several dispositions leave no row at
    all — which is precisely the fact a driver wants to see counted."""

    # A marketing-minted completion declared its deliverables and the ledger
    # holds them (spec §8's upsert path).
    recorded = "recorded"
    # A marketing-minted completion declared nothing — spec §8's "a completion
    # without one". Filed, open, resolvable.
    quarantined_missing = "quarantined_missing"
    # A declaration was attempted and could not be read. Filed with a detail
    # naming the defect — a producer's bug, not an operator's omission.
    quarantined_malformed = "quarantined_malformed"
    # The completed item was never minted by this plugin: ordinary roadmap
    # work, none of the watch's business. Nothing written, nothing logged.
    not_marketing_minted = "not_marketing_minted"
    # Not an `item_completed` event at all. Every other event type flows through
    # the handler untouched.
    not_a_completion = "not_a_completion"
    # The envelope belongs to another tenant. The engine quarantines that
    # DELIVERY (§14); the watch writes nothing under a tenant it does not own.
    foreign_tenant = "foreign_tenant"


@dataclass(frozen=True)
class WatchOutcome:
    """One event, watched (or deliberately not).

    Carries the whole envelope for `minting.MintOutcome`'s reason: every caller
    that reports on a pass wants a different projection of it, and a hand-copied
    subset falls behind the day the schema grows.

    `minted_by` is the `created` delivery-ledger row that made this completion
    the watch's business — non-None exactly for the dispositions that acted, so
    an operator reading a quarantine row can get from it to the policy that
    minted the item in the first place."""

    envelope: EventEnvelope
    disposition: WatchDisposition
    deliverables: tuple[DeliverableRecord, ...] = ()
    quarantine: QuarantineRecord | None = None
    minted_by: LedgerRecord | None = None
    detail: str | None = None

    @property
    def tenant(self) -> str:
        return self.envelope.tenant

    @property
    def event_id(self) -> str:
        return self.envelope.event_id

    @property
    def item_ref(self) -> str:
        """The completed item's ref — the envelope's subject id, which for an
        `item_completed` event is a work item by validation (`events.py`).

        Meaningful only for the dispositions that looked at an item; on a
        `not_a_completion` outcome the subject may be a milestone or a
        schedule, and reading this would be asking a question the outcome did
        not answer."""
        return self.envelope.subject.id

    @property
    def needs_operator(self) -> bool:
        """Whether this outcome is one §11's quarantine surface exists for. True
        for both quarantine dispositions and nothing else: a pass-through is not
        a problem and a recorded deliverable is the system working."""
        return self.disposition in (
            WatchDisposition.quarantined_missing,
            WatchDisposition.quarantined_malformed,
        )


@dataclass(frozen=True)
class WatchPassReport:
    """What one watch pass observed, in event order."""

    outcomes: tuple[WatchOutcome, ...]

    @property
    def counts(self) -> Mapping[WatchDisposition, int]:
        """Totals, DERIVED from `outcomes` — stored counts could disagree with
        the lines they summarize; a property cannot (same reasoning as
        `minting.MintPassReport.counts`)."""
        return Counter(outcome.disposition for outcome in self.outcomes)

    @property
    def recorded(self) -> tuple[WatchOutcome, ...]:
        """The completions whose deliverables this pass wrote."""
        return tuple(
            outcome
            for outcome in self.outcomes
            if outcome.disposition is WatchDisposition.recorded
        )

    @property
    def quarantined(self) -> tuple[WatchOutcome, ...]:
        """Everything a human has to resolve — §11's quarantine input, surfaced
        by the pass that produced it rather than left to be discovered by a
        query."""
        return tuple(outcome for outcome in self.outcomes if outcome.needs_operator)


def _source_versions(deliverable: DeclaredDeliverable) -> tuple[SourceVersion, ...]:
    """The declared versions as the STORE's record type.

    A conversion rather than one shared type: `provenance.SourceArtifactVersion`
    is a wire declaration this plugin validates and `deliverables.SourceVersion`
    is a row it owns, and the seam between them is what keeps the store free of
    any opinion about the payload schema (`ledger.py` is free of `policies.py`
    for the same reason)."""
    return tuple(
        SourceVersion(
            artifact_id=version.artifact_id,
            version_id=version.version_id,
            milestone=version.milestone,
        )
        for version in deliverable.source_artifact_versions
    )


def _record_deliverables(
    *,
    tenant: str,
    item_ref: str,
    event_id: str,
    produced_at: datetime,
    provenance: DeliverableProvenance,
    deliverables: DeliverableStore,
) -> tuple[DeliverableRecord, ...]:
    """Upsert every declared deliverable, in declaration order.

    Shared by the watch and by `resolve_quarantined` so that provenance attached
    AFTER the fact lands exactly as provenance carried ON the completion would
    have — same rows, same key, same convergence. A resolve that wrote a
    different shape than the watch would make the ledger's contents depend on how
    late the operator was."""
    return tuple(
        deliverables.upsert(
            tenant=tenant,
            item_ref=item_ref,
            channel=declared.channel,
            deliverable_class=declared.deliverable_class,
            source_versions=_source_versions(declared),
            produced_at=produced_at,
            event_id=event_id,
            external_url=declared.external_url,
        )
        for declared in provenance.deliverables
    )


# What the watch writes into a quarantine row's `resolution_detail` when a
# re-delivery of the SAME completion arrives carrying provenance the first
# delivery did not. A constant because it is the operator's explanation for a
# row that closed itself, and because a test should assert on the state rather
# than on a phrasing it copied.
_SELF_RESOLVED_DETAIL = (
    "a later delivery of this completion carried a readable provenance "
    "declaration — the deliverable ledger now holds it, so the row closed "
    "itself (spec §8)"
)


def watch_completion(
    envelope: EventEnvelope,
    *,
    tenant: str,
    ledger: MintedItemLookup,
    deliverables: DeliverableStore,
    quarantine: QuarantineStore,
) -> WatchOutcome:
    """Watch one event (spec §8).

    Four gates, in this order, each cheaper than the next and each a documented
    pass-through: not a completion, not this tenant's, not minted by this plugin,
    then — and only then — read the provenance declaration and write.

    `tenant` is passed rather than read off the envelope: the watch belongs to
    ONE tenant's pass (the handler holds it), and joining against the envelope's
    own claim would let a misrouted event decide which org's ledger it lands in.
    That is the §3/§14 boundary, and it is held here as well as at the engine
    because a boundary held in exactly one place is one refactor away from not
    being held at all.

    Raises nothing of its own; a store that cannot be written raises through, and
    the intake loop turns that into a stopped pass with the event un-acked (see
    the module docstring)."""
    if envelope.event_type is not EventType.item_completed:
        # Every other event type flows through untouched. Deliverables are
        # produced by completed WORK; a reopened or re-scoped item has produced
        # nothing new to record.
        return WatchOutcome(envelope, WatchDisposition.not_a_completion)
    if envelope.tenant != tenant:
        return WatchOutcome(
            envelope,
            WatchDisposition.foreign_tenant,
            detail=(
                f"envelope declares tenant {envelope.tenant!r}, watched for "
                f"{tenant!r} — nothing recorded; the delivery itself is "
                "quarantined by the engine (spec §14)"
            ),
        )
    item_ref = envelope.subject.id
    minted = ledger.created_for_item(tenant, item_ref)
    if not minted:
        # Ordinary roadmap work completing. SILENTLY — see the module docstring
        # on why filing anything here would be inventing a second backlog out of
        # other teams' items.
        return WatchOutcome(envelope, WatchDisposition.not_marketing_minted)

    origin = minted[0]
    parsed = parse_provenance(envelope)
    if isinstance(parsed, MissingProvenance):
        reason = (
            QuarantineReason.provenance_missing
            if parsed.is_absent
            else QuarantineReason.provenance_malformed
        )
        detail = (
            f"{parsed.detail} — completion of marketing-minted item "
            f"{item_ref!r} (delivery {origin.dedup_key!r}, policy "
            f"{origin.policy_id!r})"
        )
        write = quarantine.record(
            tenant=tenant,
            event_id=envelope.event_id,
            item_ref=item_ref,
            reason=reason,
            detail=detail,
            # The completion, whole: `intake.run_intake` only ever hands a
            # handler a VALID envelope (malformed ones never reach one), so the
            # parsed event IS the whole event, and the resolve verb reads it.
            raw_event=envelope.model_dump_json(),
            occurred_at=envelope.occurred_at,
        )
        if write.inserted:
            # Once per observation, not once per delivery: `inserted` is the
            # entitlement to treat this as news (`quarantine.QuarantineWrite`),
            # so a re-delivered completion does not re-warn about a row an
            # operator may already be working.
            log.warning(
                "completion of marketing-minted item %r (tenant %r, event %r) "
                "recorded no deliverable provenance — quarantined as %s",
                item_ref,
                tenant,
                envelope.event_id,
                reason.value,
            )
        return WatchOutcome(
            envelope,
            (
                WatchDisposition.quarantined_missing
                if parsed.is_absent
                else WatchDisposition.quarantined_malformed
            ),
            quarantine=write.record,
            minted_by=origin,
            detail=detail,
        )

    records = _record_deliverables(
        tenant=tenant,
        item_ref=item_ref,
        event_id=envelope.event_id,
        produced_at=envelope.occurred_at,
        provenance=parsed,
        deliverables=deliverables,
    )
    # A completion whose EARLIER delivery declared nothing (or something broken)
    # left an open row; this delivery carries provenance and the deliverables are
    # already written, so the row has nothing left to ask for. Attempted
    # unconditionally, because the GUARD is the check: a row that does not exist
    # (the ordinary case) or that an operator already closed refuses the
    # transition and is left exactly as it was, so there is nothing to read
    # first and no window between looking and closing. The deliverables are
    # written FIRST — the same durable-fact-before-closure discipline
    # `resolve_quarantined` follows.
    quarantine.resolve(tenant, envelope.event_id, detail=_SELF_RESOLVED_DETAIL)
    return WatchOutcome(
        envelope,
        WatchDisposition.recorded,
        deliverables=records,
        minted_by=origin,
    )


@dataclass(frozen=True)
class ResolutionOutcome:
    """The result of attaching provenance to a quarantined completion.

    `applied` is the quarantine transition's — True exactly when THIS call
    closed the row. `deliverables` is what was written before it closed, and it
    is empty on every refusal: a closed row must never be a channel for writing
    deliverable rows nobody reviewed."""

    record: QuarantineRecord | None
    applied: bool
    deliverables: tuple[DeliverableRecord, ...] = ()
    detail: str | None = None


def resolve_quarantined(
    tenant: str,
    event_id: str,
    *,
    provenance: DeliverableProvenance,
    deliverables: DeliverableStore,
    quarantine: QuarantineStore,
    note: str | None = None,
) -> ResolutionOutcome:
    """Spec §8's "resolvable by attaching provenance after the fact".

    The operator verb, composed of two durable steps in a deliberate order:
    the deliverable rows are upserted FIRST and the quarantine row is closed
    SECOND. A crash in between leaves an open row beside a recorded deliverable —
    re-running converges (the upsert lands on the same rows, the close applies
    once) — whereas closing first would lose the deliverable behind a row that
    says it was recorded. Same discipline as minting's claim-before-call.

    Refuses without writing anything when the row does not exist or is already
    closed. `provenance` is a PARSED declaration, so an operator's attachment
    goes through exactly the validation a producer's would
    (`provenance.parse_provenance`); this function cannot be handed a shape the
    watch would have quarantined.

    The row's OWN facts supply everything but the declaration: the item ref it
    was filed against and the completion's `occurred_at` as `produced_at`, so a
    deliverable recorded late is stamped with when the work actually completed
    rather than when someone got round to filing it."""
    row = quarantine.get(tenant, event_id)
    if row is None:
        return ResolutionOutcome(
            record=None,
            applied=False,
            detail=(
                f"no quarantined completion {event_id!r} for tenant {tenant!r} — "
                "nothing written"
            ),
        )
    if not row.is_open:
        return ResolutionOutcome(
            record=row,
            applied=False,
            detail=(
                f"quarantined completion {event_id!r} is already "
                f"{row.status.value} — nothing written"
            ),
        )
    written = _record_deliverables(
        tenant=tenant,
        item_ref=row.item_ref,
        event_id=row.event_id,
        produced_at=row.occurred_at,
        provenance=provenance,
        deliverables=deliverables,
    )
    detail = "provenance attached after the fact: " + ", ".join(
        f"{record.channel}/{record.deliverable_class}" for record in written
    )
    if note:
        detail = f"{detail} — {note}"
    transition: QuarantineTransition = quarantine.resolve(
        tenant, event_id, detail=detail
    )
    return ResolutionOutcome(
        record=transition.record,
        applied=transition.applied,
        deliverables=written,
        detail=detail,
    )


def dismiss_quarantined(
    tenant: str,
    event_id: str,
    *,
    quarantine: QuarantineStore,
    detail: str,
) -> QuarantineTransition:
    """Spec §4's dismiss verb: an operator's judgment that this completion
    produced no deliverable to record.

    Deliberately a THIN pass-through to the store — there is nothing to compose,
    which is exactly the difference from `resolve_quarantined` and the reason the
    two are separate verbs rather than one `close(status)`. It lives here anyway
    so both operator verbs are found in one place, and so a caller reaching for
    "how do I close this row?" cannot find one of them and miss that the other
    writes rows."""
    return quarantine.dismiss(tenant, event_id, detail=detail)


class ProvenanceWatchHandler(MintingEvaluationHandler):
    """`MintingEvaluationHandler` plus the provenance watch — per event, BEFORE
    the ack. Spec §8 as an `intake.EventHandler`.

    `__call__` watches the event first and then evaluates and mints exactly as
    the parent does (a stall still raises `EvaluationStalledError`, an
    unavailable PM still raises `MintingUnavailableError`). Watching first is
    deliberate: the mint half can stop the pass, and a completion that already
    happened should not have its provenance recorded late because an unrelated
    mint could not reach PM (module docstring). Only when both halves have
    finished does the handler return — and returning is what lets `run_intake`
    ack — so an acked event is one whose deliverables (or quarantine row) are
    durable.

    Takes `WatchLedger`, not the parent's `MintingLedger`: the same store must
    also answer the `created_item_ref` join, because the row the mint wrote is
    the row the watch reads."""

    def __init__(
        self,
        tenant: str,
        *,
        provider: PolicyProvider,
        sink: WorkItemSink,
        ledger: WatchLedger | None = None,
        deliverables: DeliverableStore | None = None,
        quarantine: QuarantineStore | None = None,
        cache: PolicyCacheStore | None = None,
        resolution: PolicyResolution | None = None,
    ) -> None:
        watch_ledger: WatchLedger = ledger if ledger is not None else DeliveryLedger()
        super().__init__(
            tenant,
            provider=provider,
            sink=sink,
            ledger=watch_ledger,
            cache=cache,
            resolution=resolution,
        )
        self._watch_ledger = watch_ledger
        self._deliverables = (
            deliverables if deliverables is not None else DeliverableProvenanceLedger()
        )
        self._quarantine = (
            quarantine if quarantine is not None else CompletionQuarantine()
        )
        self.watch_outcomes: list[WatchOutcome] = []

    def __call__(self, envelope: EventEnvelope) -> None:
        self.watch_outcomes.append(
            watch_completion(
                envelope,
                tenant=self.tenant,
                ledger=self._watch_ledger,
                deliverables=self._deliverables,
                quarantine=self._quarantine,
            )
        )
        super().__call__(envelope)

    @property
    def watch_report(self) -> WatchPassReport:
        """Everything this pass observed, in event order. Includes the event a
        pass stopped on — that event re-delivers un-acked, and its rows converge
        next pass."""
        return WatchPassReport(outcomes=tuple(self.watch_outcomes))


@dataclass(frozen=True)
class IntakeWatchResult:
    """One full drive: an intake pass that watched, evaluated and minted each
    event before its ack.

    `drive` is `run_intake_and_mint`'s own result, kept WHOLE rather than
    flattened — the mint half's story (how far the stream got, whether the
    policies resolved, whether PM was reachable) is unchanged by the watch, and
    re-exporting its fields here would be a second copy to keep in step. The
    delegating properties exist so a driver that only wants "did the pass run?"
    does not have to know which half owns the answer."""

    drive: IntakeMintResult
    watch: WatchPassReport

    @property
    def intake(self) -> IntakeResult:
        return self.drive.intake

    @property
    def stall(self) -> EvaluationStalled | None:
        return self.drive.stall

    @property
    def mint(self) -> MintPassReport:
        return self.drive.mint

    @property
    def unavailable(self) -> MintOutcome | None:
        return self.drive.unavailable

    @property
    def ok(self) -> bool:
        """The whole drive completed and nothing needs a human — the mint half's
        `ok` plus an empty quarantine.

        A quarantined completion is not a FAILURE (spec §8 is explicit that the
        PM verb keeps no gate and nothing went wrong when someone completed work
        without declaring what it produced), but it is something a person has to
        resolve, and this flag's meaning in this codebase is "nothing needs a
        human" (`minting.IntakeMintResult.ok`). A driver that wants "did the pass
        run?" reads `intake.ok`."""
        return self.drive.ok and not self.watch.quarantined


def run_intake_and_watch(
    source: EventSource,
    *,
    tenant: str,
    provider: PolicyProvider,
    sink: WorkItemSink,
    cursor_store: CursorStore,
    ledger: WatchLedger | None = None,
    deliverables: DeliverableStore | None = None,
    quarantine: QuarantineStore | None = None,
    cache: PolicyCacheStore | None = None,
    on_malformed: MalformedHandler | None = None,
    limit: int | None = None,
) -> IntakeWatchResult:
    """Drive one intake pass that watches, evaluates and mints each event BEFORE
    its ack — `minting.run_intake_and_mint` with the §8 watch composed in.

    Deliberately a second entry point rather than a flag on the first: a driver
    that only mints (the §11 dry-run's shape, and any future pass that must not
    write provenance) keeps a callable that provably cannot touch these tables,
    and the composition stays readable as "the same drive, plus one observer".

    ONE ledger serves all three halves — the engine writes the rows, the mint
    transitions them, the watch joins against them — so an in-memory ledger plus
    the in-memory stores drives a whole fixtures run with no database. Like every
    other library callable here it is NOT wired into the app lifespan
    (`MARKETING_ENABLED`, spec §2, gates the live loops)."""
    handler = ProvenanceWatchHandler(
        tenant,
        provider=provider,
        sink=sink,
        ledger=ledger,
        deliverables=deliverables,
        quarantine=quarantine,
        cache=cache,
    )
    # The pass runs through `minting.drive_intake`, the one place "handler in,
    # IntakeMintResult out" is spelled, so this composition cannot drift from
    # `run_intake_and_mint`'s in what it reports or when it stops.
    return IntakeWatchResult(
        drive=drive_intake(
            handler,
            source,
            cursor_store=cursor_store,
            on_malformed=on_malformed,
            limit=limit,
        ),
        watch=handler.watch_report,
    )

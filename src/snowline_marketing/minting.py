"""The minting pass — pending consequences in, PM work items out (spec §7).

The engine decides; this module acts. A `PendingConsequence` is a claim on work
that the ledger already records and nobody has done yet, and there are exactly
three things this layer may do with one, chosen by the matched entry's MODE and
never by anything else:

- `dry_run` — CLOSE the row and mint nothing. The row goes terminal at
  `dry_run` (`ledger.DeliveryOutcome`), which is a new state and a deliberate
  one: left at `matched` a dry-run row re-owes its consequence on every
  re-delivery, forever, so a policy armed to report would look identical to a
  mint that keeps failing. Closed, it says the honest thing — a rule selected
  this event and produced no work on purpose (§11).
- `approval_required` — mark the row `awaiting_approval` and mint nothing. The
  work stays OWED (`ledger.RE_OWED_OUTCOMES`), so it is neither lost nor
  actioned, and the mark is a guarded transition from `matched`, so a
  re-delivery that re-offers the consequence re-declines it without writing
  anything. §12's operator verb — a later item — is what releases it; so does
  revising the entry's mode to `active`, because the re-owed consequence
  carries the CURRENT entry and the claim transition accepts a gated row.
- `active` — mint, under the convergence discipline below.

**The crash window, and the claim that closes it.** Minting is two durable steps
in two different systems: PM creates an item, and the ledger row records its
ref. A crash between them used to be recoverable by re-delivery, because the
engine re-owes a `matched` row with no item ref — but that recovery is exactly
what would RE-MINT an item PM already created. So the pass writes a CLAIM first:

    claim (matched/awaiting_approval -> claimed, committed)
      -> sink.submit
      -> confirm (claimed -> created + item ref, one UPDATE)

The claim is a compare-and-set, so two passes racing one row resolve in the
database and exactly one mints. And because the claim is DURABLE and written
BEFORE the sink call, a crash anywhere after it leaves a `claimed` row, which
the engine refuses to re-own: while the claim is FRESH a re-delivery dedups
quietly (as far as it can tell, the owner is still mid-mint), and once it is
stale — no live pass can still hold it — re-delivery reports a `failed`
DELIVERY naming the row, which then waits on §11's reconciliation
(`ledger.release_stale_claim`). That is the whole design goal stated as a rule
— a silent double-mint and a silent loss are both forbidden, so the ambiguous
case is made LOUD instead of being guessed at.

The sink's four answers map onto that discipline one-to-one:

- `SinkCreated` — one UPDATE sets `created` and the item ref together (the only
  shape the ledger's CHECK allows).
- `SinkRejected` — terminal `failed` with the reason. §11 owns the replay.
- `SinkUnavailable` — the request provably never reached PM, so the claim is
  RELEASED and the delivery re-owes. There is no retry loop and no backoff timer
  here on purpose: re-delivery IS the retry loop (`intake.py` re-delivers every
  un-acked or re-owed event next pass), and a bounded in-pass retry would only
  hammer a service that is already down while holding a claim. Inside the
  composition, the handler then raises `MintingUnavailableError` so the event
  whose mint deferred is never acked (see the spine below).
- `SinkIndeterminate` — PM may have minted. The claim is HELD, and the row
  surfaces for reconciliation rather than being re-owed into a duplicate.

**What this module never does.** It never marks work complete — spec §3's
"never marks work complete because generation ran" is upheld structurally, since
nothing here can write anything but a delivery-ledger row and nothing here calls
any PM verb but the mint. It never touches GitHub (§7: GitHub involvement stays
PM mirroring). And it never calls musher: the dispatch opt-in rides the mint
payload and PM's watcher routes it (§3/§7).

**Mint BEFORE ack — the §4 convergence spine.** The composition
(`run_intake_and_mint`, through `MintingEvaluationHandler`) mints PER EVENT,
inside the intake handler, BEFORE the handler returns — and returning is what
lets `intake.run_intake` ack the event. The handler evaluates the envelope,
then immediately carries out that event's consequences; only when every one of
them has reached a settled-or-parked state — `created`, `dry_run`,
`awaiting_approval`, `failed`, deduplicated/already-minted, or a held claim on
§11's reconciliation list — does it return and the event get acked. The one
answer that is neither settled nor parked, `SinkUnavailable` (the sink's
provable "never arrived"), raises `MintingUnavailableError` at the intake seam
instead, mirroring `engine.EvaluationStalledError`: the pass stops, the event
stays UN-ACKED, and the claim was already released before the raise — so
re-delivery genuinely IS the retry loop, for an outage exactly as for a stall.
The payoff is the crash guarantee: a crash ANYWHERE before the ack re-delivers
the event, and everything the interrupted attempt already did converges
through the ledger (created rows dedup, released claims re-owe, fresh claims
wait out their owner). An acked event is therefore always a fully-minted (or
deliberately parked) event; no acked-before-minted path exists.

`run_intake_and_mint` is the driver-facing composition, for tests and for the
scheduled driver that arrives with the live cutover. Like `intake.run_intake`
it is a library callable and is deliberately NOT wired into the app lifespan:
`MARKETING_ENABLED` (spec §2) gates the live loops.
"""

from __future__ import annotations

import enum
import logging
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from snowline_marketing.cursors import CursorStore
from snowline_marketing.engine import (
    EvaluationHandler,
    EvaluationStalled,
    PendingConsequence,
    PolicyResolution,
)
from snowline_marketing.events import EventEnvelope
from snowline_marketing.intake import IntakeResult, MalformedHandler, run_intake
from snowline_marketing.ledger import (
    DeliveryLedger,
    DeliveryOutcome,
    LedgerRecord,
    LedgerTransition,
    MintingLedger,
)
from snowline_marketing.policies import PolicyMode
from snowline_marketing.policy_cache import PolicyCacheStore
from snowline_marketing.policy_source import PolicyProvider
from snowline_marketing.rendering import RenderFailure, render_mint_request
from snowline_marketing.sources import EventSource
from snowline_marketing.work_sink import (
    SinkCreated,
    SinkIndeterminate,
    SinkRejected,
    SinkUnavailable,
    WorkItemSink,
)

log = logging.getLogger("snowline_marketing.minting")


class MintDisposition(enum.StrEnum):
    """What the minting pass did with one consequence.

    Distinct from `ledger.DeliveryOutcome`, which is what the ROW says, because
    the two answer different questions and would be wrong to merge: several
    dispositions leave the row in the same state (`already_minted` and a fresh
    `created` both end on a `created` row) and one leaves it untouched
    (`reconciliation_needed` on a claim another pass holds). This is the pass's
    report; the row is the audit."""

    # The sink minted and the row now names the item.
    created = "created"
    # The row already said `created` when this pass tried — a re-delivery whose
    # work was done, or a race the other pass won. Not an error: it is the
    # convergence working (spec §4).
    already_minted = "already_minted"
    # Mode `approval_required`: marked and withheld, still owed (§12).
    awaiting_approval = "awaiting_approval"
    # Mode `dry_run`: closed terminally, nothing minted (§11).
    dry_run_closed = "dry_run_closed"
    # Permanent: PM refused, or the entry's templates would not render for this
    # event. The row is `failed` and carries the reason — §11's dead-letter.
    failed = "failed"
    # Transient: the claim was released and the delivery re-owes. Re-delivery
    # is the retry loop — `MintingEvaluationHandler` raises on this disposition
    # so the event stays un-acked and actually re-delivers (module docstring).
    deferred = "deferred"
    # An operator has to look: a claim held by another pass, a claim whose mint
    # never confirmed, or a sink answer that could not say whether PM minted.
    # Nothing was minted and nothing was written.
    reconciliation_needed = "reconciliation_needed"


@dataclass(frozen=True)
class MintOutcome:
    """One consequence, carried out (or deliberately not).

    Carries the whole `PendingConsequence` for the same reason the consequence
    carries the whole entry and envelope (`engine.PendingConsequence`): every
    caller that reports on a pass wants a different projection of it, and a
    hand-copied subset would fall behind the day either schema grew.

    `record` is the ledger row AFTER the pass touched it — None only where the
    row could not be found at all. `item_ref` is set exactly when the row names
    a PM item, whether this pass minted it or found it already there."""

    consequence: PendingConsequence
    disposition: MintDisposition
    record: LedgerRecord | None
    item_ref: str | None = None
    detail: str | None = None

    @property
    def tenant(self) -> str:
        return self.consequence.tenant

    @property
    def dedup_key(self) -> str:
        return self.consequence.dedup_key

    @property
    def policy_id(self) -> str:
        return self.consequence.policy_id

    @property
    def needs_operator(self) -> bool:
        """Whether this outcome is one §11's surfaces exist for — a dead-letter
        row or a claim to reconcile. Deliberately NOT true of `deferred`: a
        released claim re-owes by itself and needs nobody."""
        return self.disposition in (
            MintDisposition.failed,
            MintDisposition.reconciliation_needed,
        )


@dataclass(frozen=True)
class MintPassReport:
    """What one minting pass did, in consequence order."""

    outcomes: tuple[MintOutcome, ...]

    @property
    def counts(self) -> Mapping[MintDisposition, int]:
        """Totals, DERIVED from `outcomes` — stored counts could disagree with
        the lines they summarize; a property cannot (same reasoning as
        `dryrun.DryRunReport.counts`)."""
        return Counter(outcome.disposition for outcome in self.outcomes)

    @property
    def minted(self) -> tuple[MintOutcome, ...]:
        """The consequences this pass actually turned into PM items."""
        return tuple(
            outcome
            for outcome in self.outcomes
            if outcome.disposition is MintDisposition.created
        )

    @property
    def needs_operator(self) -> tuple[MintOutcome, ...]:
        """Everything a human has to resolve — §11's dead-letter and
        reconciliation input, surfaced by the pass that produced it rather than
        left to be discovered by a query."""
        return tuple(outcome for outcome in self.outcomes if outcome.needs_operator)


# The row details each transition writes. Constants because they are the
# operator's sentence on a row an hour or a month later, and because a test can
# then assert on the state rather than on a phrasing it copied.
_CLAIM_DETAIL = "claimed by a minting pass — mint in flight (spec §7)"
_AWAITING_APPROVAL_DETAIL = (
    "matched in mode 'approval_required' — minting withheld until an operator "
    "releases it (spec §12); the work is still owed"
)
_DRY_RUN_DETAIL = (
    "matched in mode 'dry_run' — reported, nothing minted, row closed (spec §11)"
)


def _outcome(
    consequence: PendingConsequence,
    disposition: MintDisposition,
    transition: LedgerTransition | None = None,
    *,
    record: LedgerRecord | None = None,
    detail: str | None = None,
) -> MintOutcome:
    """Assemble one `MintOutcome`, taking the row from the transition that
    produced it. `item_ref` is read off the ROW rather than from whatever the
    sink said, so the report can never claim an item the ledger does not."""
    row = record if transition is None else transition.record
    return MintOutcome(
        consequence=consequence,
        disposition=disposition,
        record=row,
        item_ref=row.created_item_ref if row is not None else None,
        detail=detail,
    )


def _transition_refused(
    consequence: PendingConsequence, transition: LedgerTransition
) -> MintOutcome:
    """Classify a guarded transition that did not apply — the row already moved
    on, or was never in the state this pass expected.

    Three shapes, and only one of them is ordinary. `created` is the
    convergence working (someone minted it; nothing to do). `claimed` is either
    a live pass holding the row or a crashed one that never confirmed — from the
    row alone those are indistinguishable, which is precisely why this returns
    "an operator looks" rather than guessing; `updated_at` is what tells them
    apart in practice (`engine.evaluate` makes exactly that call on repeat
    delivery — a fresh claim dedups quietly, a stale one surfaces — and §11's
    `release_stale_claim` owns the resolution). Anything else is a row this
    pass should never have been offered."""
    row = transition.record
    if row is None:
        return _outcome(
            consequence,
            MintDisposition.reconciliation_needed,
            transition,
            detail=(
                f"ledger row {consequence.dedup_key!r} for tenant "
                f"{consequence.tenant!r} does not exist — the engine records "
                "every consequence's row before emitting it, so this is a bug "
                "or an out-of-band delete; nothing minted"
            ),
        )
    if row.outcome is DeliveryOutcome.created:
        return _outcome(
            consequence,
            MintDisposition.already_minted,
            transition,
            detail=(
                f"already minted as {row.created_item_ref!r} — this delivery "
                "owes nothing (spec §4)"
            ),
        )
    detail = (
        f"transition refused: row {consequence.dedup_key!r} is in state "
        f"{row.outcome.value!r}"
        + (
            " — another pass holds it, or an earlier mint never confirmed; "
            "refusing to mint a second item, reconcile the row (spec §11)"
            if row.outcome is DeliveryOutcome.claimed
            else " — not a mintable state; nothing minted"
        )
    )
    log.warning(
        "mint transition refused for tenant %r policy %r (row state %r) — %s",
        consequence.tenant,
        consequence.policy_id,
        row.outcome.value,
        consequence.dedup_key,
    )
    return _outcome(
        consequence, MintDisposition.reconciliation_needed, transition, detail=detail
    )


def _mint_active(
    consequence: PendingConsequence, *, sink: WorkItemSink, ledger: MintingLedger
) -> MintOutcome:
    """Mode `active`: claim, render, submit, converge (see the module
    docstring for why the claim comes first and what each sink answer means)."""
    claim = ledger.claim(
        consequence.tenant, consequence.dedup_key, detail=_CLAIM_DETAIL
    )
    if not claim.applied:
        return _transition_refused(consequence, claim)

    request = render_mint_request(consequence)
    if isinstance(request, RenderFailure):
        # A per-delivery MINT failure, not a crashed pass (`rendering.py`): the
        # claim is closed as terminal `failed` carrying the operator's fix, and
        # the pass carries on with the next consequence. Rendering is
        # deterministic, so this failure would repeat on every re-delivery —
        # which is why it is terminal rather than deferred, and why §11's
        # replay (after the artifact is revised) is the right way back.
        transition = ledger.mark_failed(
            consequence.tenant, consequence.dedup_key, detail=request.detail
        )
        return _outcome(
            consequence, MintDisposition.failed, transition, detail=request.detail
        )

    result = sink.submit(request)
    if isinstance(result, SinkCreated):
        transition = ledger.confirm_created(
            consequence.tenant,
            consequence.dedup_key,
            item_ref=result.item_ref,
            detail=(
                f"minted PM work item {result.item_ref} for policy "
                f"{consequence.policy_id!r}"
            ),
        )
        if not transition.applied:
            # The row moved between this pass's claim and its confirmation —
            # only possible out of band. The item EXISTS in PM and the ledger
            # does not point at it, which is precisely the drift §11
            # reconciles; say so rather than reporting a mint the row denies.
            return _outcome(
                consequence,
                MintDisposition.reconciliation_needed,
                transition,
                detail=(
                    f"PM minted {result.item_ref!r} but the claimed row could "
                    "not be confirmed (it moved out from under this pass) — "
                    "the item exists and the ledger does not name it"
                ),
            )
        return _outcome(consequence, MintDisposition.created, transition)

    if isinstance(result, SinkRejected):
        detail = (
            f"PM permanently rejected the mint for policy "
            f"{consequence.policy_id!r}: {result.reason}"
        )
        transition = ledger.mark_failed(
            consequence.tenant, consequence.dedup_key, detail=detail
        )
        return _outcome(consequence, MintDisposition.failed, transition, detail=detail)

    if isinstance(result, SinkUnavailable):
        detail = (
            f"PM unavailable and the request never reached it "
            f"({result.detail}) — claim released, the delivery re-owes"
        )
        transition = ledger.release_claim(
            consequence.tenant, consequence.dedup_key, detail=detail
        )
        return _outcome(
            consequence, MintDisposition.deferred, transition, detail=detail
        )

    if isinstance(result, SinkIndeterminate):
        # The claim is HELD deliberately: PM may have minted, so releasing it
        # would re-mint and confirming it would name an item that may not
        # exist. The row stays `claimed` and becomes §11's reconciliation case.
        detail = (
            f"PM's answer did not say whether the item was minted "
            f"({result.detail}) — claim HELD so the delivery cannot re-mint; "
            "reconcile the row (spec §11)"
        )
        log.warning(
            "indeterminate mint for tenant %r policy %r (%s) — claim held",
            consequence.tenant,
            consequence.policy_id,
            consequence.dedup_key,
        )
        return _outcome(
            consequence,
            MintDisposition.reconciliation_needed,
            record=ledger.get(consequence.tenant, consequence.dedup_key),
            detail=detail,
        )

    raise AssertionError(f"unhandled sink result {result!r}")


def _gate_for_approval(
    consequence: PendingConsequence, *, sink: WorkItemSink, ledger: MintingLedger
) -> MintOutcome:
    """Mode `approval_required`: mark, mint nothing.

    Idempotent by the ledger's guard — a row already `awaiting_approval` refuses
    the transition, so a consequence re-offered on every re-delivery writes
    once. `applied=False` on an already-marked row is therefore the NORMAL path
    and reports the same disposition: the state is what matters, not which pass
    put the row in it."""
    transition = ledger.mark_awaiting_approval(
        consequence.tenant, consequence.dedup_key, detail=_AWAITING_APPROVAL_DETAIL
    )
    row = transition.record
    if not transition.applied and (
        row is None or row.outcome is not DeliveryOutcome.awaiting_approval
    ):
        return _transition_refused(consequence, transition)
    return _outcome(
        consequence,
        MintDisposition.awaiting_approval,
        transition,
        detail=_AWAITING_APPROVAL_DETAIL,
    )


def _close_dry_run(
    consequence: PendingConsequence, *, sink: WorkItemSink, ledger: MintingLedger
) -> MintOutcome:
    """Mode `dry_run`: close the row terminally, mint nothing (see the module
    docstring on why closure — not "leave it matched" — is the honest state)."""
    transition = ledger.close_dry_run(
        consequence.tenant, consequence.dedup_key, detail=_DRY_RUN_DETAIL
    )
    row = transition.record
    if not transition.applied and (
        row is None or row.outcome is not DeliveryOutcome.dry_run
    ):
        return _transition_refused(consequence, transition)
    return _outcome(
        consequence,
        MintDisposition.dry_run_closed,
        transition,
        detail=_DRY_RUN_DETAIL,
    )


# One handler per policy mode. A dict with an import-time pin rather than an
# if/elif chain, for the same reason `engine._DEDUP_KEY_VALUES` is one: a mode
# added to `policies.PolicyMode` without a decision about what minting does with
# it is not a case to fall through — it is a rule an operator believes is armed.
_MODE_HANDLERS: dict[
    PolicyMode,
    Callable[..., MintOutcome],
] = {
    PolicyMode.active: _mint_active,
    PolicyMode.approval_required: _gate_for_approval,
    PolicyMode.dry_run: _close_dry_run,
}

if set(_MODE_HANDLERS) != set(PolicyMode):
    raise AssertionError(
        "minting must handle every policies.PolicyMode — an unhandled mode is "
        "a policy that silently does nothing"
    )


def mint_consequence(
    consequence: PendingConsequence,
    *,
    sink: WorkItemSink,
    ledger: MintingLedger | None = None,
) -> MintOutcome:
    """Carry out one pending consequence (spec §7).

    Dispatches on the matched entry's MODE and nothing else — `mints` is a
    convenience flag on the consequence, but the three modes need three
    different row transitions, so the mode itself is read here.

    Never raises for anything the sink can answer: `WorkItemSink.submit`'s
    contract is typed failure, and a ledger that cannot be written is the one
    exception worth propagating (a claim that did not land must not be reported
    as one that did)."""
    ledger = ledger if ledger is not None else DeliveryLedger()
    return _MODE_HANDLERS[consequence.mode](consequence, sink=sink, ledger=ledger)


def mint_pass(
    consequences: Iterable[PendingConsequence],
    *,
    sink: WorkItemSink,
    ledger: MintingLedger | None = None,
) -> MintPassReport:
    """Carry out a batch of owed consequences, in order.

    Order matters and is the engine's: policy declaration order within an
    event (`MintingEvaluationHandler` calls this once per event, which is how
    event order within a pass falls out), so a fixtures run mints the same
    items in the same sequence every time.

    One consequence's failure never stops the rest — each is claimed and
    settled independently, which is what keeps one tenant's broken template
    from withholding another policy's work. (`deferred` is the exception, and
    it is the CALLER's: the handler raises on it after the batch, because a
    released claim is not settled and the event must not ack.)"""
    ledger = ledger if ledger is not None else DeliveryLedger()
    return MintPassReport(
        outcomes=tuple(
            mint_consequence(consequence, sink=sink, ledger=ledger)
            for consequence in consequences
        )
    )


class MintingUnavailableError(RuntimeError):
    """Raised by `MintingEvaluationHandler` to stop an intake pass when PM was
    unavailable for a mint the event still owes.

    The minting layer RETURNS typed outcomes; this exception exists solely at
    the intake seam, exactly like `engine.EvaluationStalledError` and with the
    same intake semantics: `run_intake`'s contract is that a handler which
    raises stops the pass with the position un-acked (`intake.py`). By the
    time this is raised the claim is already RELEASED — the `SinkUnavailable`
    arm released it before reporting `deferred` — so nothing is held: the
    event re-delivers next pass, the engine re-owes the consequence, and a
    recovered PM mints it. Re-delivery IS the retry loop, for an outage
    exactly as for a policy stall. Carries the typed outcome so a driver
    above can say which mint deferred without parsing a message."""

    def __init__(self, outcome: MintOutcome) -> None:
        super().__init__(
            f"PM unavailable for tenant {outcome.tenant!r} "
            f"(policy {outcome.policy_id!r}, key {outcome.dedup_key!r}): "
            f"{outcome.detail} — pass stopped, event un-acked"
        )
        self.outcome = outcome


class MintingEvaluationHandler(EvaluationHandler):
    """`EvaluationHandler` plus the mint — per event, BEFORE the ack. This is
    the §4 convergence spine (module docstring) as an `intake.EventHandler`.

    `__call__` evaluates exactly as the parent does (a stall still raises
    `EvaluationStalledError`), then immediately carries the event's
    consequences out against the SAME ledger the evaluation wrote. Only when
    every consequence has settled or parked does it return — and returning is
    what lets `run_intake` ack, so an acked event is always a minted (or
    deliberately parked) one. A `deferred` outcome — `SinkUnavailable`, the
    claim already released, the work re-owed — raises
    `MintingUnavailableError` instead, stopping the pass with the event
    un-acked; a crash anywhere in between re-delivers, and the interrupted
    attempt's rows converge (created dedups, released claims re-owe).

    Takes `MintingLedger`, not the parent's `LedgerStore`: the row the engine
    writes is the row the mint transitions, and the narrow protocol cannot
    transition anything."""

    def __init__(
        self,
        tenant: str,
        *,
        provider: PolicyProvider,
        sink: WorkItemSink,
        ledger: MintingLedger | None = None,
        cache: PolicyCacheStore | None = None,
        resolution: PolicyResolution | None = None,
    ) -> None:
        minting_ledger = ledger if ledger is not None else DeliveryLedger()
        super().__init__(
            tenant,
            provider=provider,
            cache=cache,
            ledger=minting_ledger,
            resolution=resolution,
        )
        self._sink = sink
        self._minting_ledger: MintingLedger = minting_ledger
        self.mint_outcomes: list[MintOutcome] = []
        # The deferred outcome the pass stopped on, if any — kept the way the
        # parent keeps `stall`, so a driver that has only the `IntakeResult`
        # (rather than the exception) can still say why.
        self.unavailable: MintOutcome | None = None

    def __call__(self, envelope: EventEnvelope) -> None:
        super().__call__(envelope)
        report = mint_pass(
            self.results[-1].consequences,
            sink=self._sink,
            ledger=self._minting_ledger,
        )
        self.mint_outcomes.extend(report.outcomes)
        for outcome in report.outcomes:
            if outcome.disposition is MintDisposition.deferred:
                self.unavailable = outcome
                raise MintingUnavailableError(outcome)

    @property
    def mint_report(self) -> MintPassReport:
        """Everything this pass minted (or deliberately did not), in event
        order. Includes the outcomes of an event the pass stopped on — that
        event re-delivers un-acked, and its rows converge next pass."""
        return MintPassReport(outcomes=tuple(self.mint_outcomes))


@dataclass(frozen=True)
class IntakeMintResult:
    """One full drive: an intake pass that minted each event before its ack.

    The parts are kept whole rather than summarized. `intake` says how far the
    stream got (and why it stopped); `stall` says the pass ended because the
    tenant's policies could not be read; `unavailable` says it ended because
    PM was down mid-mint (a different operator problem again — the claim is
    released and re-delivery retries by itself); `mint` says what became of
    every consequence the pass reached."""

    intake: IntakeResult
    stall: EvaluationStalled | None
    mint: MintPassReport
    unavailable: MintOutcome | None = None

    @property
    def ok(self) -> bool:
        """The whole drive completed and nothing needs a human: the pass did not
        fail, the policies resolved, and no mint dead-lettered or stuck."""
        return self.intake.ok and self.stall is None and not self.mint.needs_operator


def run_intake_and_mint(
    source: EventSource,
    *,
    tenant: str,
    provider: PolicyProvider,
    sink: WorkItemSink,
    cursor_store: CursorStore,
    ledger: MintingLedger | None = None,
    cache: PolicyCacheStore | None = None,
    on_malformed: MalformedHandler | None = None,
    limit: int | None = None,
) -> IntakeMintResult:
    """Drive one intake pass, minting each event BEFORE its ack — the
    composition a scheduled driver will call, and the one tests drive end to
    end (the §4 convergence spine; module docstring).

    The ordering is load-bearing in the opposite direction from the obvious
    worry. Yes, minting inside the handler means a PM outage stops event
    CONSUMPTION — deliberately: an ack is the loop's statement that the event
    is DONE, and an event whose mint has not happened is not done. Acking it
    anyway would strand its work behind a moved cursor, recoverable only by a
    later pass happening to run — the acked-before-minted window this
    composition exists to close. Stopped consumption, by contrast, is the
    same visible, self-healing stall a governance outage already produces:
    the event re-delivers, and re-delivery is the retry loop.

    ONE ledger serves both halves — the engine writes the rows, the mint
    transitions them (`ledger.MintingLedger` is `LedgerStore` plus the verbs),
    so an in-memory ledger drives a whole fixtures run with no database. Not
    wired into the app lifespan (spec §2 gates the live loops)."""
    handler = MintingEvaluationHandler(
        tenant, provider=provider, sink=sink, ledger=ledger, cache=cache
    )
    intake = run_intake(
        source,
        handler,
        cursor_store=cursor_store,
        on_malformed=on_malformed,
        limit=limit,
    )
    return IntakeMintResult(
        intake=intake,
        stall=handler.stall,
        mint=handler.mint_report,
        unavailable=handler.unavailable,
    )

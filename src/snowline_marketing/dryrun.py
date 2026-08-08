"""Dry-run — evaluate a candidate policy version against captured fixtures,
report what would have been minted, mint nothing (spec §11).

This is the operator's pre-flight surface for a policy change: before an
artifact revision goes live, run it against a capture and see what it would
have done. It is deliberately NOT a second code path. Everything that makes
the preview honest is borrowed whole from the live one:

- The candidate BODY is classified through `policy_source.InMemoryPolicyProvider`
  + `engine.resolve_policy_set` — the same two calls `EvaluationHandler` makes
  on the live path — so a malformed or tenant-mismatched draft reports the
  same quarantine the live path would produce (spec §6: "never silently
  match-all or match-none"), rather than silently evaluating nothing.
- The capture is driven through `intake.run_intake` with a real
  `sources.FixturesEventSource` and the engine's own `engine.EvaluationHandler`
  — the identical loop the live outbox will drive at cutover (spec §5). A
  handler-direct call over the fixtures would still exercise `engine.evaluate`,
  but it would skip the malformed-envelope classification, the ack-after-
  handle ordering, and the one-resolution-per-pass memoization that the loop
  owns — three ways a hand-rolled driver could quietly diverge from what
  production actually does with the same capture. Going through `run_intake`
  means there is exactly one way this plugin walks a fixtures directory.
- Every write goes to an `InMemoryDeliveryLedger` (`ledger.py`) and an
  `InMemoryPolicyCache` (`policy_cache.py`) — the ledger's and cache's own
  dry-run implementations, faithful to the real stores' guards and namespace/
  tenant rules — and the cursor is an `InMemoryCursorStore` (`cursors.py`,
  whose docstring names this module as its intended caller: "a dry-run that
  moved the real cursor would eat the events it was only supposed to
  preview"). Nothing durable is touched: no row in `delivery_ledger`, none in
  `policy_cache`, none in `consumer_cursors`.

`dry_run` returns a frozen `DryRunReport` — the typed surface a future
dashboard/CLI consumes; `render_text` is today's human-readable summary.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from snowline_marketing.cursors import InMemoryCursorStore
from snowline_marketing.engine import (
    Delivery,
    EvaluationHandler,
    EvaluationStalled,
    PendingConsequence,
    StallReason,
    resolve_policy_set,
)
from snowline_marketing.events import MalformedEnvelope
from snowline_marketing.intake import HandlerFailure, run_intake
from snowline_marketing.ledger import DeliveryOutcome, InMemoryDeliveryLedger
from snowline_marketing.policy_cache import InMemoryPolicyCache
from snowline_marketing.policy_source import InMemoryPolicyProvider
from snowline_marketing.sources import FixturesEventSource

# The version id a candidate is evaluated under when the caller does not name
# one — a scratch draft that is not (yet) any governance artifact version.
# When the operator's draft IS a real governance version (revised but not yet
# current), the caller passes its true version id so the report's audit trail
# names it, exactly as a live evaluation would.
DEFAULT_DRY_RUN_VERSION_ID = "dry-run-candidate"


def _decode_candidate_body(policy_body: str | bytes | Mapping[str, Any]) -> str:
    """Normalize the operator's candidate body to TEXT — the shape
    `policy_source.ResolvedPolicySet.body` and the policy cache both carry
    contractually ("body is TEXT, verbatim, all the way from governance to the
    cache column", `policy_source.py`).

    A mapping is the operator's in-memory draft — round-tripped through
    `json.dumps` so it classifies through the exact same TEXT path
    `classify.decode_json_object` gives every other body, rather than a
    second, divergent mapping-shaped path existing only for this caller. A
    mapping holding a non-JSON value (a datetime, a set) raises
    TypeError/ValueError out of `json.dumps`; `dry_run` catches that and
    returns a stalled report carrying the real exception text.
    Bytes are a file read verbatim, decoded permissively: a body that fails to
    decode as UTF-8 is exactly the kind of broken draft `parse_policy_set` is
    built to quarantine as `not_json`, not a reason for this function to
    raise — `errors="replace"` guarantees a `str` comes out, and the
    replacement characters land in a body that was already unparseable
    JSON."""
    if isinstance(policy_body, Mapping):
        return json.dumps(dict(policy_body))
    if isinstance(policy_body, bytes):
        return policy_body.decode("utf-8", errors="replace")
    return policy_body


@dataclass(frozen=True)
class DryRunEventResult:
    """One evaluated event, in the order the capture delivered it.

    `deliveries` carries `engine.Delivery` — the frozen engine type, whole and
    verbatim, never a field-by-field mirror that could fall behind the engine's
    schema — so the report cannot say anything evaluation itself did not
    decide."""

    event_id: str
    event_type: str
    deliveries: tuple[Delivery, ...]


@dataclass(frozen=True)
class DryRunReport:
    """§11's dry-run output: "evaluate a policy version against captured
    fixtures, report what would have been minted, mint nothing" — the typed
    surface a future dashboard/CLI consumes; `render_text` is today's human
    reader.

    `stalled` is set exactly when the CANDIDATE body itself could not be
    evaluated — malformed, or declaring a tenant other than the one being
    previewed — the same distinction `engine.EvaluationStalled` makes on the
    live path. The candidate is classified EAGERLY, before any fixture is
    read, so a broken draft stalls even against an empty capture — the
    pre-flight must never say "fine" about a draft that would stall
    production on its first real event.

    `pass_failure` is set when the CAPTURE walk itself failed (an invalid
    fixtures directory, a mid-pass read error) — `run_intake`'s recorded
    failure, surfaced instead of swallowed, so a broken capture is never
    indistinguishable from "this policy matches nothing".

    `ok` is True only when NEITHER is set: the whole capture was evaluated."""

    tenant: str
    version_id: str
    stalled: EvaluationStalled | None
    pass_failure: HandlerFailure | None
    events: tuple[DryRunEventResult, ...]
    malformed: tuple[MalformedEnvelope, ...]

    @property
    def ok(self) -> bool:
        return self.stalled is None and self.pass_failure is None

    @property
    def counts(self) -> Mapping[DeliveryOutcome, int]:
        """Delivery totals, DERIVED from `events` — stored state could
        disagree with the per-event lines it summarizes; a property cannot."""
        return Counter(
            delivery.outcome for event in self.events for delivery in event.deliveries
        )

    @property
    def would_mint(self) -> tuple[PendingConsequence, ...]:
        """Every consequence this preview says WOULD ACTUALLY MINT, in
        delivery order — the report's direct answer to §11's headline
        question, carried as the SAME `engine.PendingConsequence` the live
        minting layer (§7) would consume.

        Filtered on `mints`: a `dry_run`-mode policy's consequence is
        evaluated and reported per delivery, but production would mint
        nothing for it, and this headline must not overstate what a rollout
        does. The per-delivery consequences keep every consequence, mints
        flag visible, for the operator reading line by line.

        Deduplicated by the delivery's stored `record.dedup_key` (first
        occurrence wins): a re-owed match — the same event re-delivered while
        its mint never happened — shares its row's key and re-emits its
        consequence per delivery (spec §4's recoverable convergence), but
        production, with minting doing its job, mints ONCE per key, and so
        must the headline."""
        seen: set[str] = set()
        consequences: list[PendingConsequence] = []
        for event in self.events:
            for delivery in event.deliveries:
                consequence = delivery.consequence
                if consequence is None or not consequence.mints:
                    continue
                if delivery.record.dedup_key in seen:
                    continue
                seen.add(delivery.record.dedup_key)
                consequences.append(consequence)
        return tuple(consequences)


def dry_run(
    policy_body: str | bytes | Mapping[str, Any],
    fixtures_dir: Path | str,
    *,
    tenant: str,
    version_id: str = DEFAULT_DRY_RUN_VERSION_ID,
) -> DryRunReport:
    """Evaluate `policy_body` as `tenant`'s policy set against every fixture in
    `fixtures_dir`, mint nothing, leave no trace.

    `policy_body` is the operator's candidate — text, bytes, or an in-memory
    mapping (see `_decode_candidate_body`) — which may be a version that is
    NOT the tenant's current one; `version_id` is what the report and every
    (in-memory) ledger row records as the evaluated version, defaulting to a
    scratch id for a draft that is not yet any governance version.

    Classification and evaluation both run through the exact functions the
    live path uses (`resolve_policy_set`, `engine.evaluate`, via
    `EvaluationHandler` and `run_intake` — see the module docstring), pointed
    at three in-memory stores instead of the real ones: `InMemoryPolicyCache`,
    `InMemoryDeliveryLedger`, `InMemoryCursorStore`. Nothing durable is
    touched.

    Raises ValueError when `fixtures_dir` is not a capture directory —
    distinguishing a path that does not exist from one that exists but is not
    a directory (a fixture FILE passed where its directory belongs): a glob
    over a typo'd path yields nothing, and a clean "0 deliveries" report over
    a capture that was never read would be indistinguishable from a genuinely
    empty one — the wrong-path case must fail loudly, matching
    `sources.fixture_files`' posture toward broken captures."""
    directory = Path(fixtures_dir)
    if not directory.exists():
        raise ValueError(
            f"fixtures directory {str(directory)!r} does not exist — refusing "
            "to report a never-read capture as an empty one"
        )
    if not directory.is_dir():
        raise ValueError(
            f"fixtures path {str(directory)!r} exists but is not a directory "
            "— pass the capture DIRECTORY, not a fixture file"
        )

    try:
        body = _decode_candidate_body(policy_body)
    except (TypeError, ValueError) as exc:
        # A draft mapping holding a non-JSON value (a datetime, a set) is a
        # BROKEN DRAFT: it stalls the preview the way any quarantined
        # candidate does — a real stalled report carrying the real exception
        # text, never a traceback out of dry_run and never a sentinel body
        # smuggled through the parser.
        return DryRunReport(
            tenant=tenant,
            version_id=version_id,
            stalled=EvaluationStalled(
                tenant=tenant,
                reason=StallReason.policy_quarantined,
                detail=f"draft mapping is not JSON-serializable: {exc}",
                version_id=version_id,
            ),
            pass_failure=None,
            events=(),
            malformed=(),
        )

    provider = InMemoryPolicyProvider()
    provider.put(tenant, version_id, body)
    cache = InMemoryPolicyCache()

    # Classify the candidate EAGERLY, before any fixture is read — through the
    # same call the handler makes. Lazily, an empty or all-malformed capture
    # would never invoke the handler, never classify the draft, and report
    # ok=True about a body that stalls production on its first real event.
    resolution = resolve_policy_set(tenant, provider=provider, cache=cache)
    if isinstance(resolution, EvaluationStalled):
        return DryRunReport(
            tenant=tenant,
            version_id=version_id,
            stalled=resolution,
            pass_failure=None,
            events=(),
            malformed=(),
        )

    ledger = InMemoryDeliveryLedger()
    # The eager resolution SEEDS the handler's per-pass memo: the pass
    # evaluates against the very object the preview's stall verdict was
    # decided on, so the two can never diverge — a candidate stall is fully
    # handled by the eager path above.
    handler = EvaluationHandler(
        tenant,
        provider=provider,
        cache=cache,
        ledger=ledger,
        resolution=resolution,
    )
    malformed: list[MalformedEnvelope] = []
    result = run_intake(
        FixturesEventSource(directory),
        handler,
        cursor_store=InMemoryCursorStore(),
        on_malformed=malformed.append,
    )
    events = tuple(
        DryRunEventResult(
            event_id=evaluated.envelope.event_id,
            event_type=evaluated.envelope.event_type.value,
            deliveries=evaluated.deliveries,
        )
        for evaluated in handler.results
    )
    return DryRunReport(
        tenant=tenant,
        version_id=version_id,
        stalled=None,
        # The capture walk's own failure — a mixed-width capture, a file
        # deleted mid-pass — surfaced, never swallowed: a broken capture must
        # not read as "this policy matches nothing".
        pass_failure=result.failure,
        events=events,
        malformed=tuple(malformed),
    )


def render_text(report: DryRunReport) -> str:
    """Render `report` as an operator-readable summary: counts first, then
    per-event lines. The typed `DryRunReport` is what a future CLI/dashboard
    consumes (spec §11's "Operator surfaces"); this is for a human reading a
    terminal today."""
    lines: list[str] = [
        f"dry-run: tenant={report.tenant!r} version={report.version_id!r}"
    ]
    if report.stalled is not None:
        stall = report.stalled
        lines.append(f"STALLED ({stall.reason.value}): {stall.detail}")
        return "\n".join(lines)
    if report.pass_failure is not None:
        failure = report.pass_failure
        where = failure.position or "<pass>"
        lines.append(f"FAILED at {where}: {failure.error}")
        lines.append(
            "capture walk did not complete — results below (if any) cover "
            "only what was read before the failure"
        )

    counts = report.counts
    total = sum(counts.values())
    lines.append(f"{total} deliveries evaluated:")
    for outcome in DeliveryOutcome:
        count = counts.get(outcome, 0)
        if count:
            lines.append(f"  {outcome.value}: {count}")
    if not counts:
        lines.append("  (none)")

    if report.malformed:
        lines.append(f"{len(report.malformed)} malformed fixture(s):")
        for bad in report.malformed:
            locator = bad.ref or bad.position or "<unknown>"
            lines.append(f"  {locator}: {bad.reason.value} — {bad.detail}")

    lines.append("")
    lines.append("events:")
    for event in report.events:
        lines.append(f"  {event.event_id} ({event.event_type}):")
        for delivery in event.deliveries:
            head = f"    {delivery.outcome.value}"
            if delivery.record.policy_id:
                head += f" [{delivery.record.policy_id}]"
            head += f" dedup_key={delivery.record.dedup_key!r}"
            lines.append(head)
            consequence = delivery.consequence
            if consequence is not None:
                destination = consequence.destination.scope
                if consequence.destination.initiative:
                    destination += f"/{consequence.destination.initiative}"
                if consequence.destination.phase:
                    destination += f"/{consequence.destination.phase}"
                lines.append(
                    f"      would mint: {consequence.consequence.value} "
                    f"(mode={consequence.mode.value}, "
                    f"mints={consequence.mints}) -> {destination} "
                    f'— "{consequence.entry.title_template}"'
                )
    return "\n".join(lines)

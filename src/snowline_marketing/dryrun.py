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
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowline_marketing.cursors import InMemoryCursorStore
from snowline_marketing.engine import Delivery, EvaluationHandler, EvaluationStalled
from snowline_marketing.events import MalformedEnvelope
from snowline_marketing.intake import run_intake
from snowline_marketing.ledger import DeliveryOutcome, InMemoryDeliveryLedger
from snowline_marketing.policies import ConsequenceType, PolicyMode
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
    second, divergent mapping-shaped path existing only for this caller.
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
class WouldMintSummary:
    """What one matched delivery would have minted, had this not been a
    dry-run — §11's headline question, answered.

    Built from the SAME `engine.PendingConsequence` the live minting layer
    (§7) would consume, never from a re-derivation, so this summary cannot
    describe a mint the live path would not actually produce."""

    policy_id: str
    consequence: ConsequenceType
    mode: PolicyMode
    mints: bool
    destination_scope: str
    destination_initiative: str | None
    destination_phase: str | None
    title_template: str


def _would_mint(delivery: Delivery) -> WouldMintSummary | None:
    consequence = delivery.consequence
    if consequence is None:
        return None
    return WouldMintSummary(
        policy_id=consequence.policy_id,
        consequence=consequence.consequence,
        mode=consequence.mode,
        mints=consequence.mints,
        destination_scope=consequence.destination.scope,
        destination_initiative=consequence.destination.initiative,
        destination_phase=consequence.destination.phase,
        title_template=consequence.entry.title_template,
    )


@dataclass(frozen=True)
class DryRunDelivery:
    """One event x one policy (or one event-level row) — mirrors
    `engine.Delivery` field-for-field (the outcome, the STORED dedup key, the
    operator detail) plus the would-mint summary for a match, so the report
    cannot say anything evaluation itself did not decide."""

    outcome: DeliveryOutcome
    dedup_key: str
    policy_id: str | None
    detail: str | None
    would_mint: WouldMintSummary | None


def _delivery_report(delivery: Delivery) -> DryRunDelivery:
    return DryRunDelivery(
        outcome=delivery.outcome,
        dedup_key=delivery.record.dedup_key,
        policy_id=delivery.record.policy_id,
        # The delivery's own detail (set only for the within-evaluation
        # collision) when there is one; otherwise the ROW's detail — the
        # match/ignore/quarantine reason an operator reads for this line.
        detail=delivery.detail or delivery.record.detail,
        would_mint=_would_mint(delivery),
    )


@dataclass(frozen=True)
class DryRunEventResult:
    """One evaluated event, in the order the capture delivered it."""

    event_id: str
    event_type: str
    deliveries: tuple[DryRunDelivery, ...]


@dataclass(frozen=True)
class DryRunReport:
    """§11's dry-run output: "evaluate a policy version against captured
    fixtures, report what would have been minted, mint nothing" — the typed
    surface a future dashboard/CLI consumes; `render_text` is today's human
    reader.

    `stalled` is set exactly when the CANDIDATE body itself could not be
    evaluated — malformed, or declaring a tenant other than the one being
    previewed — the same distinction `engine.EvaluationStalled` makes on the
    live path, carried through so a broken draft is reported as broken
    rather than evaluated against nothing (spec §6). When set, `events` and
    `counts` are empty and `malformed` reports only fixtures classification
    reached before the stall: nothing was evaluated, mirroring `evaluate`'s
    "nothing is recorded and nothing is consumed" for a live stall."""

    tenant: str
    version_id: str
    stalled: EvaluationStalled | None
    events: tuple[DryRunEventResult, ...]
    malformed: tuple[MalformedEnvelope, ...]
    # A read-only view (`MappingProxyType`), same reasoning as
    # `events.EventPayload.details`: this is a frozen dataclass, and a plain
    # `dict` here would be the one hole a caller could mutate through after
    # the fact.
    counts: Mapping[DeliveryOutcome, int]

    @property
    def ok(self) -> bool:
        return self.stalled is None

    @property
    def would_mint(self) -> tuple[WouldMintSummary, ...]:
        """Every consequence this preview says would have minted, in delivery
        order — the report's direct answer to §11's headline question."""
        return tuple(
            delivery.would_mint
            for event in self.events
            for delivery in event.deliveries
            if delivery.would_mint is not None
        )


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
    touched."""
    provider = InMemoryPolicyProvider()
    provider.put(tenant, version_id, _decode_candidate_body(policy_body))
    ledger = InMemoryDeliveryLedger()
    handler = EvaluationHandler(
        tenant,
        provider=provider,
        cache=InMemoryPolicyCache(),
        ledger=ledger,
    )
    malformed: list[MalformedEnvelope] = []
    run_intake(
        FixturesEventSource(fixtures_dir),
        handler,
        cursor_store=InMemoryCursorStore(),
        on_malformed=malformed.append,
    )
    if handler.stall is not None:
        # The candidate itself quarantines (or governance-shaped resolution
        # failed, unreachable here since `InMemoryPolicyProvider` always
        # resolves the tenant it was `put` for) — nothing ran past the first
        # event, mirroring a live stall's "nothing is recorded, nothing is
        # consumed".
        return DryRunReport(
            tenant=tenant,
            version_id=version_id,
            stalled=handler.stall,
            events=(),
            malformed=tuple(malformed),
            counts=MappingProxyType({}),
        )
    events = tuple(
        DryRunEventResult(
            event_id=result.envelope.event_id,
            event_type=result.envelope.event_type.value,
            deliveries=tuple(_delivery_report(d) for d in result.deliveries),
        )
        for result in handler.results
    )
    counts: dict[DeliveryOutcome, int] = {}
    for delivery in handler.deliveries:
        counts[delivery.outcome] = counts.get(delivery.outcome, 0) + 1
    return DryRunReport(
        tenant=tenant,
        version_id=version_id,
        stalled=None,
        events=events,
        malformed=tuple(malformed),
        counts=MappingProxyType(counts),
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

    total = sum(report.counts.values())
    lines.append(f"{total} deliveries evaluated:")
    for outcome in DeliveryOutcome:
        count = report.counts.get(outcome, 0)
        if count:
            lines.append(f"  {outcome.value}: {count}")
    if not report.counts:
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
            if delivery.policy_id:
                head += f" [{delivery.policy_id}]"
            head += f" dedup_key={delivery.dedup_key!r}"
            lines.append(head)
            wm = delivery.would_mint
            if wm is not None:
                destination = wm.destination_scope
                if wm.destination_initiative:
                    destination += f"/{wm.destination_initiative}"
                if wm.destination_phase:
                    destination += f"/{wm.destination_phase}"
                lines.append(
                    f"      would mint: {wm.consequence.value} "
                    f"(mode={wm.mode.value}, mints={wm.mints}) -> {destination} "
                    f'— "{wm.title_template}"'
                )
    return "\n".join(lines)

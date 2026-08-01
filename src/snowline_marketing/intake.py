"""The intake loop — source-agnostic, at-least-once event consumption
(spec §5).

One pass: read from a source strictly after the persisted cursor, classify
each raw event (`events.parse_envelope`), hand every VALID envelope to a
handler, and acknowledge the position AFTER the handler returns. Nothing here
knows what a fixtures directory or an outbox row is; swapping the fixtures
source for PM's live outbox (snowline-pm #64) at cutover changes the
`EventSource` passed in and nothing in this file.

**At-least-once, ack-after-handle.** The ack is the last thing that happens
for an event, so a crash anywhere before it re-delivers that event on the next
pass. That is the intended failure mode, not a leak: the delivery ledger
(spec §4, a later item) keys on `tenant + policy_id + event_id` and makes a
re-delivered event converge to the same result. It is precisely because
re-delivery is safe that this loop is allowed to be simple — it never tries to
be exactly-once, and it never acks work it is not sure completed.

**A handler failure STOPS the pass.** The event is left un-acked and the loop
returns with the failure recorded rather than raising: a driver above decides
whether to retry, back off, or dead-letter (spec §11). It does not skip ahead,
because acking a LATER event would strand the failed one behind a cursor that
has already moved past it — silent loss, which the at-least-once contract
exists to prevent. Ordering is preserved for the same reason: PM lifecycle
events are causally ordered (an item completes, then reopens), and evaluating
them out of order would mint against a state that never existed.

**Malformed envelopes are reported and ACKED PAST.** This is the one place the
policy needs stating outright, because both failure modes are real. A
malformed envelope must not be silently lost (spec §4/§8: it belongs in
quarantine, operator-visible, requeuable), and it must not wedge the stream —
retrying a permanently-unparseable envelope forever would stop every valid
event behind it, turning one bad producer write into a dead pipeline. So v1:
report it through `on_malformed`, advance the cursor past it, keep going. The
report is the durable handoff — the quarantine STORE (a later item) is what
persists it; until that lands, `on_malformed` defaults to a WARNING log so a
malformed event is at least loud rather than invisible. Note the residual
window this leaves: a crash between the malformed report and the ack
re-delivers the malformed envelope, so the quarantine store must upsert on
(source_key, position) rather than insert blindly — the same idempotency the
delivery ledger owes valid events.

This is a library callable, driven by tests and (later) by a scheduled
driver. It is deliberately NOT wired into the app lifespan: `MARKETING_ENABLED`
(spec §2) gates the live loops, which arrive with the live-source cutover.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from snowline_marketing.cursors import CursorStore
from snowline_marketing.events import EventEnvelope, MalformedEnvelope, parse_envelope
from snowline_marketing.sources import EventSource

log = logging.getLogger("snowline_marketing.intake")

# What a valid envelope is handed to. Returning is success (ack); raising is
# failure (no ack, pass stops).
EventHandler = Callable[[EventEnvelope], None]

# Where malformed envelopes go. Also allowed to raise — a quarantine store that
# cannot write is not a reason to sail past a malformed event as if it had been
# recorded.
MalformedHandler = Callable[[MalformedEnvelope], None]


@dataclass(frozen=True)
class HandlerFailure:
    """The event a pass stopped on. `position` is un-acked, so the next pass
    starts by re-delivering exactly this event."""

    position: str
    event_id: str | None
    error: str
    # True when the malformed REPORT failed rather than the event handler —
    # different operator problem (the quarantine path is broken, not the
    # policy path).
    while_reporting_malformed: bool = False


@dataclass(frozen=True)
class IntakeResult:
    """The outcome of one pass. Counts are of events actually acked this pass;
    an event the pass stopped on is in neither count."""

    source_key: str
    delivered: int
    malformed: int
    # The last position acked in THIS pass — None when the pass acked nothing
    # (an empty source, or a failure on the very first event). Not the cursor's
    # current value; read the store for that.
    acked_position: str | None
    failure: HandlerFailure | None

    @property
    def ok(self) -> bool:
        return self.failure is None


def _log_malformed(malformed: MalformedEnvelope) -> None:
    """Default malformed sink until the quarantine store lands (spec §4).
    WARNING, not DEBUG: an event the plugin could not understand is an event
    whose policies may never have run."""
    log.warning(
        "malformed event envelope at %s (%s): %s",
        malformed.ref or malformed.position or "<unknown>",
        malformed.reason.value,
        malformed.detail,
    )


def run_intake(
    source: EventSource,
    handler: EventHandler,
    *,
    cursor_store: CursorStore,
    on_malformed: MalformedHandler | None = None,
    limit: int | None = None,
) -> IntakeResult:
    """Consume one pass of `source` into `handler`, acking as it goes.

    Resumption is implicit: the pass starts strictly after the position stored
    under `source.source_key`, so calling this again with the same source key
    and cursor store continues past everything already acked. `limit` bounds a
    pass (a scheduled driver takes a bounded bite and comes back); the cursor
    makes the remainder the next pass's work, not lost work.

    Returns rather than raises on a handler failure — see the module docstring
    on why the pass stops there and why the event stays un-acked."""
    source_key = source.source_key
    after = cursor_store.read(source_key)
    report = on_malformed or _log_malformed

    delivered = 0
    malformed = 0
    acked_position: str | None = None
    failure: HandlerFailure | None = None

    for index, raw in enumerate(source.read(after=after)):
        if limit is not None and index >= limit:
            break

        parsed = parse_envelope(raw.body, ref=raw.ref, position=raw.position)

        if isinstance(parsed, MalformedEnvelope):
            try:
                report(parsed)
            except Exception as exc:  # the quarantine handoff itself failed
                log.exception(
                    "malformed-event report failed at %s — pass stopped, "
                    "position left un-acked",
                    raw.position,
                )
                failure = HandlerFailure(
                    position=raw.position,
                    event_id=parsed.event_id,
                    error=repr(exc),
                    while_reporting_malformed=True,
                )
                break
            malformed += 1
            # Advance past it: reported, not handled. See the module docstring
            # on why a malformed envelope must not wedge the stream.
            cursor_store.ack(source_key, raw.position, parsed.event_id)
            acked_position = raw.position
            continue

        try:
            handler(parsed)
        except Exception as exc:
            log.exception(
                "intake handler failed on event %s at %s — pass stopped, "
                "position left un-acked (re-delivers next pass)",
                parsed.event_id,
                raw.position,
            )
            failure = HandlerFailure(
                position=raw.position,
                event_id=parsed.event_id,
                error=repr(exc),
            )
            break
        delivered += 1
        cursor_store.ack(source_key, raw.position, parsed.event_id)
        acked_position = raw.position

    return IntakeResult(
        source_key=source_key,
        delivered=delivered,
        malformed=malformed,
        acked_position=acked_position,
        failure=failure,
    )

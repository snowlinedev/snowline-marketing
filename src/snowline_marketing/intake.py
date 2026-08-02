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
(spec §4, `ledger.py`) keys on `tenant + policy_id + event_id` and makes a
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

**An ACK failure stops the pass the same way** (`while_acking=True` on the
recorded failure): a transient cursor-store error is infra, not the event's
fault — the event was already handled and safely re-delivers next pass, and a
driver written against `IntakeResult` gets to back off instead of crashing on
an exception this module's contract says it never raises.

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

import itertools
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

    # None when the pass failed in the READ machinery (the initial cursor
    # read, or the source iterator itself) — there was no single event in
    # hand to point at, and nothing was skipped: the next pass re-reads from
    # the same cursor.
    position: str | None
    event_id: str | None
    error: str
    # True when the malformed REPORT failed rather than the event handler —
    # different operator problem (the quarantine path is broken, not the
    # policy path).
    while_reporting_malformed: bool = False
    # True when the ACK failed after the event was already handled/reported —
    # infra (the cursor store), not the event. The event re-delivers next
    # pass, which the at-least-once contract makes safe; a driver seeing this
    # backs off rather than dead-lettering the event.
    while_acking: bool = False
    # True when the failure was in READING — the cursor at pass start, or the
    # source mid-iteration (an unreadable fixture directory, a dropped outbox
    # connection). Also infra-shaped: nothing was handled and nothing acked.
    while_reading: bool = False


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
    report = on_malformed or _log_malformed

    delivered = 0
    malformed = 0
    acked_position: str | None = None
    failure: HandlerFailure | None = None

    def attempt(
        action: Callable[[], None],
        describe: str,
        *,
        position: str,
        event_id: str | None,
        while_reporting_malformed: bool = False,
        while_acking: bool = False,
    ) -> HandlerFailure | None:
        """One guarded step. EVERY step of an event's lifecycle — report,
        handle, ack — goes through here, so all three fail the same way: log,
        record, stop the pass with the position un-acked. The returns-not-
        raises contract holds even when the failing step is the cursor store
        itself (a transient DB error at ack time is infra, not a reason to
        crash a driver written against IntakeResult)."""
        try:
            action()
            return None
        except Exception as exc:
            log.exception(
                "%s failed at %s — pass stopped, position left un-acked "
                "(re-delivers next pass)",
                describe,
                position,
            )
            return HandlerFailure(
                position=position,
                event_id=event_id,
                error=repr(exc),
                while_reporting_malformed=while_reporting_malformed,
                while_acking=while_acking,
            )

    # The initial cursor read is READ machinery, guarded like everything else:
    # a DB blip at pass start is a returned failure, not a crashed driver.
    try:
        after = cursor_store.read(source_key)
    except Exception as exc:
        log.exception("cursor read for %s failed — pass never started", source_key)
        return IntakeResult(
            source_key=source_key,
            delivered=0,
            malformed=0,
            acked_position=None,
            failure=HandlerFailure(
                position=None, event_id=None, error=repr(exc), while_reading=True
            ),
        )

    # islice, not enumerate+break: a bounded pass must consume EXACTLY `limit`
    # events from the source — pulling one extra means a wasted file read (or,
    # at cutover, a wasted outbox fetch) discarded and re-fetched every pass.
    # A negative limit clamps to an empty pass (islice would raise ValueError,
    # and a driver whose dynamic bite size went negative asked for nothing,
    # not for a crash).
    events = itertools.islice(
        source.read(after=after), max(0, limit) if limit is not None else None
    )
    while True:
        # The source iterator is guarded too: it can raise lazily on any pull
        # (an invalid fixture directory, a file deleted mid-pass, a dropped
        # outbox connection at cutover). That is a READ failure — no event in
        # hand, nothing skipped; the next pass re-reads from the same cursor.
        try:
            raw = next(events)
        except StopIteration:
            break
        except Exception as exc:
            log.exception(
                "event source %s read failed — pass stopped, cursor unmoved",
                source_key,
            )
            failure = HandlerFailure(
                position=None, event_id=None, error=repr(exc), while_reading=True
            )
            break

        parsed = parse_envelope(raw.body, ref=raw.ref, position=raw.position)
        is_malformed = isinstance(parsed, MalformedEnvelope)

        if is_malformed:
            # Advance-past policy: reported, not handled. See the module
            # docstring on why a malformed envelope must not wedge the stream.
            failure = attempt(
                lambda: report(parsed),
                "malformed-event report",
                position=raw.position,
                event_id=parsed.event_id,
                while_reporting_malformed=True,
            )
        else:
            failure = attempt(
                lambda: handler(parsed),
                f"intake handler (event {parsed.event_id})",
                position=raw.position,
                event_id=parsed.event_id,
            )
        if failure is not None:
            break

        failure = attempt(
            lambda: cursor_store.ack(source_key, raw.position, parsed.event_id),
            "cursor ack",
            position=raw.position,
            event_id=parsed.event_id,
            while_acking=True,
        )
        if failure is not None:
            break

        # Counted only once acked — the counts are "events this pass is DONE
        # with", and an event whose ack failed re-delivers next pass.
        if is_malformed:
            malformed += 1
        else:
            delivered += 1
        acked_position = raw.position

    return IntakeResult(
        source_key=source_key,
        delivered=delivered,
        malformed=malformed,
        acked_position=acked_position,
        failure=failure,
    )

"""The intake loop (spec §5): ordering, ack-after-handle, resumption, and the
malformed path.

Runs against the shipped capture through `FixturesEventSource` and an
`InMemoryCursorStore` — the loop's contract is about WHEN it acks, not where
the cursor is stored, and `test_cursors.py` covers the persistence. The
recurring assertion here is the at-least-once rule: nothing is acked before
the handler that consumed it returned.
"""

from __future__ import annotations

import logging

import pytest

from snowline_marketing.cursors import InMemoryCursorStore
from snowline_marketing.events import EventEnvelope, MalformedEnvelope, parse_envelope
from snowline_marketing.intake import run_intake
from snowline_marketing.sources import FixturesEventSource, RawEvent, fixture_files


class Collector:
    """A handler that records what it was given, and can be told to fail on a
    chosen event id (the crash-before-ack case)."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.seen: list[EventEnvelope] = []
        self.fail_on = fail_on

    def __call__(self, envelope: EventEnvelope) -> None:
        if envelope.event_id == self.fail_on:
            raise RuntimeError(f"handler blew up on {envelope.event_id}")
        self.seen.append(envelope)

    @property
    def ids(self) -> list[str]:
        return [e.event_id for e in self.seen]


def _capture(directory) -> list[tuple[str, object]]:
    """The shipped capture as (position, parsed) pairs in stream order — what
    the loop SHOULD see, derived independently of the loop."""
    return [
        (path.name, parse_envelope(path.read_bytes(), position=path.name))
        for path in fixture_files(directory)
    ]


def _valid_ids(directory, *, start: int = 0) -> list[str]:
    return [
        parsed.event_id
        for _, parsed in _capture(directory)[start:]
        if isinstance(parsed, EventEnvelope)
    ]


def _positions(directory) -> list[str]:
    return [position for position, _ in _capture(directory)]


def _first_malformed_position(directory) -> str:
    return next(
        position
        for position, parsed in _capture(directory)
        if isinstance(parsed, MalformedEnvelope)
    )


def _quietly(**kwargs):
    """Default kwargs for passes that are not testing the malformed sink — a
    no-op sink keeps the warning log out of unrelated tests."""
    return {"on_malformed": lambda malformed: None, **kwargs}


def test_delivers_every_valid_envelope_in_stream_order(event_fixtures_dir):
    handler = Collector()
    result = run_intake(
        FixturesEventSource(event_fixtures_dir),
        handler,
        cursor_store=InMemoryCursorStore(),
        **_quietly(),
    )
    assert handler.ids == _valid_ids(event_fixtures_dir)
    assert result.delivered == len(handler.ids)
    assert result.ok


def test_acks_the_last_position_after_a_clean_pass(event_fixtures_dir):
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)
    result = run_intake(source, Collector(), cursor_store=store, **_quietly())
    last = _positions(event_fixtures_dir)[-1]
    assert store.read(source.source_key) == last
    assert result.acked_position == last


def test_a_second_pass_delivers_nothing(event_fixtures_dir):
    # Resumption: the cursor is the whole memory of what has been consumed.
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)
    run_intake(source, Collector(), cursor_store=store, **_quietly())
    second = run_intake(source, Collector(), cursor_store=store, **_quietly())
    assert second.delivered == 0
    assert second.malformed == 0
    assert second.acked_position is None
    assert second.ok


def test_resumes_past_acked_events(event_fixtures_dir):
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)
    first = Collector()
    run_intake(source, first, cursor_store=store, limit=3, **_quietly())
    assert store.read(source.source_key) == _positions(event_fixtures_dir)[2]

    second = Collector()
    run_intake(source, second, cursor_store=store, **_quietly())
    # Exactly the tail, exactly once, and the two passes partition the stream.
    assert second.ids == _valid_ids(event_fixtures_dir, start=3)
    assert first.ids + second.ids == _valid_ids(event_fixtures_dir)


def test_handler_failure_stops_the_pass_and_does_not_ack(event_fixtures_dir):
    # The crash-before-ack case: the failing event stays un-acked so the next
    # pass re-delivers it (at-least-once). The delivery ledger — a later item —
    # is what makes that re-delivery idempotent.
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)
    doomed = _valid_ids(event_fixtures_dir)[1]
    doomed_position = next(
        position
        for position, parsed in _capture(event_fixtures_dir)
        if isinstance(parsed, EventEnvelope) and parsed.event_id == doomed
    )

    handler = Collector(fail_on=doomed)
    result = run_intake(source, handler, cursor_store=store, **_quietly())

    assert not result.ok
    assert result.failure.event_id == doomed
    assert result.failure.position == doomed_position
    assert not result.failure.while_reporting_malformed
    assert "RuntimeError" in result.failure.error
    # The failing event was neither handled nor acked, and nothing behind it
    # was consumed.
    assert doomed not in handler.ids
    assert store.read(source.source_key) == result.acked_position
    assert store.read(source.source_key) < doomed_position


def test_the_failed_event_is_redelivered_next_pass(event_fixtures_dir):
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)
    doomed = _valid_ids(event_fixtures_dir)[1]

    run_intake(source, Collector(fail_on=doomed), cursor_store=store, **_quietly())
    recovered = Collector()
    result = run_intake(source, recovered, cursor_store=store, **_quietly())

    assert recovered.ids[0] == doomed
    assert result.ok


def test_malformed_envelopes_are_reported_and_do_not_stop_the_pass(event_fixtures_dir):
    reported: list[MalformedEnvelope] = []
    result = run_intake(
        FixturesEventSource(event_fixtures_dir),
        Collector(),
        cursor_store=InMemoryCursorStore(),
        on_malformed=reported.append,
    )
    assert result.ok
    assert result.malformed == len(reported) > 0
    # Every report is self-contained enough for the quarantine store (a later
    # item) to persist without asking the loop anything.
    for malformed in reported:
        assert malformed.position
        assert malformed.ref
        assert malformed.detail
        assert malformed.raw


def test_malformed_envelopes_are_not_delivered_to_the_handler(event_fixtures_dir):
    handler = Collector()
    run_intake(
        FixturesEventSource(event_fixtures_dir),
        handler,
        cursor_store=InMemoryCursorStore(),
        **_quietly(),
    )
    assert handler.ids == _valid_ids(event_fixtures_dir)


def test_malformed_envelopes_advance_the_cursor(event_fixtures_dir):
    # The documented v1 policy (see intake.py): a permanently-unparseable
    # envelope must not wedge the stream behind it forever, so the loop reports
    # it and acks PAST it. The report is the durable handoff to quarantine —
    # it is not acked as *handled*, it is acked as *seen*.
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)
    first_bad = _first_malformed_position(event_fixtures_dir)
    through_first_bad = _positions(event_fixtures_dir).index(first_bad) + 1

    result = run_intake(
        source,
        Collector(),
        cursor_store=store,
        limit=through_first_bad,
        **_quietly(),
    )
    assert result.malformed == 1
    assert store.read(source.source_key) == first_bad
    # ...and the next pass does not re-deliver it.
    reported: list[MalformedEnvelope] = []
    run_intake(source, Collector(), cursor_store=store, on_malformed=reported.append)
    assert first_bad not in [m.position for m in reported]


def test_a_failing_malformed_report_stops_the_pass_unacked(event_fixtures_dir):
    # A quarantine sink that cannot write is not a reason to sail past a
    # malformed event as though it had been recorded.
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)

    def explode(malformed: MalformedEnvelope) -> None:
        raise OSError("quarantine store unavailable")

    result = run_intake(source, Collector(), cursor_store=store, on_malformed=explode)

    assert not result.ok
    assert result.failure.while_reporting_malformed
    first_bad = _first_malformed_position(event_fixtures_dir)
    assert result.failure.position == first_bad
    assert store.read(source.source_key) != first_bad


def test_malformed_events_warn_by_default(event_fixtures_dir, caplog):
    # Until the quarantine store lands, the default sink must at least be loud:
    # an event the plugin could not understand is an event whose policies may
    # never have run.
    with caplog.at_level(logging.WARNING, logger="snowline_marketing.intake"):
        result = run_intake(
            FixturesEventSource(event_fixtures_dir),
            Collector(),
            cursor_store=InMemoryCursorStore(),
        )
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == result.malformed > 0


def test_limit_bounds_a_pass_and_leaves_the_rest(event_fixtures_dir):
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)
    first = run_intake(source, Collector(), cursor_store=store, limit=2, **_quietly())
    assert first.delivered + first.malformed == 2
    assert store.read(source.source_key) == _positions(event_fixtures_dir)[1]

    rest = run_intake(source, Collector(), cursor_store=store, **_quietly())
    assert rest.delivered + rest.malformed == len(_positions(event_fixtures_dir)) - 2


def test_an_empty_source_is_a_no_op(tmp_path):
    store = InMemoryCursorStore()
    result = run_intake(FixturesEventSource(tmp_path), Collector(), cursor_store=store)
    assert result.delivered == 0
    assert result.malformed == 0
    assert result.acked_position is None
    assert result.ok


def test_loop_is_source_agnostic():
    """The loop knows `EventSource`, not fixtures: a source of already-decoded
    rows keyed by event id — the shape the live PM outbox will have — drives it
    unchanged. That is the whole point of the seam (spec §5)."""

    class ListSource:
        source_key = "outbox:pm"

        def __init__(self, rows):
            self.rows = rows

        def read(self, *, after=None):
            for position, body in self.rows:
                if after is not None and position <= after:
                    continue
                yield RawEvent(position=position, body=body, ref=f"outbox/{position}")

    body = {
        "schema_version": 1,
        "event_id": "pm-evt-9001",
        "event_type": "item_completed",
        "tenant": "turtlesedge",
        "occurred_at": "2026-07-20T12:00:00+00:00",
        "subject": {"kind": "work_item", "id": "3f1c9a20"},
        "payload": {"scope": "turtlesedge/turtletracks"},
    }
    store = InMemoryCursorStore()
    handler = Collector()
    result = run_intake(
        ListSource([("pm-evt-9001", body)]), handler, cursor_store=store
    )

    assert result.delivered == 1
    assert handler.ids == ["pm-evt-9001"]
    # For a source whose ids ARE its positions, the cursor and the dedup key
    # coincide — spec §5's "acknowledged by stable event id".
    assert store.read("outbox:pm") == "pm-evt-9001"
    assert store.last_event_id("outbox:pm") == "pm-evt-9001"


@pytest.mark.parametrize("passes", [1, 2, 3])
def test_repeated_passes_deliver_each_event_exactly_once(event_fixtures_dir, passes):
    # However many times the loop runs, each event reaches the handler once —
    # the cursor is what decides, not the handler's memory.
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)
    delivered: list[str] = []
    for _ in range(passes):
        handler = Collector()
        run_intake(source, handler, cursor_store=store, **_quietly())
        delivered.extend(handler.ids)
    assert delivered == _valid_ids(event_fixtures_dir)


def test_cursor_is_persisted_between_passes(migrated_db, event_fixtures_dir):
    """DB-backed end-to-end: resumption across processes is what the cursor
    table exists for, so the loop is exercised once against the real store."""
    from snowline_marketing.cursors import DbCursorStore

    source = FixturesEventSource(event_fixtures_dir, source_key="fixtures:intake-test")
    first = Collector()
    run_intake(source, first, cursor_store=DbCursorStore(), limit=3, **_quietly())

    # A FRESH store instance, as a restarted process would build.
    second = Collector()
    run_intake(source, second, cursor_store=DbCursorStore(), **_quietly())

    assert first.ids + second.ids == _valid_ids(event_fixtures_dir)
    assert (
        DbCursorStore().read("fixtures:intake-test")
        == (_positions(event_fixtures_dir)[-1])
    )

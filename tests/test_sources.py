"""The fixtures event source (spec §5) and the shipped capture.

Fixtures mode is a first-class dev/CI surface, so the shipped capture is under
test like any other code: every v1 event type must be represented, every file
must classify the way its NAME claims, and iteration must be ordered by the
file name rather than by whatever the filesystem happens to enumerate.

Naming convention the tests rely on: a fixture whose name contains
`-malformed-` is a deliberate bad envelope; every other fixture must parse.
That keeps the expected classification visible in a directory listing instead
of buried in an assertion list here.
"""

from __future__ import annotations

import json

import pytest

from snowline_marketing.events import (
    EventEnvelope,
    EventType,
    MalformedEnvelope,
    MalformedReason,
)
from snowline_marketing.sources import (
    FixturesEventSource,
    fixture_files,
    iter_fixture_envelopes,
)


def _is_malformed_fixture(name: str) -> bool:
    return "-malformed-" in name


def test_fixture_files_are_ordered_by_name(event_fixtures_dir):
    names = [p.name for p in fixture_files(event_fixtures_dir)]
    assert names == sorted(names)
    assert names, "the shipped capture must not be empty"
    # The numeric prefix is what makes name order equal stream order — a
    # fixture added without one would sort somewhere arbitrary.
    assert all(name[:4].isdigit() for name in names), names


def test_every_shipped_fixture_classifies_as_its_name_claims(event_fixtures_dir):
    # `strict=True` also pins the two read APIs to the same files in the same
    # order — the library API and the source must not diverge.
    for path, parsed in zip(
        fixture_files(event_fixtures_dir),
        iter_fixture_envelopes(event_fixtures_dir),
        strict=True,
    ):
        if _is_malformed_fixture(path.name):
            assert isinstance(parsed, MalformedEnvelope), path.name
            assert parsed.detail, f"{path.name} must carry an operator-visible reason"
            # Locators come through, so the quarantine row can point at the file.
            assert parsed.position == path.name
            assert parsed.ref == str(path)
        else:
            assert isinstance(parsed, EventEnvelope), (
                f"{path.name} should parse: {getattr(parsed, 'detail', '')}"
            )


def test_capture_covers_every_v1_event_type(event_fixtures_dir):
    # The point of the capture: the deterministic core is built and tested
    # fixtures-first (spec §5), which is only true if the fixtures exercise the
    # whole v1 vocabulary.
    seen = {
        parsed.event_type
        for parsed in iter_fixture_envelopes(event_fixtures_dir)
        if isinstance(parsed, EventEnvelope)
    }
    assert seen == set(EventType)


def test_capture_covers_the_malformed_classes(event_fixtures_dir):
    malformed = [
        parsed
        for parsed in iter_fixture_envelopes(event_fixtures_dir)
        if isinstance(parsed, MalformedEnvelope)
    ]
    assert len(malformed) >= 2
    # Both transport-level (unreadable bytes) and contract-level (readable but
    # wrong) failures are represented — they reach quarantine by different
    # paths and an operator sees different reasons.
    reasons = {m.reason for m in malformed}
    assert MalformedReason.not_json in reasons
    assert MalformedReason.invalid_envelope in reasons


def test_source_reads_every_event_in_order(event_fixtures_dir):
    source = FixturesEventSource(event_fixtures_dir)
    positions = [raw.position for raw in source.read()]
    assert positions == [p.name for p in fixture_files(event_fixtures_dir)]


def test_source_read_is_replayable(event_fixtures_dir):
    # At-least-once (spec §5) requires an un-acked event to come back: reading
    # twice from the same position yields the same stream.
    source = FixturesEventSource(event_fixtures_dir)
    first = [raw.position for raw in source.read()]
    second = [raw.position for raw in source.read()]
    assert first == second


def test_source_resumes_strictly_after_a_position(event_fixtures_dir):
    source = FixturesEventSource(event_fixtures_dir)
    all_positions = [raw.position for raw in source.read()]
    resumed = [raw.position for raw in source.read(after=all_positions[2])]
    assert resumed == all_positions[3:]


def test_source_at_the_last_position_yields_nothing(event_fixtures_dir):
    source = FixturesEventSource(event_fixtures_dir)
    last = [raw.position for raw in source.read()][-1]
    assert list(source.read(after=last)) == []


def test_source_key_is_stable_and_not_a_path(event_fixtures_dir):
    # A cursor row keyed by an absolute path would silently restart from zero
    # on another checkout.
    source = FixturesEventSource(event_fixtures_dir)
    assert source.source_key == "fixtures:events"
    assert str(event_fixtures_dir) not in source.source_key
    assert FixturesEventSource(event_fixtures_dir, source_key="custom").source_key == (
        "custom"
    )


def test_source_yields_undecoded_bodies_with_locators(event_fixtures_dir):
    raw = next(iter(FixturesEventSource(event_fixtures_dir).read()))
    # Undecoded on purpose: a corrupt capture becomes a malformed CLASSIFICATION
    # in parse_envelope rather than an exception thrown mid-iteration.
    assert isinstance(raw.body, bytes)
    assert json.loads(raw.body)["event_type"]
    assert raw.ref.endswith(raw.position)


def test_source_ignores_non_fixture_and_dotfile_entries(tmp_path):
    (tmp_path / "0010-a.json").write_text("{}")
    (tmp_path / "notes.md").write_text("not an event")
    (tmp_path / "._0010-a.json").write_text("AppleDouble sibling")
    (tmp_path / "nested.json").mkdir()
    assert [p.name for p in fixture_files(tmp_path)] == ["0010-a.json"]


def test_empty_directory_is_an_empty_stream(tmp_path):
    source = FixturesEventSource(tmp_path)
    assert list(source.read()) == []
    assert list(iter_fixture_envelopes(tmp_path)) == []


def test_mixed_prefix_widths_fail_loudly(tmp_path):
    # '10000-' sorts lexicographically before '2000-', so a mixed-width capture
    # would deliver out of order and silently skip events on resume — that must
    # be a loud listing-time error, never a quiet loss.
    (tmp_path / "2000-a.json").write_text("{}")
    (tmp_path / "10000-b.json").write_text("{}")
    with pytest.raises(ValueError, match="mix widths"):
        fixture_files(tmp_path)


def test_prefixless_fixture_fails_loudly(tmp_path):
    (tmp_path / "no-prefix.json").write_text("{}")
    with pytest.raises(ValueError, match="numeric"):
        fixture_files(tmp_path)

"""Event SOURCES — where raw events come from, behind one interface.

Spec §5 makes fixtures mode a first-class dev/CI surface, not a shim: the
intake loop runs identically against captured JSON envelopes on disk and
against PM's durable lifecycle outbox (snowline-pm #64) when it lands. That
identity is this module's whole job — `EventSource` is the only seam the loop
knows, so the live-outbox cutover adds ONE class here and changes nothing in
`intake.py`, `events.py`, or the policy engine above them.

**Position vs event id.** A source yields `RawEvent`s carrying a POSITION: an
opaque, source-defined token that is monotonically increasing in that source's
iteration order. Spec §5 says delivery is "acknowledged by stable event id
against a cursor", and for the live outbox the position IS the event id (PM's
ids are monotone in outbox order, and the id is also the ledger's dedup key).
The fixtures source cannot use the event id: a malformed fixture may have no
id at all, and the cursor still has to advance past it (see
`intake.run_intake` on the malformed policy). So the fixtures source's
position is the FILE NAME, which exists whatever the file contains. The cursor
records both (position to resume from, event id for the audit trail).

**Fixtures are one JSON envelope per file, not a JSONL stream.** Both give
deterministic ordering; the per-file layout wins on the two things these
fixtures are actually for. (1) They are curated by hand and reviewed in pull
requests — a new case is an added file, not an edit inside a shared blob, and
a diff names the event type it changes. (2) A malformed fixture is
representable *as a unit*: an unparseable file is one bad event, whereas one
bad line in a JSONL stream makes "how much of this file is still readable" a
judgement call the intake loop should never have to make. Stream position
comes from a numeric filename prefix, so lexicographic filename order IS
stream order.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from snowline_marketing.events import ParsedEnvelope, parse_envelope

# Fixture files are `NNNN-slug.json`; the numeric prefix is what makes
# lexicographic name order equal stream order.
FIXTURE_SUFFIX = ".json"


@dataclass(frozen=True)
class RawEvent:
    """One undecoded event as its source handed it over.

    `body` is whatever the source has: JSON text/bytes (fixtures — deliberately
    undecoded, so a corrupt file classifies as malformed in `parse_envelope`
    rather than raising inside the iteration) or an already-decoded mapping
    (an outbox row). `ref` is a human locator for the operator-facing
    quarantine surface; `position` is the ack token (see module docstring)."""

    position: str
    body: object
    ref: str


class EventSource(Protocol):
    """The only thing the intake loop knows about where events come from.

    `source_key` names the cursor row (spec §4: per-source consumer cursors),
    so it must be STABLE across runs and machines — never a path or a pid.

    `read(after=...)` yields events strictly after `after` in the source's own
    order, oldest first, and must be replayable: an un-acked event is yielded
    again on the next call. That is the at-least-once contract (spec §5); the
    delivery ledger (§4) makes the resulting re-delivery idempotent, which is
    explicitly not this layer's problem."""

    source_key: str

    def read(self, *, after: str | None = None) -> Iterator[RawEvent]: ...


def fixture_files(directory: Path | str) -> list[Path]:
    """The fixture files of `directory`, in stream order.

    Sorted by NAME, not by whatever order the filesystem enumerates (which is
    neither stable across machines nor meaningful anywhere). Dot-prefixed
    files are skipped — macOS AppleDouble siblings (`._foo.json`) are not
    events."""
    path = Path(directory)
    return sorted(
        (
            p
            for p in path.glob(f"*{FIXTURE_SUFFIX}")
            if p.is_file() and not p.name.startswith(".")
        ),
        key=lambda p: p.name,
    )


def iter_fixture_envelopes(directory: Path | str) -> Iterator[ParsedEnvelope]:
    """Parse every fixture in `directory`, in stream order.

    The small library API over a fixtures directory: yields an `EventEnvelope`
    or a `MalformedEnvelope` per file, never raising on a bad one. This is the
    read surface for anything that wants the events without running the intake
    loop — the §11 dry-run ("evaluate a policy version against captured
    fixtures, mint nothing") is the intended second caller."""
    for path in fixture_files(directory):
        yield parse_envelope(path.read_bytes(), ref=str(path), position=path.name)


class FixturesEventSource:
    """An `EventSource` over a directory of captured JSON envelopes (spec §5).

    Position is the file name; `after` filtering is a plain `>` on it, which is
    correct exactly because the files are ordered by name (see module
    docstring). Files are read lazily, one at a time, so a large capture
    directory is streamed rather than slurped."""

    def __init__(self, directory: Path | str, *, source_key: str | None = None) -> None:
        self.directory = Path(directory)
        # Default key names the capture, NOT its path: an absolute path would
        # make the cursor row machine-specific, so the same capture replayed on
        # another checkout would silently start from zero. A caller running
        # several captures with the same directory name passes its own key.
        self.source_key = source_key or f"fixtures:{self.directory.name}"

    def read(self, *, after: str | None = None) -> Iterator[RawEvent]:
        for path in fixture_files(self.directory):
            if after is not None and path.name <= after:
                continue
            yield RawEvent(position=path.name, body=path.read_bytes(), ref=str(path))

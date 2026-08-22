"""The shared never-raises classification skeleton.

`events.parse_envelope` and `policies.parse_policy_set` are the same shape:
decode JSON text/bytes, require a JSON object, best-effort-extract an
identifying key, validate against a frozen model, and compact the validation
errors into an operator-readable line. The shape lives HERE, once — a decode
edge case hardened in one classifier must not have to be remembered in the
other (`UnicodeDecodeError` already had to be added to both before this module
existed). The classifiers keep their own result types, reason enums and
best-effort keys; what they share is the skeleton, not the vocabulary.

Shared validation-reporting helpers live here too (`duplicates`): the small
shapes several validators phrase the same refusal with, kept in one place so
the phrasing's inputs cannot drift between the modules that report them.
"""

from __future__ import annotations

import enum
import json
from collections import Counter
from collections.abc import Hashable, Iterable, Mapping
from typing import Any

from pydantic import ValidationError

# Detail lines are read by humans in a table cell; an `extra="forbid"`
# violation over a large body can produce one error per stray field, so the
# rendering is capped rather than unbounded.
MAX_REPORTED_ERRORS = 5

# How much of an offending value to quote (when quoting is on). Long enough to
# recognize a typo'd slug, short enough to stay in a table cell.
MAX_QUOTED_INPUT = 60


class DecodeFailure(enum.StrEnum):
    """Why raw bytes never made it to model validation. The values match the
    classifiers' own reason enums (`MalformedReason` / `MalformedPolicyReason`)
    so each maps this by value into its own vocabulary."""

    # The bytes were not JSON at all (a truncated capture, an artifact revised
    # to prose, a half-written file).
    not_json = "not_json"
    # Valid JSON, but not a JSON object (a bare list/string/number).
    not_an_object = "not_an_object"


def decode_json_object(
    raw: object,
) -> tuple[Mapping[str, Any] | None, tuple[DecodeFailure, str] | None]:
    """Decode `raw` to a JSON object, classifying instead of raising.

    Returns `(body, None)` on success or `(None, (failure, detail))` when the
    input never reached object shape. Accepts text/bytes (decoded here, so
    "not JSON" classifies in one place) or an already-decoded value."""
    body: object = raw
    if isinstance(body, (str, bytes, bytearray)):
        try:
            body = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return None, (DecodeFailure.not_json, str(exc))
    if not isinstance(body, Mapping):
        return None, (
            DecodeFailure.not_an_object,
            f"expected a JSON object, got {type(body).__name__}",
        )
    return body, None


def duplicates[T: Hashable](items: Iterable[T]) -> list[T]:
    """The values appearing more than once in `items`, sorted.

    The shared shape of every "each X exactly once" refusal — the payload
    validators in `provenance.py` and the store guard in
    `deliverables._validated_versions` all report exactly this list. Sorted so
    a detail line names the offenders deterministically whatever order the
    input listed them in."""
    counts = Counter(items)
    return sorted(item for item, count in counts.items() if count > 1)


def best_effort_str(body: Mapping[str, Any], key: str) -> str | None:
    """A pre-validation read of one identifying string field — the quarantine
    surface's "whose/which was it?" answer, which usually survives whatever
    else is wrong with the body. None when absent, non-string, or blank."""
    candidate = body.get(key)
    if not isinstance(candidate, str):
        return None
    return candidate.strip() or None


def _quote_input(err: dict[str, Any]) -> str:
    """The offending VALUE, when quoting it helps — used for hand-authored
    bodies (policies), where the operator's question is "which of my values is
    wrong?". Only scalars are quoted: a missing/extra-field error reports the
    whole surrounding object as its input, and pasting that into a table cell
    would bury the message."""
    if "input" not in err:
        return ""
    value = err["input"]
    if not isinstance(value, (str, int, float, bool)) and value is not None:
        return ""
    text = repr(value)
    if len(text) > MAX_QUOTED_INPUT:
        text = text[: MAX_QUOTED_INPUT - 3] + "..."
    return f" (got {text})"


def compact_errors(exc: ValidationError, *, quote_scalars: bool = False) -> str:
    """Render a ValidationError as one capped, operator-readable line.

    `quote_scalars` is the one divergence the classifiers keep: machine-
    produced bodies (events) need only the field path; hand-authored bodies
    (policies) also quote the offending scalar."""
    errors = exc.errors()
    parts = [
        f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
        f"{_quote_input(err) if quote_scalars else ''}"
        for err in errors[:MAX_REPORTED_ERRORS]
    ]
    if len(errors) > MAX_REPORTED_ERRORS:
        parts.append(f"(+{len(errors) - MAX_REPORTED_ERRORS} more)")
    return "; ".join(parts)

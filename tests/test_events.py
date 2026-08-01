"""The event envelope contract (spec §5): what parses, what quarantines.

No database and no filesystem — envelopes are validated in memory, so these
run everywhere. The house rule under test throughout: `parse_envelope` NEVER
raises. Malformed input is a returned, explained result, because malformed
events are an expected input class (spec §4 quarantine) rather than an
exception the intake loop has to defend itself against.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from snowline_marketing.events import (
    SCHEMA_VERSION,
    EntityKind,
    EventEnvelope,
    EventType,
    MalformedEnvelope,
    MalformedReason,
    parse_envelope,
)

TENANT = "turtlesedge"
SCOPE = "turtlesedge/turtletracks"


def _envelope(event_type: EventType, **overrides) -> dict:
    """A minimal VALID envelope for `event_type` — only what that type
    requires, so a test that mutates one field is testing that field."""
    base: dict = {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"pm-evt-{event_type.value}",
        "event_type": event_type.value,
        "tenant": TENANT,
        "occurred_at": "2026-07-20T12:00:00+00:00",
        "subject": {"kind": "work_item", "id": "3f1c9a20"},
        "payload": {"scope": SCOPE},
    }
    base.update(_TYPE_SPECIFIC[event_type])
    for key, value in overrides.items():
        base[key] = value
    return base


# The per-type shape each event REQUIRES (spec §5 subject refs + §6 predicate
# surface). Keyed by every EventType member — `test_every_v1_event_type_...`
# asserts the coverage, so a new event type cannot be added without landing
# here.
_TYPE_SPECIFIC: dict[EventType, dict] = {
    EventType.item_completed: {},
    EventType.item_reopened: {},
    EventType.item_abandoned: {},
    EventType.item_rescoped: {
        "payload": {"scope": SCOPE, "details": {"from_scope": "turtlesedge/legacy"}}
    },
    EventType.initiative_phase_completed: {
        "subject": {"kind": "initiative", "id": "8ad41b77", "phase": "build"},
        "payload": {"scope": SCOPE, "initiative": "summer-release", "phase": "build"},
    },
    EventType.milestone_state_changed: {
        "subject": {"kind": "milestone", "id": "ms-4c72a1"},
        "payload": {
            "scope": SCOPE,
            "milestone": "v1.4",
            "details": {"from_state": "planned", "to_state": "active"},
        },
    },
    EventType.milestone_released: {
        "subject": {"kind": "milestone", "id": "ms-4c72a1"},
        "payload": {"scope": SCOPE, "milestone": "v1.4"},
    },
    EventType.recurring_item_fired: {
        "subject": {"kind": "schedule", "id": "sched-monthly-metrics"},
    },
    EventType.semantic_signal: {
        "payload": {"scope": SCOPE, "signals": ["marketing-impact"]},
    },
}


@pytest.mark.parametrize("event_type", list(EventType))
def test_every_v1_event_type_parses(event_type):
    parsed = parse_envelope(_envelope(event_type))
    assert isinstance(parsed, EventEnvelope), parsed
    assert parsed.event_type is event_type


@pytest.mark.parametrize("event_type", list(EventType))
def test_every_v1_event_type_round_trips_through_json(event_type):
    # Envelopes are captured to disk and replayed (spec §5 fixtures mode) and
    # a quarantine row keeps the raw body — an envelope that cannot survive
    # dict -> model -> JSON -> model is not a wire contract.
    original = parse_envelope(_envelope(event_type))
    assert isinstance(original, EventEnvelope)
    again = parse_envelope(original.model_dump_json())
    assert again == original


def test_every_v1_event_type_has_a_sample():
    # Guards the parametrized tests above: adding an EventType without a shape
    # here would otherwise quietly test nothing for it.
    assert set(_TYPE_SPECIFIC) == set(EventType)


def test_envelope_fields_are_populated():
    parsed = parse_envelope(
        _envelope(
            EventType.item_completed,
            payload={
                "scope": SCOPE,
                "initiative": "summer-release",
                "phase": "build",
                "milestone": "v1.4",
                "work_kind": "implementation",
                "relations": [{"kind": "marketing-impact", "target": "8ad41b77"}],
                "signals": ["marketing-impact"],
                "external_refs": [
                    {"kind": "pull_request", "url": "https://example.test/pull/1"}
                ],
                "details": {"title": "Offline trip log sync"},
            },
        )
    )
    assert isinstance(parsed, EventEnvelope)
    assert parsed.event_id == "pm-evt-item_completed"
    assert parsed.tenant == TENANT
    assert parsed.subject.kind is EntityKind.work_item
    assert parsed.subject.id == "3f1c9a20"
    assert parsed.occurred_at == datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
    # The §6 predicate surface, all first-class.
    assert parsed.payload.scope == SCOPE
    assert parsed.payload.initiative == "summer-release"
    assert parsed.payload.phase == "build"
    assert parsed.payload.milestone == "v1.4"
    assert parsed.payload.work_kind == "implementation"
    assert [r.kind for r in parsed.payload.relations] == ["marketing-impact"]
    assert parsed.payload.signals == ("marketing-impact",)
    assert parsed.payload.external_refs[0].url == "https://example.test/pull/1"
    # Type-specific facts stay out of the predicate surface.
    assert parsed.payload.details["title"] == "Offline trip log sync"


def test_envelope_is_immutable():
    # An envelope records something that already happened; a policy must not be
    # able to hand the next policy a mutated event.
    parsed = parse_envelope(_envelope(EventType.item_completed))
    assert isinstance(parsed, EventEnvelope)
    with pytest.raises(Exception):
        parsed.event_id = "rewritten"
    with pytest.raises(TypeError):
        parsed.payload.details["injected"] = True


def test_non_utc_offset_is_preserved_not_normalized_away():
    # tz-aware is the requirement, UTC is not: a producer in another offset is
    # legitimate, and the instant is what ordering compares.
    parsed = parse_envelope(
        _envelope(EventType.item_completed, occurred_at="2026-07-20T14:00:00+02:00")
    )
    assert isinstance(parsed, EventEnvelope)
    assert parsed.occurred_at.utcoffset() == timedelta(hours=2)
    assert parsed.occurred_at == datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


# --- malformed classification ------------------------------------------------
# `field` is a substring the operator-facing detail must name: a quarantine
# reason that does not say WHICH field is wrong is not actionable.
MALFORMED_CASES = [
    ("missing event_id", {"event_id": None}, "event_id"),
    ("empty event_id", {"event_id": "   "}, "event_id"),
    ("unknown event type", {"event_type": "item_archived"}, "event_type"),
    ("naive timestamp", {"occurred_at": "2026-07-20T12:00:00"}, "occurred_at"),
    ("unparseable timestamp", {"occurred_at": "last thursday"}, "occurred_at"),
    ("wrong schema_version", {"schema_version": 2}, "schema_version"),
    ("missing schema_version", {"schema_version": None}, "schema_version"),
    ("missing tenant", {"tenant": None}, "tenant"),
    ("missing subject", {"subject": None}, "subject"),
    ("unknown subject kind", {"subject": {"kind": "epic", "id": "x"}}, "kind"),
    ("missing payload scope", {"payload": {}}, "scope"),
    ("unknown envelope field", {"org": "turtlesedge"}, "org"),
]


@pytest.mark.parametrize(
    "label,mutation,field", MALFORMED_CASES, ids=[c[0] for c in MALFORMED_CASES]
)
def test_malformed_envelopes_classify_with_a_reason(label, mutation, field):
    body = _envelope(EventType.item_completed)
    for key, value in mutation.items():
        if value is None:
            body.pop(key, None)
        else:
            body[key] = value
    parsed = parse_envelope(body, ref="unit-test")
    assert isinstance(parsed, MalformedEnvelope), f"{label} should be malformed"
    assert parsed.reason is MalformedReason.invalid_envelope
    assert field in parsed.detail, parsed.detail
    # The raw body is kept whole — the quarantine requeue verb (spec §4)
    # replays what arrived, not a normalized copy.
    assert parsed.raw is body
    assert parsed.ref == "unit-test"


# Type-specific requirements: an event that cannot drive its own policies is
# malformed, NOT a silently unmatched event.
SHAPE_CASES = [
    (
        "completion whose subject is a milestone",
        EventType.item_completed,
        {"subject": {"kind": "milestone", "id": "ms-1"}},
        "subject of kind",
    ),
    (
        "milestone_released with no milestone",
        EventType.milestone_released,
        {"payload": {"scope": SCOPE}},
        "payload.milestone",
    ),
    (
        "milestone_state_changed with no to_state",
        EventType.milestone_state_changed,
        {"payload": {"scope": SCOPE, "milestone": "v1.4", "details": {}}},
        "payload.details.to_state",
    ),
    (
        "item_rescoped with no from_scope",
        EventType.item_rescoped,
        {"payload": {"scope": SCOPE}},
        "payload.details.from_scope",
    ),
    (
        "semantic_signal with no signal",
        EventType.semantic_signal,
        {"payload": {"scope": SCOPE, "signals": []}},
        "signals",
    ),
    (
        "phase completion whose two phase names disagree",
        EventType.initiative_phase_completed,
        {
            "payload": {
                "scope": SCOPE,
                "initiative": "summer-release",
                "phase": "launch",
            }
        },
        "must agree",
    ),
]


@pytest.mark.parametrize(
    "label,event_type,mutation,detail",
    SHAPE_CASES,
    ids=[c[0] for c in SHAPE_CASES],
)
def test_type_specific_shape_violations_are_malformed(
    label, event_type, mutation, detail
):
    parsed = parse_envelope(_envelope(event_type, **mutation))
    assert isinstance(parsed, MalformedEnvelope), f"{label} should be malformed"
    assert detail in parsed.detail, parsed.detail


def test_malformed_keeps_a_best_effort_event_id():
    # A quarantine row wants the id even when the envelope failed for some
    # unrelated reason — that is what an operator searches by.
    body = _envelope(EventType.item_completed, occurred_at="nonsense")
    parsed = parse_envelope(body)
    assert isinstance(parsed, MalformedEnvelope)
    assert parsed.event_id == "pm-evt-item_completed"


def test_malformed_event_id_is_none_when_unusable():
    body = _envelope(EventType.item_completed)
    body["event_id"] = 17
    parsed = parse_envelope(body)
    assert isinstance(parsed, MalformedEnvelope)
    assert parsed.event_id is None


def test_unparseable_json_is_malformed_not_an_exception():
    parsed = parse_envelope('{"schema_version": 1, "event_id": "trunc', ref="capture")
    assert isinstance(parsed, MalformedEnvelope)
    assert parsed.reason is MalformedReason.not_json
    assert parsed.detail
    assert parsed.ref == "capture"


@pytest.mark.parametrize("body", ["[]", "42", '"a string"', "null"])
def test_json_that_is_not_an_object_is_malformed(body):
    parsed = parse_envelope(body)
    assert isinstance(parsed, MalformedEnvelope)
    assert parsed.reason is MalformedReason.not_an_object


def test_parse_accepts_bytes_and_decoded_mappings_alike():
    # The fixtures source hands over undecoded file bytes; the live outbox will
    # hand over decoded rows. Both must reach the same envelope.
    body = _envelope(EventType.item_completed)
    from_mapping = parse_envelope(body)
    from_bytes = parse_envelope(json.dumps(body).encode("utf-8"))
    assert isinstance(from_mapping, EventEnvelope)
    assert from_bytes == from_mapping


def test_locators_ride_through_to_the_malformed_record():
    parsed = parse_envelope("not json", ref="/captures/0010.json", position="0010.json")
    assert isinstance(parsed, MalformedEnvelope)
    assert parsed.ref == "/captures/0010.json"
    assert parsed.position == "0010.json"

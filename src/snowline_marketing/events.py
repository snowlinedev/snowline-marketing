"""The marketing event envelope — the versioned wire contract for intake
(spec §5).

Everything the plugin consumes arrives as ONE shape: a JSON envelope carrying
a stable event id, an event type, the tenant scope it belongs to, when it
happened, the subject entity it happened to, and a payload holding exactly the
facts §6 policies predicate on. The envelope is the seam between the event
PRODUCER (captured fixtures today, PM's durable lifecycle outbox at cutover —
snowline-pm #64) and the deterministic core: the policy engine, delivery
ledger and minting (spec §6-§7) read envelopes, never source-specific rows.
Adding the live source must add a source, not a second event vocabulary.

Constraints this module encodes, and why:

- `event_id` is the at-least-once DEDUP KEY — the delivery ledger's logical
  key is `tenant + policy_id + event_id` (spec §4). Nothing here dedups (that
  is the ledger's job), but the key must be present and non-empty by the time
  an envelope reaches a policy, so it is required.
- The PREDICATE SURFACE is uniform across event types. §6 predicates are
  declarative data — values and globs over scope, initiative, phase,
  milestone, work kind, relations and semantic signals — written against one
  field set, so those live as first-class payload fields on every event type.
  Per-type variation (state transitions, abandon reasons, the schedule that
  fired) lives in the free-form `details` map, which no predicate reads. That
  keeps the policy language finite while leaving room for facts a consequence
  template wants to quote.
- Type-specific REQUIREMENTS are enforced here (a milestone event must name a
  milestone; a phase-completion must name its initiative and phase; a
  semantic-signal event must carry at least one signal), so a policy never
  has to defend itself against a shapeless event. A violation is malformed,
  not a silently-unmatched event.
- `payload.scope` is required on every event type. Routing is isolation-safe
  by contract (spec §14) and an event that names no scope cannot be routed
  safely; `tenant` alone is the org boundary, not the project one. An event
  without a scope quarantines rather than routing somewhere plausible.
- Every model is `extra="forbid"`. A producer that grew a field we silently
  drop is exactly the drift quarantine exists to surface (spec §4), and
  `schema_version` is the intended evolution knob — an additive producer
  change bumps it and lands here deliberately, rather than half-arriving.
- `occurred_at` must be timezone-aware. A naive timestamp is ambiguous the
  moment it crosses a process boundary, and staleness comparisons (§8) are
  ordering arguments; the plugin never guesses a producer's timezone.
- Validation NEVER raises through. `parse_envelope` returns either an
  `EventEnvelope` or a `MalformedEnvelope`: malformed input is an expected
  input class with an operator-visible reason (spec §4 quarantine), not an
  exception. The quarantine STORE is a later item — this module produces the
  record that item will persist.
"""

from __future__ import annotations

import enum
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)

# The envelope's own version. Bumped only when the ENVELOPE shape changes
# incompatibly; a producer speaking any other version is malformed here rather
# than best-effort-parsed, so a version skew is visible in quarantine instead
# of silently half-understood.
SCHEMA_VERSION = 1

# Identifiers are refs, not prose: whitespace-trimmed and never empty. An
# empty-string id would pass a bare `str` and then poison the ledger's dedup
# key with a value that collides across unrelated events.
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class EventType(enum.StrEnum):
    """The v1 consumed event vocabulary (spec §5). Values are the wire
    strings; the set is closed — an unrecognized type is malformed (an
    "unmapped event", spec §4), never a no-op ignore, because an event we
    cannot name is an event whose policies we cannot know we missed."""

    item_completed = "item_completed"
    item_reopened = "item_reopened"
    item_abandoned = "item_abandoned"
    item_rescoped = "item_rescoped"
    initiative_phase_completed = "initiative_phase_completed"
    milestone_state_changed = "milestone_state_changed"
    milestone_released = "milestone_released"
    recurring_item_fired = "recurring_item_fired"
    semantic_signal = "semantic_signal"


class EntityKind(enum.StrEnum):
    """What the event happened TO — the subject ref's kind (spec §5)."""

    work_item = "work_item"
    initiative = "initiative"
    milestone = "milestone"
    schedule = "schedule"


class _Model(BaseModel):
    """Shared model config: frozen (an envelope is a record of something that
    already happened — nothing downstream may edit one in place and hand the
    mutation to the next policy) and `extra="forbid"` (see module docstring)."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class EntityRef(_Model):
    """The subject entity. `phase` is set only for initiative+phase subjects —
    PM's phase is a name within an initiative, not an entity with its own id,
    so it rides on the initiative ref rather than being a fifth kind."""

    kind: EntityKind
    id: NonEmptyStr
    phase: NonEmptyStr | None = None


class Relation(_Model):
    """One entry of the event's relation set. The relation KIND is an open
    string, not an enum: §9's compatibility path reads an explicit
    `marketing-impact` relation until PM exposes a first-class semantic
    signal, and tenants may key policies on relation kinds this plugin has
    never heard of. `target` is optional — a relation's existence is itself
    the predicate in the §9 path."""

    kind: NonEmptyStr
    target: NonEmptyStr | None = None


class ExternalRef(_Model):
    """A reconciled external ref (spec §7: PR / release URL). `kind` is an
    open string for the same reason as `Relation.kind` — PM's reconciliation
    vocabulary grows on PM's schedule, not this plugin's."""

    kind: NonEmptyStr
    url: NonEmptyStr


class EventPayload(_Model):
    """The predicate surface (spec §6) plus a free-form `details` map.

    Every field above `details` is something a policy predicate may key on;
    `details` is everything a consequence TEMPLATE might quote but no
    predicate reads (spec §7 provenance). Keeping the split explicit is what
    stops the policy language from growing a new operator per event type."""

    # Required: routing is isolation-safe (spec §14) — see module docstring.
    scope: NonEmptyStr
    initiative: NonEmptyStr | None = None
    phase: NonEmptyStr | None = None
    milestone: NonEmptyStr | None = None
    work_kind: NonEmptyStr | None = None
    # Tuples, not lists: the model is frozen, and a mutable default would let
    # one policy's evaluation mutate the set the next policy predicates on.
    relations: tuple[Relation, ...] = ()
    signals: tuple[NonEmptyStr, ...] = ()
    external_refs: tuple[ExternalRef, ...] = ()
    # `validate_default` so the omitted case is frozen too — an unvalidated
    # default would leave exactly one mutable dict on otherwise-immutable
    # envelopes, which is the copy someone would eventually write into.
    details: Mapping[str, Any] = Field(default_factory=dict, validate_default=True)

    @field_validator("details", mode="after")
    @classmethod
    def _freeze_details(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        # `details` is deliberately unschema'd, which would otherwise be the
        # one mutable hole in a frozen envelope. A read-only proxy keeps the
        # record immutable without forcing every type-specific fact through a
        # model this item cannot yet know the shape of.
        return MappingProxyType(dict(value))

    @field_serializer("details")
    def _unfreeze_details(self, value: Mapping[str, Any]) -> dict[str, Any]:
        # The read-only proxy is an in-process guard, not a wire type: pydantic
        # has no serializer for `mappingproxy`, and an envelope that cannot
        # round-trip through JSON is useless to a capture or a quarantine row.
        return dict(value)


# Declared UNHASHABLE, introspectably: `frozen=True` would otherwise generate
# a by-fields hash that raises at runtime (`details` is a mappingproxy over
# arbitrary JSON), while still advertising `collections.abc.Hashable` — so a
# consumer gating on Hashable before caching would accept the payload and
# crash deep inside its container. `__hash__ = None` is the standard contract:
# the isinstance check says no up front. The payload has no identity of its
# own — hash the EventEnvelope.
EventPayload.__hash__ = None  # type: ignore[assignment]


# Which subject kinds each event type may legally have. A completion event
# whose subject is a milestone is a producer bug, and mapping it into a policy
# would mint work against the wrong entity.
_ALLOWED_SUBJECT_KINDS: dict[EventType, frozenset[EntityKind]] = {
    EventType.item_completed: frozenset({EntityKind.work_item}),
    EventType.item_reopened: frozenset({EntityKind.work_item}),
    EventType.item_abandoned: frozenset({EntityKind.work_item}),
    EventType.item_rescoped: frozenset({EntityKind.work_item}),
    EventType.initiative_phase_completed: frozenset({EntityKind.initiative}),
    EventType.milestone_state_changed: frozenset({EntityKind.milestone}),
    EventType.milestone_released: frozenset({EntityKind.milestone}),
    EventType.recurring_item_fired: frozenset({EntityKind.schedule}),
    # A marketing-impact signal can be raised on an item or on the initiative
    # that item belongs to (spec §9) — both are things a policy targets.
    EventType.semantic_signal: frozenset({EntityKind.work_item, EntityKind.initiative}),
}

# Payload fields without which the event cannot drive its own policies.
_REQUIRED_PAYLOAD_FIELDS: dict[EventType, tuple[str, ...]] = {
    EventType.initiative_phase_completed: ("initiative", "phase"),
    EventType.milestone_state_changed: ("milestone",),
    EventType.milestone_released: ("milestone",),
}

# `details` keys required per type — the type-specific facts that carry the
# event's actual news. A state change that does not say what it changed TO,
# or a re-scope that does not say where it came FROM, is not a usable event:
# the second is what makes a cross-scope move auditable at all.
_REQUIRED_DETAIL_KEYS: dict[EventType, tuple[str, ...]] = {
    EventType.milestone_state_changed: ("to_state",),
    EventType.item_rescoped: ("from_scope",),
}


class EventEnvelope(_Model):
    """One consumed event (spec §5)."""

    # A Literal, not an int compared later: a v2 producer's envelope fails
    # validation at the same seam as any other shape violation, so version
    # skew reaches quarantine through one path.
    schema_version: Literal[1]
    # Stable across re-delivery — the ledger's dedup key (spec §4).
    event_id: NonEmptyStr
    event_type: EventType
    # The isolating org scope (spec §6: one policy-set artifact per tenant org
    # scope). Distinct from `payload.scope`, which is the project scope the
    # subject lives on.
    tenant: NonEmptyStr
    occurred_at: datetime
    subject: EntityRef
    payload: EventPayload

    def __hash__(self) -> int:
        # The generated frozen-model hash would recurse into `payload`, which
        # is unhashable by construction (free-form `details`). Hash on the
        # event's IDENTITY instead: `tenant + event_id` is the spec's dedup
        # key (§4 — the ledger scopes it per policy). Equality stays pydantic's
        # FIELD-BASED eq — two envelopes that compare equal necessarily share
        # the identity, so the eq/hash contract holds — which means a set
        # dedups IDENTICAL envelopes only. Deduping re-deliveries whose
        # content a producer refreshed is deliberately NOT a set's job: key on
        # `(envelope.tenant, envelope.event_id)` explicitly (as the delivery
        # ledger does), or content differences silently vanish.
        return hash((self.tenant, self.event_id))

    @field_validator("occurred_at", mode="after")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError(
                "occurred_at must be timezone-aware (the plugin never guesses "
                "a producer's timezone)"
            )
        return value

    @model_validator(mode="after")
    def _check_type_shape(self) -> EventEnvelope:
        allowed = _ALLOWED_SUBJECT_KINDS[self.event_type]
        if self.subject.kind not in allowed:
            raise ValueError(
                f"event_type {self.event_type.value!r} requires a subject of kind "
                f"{'/'.join(sorted(k.value for k in allowed))}, "
                f"got {self.subject.kind.value!r}"
            )
        for field in _REQUIRED_PAYLOAD_FIELDS.get(self.event_type, ()):
            if getattr(self.payload, field) is None:
                raise ValueError(
                    f"event_type {self.event_type.value!r} requires payload.{field}"
                )
        for key in _REQUIRED_DETAIL_KEYS.get(self.event_type, ()):
            if key not in self.payload.details:
                raise ValueError(
                    f"event_type {self.event_type.value!r} requires "
                    f"payload.details.{key}"
                )
        if self.event_type is EventType.semantic_signal and not self.payload.signals:
            # The whole point of the event (spec §9). Without a signal it is
            # indistinguishable from noise, and §9 forbids falling back to
            # matching every item.
            raise ValueError(
                "event_type 'semantic_signal' requires at least one payload.signals "
                "entry"
            )
        if self.event_type is EventType.initiative_phase_completed and (
            self.subject.phase != self.payload.phase
        ):
            # Two places name the phase (the subject ref and the predicate
            # surface); a disagreement means one of them is wrong and we
            # cannot tell which.
            raise ValueError(
                "initiative_phase_completed: subject.phase "
                f"({self.subject.phase!r}) and payload.phase "
                f"({self.payload.phase!r}) must agree"
            )
        return self


class MalformedReason(enum.StrEnum):
    """Why an envelope could not be understood — the operator-visible reason
    the quarantine surface (spec §4/§11) shows next to the raw body. Coarse by
    design: the actionable specifics live in `MalformedEnvelope.detail`, which
    names the offending fields."""

    # The bytes were not JSON at all (a truncated capture, a half-written file).
    not_json = "not_json"
    # Valid JSON, but not a JSON object (a bare list/string/number).
    not_an_object = "not_an_object"
    # A JSON object that does not satisfy the envelope contract.
    invalid_envelope = "invalid_envelope"


@dataclass(frozen=True)
class MalformedEnvelope:
    """A rejected envelope, kept whole.

    This is a RESULT, not an error: intake classifies malformed input and
    keeps going (see `intake.run_intake`). The `raw` body is retained verbatim
    because the quarantine surface's requeue verb (spec §4) replays exactly
    what arrived — a normalized copy would replay a fiction. `event_id` is
    best-effort: a malformed envelope may have no id at all, which is why the
    cursor acks a source-defined POSITION rather than the event id."""

    reason: MalformedReason
    detail: str
    raw: Any
    # Where it came from, for the operator: a human locator (fixture filename,
    # outbox row ref) and the source's ack position.
    ref: str | None = None
    position: str | None = None
    event_id: str | None = None


# Same introspectable unhashability as EventPayload: the frozen dataclass
# would auto-generate a field hash over `raw` — arbitrary JSON, a dict in the
# live-outbox case — that raises only at runtime. Quarantine-side dedup keys
# on (source_key, position), never on the object.
MalformedEnvelope.__hash__ = None  # type: ignore[assignment]

# What intake gets back for any one event: understood, or explained.
ParsedEnvelope = EventEnvelope | MalformedEnvelope

# Quarantine reasons are read by humans in a table cell; a wildly wrong body
# under `extra="forbid"` can produce one error per stray field, so the detail
# is capped rather than unbounded.
_MAX_REPORTED_ERRORS = 5


def _compact_errors(exc: ValidationError) -> str:
    errors = exc.errors()
    parts = [
        f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
        for err in errors[:_MAX_REPORTED_ERRORS]
    ]
    if len(errors) > _MAX_REPORTED_ERRORS:
        parts.append(f"(+{len(errors) - _MAX_REPORTED_ERRORS} more)")
    return "; ".join(parts)


def parse_envelope(
    raw: object,
    *,
    ref: str | None = None,
    position: str | None = None,
) -> ParsedEnvelope:
    """Classify one raw event as a valid envelope or a malformed one.

    Accepts either an already-decoded mapping (the live outbox hands back
    rows) or JSON text/bytes (the fixtures source hands back file contents
    undecoded, so "not JSON" classifies as malformed here instead of blowing
    up mid-iteration inside the source).

    Never raises. `ref` and `position` are locators the caller knows and this
    function only carries through, so a `MalformedEnvelope` is self-contained
    for the quarantine store that persists it later."""
    body: object = raw
    if isinstance(body, (str, bytes, bytearray)):
        try:
            body = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return MalformedEnvelope(
                reason=MalformedReason.not_json,
                detail=str(exc),
                raw=raw,
                ref=ref,
                position=position,
            )
    if not isinstance(body, Mapping):
        return MalformedEnvelope(
            reason=MalformedReason.not_an_object,
            detail=f"expected a JSON object, got {type(body).__name__}",
            raw=raw,
            ref=ref,
            position=position,
        )
    # Best-effort id BEFORE validation: an envelope can be malformed for some
    # other reason and still carry the id the quarantine row wants to key on.
    candidate_id = body.get("event_id")
    event_id = candidate_id.strip() or None if isinstance(candidate_id, str) else None
    try:
        return EventEnvelope.model_validate(dict(body))
    except ValidationError as exc:
        return MalformedEnvelope(
            reason=MalformedReason.invalid_envelope,
            detail=_compact_errors(exc),
            raw=raw,
            ref=ref,
            position=position,
            event_id=event_id,
        )

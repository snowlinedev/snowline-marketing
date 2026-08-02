"""Predicate matching — does this event satisfy this policy entry? (spec §6).

The semantics implemented here are DEFINED in `policies.py`'s module docstring,
which is the binding statement of the contract; this module is the
implementation and repeats only the parts a reader needs while looking at the
code. In one paragraph: patterns within one predicate field are a DISJUNCTION,
the fields are a CONJUNCTION, an empty pattern list is UNCONSTRAINED, a `None`
scalar fails any non-empty pattern list (so `"*"` means "has a value", never
"may be absent"), relations match on relation KIND and signals on the signal
string with ANY-member semantics, and every comparison is
`fnmatch.fnmatchcase`.

Three properties this module is built to have, because the engine above it
depends on all three:

- **Pure and deterministic.** Every function here is a total function of two
  frozen inputs (`PolicyEntry`, `EventEnvelope`) with no I/O, no clock and no
  ordering dependence. That is what makes the §11 dry-run ("evaluate a policy
  version against captured fixtures") honest: the same version against the same
  capture matches the same entries, in the same order, on any machine. The
  `fnmatchcase` rule is part of this — `fnmatch.fnmatch` folds case through
  `os.path.normcase`, so the same policy would match differently on macOS and
  Linux.

- **No special case for §9.** Feature signaling has two paths — the durable
  first-class `payload.signals` entry and the compatibility path where the
  marketing-impact fact arrives as an item RELATION — and neither is named
  anywhere in this file. Both are ordinary predicate fields a policy writes
  against (`signals: ["marketing-impact"]` or `relations:
  ["marketing-impact"]`), which is exactly what §9 requires: the compatibility
  path is "config-visible and replaceable", so retiring it must be a policy
  revision, not a code change. A `marketing-impact` branch in the matcher would
  make it neither.

- **No silent match-all.** The predicate fields are covered by an explicit
  dispatch below plus an IMPORT-TIME PIN against `PolicyPredicates.model_fields`.
  A predicate field added to the schema without an arm here would otherwise be
  read by nobody and therefore never constrain anything — a policy the operator
  believes narrows to one scope, matching every event in the tenant. That is
  the silent match-all §6 forbids, and it is precisely the kind of drift a
  hand-maintained field list stops catching six months later.

`matching_entries` returns entries in the policy set's DECLARATION ORDER. The
order is load-bearing downstream: the engine writes one ledger row per matched
entry and returns consequences in the same sequence, so a policy artifact
reviewed top-to-bottom produces an audit trail an operator can read
top-to-bottom.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from fnmatch import fnmatchcase

from snowline_marketing.events import EventEnvelope
from snowline_marketing.policies import PolicyEntry, PolicyPredicates, PolicySet

# The predicate fields read straight off `events.EventPayload` as OPTIONAL
# SCALARS. Same names on both models by construction (`PolicyPredicates` is
# written "field-for-field against EventPayload"), so the read is a getattr on
# each rather than a mapping someone has to keep in sync.
SCALAR_PREDICATE_FIELDS: tuple[str, ...] = (
    "scope",
    "initiative",
    "phase",
    "milestone",
    "work_kind",
)

# The predicate fields matched against a SET of strings with any-member
# semantics: `relations` against each relation's KIND (not its target — §9 asks
# whether a `marketing-impact` relation EXISTS), `signals` against each signal.
MEMBER_PREDICATE_FIELDS: tuple[str, ...] = ("relations", "signals")

# Import-time pin (see module docstring): every declared predicate field must be
# matched by one of the two arms above. A field this module does not read is a
# field that constrains nothing — a silent match-all. Failing at import puts
# that in front of whoever adds the field, instead of in front of an operator
# whose policy fired on every event in the tenant.
_COVERED_PREDICATE_FIELDS = frozenset(SCALAR_PREDICATE_FIELDS) | frozenset(
    MEMBER_PREDICATE_FIELDS
)
_DECLARED_PREDICATE_FIELDS = frozenset(PolicyPredicates.model_fields)
if _COVERED_PREDICATE_FIELDS != _DECLARED_PREDICATE_FIELDS:
    raise AssertionError(
        "matching.py must read every PolicyPredicates field; unread: "
        f"{sorted(_DECLARED_PREDICATE_FIELDS - _COVERED_PREDICATE_FIELDS)}; "
        f"unknown: {sorted(_COVERED_PREDICATE_FIELDS - _DECLARED_PREDICATE_FIELDS)}"
    )


def matches_scalar(patterns: Sequence[str], value: str | None) -> bool:
    """One scalar predicate field against one envelope value.

    Empty `patterns` is UNCONSTRAINED — the field is not tested at all, which
    is how a policy says "regardless of initiative". A `None` value against a
    non-empty list is a NON-MATCH even for `"*"`: an absent initiative is not
    the empty string, and treating "has no value" as satisfying "has any value"
    would let a policy scoped to milestone releases fire on events carrying no
    milestone."""
    if not patterns:
        return True
    if value is None:
        return False
    return any(fnmatchcase(value, pattern) for pattern in patterns)


def matches_any(patterns: Sequence[str], values: Iterable[str]) -> bool:
    """One set-valued predicate field (relations/signals) against the event's
    set, with ANY-member semantics: the field matches when ANY member matches
    ANY pattern.

    Empty `patterns` is UNCONSTRAINED, as above. An EMPTY value set against a
    non-empty pattern list is a non-match by the same rule that governs a `None`
    scalar — an event carrying no signals does not "have any signal", so `"*"`
    fails on it."""
    if not patterns:
        return True
    return any(fnmatchcase(value, pattern) for value in values for pattern in patterns)


def matches(entry: PolicyEntry, envelope: EventEnvelope) -> bool:
    """Whether `entry` selects `envelope` — the whole matching contract, in
    order of cheapest-and-most-selective first.

    The EVENT SELECTOR is a plain membership test, not a glob: `event_types`
    holds `EventType` members validated against a closed vocabulary at parse
    time, so there is nothing to pattern-match and a typo'd selector already
    quarantined the version rather than reaching here.

    NOTE what is deliberately absent: any comparison of tenants. A policy set
    governs exactly one tenant and cannot predicate on one (`policies.py`), and
    an envelope from a different tenant is not a non-match — it is a
    cross-tenant delivery the ENGINE quarantines (§14). Answering it here with
    a quiet `False` would turn an isolation breach into an ordinary
    uninteresting event."""
    if envelope.event_type not in entry.event_types:
        return False
    predicates = entry.predicates
    payload = envelope.payload
    for field in SCALAR_PREDICATE_FIELDS:
        if not matches_scalar(getattr(predicates, field), getattr(payload, field)):
            return False
    if not matches_any(
        predicates.relations, tuple(relation.kind for relation in payload.relations)
    ):
        return False
    return matches_any(predicates.signals, payload.signals)


def matching_entries(
    policy_set: PolicySet, envelope: EventEnvelope
) -> tuple[PolicyEntry, ...]:
    """Every entry of `policy_set` that selects `envelope`, in declaration
    order (see the module docstring on why the order is part of the contract).

    An empty result is a legitimate, fully-evaluated answer: the tenant has
    policies and none of them are about this event, which the engine audits as
    `ignored` (spec §14). It is categorically different from "we could not
    evaluate", which never reaches this function — a quarantined or
    unresolvable policy version stalls above it."""
    return tuple(entry for entry in policy_set.policies if matches(entry, envelope))

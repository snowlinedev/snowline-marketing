"""Predicate matching (spec §6, §9) — one test per semantic bullet.

The semantics under test are the ones `policies.py`'s module docstring defines;
this file is where each of those sentences becomes executable. No database and
no policy artifact: matching is a pure function of two frozen objects, which is
exactly the property that lets the deterministic core be built fixtures-first
(spec §5).

The §9 tests deserve their own note. Feature signaling has two paths — the
durable first-class `payload.signals` entry and the compatibility path where the
fact arrives as an item RELATION — and the matcher special-cases NEITHER. What
is pinned here is that both work through ordinary predicates AND that neither
leaks into the other: a signals predicate must not fire on a relation-only
event, because §9's rule is that the compatibility path "never falls back to
matching every implementation item".
"""

from __future__ import annotations

import pytest
from conftest import (
    EVENT_FIXTURES_DIR,
    POLICY_FIXTURES_DIR,
    SCOPE,
    TENANT,
    make_envelope,
)

from snowline_marketing.events import EventEnvelope, EventType, parse_envelope
from snowline_marketing.matching import (
    MEMBER_PREDICATE_FIELDS,
    SCALAR_PREDICATE_FIELDS,
    matches,
    matches_any,
    matches_scalar,
    matching_entries,
)
from snowline_marketing.policies import (
    ConsequenceType,
    PolicyDestination,
    PolicyEntry,
    PolicyPredicates,
    PolicySet,
    parse_policy_set,
)

TURTLESEDGE_POLICIES = parse_policy_set(
    (POLICY_FIXTURES_DIR / "turtlesedge.json").read_text()
)


def entry(
    *,
    policy_id: str = "p",
    event_types: tuple[EventType, ...] = (EventType.item_completed,),
    **predicates: tuple[str, ...],
) -> PolicyEntry:
    """A minimal valid entry carrying only the predicates under test — the
    policy analogue of conftest's `make_envelope`, so a test that sets one
    predicate is testing that predicate."""
    return PolicyEntry(
        policy_id=policy_id,
        event_types=event_types,
        predicates=PolicyPredicates(**predicates),
        consequence=ConsequenceType.messaging_refresh,
        destination=PolicyDestination(scope="turtlesedge/marketing"),
        title_template="Refresh {scope}",
        body_template="Something happened on {scope}.",
    )


def envelope(
    event_type: EventType = EventType.item_completed, **payload: object
) -> EventEnvelope:
    body = make_envelope(event_type)
    body["payload"].update(payload)
    parsed = parse_envelope(body)
    assert isinstance(parsed, EventEnvelope), getattr(parsed, "detail", "")
    return parsed


# --- the event selector ------------------------------------------------------


def test_event_type_must_be_selected():
    # `event_types` is a membership test over a closed vocabulary, not a glob:
    # an unrecognized selector already quarantined the version at parse time.
    completed = entry(event_types=(EventType.item_completed,))
    assert matches(completed, envelope(EventType.item_completed))
    assert not matches(completed, envelope(EventType.item_reopened))


def test_event_types_are_a_disjunction_too():
    both = entry(event_types=(EventType.item_completed, EventType.semantic_signal))
    assert matches(both, envelope(EventType.item_completed))
    assert matches(both, envelope(EventType.semantic_signal))
    assert not matches(both, envelope(EventType.item_abandoned))


# --- unconstrained / disjunction / conjunction -------------------------------


def test_no_predicates_matches_every_selected_event():
    # An entry with no predicates is legitimately broad (the monthly sweep
    # policy is exactly this shape); "unconstrained" is a real, authored state.
    assert matches(entry(), envelope(scope="anything/at-all"))


def test_an_empty_pattern_list_does_not_test_the_field():
    # The field-level version of the same rule: `initiative=()` means
    # "regardless of initiative", including when the event carries none.
    unconstrained = entry(initiative=())
    assert matches(unconstrained, envelope())
    assert matches(unconstrained, envelope(initiative="summer-release"))


def test_patterns_within_a_field_are_a_disjunction():
    either = entry(work_kind=("ui", "design*"))
    assert matches(either, envelope(work_kind="ui"))
    assert matches(either, envelope(work_kind="design-review"))
    assert not matches(either, envelope(work_kind="implementation"))


def test_fields_are_a_conjunction():
    both = entry(scope=("turtlesedge/*",), work_kind=("ui",))
    assert matches(both, envelope(scope=SCOPE, work_kind="ui"))
    # Each field alone would have matched; together they must not.
    assert not matches(both, envelope(scope=SCOPE, work_kind="implementation"))
    assert not matches(both, envelope(scope="other/repo", work_kind="ui"))


# --- "*" vs None -------------------------------------------------------------


def test_star_requires_a_present_value():
    # `"*"` means "has any value", never "may be absent" — an absent initiative
    # is not the empty string.
    any_initiative = entry(initiative=("*",))
    assert matches(any_initiative, envelope(initiative="summer-release"))
    assert not matches(any_initiative, envelope())


def test_none_fails_every_non_empty_pattern_list():
    # Not a quirk of `"*"`: a missing value fails any pattern, including one
    # that would otherwise match the empty string.
    for pattern in ("*", "summer-*", "?*", "[abc]*"):
        assert not matches_scalar((pattern,), None), pattern
    assert not matches(entry(milestone=("v*",)), envelope())


def test_an_empty_pattern_list_matches_a_none_value():
    # The other half: unconstrained means unconstrained even when the envelope
    # left the field out.
    assert matches_scalar((), None)


# --- relations and signals (set-valued, ANY-member) --------------------------


def test_relations_match_on_kind_not_target():
    # §9's compatibility path asks whether a `marketing-impact` relation
    # EXISTS; the relation's target is provenance, not a selector.
    on_kind = entry(relations=("marketing-impact",))
    assert matches(
        on_kind,
        envelope(relations=[{"kind": "marketing-impact", "target": "8ad41b77"}]),
    )
    # A policy that tried to select the TARGET must not match — targets are not
    # in the predicate surface at all.
    on_target = entry(relations=("8ad41b77",))
    assert not matches(
        on_target,
        envelope(relations=[{"kind": "marketing-impact", "target": "8ad41b77"}]),
    )


def test_any_member_of_the_set_may_match():
    on_signal = entry(signals=("store-listing-affecting",))
    assert matches(
        on_signal, envelope(signals=["marketing-impact", "store-listing-affecting"])
    )
    assert matches(
        entry(signals=("marketing-*",)), envelope(signals=["marketing-impact"])
    )


def test_an_empty_set_fails_a_non_empty_pattern_list():
    # The set-valued form of the `None` rule: an event carrying no signals does
    # not "have any signal", so even `"*"` must fail on it.
    assert not matches(entry(signals=("*",)), envelope(signals=[]))
    assert not matches(entry(relations=("*",)), envelope(relations=[]))
    assert not matches_any(("*",), ())


def test_relations_and_signals_are_independent_fields():
    # Conjunction across the two set fields, same as the scalars.
    both = entry(relations=("marketing-impact",), signals=("marketing-impact",))
    assert not matches(
        both, envelope(relations=[{"kind": "marketing-impact"}], signals=[])
    )
    assert matches(
        both,
        envelope(
            relations=[{"kind": "marketing-impact"}], signals=["marketing-impact"]
        ),
    )


# --- case sensitivity --------------------------------------------------------


def test_matching_is_case_sensitive():
    # `fnmatch.fnmatch` folds case through `os.path.normcase`, so the same
    # policy would match differently on macOS and Linux. A deterministic policy
    # machine cannot have a platform-dependent match, hence `fnmatchcase`.
    assert not matches(entry(work_kind=("UI",)), envelope(work_kind="ui"))
    assert not matches(entry(scope=("TurtlesEdge/*",)), envelope(scope=SCOPE))
    assert matches(entry(work_kind=("ui",)), envelope(work_kind="ui"))


def test_globs_are_full_fnmatch_patterns_over_data():
    # Predicates are DATA (spec §3): fnmatch's own vocabulary, nothing compiled
    # and nothing executed.
    assert matches(entry(milestone=("v1.[0-9]",)), envelope(milestone="v1.4"))
    assert not matches(entry(milestone=("v1.[0-9]",)), envelope(milestone="v1.10"))
    assert matches(entry(milestone=("v?.*",)), envelope(milestone="v1.4"))


# --- §9 feature signaling: both paths, no fallback ---------------------------


def test_first_class_signal_path_matches_through_an_ordinary_predicate():
    # §9's durable contract: an explicit marketing-impact semantic signal,
    # carried on the event and predicated on like any other field.
    on_signal = entry(signals=("marketing-impact",))
    assert matches(on_signal, envelope(signals=["marketing-impact"]))


def test_relation_compatibility_path_matches_through_an_ordinary_predicate():
    # §9's compatibility path until PM exposes the first-class signal: the same
    # fact as an item relation. Note there is no `marketing-impact` string
    # anywhere in the matcher — retiring this path is a policy revision.
    on_relation = entry(relations=("marketing-impact",))
    assert matches(on_relation, envelope(relations=[{"kind": "marketing-impact"}]))


def test_neither_signaling_path_falls_back_to_the_other():
    # The rule that makes §9 safe: a policy written against one path must NOT
    # fire on an event carrying only the other, and neither may degrade into
    # "match every implementation item".
    on_signal = entry(signals=("marketing-impact",))
    on_relation = entry(relations=("marketing-impact",))
    relation_only = envelope(relations=[{"kind": "marketing-impact"}], signals=[])
    signal_only = envelope(relations=[], signals=["marketing-impact"])

    assert not matches(on_signal, relation_only)
    assert not matches(on_relation, signal_only)
    # ...and an event carrying neither matches neither, however ordinary it is.
    plain = envelope(work_kind="implementation")
    assert not matches(on_signal, plain)
    assert not matches(on_relation, plain)


def test_a_policy_may_accept_either_signaling_path():
    # Both paths at once is a DISJUNCTION the operator writes, not a fallback
    # the plugin invents — which is what "config-visible and replaceable" means.
    either_relation = entry(relations=("marketing-impact",))
    either_signal = entry(signals=("marketing-impact",))
    policy_set = PolicySet(
        schema_version=1,
        tenant=TENANT,
        policies=(
            either_signal.model_copy(update={"policy_id": "by-signal"}),
            either_relation.model_copy(update={"policy_id": "by-relation"}),
        ),
    )
    relation_only = envelope(relations=[{"kind": "marketing-impact"}], signals=[])
    signal_only = envelope(signals=["marketing-impact"])
    assert [e.policy_id for e in matching_entries(policy_set, relation_only)] == [
        "by-relation"
    ]
    assert [e.policy_id for e in matching_entries(policy_set, signal_only)] == [
        "by-signal"
    ]


# --- entry selection ---------------------------------------------------------


def test_matching_entries_preserves_declaration_order():
    # The order is part of the contract: the engine writes one ledger row per
    # match in this sequence, so a policy artifact read top-to-bottom produces
    # an audit trail read top-to-bottom.
    policy_set = PolicySet(
        schema_version=1,
        tenant=TENANT,
        policies=(
            entry(policy_id="first"),
            entry(policy_id="never", work_kind=("ui",)),
            entry(policy_id="second"),
            entry(policy_id="third"),
        ),
    )
    selected = matching_entries(policy_set, envelope(work_kind="implementation"))
    assert [e.policy_id for e in selected] == ["first", "second", "third"]


def test_an_empty_policy_set_matches_nothing_without_error():
    # A tenant whose artifact exists but declares no rules yet is evaluable and
    # matches nothing — categorically unlike a quarantined version, which never
    # reaches the matcher at all.
    empty = PolicySet(schema_version=1, tenant=TENANT, policies=())
    assert matching_entries(empty, envelope()) == ()


# --- the shipped fixtures ----------------------------------------------------


@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        # A completed item carrying the marketing-impact signal, on a
        # turtlesedge scope.
        ("0010-item-completed.json", ["messaging-refresh-on-marketing-impact"]),
        # A reopen: no entry selects the type at all.
        ("0030-item-reopened.json", []),
        # A phase completion with NO marketing-impact relation — the launch-plan
        # policy must not fire (§9: no fallback to every implementation item).
        ("0070-initiative-phase-completed.json", []),
        # A release: three entries select it, in declaration order.
        (
            "0100-milestone-released.json",
            [
                "listing-regeneration-on-release",
                "announcement-preparation-on-release",
                "app-store-listing-publish",
            ],
        ),
        # The schedule fires: both recurring policies, one of them dry-run.
        (
            "0110-recurring-item-fired.json",
            ["monthly-review-sweep", "monthly-metrics-snapshot"],
        ),
        # An explicit semantic-signal event (§9's durable path).
        ("0130-semantic-signal.json", ["messaging-refresh-on-marketing-impact"]),
        # §9's COMPATIBILITY path: marketing-impact as a relation, no signal.
        (
            "0160-phase-completed-relation-signal.json",
            ["launch-plan-on-build-phase-completed"],
        ),
    ],
)
def test_shipped_capture_against_the_shipped_policy_set(
    event_fixtures_dir, fixture, expected
):
    """The real Turtle's Edge policy artifact against the real capture — the
    fixtures-first contract (spec §5/§6) at the matcher level, before any
    database or ledger is involved."""
    assert isinstance(TURTLESEDGE_POLICIES, PolicySet)
    parsed = parse_envelope((event_fixtures_dir / fixture).read_bytes())
    assert isinstance(parsed, EventEnvelope), getattr(parsed, "detail", "")
    selected = matching_entries(TURTLESEDGE_POLICIES, parsed)
    assert [e.policy_id for e in selected] == expected


def test_a_cross_tenant_envelope_is_not_answered_by_the_matcher():
    """Isolation is NOT a predicate (spec §3/§14). A foreign envelope whose
    fields happen to satisfy an entry still MATCHES here — the engine is what
    refuses it, with a quarantine row. A quiet `False` in the matcher would turn
    an isolation breach into an ordinary uninteresting event."""
    assert isinstance(TURTLESEDGE_POLICIES, PolicySet)
    parsed = parse_envelope(
        (EVENT_FIXTURES_DIR / "0150-cross-tenant-item-completed.json").read_bytes()
    )
    assert isinstance(parsed, EventEnvelope), getattr(parsed, "detail", "")
    assert parsed.tenant != TURTLESEDGE_POLICIES.tenant
    # An entry whose predicates the foreign envelope satisfies outright.
    scoped = entry(signals=("marketing-impact",))
    assert matches(scoped, parsed), (
        "the matcher must not silently reject foreign tenants — that is the "
        "engine's quarantine, not a non-match"
    )


# --- the import-time pin -----------------------------------------------------


def test_every_predicate_field_is_read_by_the_matcher():
    """The pin `matching.py` enforces at import, asserted where the suite
    reports it: a predicate field nothing reads constrains nothing, so a policy
    the operator believes narrows to one scope would match every event in the
    tenant."""
    covered = set(SCALAR_PREDICATE_FIELDS) | set(MEMBER_PREDICATE_FIELDS)
    assert covered == set(PolicyPredicates.model_fields)

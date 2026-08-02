"""The policy-set contract (spec §6): what parses, what quarantines the version.

No database and no gateway — policy bodies are validated in memory, so these
run everywhere. The house rule under test throughout: `parse_policy_set` NEVER
raises, and a malformed version comes back as a DIFFERENT TYPE rather than as
an empty policy set. That distinction is the whole of §6's "never silently
match-all or match-none": a caller who forgot to check cannot accidentally
evaluate a broken version as "no rules".
"""

from __future__ import annotations

import json
from collections.abc import Hashable

import pytest
from conftest import POLICY_FIXTURES_DIR, TENANT

from snowline_marketing.events import EventType
from snowline_marketing.policies import (
    DEFAULT_DEDUP_KEY_TEMPLATE,
    ConsequenceType,
    MalformedPolicyReason,
    MalformedPolicySet,
    PolicyMode,
    PolicySet,
    parse_policy_set,
)

VALID_FIXTURE = POLICY_FIXTURES_DIR / "turtlesedge.json"


def _valid_body() -> dict:
    return json.loads(VALID_FIXTURE.read_text())


def _entry(policy_id: str) -> dict:
    """A minimal VALID entry dict — only what an entry requires, so a test that
    mutates one field is testing that field."""
    return {
        "policy_id": policy_id,
        "event_types": ["item_completed"],
        "consequence": "messaging_refresh",
        "destination": {"scope": "turtlesedge/marketing"},
        "title_template": "Refresh messaging",
        "body_template": "Body.",
    }


def _set(*entries: dict, **overrides) -> dict:
    body: dict = {
        "schema_version": 1,
        "tenant": TENANT,
        "policies": list(entries),
    }
    body.update(overrides)
    return body


# --- the shipped valid set -------------------------------------------------


def test_the_shipped_policy_set_parses():
    parsed = parse_policy_set(VALID_FIXTURE.read_bytes(), version_id="pv-0001")
    assert isinstance(parsed, PolicySet), parsed
    assert parsed.tenant == TENANT
    assert parsed.schema_version == 1


def test_the_shipped_set_exercises_every_consequence_type():
    # Guards the fixture: a consequence added to the enum without landing in
    # the shipped set would otherwise be shipped untested.
    parsed = parse_policy_set(VALID_FIXTURE.read_bytes())
    assert isinstance(parsed, PolicySet)
    assert {e.consequence for e in parsed.policies} == set(ConsequenceType)


def test_the_shipped_set_exercises_both_dedup_forms():
    # Both the §4 default (omitted in the body) and a custom template, because
    # the default is the one nobody writes down and therefore the one that
    # regresses silently.
    parsed = parse_policy_set(VALID_FIXTURE.read_bytes())
    assert isinstance(parsed, PolicySet)
    templates = {e.dedup_key_template for e in parsed.policies}
    assert DEFAULT_DEDUP_KEY_TEMPLATE in templates
    assert templates - {DEFAULT_DEDUP_KEY_TEMPLATE}


def test_the_shipped_set_round_trips_through_json():
    # A policy version is stored as text in the cache and diffed against the
    # artifact in governance — a body that cannot survive
    # dict -> model -> JSON -> model is not a wire contract.
    original = parse_policy_set(_valid_body())
    assert isinstance(original, PolicySet)
    again = parse_policy_set(original.model_dump_json())
    assert again == original


def test_entry_lookup_by_policy_id():
    parsed = parse_policy_set(_valid_body())
    assert isinstance(parsed, PolicySet)
    entry = parsed.entry("listing-regeneration-on-release")
    assert entry is not None
    assert entry.consequence is ConsequenceType.listing_regeneration
    assert entry.destination.scope == "turtlesedge/marketing"
    assert entry.destination.phase == "release"
    assert parsed.entry("no-such-policy") is None


@pytest.mark.parametrize("consequence", list(ConsequenceType))
def test_every_consequence_type_parses(consequence):
    entry = _entry("p1") | {"consequence": consequence.value}
    if consequence is ConsequenceType.channel_publish:
        # Approval-gated by contract — see the dedicated test below.
        entry["mode"] = "approval_required"
    parsed = parse_policy_set(_set(entry))
    assert isinstance(parsed, PolicySet), parsed
    assert parsed.policies[0].consequence is consequence


@pytest.mark.parametrize("mode", list(PolicyMode))
def test_every_mode_parses(mode):
    parsed = parse_policy_set(_set(_entry("p1") | {"mode": mode.value}))
    assert isinstance(parsed, PolicySet), parsed
    assert parsed.policies[0].mode is mode


@pytest.mark.parametrize("event_type", list(EventType))
def test_every_intake_event_type_is_a_legal_selector(event_type):
    # The selector vocabulary IS the intake vocabulary; an event type intake
    # can deliver but a policy cannot select would be undeliverable work.
    parsed = parse_policy_set(_set(_entry("p1") | {"event_types": [event_type.value]}))
    assert isinstance(parsed, PolicySet), parsed
    assert parsed.policies[0].event_types == (event_type,)


def test_defaults_are_the_documented_ones():
    parsed = parse_policy_set(_set(_entry("p1")))
    assert isinstance(parsed, PolicySet)
    entry = parsed.policies[0]
    assert entry.dedup_key_template == DEFAULT_DEDUP_KEY_TEMPLATE
    assert entry.mode is PolicyMode.active
    assert entry.human_owned is False
    assert entry.musher_dispatch is False
    assert entry.artifact_refs == ()
    assert entry.channels == ()
    assert entry.deliverable_classes == ()
    assert entry.owner_template is None


def test_a_policy_set_with_no_entries_is_valid_not_quarantined():
    # A tenant whose artifact exists but declares no rules yet is an evaluable
    # state that matches nothing — categorically unlike a quarantined version.
    parsed = parse_policy_set(_set())
    assert isinstance(parsed, PolicySet)
    assert parsed.policies == ()


# --- predicates are data ---------------------------------------------------


def test_glob_shapes_are_accepted_as_data():
    # Patterns are stored, never compiled: fnmatch has no invalid-syntax class,
    # so every one of these is legal DATA and the engine decides what it means.
    patterns = ["turtlesedge/*", "v1.?", "[abc]-scope", "release-*-final", "*"]
    parsed = parse_policy_set(
        _set(_entry("p1") | {"predicates": {"scope": patterns, "milestone": ["v*"]}})
    )
    assert isinstance(parsed, PolicySet), parsed
    assert parsed.policies[0].predicates.scope == tuple(patterns)


def test_every_predicate_field_carries_patterns():
    predicates = {
        "scope": ["turtlesedge/*"],
        "initiative": ["summer-*"],
        "phase": ["build"],
        "milestone": ["v1.*"],
        "work_kind": ["implementation"],
        "relations": ["marketing-impact"],
        "signals": ["marketing-*"],
    }
    parsed = parse_policy_set(_set(_entry("p1") | {"predicates": predicates}))
    assert isinstance(parsed, PolicySet), parsed
    got = parsed.policies[0].predicates
    for field, values in predicates.items():
        assert getattr(got, field) == tuple(values)


def test_omitted_predicates_are_empty_meaning_unconstrained():
    parsed = parse_policy_set(_set(_entry("p1")))
    assert isinstance(parsed, PolicySet)
    assert parsed.policies[0].predicates.scope == ()
    assert parsed.policies[0].predicates.signals == ()


def test_an_empty_glob_pattern_is_malformed():
    # An all-whitespace pattern would pass a bare `str` and then match nothing
    # forever without ever being wrong out loud.
    parsed = parse_policy_set(
        _set(_entry("p1") | {"predicates": {"scope": ["   "]}}),
    )
    assert isinstance(parsed, MalformedPolicySet)
    assert "predicates.scope" in parsed.detail


def test_there_is_no_tenant_predicate():
    # The set's tenant IS the isolation boundary (spec §3): letting an entry
    # express one would make cross-org routing a matter of configuration.
    parsed = parse_policy_set(
        _set(_entry("p1") | {"predicates": {"tenant": ["other-org"]}}),
    )
    assert isinstance(parsed, MalformedPolicySet)
    assert "tenant" in parsed.detail


def test_unknown_entry_fields_are_malformed_not_ignored():
    # extra="forbid": a field we silently dropped is a rule the operator
    # believes is in force and is not.
    parsed = parse_policy_set(_set(_entry("p1") | {"probability": 0.5}))
    assert isinstance(parsed, MalformedPolicySet)
    assert "probability" in parsed.detail


# --- the malformed classes -------------------------------------------------


def test_unknown_event_selector_quarantines_the_version(policy_fixtures_dir):
    path = policy_fixtures_dir / "malformed-unknown-event-type.json"
    parsed = parse_policy_set(path.read_bytes(), ref=path.name, version_id="pv-bad-1")
    assert isinstance(parsed, MalformedPolicySet)
    assert parsed.reason is MalformedPolicyReason.invalid_policy_set
    # The detail names the offending entry INDEX and field, which is what an
    # operator needs to find it in a long artifact.
    assert "policies.0.event_types" in parsed.detail
    assert "item_archived" in parsed.detail
    assert parsed.version_id == "pv-bad-1"
    assert parsed.ref == path.name
    assert parsed.tenant == TENANT


def test_duplicate_policy_id_quarantines_the_version(policy_fixtures_dir):
    path = policy_fixtures_dir / "malformed-duplicate-policy-id.json"
    parsed = parse_policy_set(path.read_bytes(), ref=path.name)
    assert isinstance(parsed, MalformedPolicySet)
    assert parsed.reason is MalformedPolicyReason.invalid_policy_set
    assert "listing-regeneration-on-release" in parsed.detail
    assert "dedup" in parsed.detail


def test_missing_destination_scope_quarantines_the_version(policy_fixtures_dir):
    path = policy_fixtures_dir / "malformed-missing-destination-scope.json"
    parsed = parse_policy_set(path.read_bytes(), ref=path.name)
    assert isinstance(parsed, MalformedPolicySet)
    assert "policies.0.destination.scope" in parsed.detail


def test_wrong_schema_version_quarantines_the_version(policy_fixtures_dir):
    # A body written against a shape this deploy does not understand reaches
    # quarantine by the same path as any other violation.
    path = policy_fixtures_dir / "malformed-schema-version.json"
    parsed = parse_policy_set(path.read_bytes(), ref=path.name)
    assert isinstance(parsed, MalformedPolicySet)
    assert parsed.reason is MalformedPolicyReason.invalid_policy_set
    assert "schema_version" in parsed.detail


def test_a_non_object_body_quarantines_the_version(policy_fixtures_dir):
    path = policy_fixtures_dir / "malformed-not-an-object.json"
    parsed = parse_policy_set(path.read_bytes(), ref=path.name)
    assert isinstance(parsed, MalformedPolicySet)
    assert parsed.reason is MalformedPolicyReason.not_an_object
    assert "list" in parsed.detail
    # No tenant to report: the body never named one.
    assert parsed.tenant is None


def test_a_prose_body_quarantines_the_version(policy_fixtures_dir):
    # Governance stores artifact bodies as text, so a policy artifact revised
    # to prose is a real accident. It must classify, not raise.
    path = policy_fixtures_dir / "malformed-not-json.json"
    parsed = parse_policy_set(path.read_bytes(), ref=path.name)
    assert isinstance(parsed, MalformedPolicySet)
    assert parsed.reason is MalformedPolicyReason.not_json


def test_every_shipped_fixture_classifies_as_its_name_claims(policy_fixtures_dir):
    # Guards the fixture directory itself: a file named `malformed-*` that
    # quietly started parsing (or a valid one that stopped) is a test suite
    # asserting nothing.
    files = sorted(p for p in policy_fixtures_dir.glob("*.json") if p.is_file())
    assert files, "no policy fixtures shipped"
    for path in files:
        parsed = parse_policy_set(path.read_bytes(), ref=path.name)
        expected_malformed = path.name.startswith("malformed-")
        assert isinstance(parsed, MalformedPolicySet) is expected_malformed, (
            f"{path.name} classified as {type(parsed).__name__}"
        )


def test_the_raw_body_is_kept_whole(policy_fixtures_dir):
    # The operator's fix is to diff the quarantined body against the artifact
    # in governance and revise; a normalized copy would diff against a fiction.
    path = policy_fixtures_dir / "malformed-not-json.json"
    raw = path.read_bytes()
    parsed = parse_policy_set(raw)
    assert isinstance(parsed, MalformedPolicySet)
    assert parsed.raw == raw


def test_malformed_policy_sets_are_introspectably_unhashable():
    # Same contract as MalformedEnvelope: `raw` is arbitrary JSON, so a
    # generated field hash would raise at runtime while still advertising
    # Hashable to a consumer gating on it.
    parsed = parse_policy_set("not json at all")
    assert isinstance(parsed, MalformedPolicySet)
    assert not isinstance(parsed, Hashable)


def test_parse_never_raises_on_anything():
    # The contract, stated bluntly. None of these may escape as an exception.
    for body in (None, 7, [], "", b"\xff\xfe", {"schema_version": "one"}, object()):
        assert isinstance(parse_policy_set(body), MalformedPolicySet), body


# --- the entry-level semantic rules ----------------------------------------


def test_an_entry_selecting_no_event_type_is_malformed():
    parsed = parse_policy_set(_set(_entry("p1") | {"event_types": []}))
    assert isinstance(parsed, MalformedPolicySet)
    assert "at least one" in parsed.detail


def test_channel_publish_may_not_be_active():
    # Spec §3/§12: publishing is never implicit. An active publish policy would
    # push governed content to a live external channel with no human in the
    # loop the moment an event matched.
    entry = _entry("p1") | {"consequence": "channel_publish", "mode": "active"}
    parsed = parse_policy_set(_set(entry))
    assert isinstance(parsed, MalformedPolicySet)
    assert "approval" in parsed.detail


def test_channel_publish_defaults_are_also_rejected():
    # `mode` defaults to `active`, so an author who simply omits it gets the
    # same loud rejection rather than a silently-injected default.
    entry = _entry("p1") | {"consequence": "channel_publish"}
    parsed = parse_policy_set(_set(entry))
    assert isinstance(parsed, MalformedPolicySet)


@pytest.mark.parametrize("mode", ["approval_required", "dry_run"])
def test_channel_publish_is_fine_when_gated(mode):
    entry = _entry("p1") | {"consequence": "channel_publish", "mode": mode}
    assert isinstance(parse_policy_set(_set(entry)), PolicySet)


# --- the dedup-key template ------------------------------------------------


def test_the_default_dedup_template_is_the_spec_logical_key():
    # Spec §4: the delivery ledger's logical key is tenant + policy_id +
    # event_id. If this default ever drifts, every dedup guarantee in the
    # acceptance criteria drifts with it.
    assert DEFAULT_DEDUP_KEY_TEMPLATE == "{tenant}:{policy_id}:{event_id}"


@pytest.mark.parametrize(
    "template",
    [
        "{tenant}:{policy_id}:{event_id}",
        "{tenant}:{policy_id}:{milestone}",
        "{policy_id}/{scope}/{initiative}/{phase}",
        "{tenant}:{policy_id}:{entity_kind}:{entity_id}",
        "{consequence}:{event_type}:{work_kind}",
    ],
)
def test_known_dedup_placeholders_are_accepted(template):
    parsed = parse_policy_set(_set(_entry("p1") | {"dedup_key_template": template}))
    assert isinstance(parsed, PolicySet), parsed


def test_an_unknown_dedup_placeholder_is_malformed():
    # The engine could not fill it: the key would either explode per event or
    # render constant, and a constant dedup key silently swallows all later
    # work for that policy.
    parsed = parse_policy_set(
        _set(_entry("p1") | {"dedup_key_template": "{tenant}:{sprint}"}),
    )
    assert isinstance(parsed, MalformedPolicySet)
    assert "'sprint'" in parsed.detail


def test_a_constant_dedup_template_is_malformed():
    parsed = parse_policy_set(
        _set(_entry("p1") | {"dedup_key_template": "always-the-same"}),
    )
    assert isinstance(parsed, MalformedPolicySet)
    assert "collapses" in parsed.detail


def test_an_unbalanced_dedup_template_is_malformed():
    # `str.format` would raise this per event at mint time instead — a policy
    # that fails only on the events it matches.
    parsed = parse_policy_set(
        _set(_entry("p1") | {"dedup_key_template": "{tenant}:{policy_id"}),
    )
    assert isinstance(parsed, MalformedPolicySet)
    assert "not a valid format string" in parsed.detail


@pytest.mark.parametrize("template", ["{0}", "{tenant.upper}", "{tenant[0]}"])
def test_dedup_templates_cannot_index_or_attribute_access(template):
    # Not code execution, but not renderable either — the field name is
    # compared whole, so these land on the unknown-placeholder path.
    parsed = parse_policy_set(_set(_entry("p1") | {"dedup_key_template": template}))
    assert isinstance(parsed, MalformedPolicySet)


def test_title_and_body_templates_stay_opaque():
    # Deliberately NOT validated for syntax: the rendering vocabulary belongs
    # to the minting item (spec §7), and pinning it here would make every new
    # provenance field a policy-schema change.
    entry = _entry("p1") | {
        "title_template": "Anything at all {not_a_known_field} <<>>",
        "body_template": "{{literal braces}} and {whatever}",
    }
    assert isinstance(parse_policy_set(_set(entry)), PolicySet)


def test_a_missing_title_template_is_malformed():
    entry = _entry("p1")
    del entry["title_template"]
    parsed = parse_policy_set(_set(entry))
    assert isinstance(parsed, MalformedPolicySet)
    assert "title_template" in parsed.detail


# --- whole-version quarantine ----------------------------------------------


def test_one_bad_entry_quarantines_the_whole_version():
    # THE decision (see the module docstring in policies.py): keeping the
    # parseable entries would evaluate a policy version nobody authored and
    # nobody reviewed, then record that version id on the ledger as if it had
    # been applied whole.
    good = _entry("keeps-working")
    bad = _entry("broken") | {"consequence": "teleportation"}
    parsed = parse_policy_set(_set(good, bad))
    assert isinstance(parsed, MalformedPolicySet)
    assert "policies.1.consequence" in parsed.detail


def test_a_quarantined_version_is_not_an_empty_policy_set():
    # The two must never be confusable by a caller that forgot to check —
    # which is exactly why they are different types rather than one type with
    # an empty list.
    quarantined = parse_policy_set(_set(_entry("p1") | {"consequence": "nope"}))
    empty = parse_policy_set(_set())
    assert isinstance(quarantined, MalformedPolicySet)
    assert isinstance(empty, PolicySet)
    assert not isinstance(quarantined, PolicySet)


def test_models_are_frozen():
    parsed = parse_policy_set(_set(_entry("p1")))
    assert isinstance(parsed, PolicySet)
    with pytest.raises(Exception):
        parsed.policies[0].mode = PolicyMode.dry_run

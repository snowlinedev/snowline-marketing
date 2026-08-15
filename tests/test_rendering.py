"""The title/body/owner render vocabulary and the §7 provenance block.

No DB and no sink: rendering is a pure function of the frozen (entry, envelope)
pair a `PendingConsequence` carries, which is exactly what makes it testable
this way and what makes a re-delivery's mint the same item.

Two properties carry most of the weight here. Rendering is DETERMINISTIC, and a
template the delivery cannot fill is a per-delivery FAILURE — never a crash and
never a body with "None" in it. The second is the asymmetry `rendering.py`
documents: dedup templates are validated at parse time against a closed
vocabulary, title/body templates are open over `payload.details` and can only
fail at render time, per event.
"""

from __future__ import annotations

from conftest import SCOPE, TENANT, make_envelope

from snowline_marketing.engine import PendingConsequence
from snowline_marketing.events import EventEnvelope, EventType, parse_envelope
from snowline_marketing.policies import (
    ConsequenceType,
    PolicyDestination,
    PolicyEntry,
    PolicyMode,
)
from snowline_marketing.rendering import (
    PROVENANCE_HEADING,
    TEMPLATE_FIELD_NAMES,
    RenderFailure,
    provenance_block,
    render_mint_request,
    template_values,
)
from snowline_marketing.work_sink import ORIGIN_AI_GENERATED, MintRequest

VERSION_ID = "gv-7f3a91c4"


def envelope(
    event_type: EventType = EventType.item_completed, **payload
) -> EventEnvelope:
    body = make_envelope(event_type)
    body["payload"].update(payload)
    parsed = parse_envelope(body)
    assert isinstance(parsed, EventEnvelope), getattr(parsed, "detail", "")
    return parsed


def entry(**overrides) -> PolicyEntry:
    values = {
        "policy_id": "messaging-refresh",
        "event_types": (EventType.item_completed,),
        "consequence": ConsequenceType.messaging_refresh,
        "destination": PolicyDestination(
            scope="turtlesedge/marketing", initiative="messaging"
        ),
        "title_template": "Refresh messaging for {details.title}",
        "body_template": "{event_type} on {scope} needs a messaging pass.",
    }
    values.update(overrides)
    return PolicyEntry(**values)


def consequence(**overrides) -> PendingConsequence:
    values = {
        "tenant": TENANT,
        "envelope": envelope(),
        "entry": entry(),
        "policy_version_id": VERSION_ID,
        "dedup_key": f"p:{TENANT}:messaging-refresh:pm-evt-1",
    }
    values.update(overrides)
    return PendingConsequence(**values)


def rendered(**overrides) -> MintRequest:
    request = render_mint_request(consequence(**overrides))
    assert isinstance(request, MintRequest), getattr(request, "detail", "")
    return request


def failure(**overrides) -> RenderFailure:
    result = render_mint_request(consequence(**overrides))
    assert isinstance(result, RenderFailure), result
    return result


# --- the vocabulary ----------------------------------------------------------


def test_the_closed_vocabulary_renders_for_a_fully_populated_event():
    # Every advertised name is fillable from one rich delivery — the pin that
    # stops a name being documented and then not implemented (or vice versa).
    # A phase-completion event, because it is the one whose SUBJECT carries a
    # phase as well as its payload.
    rich = envelope(
        EventType.initiative_phase_completed,
        milestone="v1.4",
        work_kind="implementation",
        signals=["marketing-impact"],
        relations=[{"kind": "marketing-impact", "target": "8ad41b77"}],
        external_refs=[{"kind": "pull_request", "url": "https://example/pr/1"}],
        details={"title": "Offline trip log sync"},
    )
    entry_with_lists = entry(
        event_types=(EventType.initiative_phase_completed,),
        destination=PolicyDestination(
            scope="turtlesedge/marketing", initiative="messaging", phase="draft"
        ),
        artifact_refs=("b964d217",),
        channels=("app_store",),
        deliverable_classes=("store_listing",),
        owner_template="{tenant}-marketing",
    )
    values = template_values(consequence(envelope=rich, entry=entry_with_lists))
    assert TEMPLATE_FIELD_NAMES <= set(values)
    assert values["scope"] == SCOPE
    assert values["event_type"] == "initiative_phase_completed"
    assert values["policy_version_id"] == VERSION_ID
    assert values["destination_initiative"] == "messaging"
    assert values["destination_phase"] == "draft"
    assert values["entity_phase"] == "build"
    assert values["relations"] == "marketing-impact"
    assert values["external_refs"] == "pull_request: https://example/pr/1"


def test_details_are_the_open_half_of_the_vocabulary():
    # The room `events.py` deliberately leaves for "facts a consequence template
    # wants to quote" — and the one no closed vocabulary could contain.
    request = rendered(envelope=envelope(details={"title": "Offline trip log sync"}))
    assert request.title == "Refresh messaging for Offline trip log sync"


def test_non_string_details_render_as_the_operator_saw_them_on_the_wire():
    values = template_values(
        consequence(
            envelope=envelope(details={"title": "x", "human_owned": False, "count": 3})
        )
    )
    # json, not Python's repr: `false`, not `False`.
    assert values["details.human_owned"] == "false"
    assert values["details.count"] == "3"


def test_rendering_is_deterministic():
    first = rendered(envelope=envelope(details={"title": "t"}))
    second = rendered(envelope=envelope(details={"title": "t"}))
    assert first == second


def test_an_empty_list_field_is_absent_rather_than_an_empty_string():
    # "This policy declares no channels" and "this event named no signals" are
    # the same fact; a body reading "channels: " with nothing after it is a
    # broken item, not a terse one.
    values = template_values(consequence())
    assert "channels" not in values
    assert "signals" not in values
    result = failure(
        entry=entry(title_template="t", body_template="channels: {channels}")
    )
    assert result.missing == ("channels",)


# --- failure is per delivery, never a crash ----------------------------------


def test_a_template_naming_a_field_this_event_lacks_fails_the_delivery():
    # The asymmetry with the dedup template, made concrete: this template is
    # perfectly renderable for a milestone event and unrenderable for this one,
    # so no parse-time check could have caught it.
    result = failure(entry=entry(title_template="Listing for {milestone}"))
    assert result.missing == ("milestone",)
    assert result.policy_id == "messaging-refresh"
    assert "title_template" in result.detail
    assert "milestone" in result.detail


def test_a_details_key_the_producer_stopped_sending_fails_the_delivery():
    result = failure(envelope=envelope(details={"other": "x"}))
    assert result.missing == ("details.title",)


def test_every_broken_template_is_reported_at_once():
    # An operator fixing a policy sees all of it, rather than one broken
    # template per replay.
    result = failure(
        entry=entry(
            title_template="{milestone}",
            body_template="{details.nope}",
            owner_template="{phase}",
        )
    )
    assert result.missing == ("details.nope", "milestone", "phase")
    assert "title_template" in result.detail
    assert "body_template" in result.detail
    assert "owner_template" in result.detail


def test_an_unbalanced_template_fails_the_delivery_rather_than_raising():
    # Unlike a dedup template — dry-rendered at parse time — this reaches the
    # renderer unvalidated by design.
    result = failure(entry=entry(title_template="Refresh {details.title"))
    assert "not a valid format string" in result.detail


def test_a_format_spec_that_does_not_apply_to_text_fails_the_delivery():
    result = failure(entry=entry(title_template="{scope:d}"))
    assert "format spec" in result.detail


def test_an_unknown_conversion_fails_the_delivery():
    result = failure(entry=entry(title_template="{scope!q}"))
    assert "conversion" in result.detail


def test_a_nested_placeholder_in_a_format_spec_is_refused():
    result = failure(entry=entry(title_template="{scope:{phase}}"))
    assert "nested placeholder" in result.detail


def test_a_template_cannot_traverse_into_plugin_objects():
    # The reason rendering is exact-name lookup instead of `str.format`: a
    # tenant-authored string must never resolve an attribute on a Python object
    # (spec §3 — a policy is data, never code). Dotted names that are not
    # vocabulary keys resolve to nothing and fail cleanly.
    result = failure(entry=entry(title_template="{entry.__class__} {envelope.payload}"))
    assert result.missing == ("entry.__class__", "envelope.payload")


def test_escaped_braces_survive_rendering():
    request = rendered(
        entry=entry(title_template="{{literal}} {details.title}"),
        envelope=envelope(details={"title": "t"}),
    )
    assert request.title == "{literal} t"


def test_conversions_apply():
    request = rendered(entry=entry(title_template="{scope!r}"))
    assert request.title == repr(SCOPE)


# --- the provenance block ----------------------------------------------------


def test_the_body_carries_the_policy_text_then_the_provenance_block():
    request = rendered(envelope=envelope(details={"title": "t"}))
    body, _, block = request.body.partition(PROVENANCE_HEADING)
    assert (
        body.strip() == "item_completed on turtlesedge/turtletracks needs a "
        "messaging pass."
    )
    assert block  # the block is appended regardless of what the template said


def test_the_provenance_block_carries_every_fact_spec_7_names():
    rich = envelope(
        EventType.milestone_released,
        initiative="summer-release",
        phase="build",
        milestone="v1.4",
        external_refs=[{"kind": "release", "url": "https://example/releases/v1.4.0"}],
    )
    block = provenance_block(
        consequence(
            envelope=rich,
            entry=entry(
                event_types=(EventType.milestone_released,),
                title_template="t",
                body_template="b",
                artifact_refs=("b964d217",),
                channels=("app_store",),
                deliverable_classes=("store_listing",),
                musher_dispatch=True,
            ),
        )
    )
    # Originating event id/type + subject entity.
    assert f"originating event: {rich.event_id} (milestone_released)" in block
    assert "subject entity: milestone ms-4c72a1" in block
    # Matched policy id + evaluated policy artifact version id.
    assert "matched policy: messaging-refresh" in block
    assert f"evaluated policy artifact version: {VERSION_ID}" in block
    # Source scope / initiative / phase / milestone.
    assert f"source scope: {SCOPE}" in block
    assert "source initiative: summer-release" in block
    assert "source phase: build" in block
    assert "source milestone: v1.4" in block
    # External refs (the reconciled release URL), on their own line so the URL
    # is clickable rather than buried in a comma-separated run.
    assert "external ref (release): https://example/releases/v1.4.0" in block
    # Affected artifact refs + channels + deliverable classes.
    assert "affected artifact refs: b964d217" in block
    assert "channels: app_store" in block
    assert "deliverable classes: store_listing" in block
    # The dedup key: what an operator greps to find the delivery that made this.
    assert "delivery ledger key: p:turtlesedge:messaging-refresh:pm-evt-1" in block


def test_the_dispatch_intent_is_stated_both_ways_round():
    # PM has no dispatch field yet (snowline-pm #65), so if the payload key is
    # ignored this line is the only durable record of the policy's intent — and
    # printing "no" too means its absence never has to be interpreted.
    asked = provenance_block(consequence(entry=entry(musher_dispatch=True)))
    assert "musher dispatch requested: yes" in asked
    assert "never calls musher" in asked
    quiet = provenance_block(consequence(entry=entry(musher_dispatch=False)))
    assert "musher dispatch requested: no" in quiet


def test_an_absent_optional_fact_is_omitted_rather_than_printed_empty():
    block = provenance_block(consequence())
    assert "source milestone" not in block
    assert "source scope" in block


# --- the request the sink receives -------------------------------------------


def test_the_request_carries_the_destination_flags_and_provenance_handles():
    request = rendered(
        envelope=envelope(details={"title": "t"}),
        entry=entry(
            human_owned=True,
            musher_dispatch=True,
            owner_template="{tenant}-marketing",
            destination=PolicyDestination(
                scope="turtlesedge/marketing",
                initiative="messaging",
                phase="draft",
            ),
        ),
    )
    assert request.scope == "turtlesedge/marketing"
    assert request.initiative == "messaging"
    assert request.phase == "draft"
    assert request.human_owned is True
    assert request.musher_dispatch is True
    assert request.owner == "turtlesedge-marketing"
    # PM's verified origin vocabulary: policy-minted work is `ai_generated`.
    assert request.origin == ORIGIN_AI_GENERATED
    assert request.policy_id == "messaging-refresh"
    assert request.policy_version_id == VERSION_ID
    assert request.dedup_key.startswith("p:")


def test_a_policy_without_an_owner_template_mints_without_an_owner():
    assert rendered(envelope=envelope(details={"title": "t"})).owner is None


def test_the_mode_is_rendered_from_the_entry_not_assumed():
    values = template_values(
        consequence(entry=entry(mode=PolicyMode.approval_required))
    )
    assert values["mode"] == "approval_required"

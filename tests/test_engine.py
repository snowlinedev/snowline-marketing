"""The evaluation engine and the §14 acceptance criteria (spec §4, §6, §9, §14).

DB-backed throughout: the properties under test — "duplicate delivery of the
same event creates exactly one result", "unmatched events audit as `ignored`",
"cross-tenant fixtures are rejected with quarantine" — are LEDGER-PROVEN by
construction. A stubbed store would let a test pass while the uniqueness that
actually makes re-delivery safe was broken, which is the one thing this suite
must never do.

The acceptance tests drive `run_intake` over the SHIPPED capture and the SHIPPED
policy artifacts, twice, and assert on the rows that came out — the same code
path the live outbox will drive at cutover (spec §5: fixtures mode is a
first-class dev/CI surface, not a shim).
"""

from __future__ import annotations

import json
import logging

import pytest
import sqlalchemy as sa
from conftest import EVENT_FIXTURES_DIR, POLICY_FIXTURES_DIR, SCOPE, TENANT

from snowline_marketing.cursors import InMemoryCursorStore
from snowline_marketing.db import session_scope
from snowline_marketing.engine import (
    DedupKeyUnrenderable,
    Delivery,
    EvaluatedPolicySet,
    EvaluationHandler,
    EvaluationResult,
    EvaluationStalled,
    EvaluationStalledError,
    NoPolicySet,
    PendingConsequence,
    StallReason,
    evaluate,
    render_dedup_key,
    resolve_policy_set,
)
from snowline_marketing.events import EventEnvelope, EventType, parse_envelope
from snowline_marketing.intake import run_intake
from snowline_marketing.ledger import DeliveryLedger, DeliveryOutcome
from snowline_marketing.models import DeliveryLedgerEntry
from snowline_marketing.policies import PolicyMode, PolicySet
from snowline_marketing.policy_cache import ParseOutcome, PolicyCache
from snowline_marketing.policy_source import (
    InMemoryPolicyProvider,
    PolicyResolutionError,
    ResolutionFailure,
)
from snowline_marketing.sources import FixturesEventSource

TURTLESEDGE_BODY = (POLICY_FIXTURES_DIR / "turtlesedge.json").read_text()
SNOWLINEDEV_BODY = (POLICY_FIXTURES_DIR / "snowlinedev.json").read_text()
PROSE_BODY = (POLICY_FIXTURES_DIR / "malformed-not-json.json").read_text()
VERSION_ID = "gv-7f3a91c4"

# The shipped capture, as the intake loop sees it. Names rather than magic
# numbers: an assertion that says which fixture it means survives someone
# adding a fifteenth event.
CROSS_TENANT_FIXTURE = "0150-cross-tenant-item-completed.json"
RELEASE_FIXTURE = "0100-milestone-released.json"
COMPLETED_FIXTURE = "0010-item-completed.json"
REOPENED_FIXTURE = "0030-item-reopened.json"
RECURRING_FIXTURE = "0110-recurring-item-fired.json"


def fixture_envelope(name: str) -> EventEnvelope:
    parsed = parse_envelope((EVENT_FIXTURES_DIR / name).read_bytes())
    assert isinstance(parsed, EventEnvelope), getattr(parsed, "detail", "")
    return parsed


def _mint(dedup_key: str, *, tenant: str = TENANT, ref: str = "pm-item-1") -> None:
    """Advance a matched row to `created` the way the minting layer (§7) will:
    outcome and item ref in one UPDATE — which the bidirectional
    ck_delivery_ledger_created_item_ref makes the only writable shape of that
    transition anyway."""
    with session_scope() as session:
        session.execute(
            sa.update(DeliveryLedgerEntry)
            .where(
                DeliveryLedgerEntry.tenant == tenant,
                DeliveryLedgerEntry.dedup_key == dedup_key,
            )
            .values(outcome=DeliveryOutcome.created.value, created_item_ref=ref)
        )


def provider_for(
    tenant: str = TENANT, body: str = TURTLESEDGE_BODY, version_id: str = VERSION_ID
) -> InMemoryPolicyProvider:
    provider = InMemoryPolicyProvider()
    provider.put(tenant, version_id, body)
    return provider


def resolved(
    tenant: str = TENANT, body: str = TURTLESEDGE_BODY, version_id: str = VERSION_ID
):
    return resolve_policy_set(tenant, provider=provider_for(tenant, body, version_id))


class DownProvider:
    """A provider that is unreachable until told otherwise — governance having
    a bad afternoon. Counts calls, because "one resolution per pass" is part of
    the handler's contract."""

    def __init__(self, inner: InMemoryPolicyProvider, *, down: bool = True) -> None:
        self.inner = inner
        self.down = down
        self.calls = 0

    def resolve(self, tenant: str):
        self.calls += 1
        if self.down:
            return PolicyResolutionError(
                tenant=tenant,
                failure=ResolutionFailure.unavailable,
                detail="governance unreachable",
            )
        return self.inner.resolve(tenant)


# --- resolution --------------------------------------------------------------


def test_a_tenant_with_no_artifact_resolves_to_no_policies(migrated_db):
    # `not_found` is an ANSWER (spec §14): evaluable, and every event audits as
    # ignored. Never a stall.
    resolution = resolve_policy_set("nobody", provider=InMemoryPolicyProvider())
    assert isinstance(resolution, NoPolicySet)
    assert resolution.tenant == "nobody"


def test_an_unreachable_governance_stalls_rather_than_meaning_no_policies(migrated_db):
    # The distinction the whole design turns on: we did not learn the tenant's
    # rules, so rounding down to "no policies" would silently stop minting the
    # moment governance hiccuped.
    resolution = resolve_policy_set(
        TENANT, provider=DownProvider(provider_for(), down=True)
    )
    assert isinstance(resolution, EvaluationStalled)
    assert resolution.reason is StallReason.policy_unavailable
    assert resolution.version_id is None


def test_a_malformed_governance_response_stalls_too(migrated_db):
    class ShapeShifter:
        def resolve(self, tenant):
            return PolicyResolutionError(
                tenant=tenant,
                failure=ResolutionFailure.malformed_response,
                detail="200 with a proxy error page",
            )

    resolution = resolve_policy_set(TENANT, provider=ShapeShifter())
    assert isinstance(resolution, EvaluationStalled)
    assert resolution.reason is StallReason.policy_unavailable


def test_a_quarantined_version_stalls_distinguishably(migrated_db):
    # Same control flow as unavailable, different sentence for the operator:
    # "fix your artifact", not "governance is down". And the version id is
    # carried, because that is what they quote when revising.
    resolution = resolve_policy_set(
        TENANT, provider=provider_for(body=PROSE_BODY, version_id="gv-prose")
    )
    assert isinstance(resolution, EvaluationStalled)
    assert resolution.reason is StallReason.policy_quarantined
    assert resolution.version_id == "gv-prose"
    # ...and the quarantine is DURABLE: the §11 operator listing shows it.
    row = PolicyCache().get("gv-prose")
    assert row is not None and row.outcome is ParseOutcome.quarantined


def test_a_valid_version_resolves_with_its_id(migrated_db):
    resolution = resolved()
    assert isinstance(resolution, EvaluatedPolicySet)
    assert resolution.version_id == VERSION_ID
    assert isinstance(resolution.policy_set, PolicySet)
    assert resolution.policy_set.tenant == TENANT


# --- the four event endings --------------------------------------------------


def test_a_tenant_with_no_policies_audits_every_event_as_ignored(migrated_db):
    # Spec §14: "unmatched events audit as `ignored` and create no work". The
    # row exists precisely so that "nothing happened" is a recorded decision.
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    result = evaluate(envelope, NoPolicySet(tenant=TENANT, detail="no artifact"))
    assert isinstance(result, EvaluationResult)
    assert result.outcomes == (DeliveryOutcome.ignored,)
    assert result.consequences == ()
    (row,) = DeliveryLedger().for_event(TENANT, envelope.event_id)
    assert row.outcome is DeliveryOutcome.ignored
    assert row.policy_id is None
    # No policy set applied, so there is no version to name — the one case the
    # nullable column exists for.
    assert row.policy_version_id is None
    assert row.detail


def test_an_unmatched_event_is_ignored_against_a_named_version(migrated_db):
    # The other ignored case, and the reason the column is only CONDITIONALLY
    # nullable: a version WAS evaluated here, and "which version decided this
    # event was uninteresting?" is a real audit question.
    envelope = fixture_envelope(REOPENED_FIXTURE)
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    assert result.outcomes == (DeliveryOutcome.ignored,)
    (row,) = DeliveryLedger().for_event(TENANT, envelope.event_id)
    assert row.policy_version_id == VERSION_ID
    assert row.policy_id is None


def test_a_cross_tenant_envelope_is_quarantined_not_dropped(migrated_db):
    # Spec §14: "cross-tenant fixtures are rejected with quarantine, not
    # silently dropped". Consumed, because it can never match legitimately no
    # matter how often it comes back.
    envelope = fixture_envelope(CROSS_TENANT_FIXTURE)
    assert envelope.tenant != TENANT
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    assert result.outcomes == (DeliveryOutcome.quarantined,)
    assert result.consequences == ()
    # Filed under the tenant whose pass rejected it, NOT under the tenant the
    # envelope claimed — writing rows under a foreign org's name would be the
    # cross-tenant attribution §3 forbids.
    assert DeliveryLedger().for_event(envelope.tenant, envelope.event_id) == []
    (row,) = DeliveryLedger().for_event(TENANT, envelope.event_id)
    assert row.outcome is DeliveryOutcome.quarantined
    assert envelope.tenant in row.detail
    # The version that was in force when we refused: a real audit fact.
    assert row.policy_version_id == VERSION_ID


def test_two_breaches_sharing_an_event_id_get_distinct_quarantine_rows(migrated_db):
    # The quarantine key names the envelope's declared tenant, per breach: two
    # misrouted envelopes from DIFFERENT foreign orgs sharing an event id are
    # two distinct routing failures, and each must leave its own record —
    # folding them into one row would report the second breach as a mere
    # re-delivery of the first.
    resolution = resolved()
    first_foreign = fixture_envelope(CROSS_TENANT_FIXTURE)
    second_foreign = first_foreign.model_copy(update={"tenant": "acme-corp"})
    first = evaluate(first_foreign, resolution)
    second = evaluate(second_foreign, resolution)
    assert isinstance(first, EvaluationResult) and isinstance(second, EvaluationResult)
    assert first.outcomes == (DeliveryOutcome.quarantined,)
    # Not deduplicated: this is a NEW breach, not a repeat of the first.
    assert second.outcomes == (DeliveryOutcome.quarantined,)
    rows = DeliveryLedger().for_event(TENANT, first_foreign.event_id)
    assert {r.dedup_key for r in rows} == {
        f"e:{TENANT}:{first_foreign.event_id}:quarantined:{first_foreign.tenant}",
        f"e:{TENANT}:{first_foreign.event_id}:quarantined:acme-corp",
    }


def test_a_match_produces_a_row_and_a_pending_consequence(migrated_db):
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    assert result.outcomes == (DeliveryOutcome.matched,)
    (consequence,) = result.consequences
    assert isinstance(consequence, PendingConsequence)
    assert consequence.policy_id == "messaging-refresh-on-marketing-impact"
    assert consequence.consequence.value == "messaging_refresh"
    assert consequence.destination.scope == "turtlesedge/marketing"
    assert consequence.destination.initiative == "messaging"
    assert consequence.mode is PolicyMode.active
    assert consequence.mints
    assert consequence.policy_version_id == VERSION_ID
    # Everything §7's provenance needs travels whole rather than as a copied
    # projection: the envelope (event, entity, scope, external refs, details)
    # and the entry (templates, flags, artifact refs, channels).
    assert consequence.envelope is envelope
    assert consequence.entry.title_template
    assert consequence.entry.human_owned is True
    assert consequence.entry.artifact_refs == ("b964d217",)

    (row,) = DeliveryLedger().for_event(TENANT, envelope.event_id)
    assert row.outcome is DeliveryOutcome.matched
    assert row.policy_id == consequence.policy_id
    assert row.policy_version_id == VERSION_ID
    assert row.dedup_key == consequence.dedup_key
    assert row.created_item_ref is None  # the minting layer's column


def test_every_matching_entry_gets_its_own_row_in_declaration_order(migrated_db):
    # A release matches three entries of the shipped artifact; each is its own
    # delivery, its own dedup key and its own row.
    envelope = fixture_envelope(RELEASE_FIXTURE)
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    assert [c.policy_id for c in result.consequences] == [
        "listing-regeneration-on-release",
        "announcement-preparation-on-release",
        "app-store-listing-publish",
    ]
    rows = DeliveryLedger().for_event(TENANT, envelope.event_id)
    assert len(rows) == 3
    assert all(r.policy_version_id == VERSION_ID for r in rows)


def test_a_stall_is_passed_through_and_writes_nothing(migrated_db):
    stall = EvaluationStalled(
        tenant=TENANT, reason=StallReason.policy_unavailable, detail="down"
    )
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    assert evaluate(envelope, stall) is stall
    assert DeliveryLedger().for_event(TENANT, envelope.event_id) == []


# --- dedup ------------------------------------------------------------------


def test_a_repeat_of_an_unminted_match_re_owes_the_consequence(migrated_db):
    # Spec §4's "recoverably convergent", the recovery half: a `matched` row
    # with no item ref is a mint that never happened (a crash between the
    # ledger commit and §7's mint/ack), so re-delivery must re-emit the
    # consequence against the SAME row — reporting `deduplicated` here would
    # lose the work forever, silently.
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    resolution = resolved()
    first = evaluate(envelope, resolution)
    second = evaluate(envelope, resolution)
    assert isinstance(first, EvaluationResult) and isinstance(second, EvaluationResult)
    assert first.outcomes == (DeliveryOutcome.matched,)
    assert second.outcomes == (DeliveryOutcome.matched,)
    (original,) = first.consequences
    (re_owed,) = second.consequences
    assert re_owed.dedup_key == original.dedup_key  # the same claim, the same work
    # One row, and the SAME row — convergence: the re-owed consequence is keyed
    # to the claim the first delivery made, not to a second row.
    assert len(DeliveryLedger().for_event(TENANT, envelope.event_id)) == 1
    assert second.deliveries[0].record == first.deliveries[0].record


def test_a_repeat_of_a_minted_match_deduplicates_and_owes_nothing(migrated_db):
    # ...and the converged half: once the row shows the work was produced, a
    # repeat delivery owes nothing and reports the existing result.
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    resolution = resolved()
    first = evaluate(envelope, resolution)
    assert isinstance(first, EvaluationResult)
    (consequence,) = first.consequences
    _mint(consequence.dedup_key, ref="pm-item-77")
    second = evaluate(envelope, resolution)
    assert isinstance(second, EvaluationResult)
    assert second.outcomes == (DeliveryOutcome.deduplicated,)
    assert second.consequences == ()
    # The record reports the state of the work, item ref and all.
    assert second.deliveries[0].record.outcome is DeliveryOutcome.created
    assert second.deliveries[0].record.created_item_ref == "pm-item-77"


def test_a_re_owed_consequence_carries_the_current_version(migrated_db):
    # The row keeps the version that first claimed the key — it audits the
    # claim as it was decided — while the re-owed consequence carries the
    # CURRENT one: the mint that eventually happens is the one the current
    # policy would produce.
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    evaluate(envelope, resolved())
    second = evaluate(envelope, resolved(version_id="gv-revised"))
    assert isinstance(second, EvaluationResult)
    (re_owed,) = second.consequences
    assert re_owed.policy_version_id == "gv-revised"
    assert second.deliveries[0].record.policy_version_id == VERSION_ID


def test_a_repeat_ignored_event_is_a_no_op_too(migrated_db):
    # Event-level rows converge exactly like policy rows: an unmatched event
    # re-delivered ten times leaves one audit row, not ten.
    envelope = fixture_envelope(REOPENED_FIXTURE)
    resolution = resolved()
    evaluate(envelope, resolution)
    second = evaluate(envelope, resolution)
    assert isinstance(second, EvaluationResult)
    assert second.outcomes == (DeliveryOutcome.deduplicated,)
    # The ROW still says what it is; only the DELIVERY reports "nothing new".
    assert second.deliveries[0].record.outcome is DeliveryOutcome.ignored
    assert len(DeliveryLedger().for_event(TENANT, envelope.event_id)) == 1


def test_the_default_dedup_key_is_the_spec_logical_key(migrated_db):
    # The rendered template under the ledger's policy-level namespace ("p:",
    # ledger.py) — the consequence carries the STORED key, the handle §7
    # writes the created item's ref back to.
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    (consequence,) = result.consequences
    assert consequence.dedup_key == (
        f"p:{TENANT}:messaging-refresh-on-marketing-impact:{envelope.event_id}"
    )


def test_a_custom_dedup_template_renders_from_the_envelope(migrated_db):
    # The release-listing policy keys on the MILESTONE, not the event id: two
    # different release events for one milestone must not regenerate the listing
    # twice.
    envelope = fixture_envelope(RELEASE_FIXTURE)
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    listing = next(
        c
        for c in result.consequences
        if c.policy_id == "listing-regeneration-on-release"
    )
    assert listing.dedup_key == f"p:{TENANT}:listing-regeneration-on-release:v1.4"


def test_a_minted_milestone_keyed_policy_dedups_across_events(migrated_db):
    envelope = fixture_envelope(RELEASE_FIXTURE)
    resolution = resolved()
    first = evaluate(envelope, resolution)
    assert isinstance(first, EvaluationResult)
    # §7 does its job for the milestone-keyed match — the row now shows the
    # listing was regenerated. (Unminted, it would be re-owed instead: see the
    # recoverable-convergence tests above.)
    listing = next(
        c
        for c in first.consequences
        if c.policy_id == "listing-regeneration-on-release"
    )
    _mint(listing.dedup_key, ref="pm-item-listing")
    # A SECOND, distinct release event for the same milestone (a re-release, a
    # replayed producer): the event-keyed policies fire again, the minted
    # milestone-keyed one does not.
    again = envelope.model_copy(update={"event_id": "pm-evt-0000110-again"})
    result = evaluate(again, resolution)
    assert isinstance(result, EvaluationResult)
    deduped = {
        d.record.policy_id
        for d in result.deliveries
        if d.outcome is DeliveryOutcome.deduplicated
    }
    assert deduped == {"listing-regeneration-on-release"}


def test_a_within_evaluation_key_collision_fails_loudly_not_silently(
    migrated_db, caplog
):
    """The runtime guard behind the parse-time identical-template check: two
    DIFFERENT templates whose field VALUES coincide on one envelope render one
    key. Silent swallowing is the one forbidden outcome, so the second entry's
    delivery is `failed` — §11's retry/dead-letter vocabulary, operator-visible
    — with a detail naming both policies, and the ledger is never touched for
    it (the row belongs to the earlier entry)."""
    body = json.dumps(
        {
            "schema_version": 1,
            "tenant": TENANT,
            "policies": [
                {
                    "policy_id": "first-claimant",
                    "event_types": ["item_completed"],
                    "consequence": "messaging_refresh",
                    "destination": {"scope": "turtlesedge/marketing"},
                    "title_template": "Refresh",
                    "body_template": "Body.",
                    "dedup_key_template": "{tenant}:{event_id}",
                },
                {
                    "policy_id": "value-collider",
                    "event_types": ["item_completed"],
                    "consequence": "review_sweep",
                    "destination": {"scope": "turtlesedge/marketing"},
                    "title_template": "Sweep",
                    "body_template": "Body.",
                    # A different template — the static check passes — that
                    # renders the same string when entity_id == event_id.
                    "dedup_key_template": "{tenant}:{entity_id}",
                },
            ],
        }
    )
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    same_id_subject = envelope.subject.model_copy(update={"id": envelope.event_id})
    envelope = envelope.model_copy(update={"subject": same_id_subject})
    with caplog.at_level(logging.ERROR, logger="snowline_marketing.engine"):
        result = evaluate(envelope, resolved(body=body, version_id="gv-collide"))
    assert isinstance(result, EvaluationResult)
    assert result.outcomes == (DeliveryOutcome.matched, DeliveryOutcome.failed)
    matched, failed = result.deliveries
    assert failed.consequence is None
    assert failed.record == matched.record  # the earlier entry's row, read-only
    assert failed.detail is not None
    assert "first-claimant" in failed.detail and "value-collider" in failed.detail
    # Only the winner's row exists — the collider never touched the ledger.
    assert len(DeliveryLedger().for_event(TENANT, envelope.event_id)) == 1
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors, "the collision must be logged loudly"
    assert "first-claimant" in errors[0].getMessage()
    assert "value-collider" in errors[0].getMessage()


def test_render_dedup_key_refuses_to_render_an_absent_field(migrated_db):
    """Unreachable through matching — parse-time validation guarantees the
    conditional fields for every event type an entry selects — but pinned
    because the failure it guards against is silent and permanent: a template
    rendering the constant "None" swallows every later delivery as a duplicate,
    and the work is never done and never reported missing."""
    resolution = resolved()
    assert isinstance(resolution, EvaluatedPolicySet)
    milestone_keyed = resolution.policy_set.entry("listing-regeneration-on-release")
    assert milestone_keyed is not None
    # An abandon event carries no milestone; the milestone-keyed template would
    # render the constant "...:None" for every such event.
    with pytest.raises(DedupKeyUnrenderable) as excinfo:
        render_dedup_key(milestone_keyed, fixture_envelope("0040-item-abandoned.json"))
    assert "milestone" in str(excinfo.value)


def test_the_render_vocabulary_is_pinned_to_the_policy_vocabulary():
    # The import-time assertion's other half, visible in the suite: the
    # engine's renderer set and the template vocabulary policies validate
    # against are the SAME set, so a field cannot be admitted on one side and
    # dropped on the other.
    from snowline_marketing import engine, policies

    assert set(engine._DEDUP_KEY_VALUES) == policies.DEDUP_KEY_FIELDS


def test_a_result_cannot_be_built_without_deliveries():
    # `deliveries` has no default: every consumed event carries at least one
    # delivery (the docstring's contract), and a default would let a code path
    # construct the "nothing happened, trust me" result the type forbids.
    envelope = fixture_envelope(COMPLETED_FIXTURE)
    with pytest.raises(TypeError):
        EvaluationResult(  # type: ignore[call-arg]
            tenant=TENANT, envelope=envelope, policy_version_id=None
        )


# --- modes -------------------------------------------------------------------


def test_a_dry_run_match_records_a_row_but_mints_nothing(migrated_db):
    # §11's dry-run posture, at the policy level: the match is real and audited,
    # the consequence is marked non-minting. `mode` is recorded on the ROW too,
    # so an auditor can see why it never became an item without re-resolving a
    # policy version that may since have been revised.
    envelope = fixture_envelope(RECURRING_FIXTURE)
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    dry = next(
        c for c in result.consequences if c.policy_id == "monthly-metrics-snapshot"
    )
    assert dry.mode is PolicyMode.dry_run
    assert not dry.mints
    row = DeliveryLedger().get(TENANT, dry.dedup_key)
    assert row is not None
    assert row.outcome is DeliveryOutcome.matched
    assert "dry_run" in row.detail


def test_approval_required_is_a_gate_not_a_negation(migrated_db):
    # `mints` says "this may produce nothing, ever" — only dry-run is that. An
    # approval-gated match IS owed work; an operator verb releases it (§12).
    # Collapsing the two would make a gated policy look disarmed.
    envelope = fixture_envelope(RELEASE_FIXTURE)
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    publish = next(
        c for c in result.consequences if c.policy_id == "app-store-listing-publish"
    )
    assert publish.mode is PolicyMode.approval_required
    assert publish.mints


# --- the intake adapter ------------------------------------------------------


def test_the_handler_resolves_once_per_pass(migrated_db, event_fixtures_dir):
    # A resolution per event would be an HTTP round trip per event against
    # governance, and would let a mid-pass revision split one pass's rows across
    # two policy versions.
    provider = DownProvider(provider_for(), down=False)
    handler = EvaluationHandler(TENANT, provider=provider)
    run_intake(
        FixturesEventSource(event_fixtures_dir),
        handler,
        cursor_store=InMemoryCursorStore(),
        on_malformed=lambda m: None,
    )
    assert len(handler.results) > 1
    assert provider.calls == 1


def test_the_handler_does_not_resolve_for_an_empty_pass(migrated_db, tmp_path):
    # Lazily, so a pass with nothing to consume never calls governance at all.
    provider = DownProvider(provider_for(), down=False)
    handler = EvaluationHandler(TENANT, provider=provider)
    run_intake(
        FixturesEventSource(tmp_path), handler, cursor_store=InMemoryCursorStore()
    )
    assert provider.calls == 0


def test_the_handler_raises_on_a_stall(migrated_db):
    handler = EvaluationHandler(TENANT, provider=DownProvider(provider_for()))
    with pytest.raises(EvaluationStalledError) as excinfo:
        handler(fixture_envelope(COMPLETED_FIXTURE))
    assert excinfo.value.stall.reason is StallReason.policy_unavailable
    assert handler.stall is not None
    assert handler.results == []


def test_the_handler_returns_for_a_consumed_event(migrated_db):
    handler = EvaluationHandler(TENANT, provider=provider_for())
    handler(fixture_envelope(COMPLETED_FIXTURE))
    assert len(handler.results) == 1
    assert len(handler.consequences) == 1
    assert all(isinstance(d, Delivery) for d in handler.deliveries)


# --- §14 acceptance ----------------------------------------------------------


def _pass(event_fixtures_dir, provider, tenant=TENANT, **kwargs):
    """One intake pass with a fresh cursor — the deliberate re-delivery the
    at-least-once contract makes safe. A fresh cursor is exactly what a crash
    before any ack looks like."""
    handler = EvaluationHandler(tenant, provider=provider)
    result = run_intake(
        FixturesEventSource(event_fixtures_dir),
        handler,
        cursor_store=kwargs.pop("cursor_store", InMemoryCursorStore()),
        on_malformed=lambda malformed: None,
        **kwargs,
    )
    return handler, result


def _census(handler: EvaluationHandler) -> dict[DeliveryOutcome, int]:
    census: dict[DeliveryOutcome, int] = {}
    for delivery in handler.deliveries:
        census[delivery.outcome] = census.get(delivery.outcome, 0) + 1
    return census


def test_a_full_pass_over_the_capture(migrated_db, event_fixtures_dir):
    """The whole deterministic core end to end: shipped capture, shipped policy
    artifact, real ledger."""
    handler, result = _pass(event_fixtures_dir, provider_for())
    assert result.ok
    census = _census(handler)
    assert census[DeliveryOutcome.matched] == 8
    assert census[DeliveryOutcome.ignored] == 5
    assert census[DeliveryOutcome.quarantined] == 1
    assert DeliveryOutcome.deduplicated not in census
    assert len(handler.consequences) == 8
    # Every policy-level row names the version that decided it (spec §6's
    # contract requirement).
    rows = DeliveryLedger().list_for_tenant(TENANT)
    assert len(rows) == 14
    assert all(
        r.policy_version_id == VERSION_ID
        for r in rows
        if r.outcome is DeliveryOutcome.matched
    )


def test_duplicate_delivery_creates_exactly_one_result(migrated_db, event_fixtures_dir):
    """Spec §14's headline criterion, ledger-proven: the same capture consumed
    twice produces exactly one ROW per delivery. Nothing was minted between the
    passes, so every matched claim is still owed — the second pass re-emits
    those consequences against the SAME rows (spec §4's recoverable
    convergence) while the event-level deliveries dedup. Not one extra row,
    and not one row touched."""
    provider = provider_for()
    first, _ = _pass(event_fixtures_dir, provider)
    before = DeliveryLedger().list_for_tenant(TENANT)

    second, second_result = _pass(event_fixtures_dir, provider)
    after = DeliveryLedger().list_for_tenant(TENANT)

    assert second_result.ok
    assert second_result.delivered == len(first.results)
    assert _census(second) == {
        DeliveryOutcome.matched: 8,
        DeliveryOutcome.deduplicated: 6,
    }
    # The re-owed consequences are the SAME work — identical keys, same rows —
    # so §7 minting them still mints once per claim.
    assert {c.dedup_key for c in second.consequences} == {
        c.dedup_key for c in first.consequences
    }
    assert len(after) == len(before)
    # Not merely the same COUNT — the same rows, untouched, including the
    # timestamp that says when each delivery first converged.
    assert {(r.dedup_key, r.created_at) for r in after} == {
        (r.dedup_key, r.created_at) for r in before
    }


def test_a_minted_capture_re_delivers_as_a_pure_no_op(migrated_db, event_fixtures_dir):
    """The steady state after §7 has done its job: every consequence carried
    out, and a full re-delivery of the capture owes nothing, mints nothing,
    and touches no row."""
    provider = provider_for()
    first, _ = _pass(event_fixtures_dir, provider)
    for index, consequence in enumerate(first.consequences):
        _mint(consequence.dedup_key, ref=f"pm-item-{index}")
    before = DeliveryLedger().list_for_tenant(TENANT)

    second, second_result = _pass(event_fixtures_dir, provider)
    after = DeliveryLedger().list_for_tenant(TENANT)

    assert second_result.ok
    assert set(_census(second)) == {DeliveryOutcome.deduplicated}
    assert second.consequences == ()
    assert {(r.dedup_key, r.created_at) for r in after} == {
        (r.dedup_key, r.created_at) for r in before
    }


def test_unmatched_events_audit_as_ignored_and_create_no_work(
    migrated_db, event_fixtures_dir
):
    handler, _ = _pass(event_fixtures_dir, provider_for())
    ignored = [
        d.record.event_id
        for d in handler.deliveries
        if d.record.outcome is DeliveryOutcome.ignored
    ]
    # The reopen/abandon/rescope/state-change events, and the phase completion
    # that carries NO marketing-impact relation — §9's "never falls back to
    # matching every implementation item", proven at the fixtures layer.
    assert fixture_envelope(REOPENED_FIXTURE).event_id in ignored
    assert fixture_envelope("0070-initiative-phase-completed.json").event_id in ignored
    assert fixture_envelope(
        "0160-phase-completed-relation-signal.json"
    ).event_id not in (ignored)
    for event_id in ignored:
        rows = DeliveryLedger().for_event(TENANT, event_id)
        assert [r.outcome for r in rows] == [DeliveryOutcome.ignored]
        assert rows[0].policy_id is None
        assert rows[0].created_item_ref is None


def test_the_cross_tenant_fixture_is_quarantined_in_a_full_pass(
    migrated_db, event_fixtures_dir
):
    handler, result = _pass(event_fixtures_dir, provider_for())
    # Consumed, not stalled: the pass completes and the cursor moves past it.
    assert result.ok
    foreign = fixture_envelope(CROSS_TENANT_FIXTURE)
    (row,) = DeliveryLedger().for_event(TENANT, foreign.event_id)
    assert row.outcome is DeliveryOutcome.quarantined
    assert "snowlinedev" in row.detail
    assert not any(
        c.envelope.event_id == foreign.event_id for c in handler.consequences
    )


def test_a_stall_leaves_the_events_unacked_and_re_delivers_on_recovery(
    migrated_db, event_fixtures_dir
):
    """The visible stall spec §6 requires, and the recovery §4 promises: while
    governance is unreachable nothing is consumed and nothing is recorded; when
    it comes back the same events evaluate normally."""
    provider = DownProvider(provider_for(), down=True)
    store = InMemoryCursorStore()
    source = FixturesEventSource(event_fixtures_dir)

    stalled_handler = EvaluationHandler(TENANT, provider=provider)
    stalled = run_intake(
        source, stalled_handler, cursor_store=store, on_malformed=lambda m: None
    )
    assert not stalled.ok
    assert stalled.acked_position is None
    assert store.read(source.source_key) is None  # the cursor never moved
    assert "EvaluationStalledError" in stalled.failure.error
    assert stalled_handler.stall is not None
    assert DeliveryLedger().list_for_tenant(TENANT) == []

    provider.down = False
    recovered_handler = EvaluationHandler(TENANT, provider=provider)
    recovered = run_intake(
        source, recovered_handler, cursor_store=store, on_malformed=lambda m: None
    )
    assert recovered.ok
    # The very first event of the capture is delivered again — nothing was lost
    # to the stalled pass.
    assert recovered_handler.results[0].envelope.event_id == (
        fixture_envelope(COMPLETED_FIXTURE).event_id
    )
    assert len(recovered_handler.consequences) == 8


def test_a_quarantined_artifact_stalls_the_pass_visibly(
    migrated_db, event_fixtures_dir
):
    # Distinguishable from "governance is down", because the operator's action
    # is different: revise the artifact.
    handler, result = _pass(
        event_fixtures_dir, provider_for(body=PROSE_BODY, version_id="gv-prose")
    )
    assert not result.ok
    assert handler.stall is not None
    assert handler.stall.reason is StallReason.policy_quarantined
    assert handler.stall.version_id == "gv-prose"
    assert DeliveryLedger().list_for_tenant(TENANT) == []


def test_two_tenants_run_on_the_same_code_with_separate_artifacts(
    migrated_db, event_fixtures_dir
):
    """Spec §14: "TurtleTracks and a second tenant (Snowline itself) run on the
    same code with separate policy artifacts". Same capture, same engine, two
    tenants — and the isolation boundary is the artifact's own tenant, not a
    predicate anyone had to remember to write."""
    turtlesedge, _ = _pass(event_fixtures_dir, provider_for())
    snowlinedev, _ = _pass(
        event_fixtures_dir,
        provider_for("snowlinedev", SNOWLINEDEV_BODY, "gv-snowline-1"),
        tenant="snowlinedev",
    )

    # The one snowlinedev event in the capture matches ITS artifact...
    foreign = fixture_envelope(CROSS_TENANT_FIXTURE)
    assert [c.policy_id for c in snowlinedev.consequences] == [
        "messaging-refresh-on-marketing-impact"
    ]
    assert snowlinedev.consequences[0].envelope.event_id == foreign.event_id
    assert snowlinedev.consequences[0].destination.scope == "snowlinedev/marketing"
    # ...and every turtlesedge event is quarantined out of that tenant's pass,
    # rather than matching rules that look identical.
    snowline_rows = DeliveryLedger().list_for_tenant("snowlinedev")
    quarantined = [r for r in snowline_rows if r.outcome is DeliveryOutcome.quarantined]
    assert len(quarantined) == 10
    assert all(TENANT in r.detail for r in quarantined)

    # Neither tenant's ledger contains a row belonging to the other.
    assert all(r.tenant == "snowlinedev" for r in snowline_rows)
    turtle_rows = DeliveryLedger().list_for_tenant(TENANT)
    assert all(r.policy_version_id in (VERSION_ID, None) for r in turtle_rows)
    assert len(turtlesedge.consequences) == 8


def test_scopes_are_not_the_isolation_boundary(migrated_db):
    """A guard against the plausible wrong design: `payload.scope` is a
    predicate an operator writes, while `tenant` is the boundary the engine
    holds. An envelope on an unexpected project scope is an ordinary
    non-match (`ignored`); an envelope from another org is a quarantine."""
    on_scope = fixture_envelope("0060-item-rescoped.json")
    assert on_scope.payload.scope != SCOPE
    result = evaluate(on_scope, resolved())
    assert isinstance(result, EvaluationResult)
    assert result.outcomes == (DeliveryOutcome.ignored,)


@pytest.mark.parametrize("event_type", list(EventType))
def test_every_event_type_evaluates_without_special_casing(
    migrated_db, event_type, event_fixtures_dir
):
    # The engine has no per-type branch anywhere; this pins that claim across
    # the whole v1 vocabulary rather than the types the shipped policies happen
    # to select.
    from snowline_marketing.sources import iter_fixture_envelopes

    envelope = next(
        e
        for e in iter_fixture_envelopes(event_fixtures_dir)
        if isinstance(e, EventEnvelope) and e.event_type is event_type
    )
    result = evaluate(envelope, resolved())
    assert isinstance(result, EvaluationResult)
    assert result.deliveries  # every consumed event leaves an audit row

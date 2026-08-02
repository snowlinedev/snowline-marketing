"""§11's dry-run: evaluate a candidate policy version against captured
fixtures, report what would have been minted, mint nothing.

Most of these run against the shipped capture and the shipped turtlesedge
artifact alone — no `migrated_db` needed, which is the point: a dry-run must
be exercisable with no Postgres in sight, same as the deterministic core it
drives. The no-trace tests are the exception; they need Postgres UP
precisely to prove nothing landed in it.
"""

from __future__ import annotations

import json

import sqlalchemy as sa
from conftest import EVENT_FIXTURES_DIR, POLICY_FIXTURES_DIR, TENANT

from snowline_marketing.db import session_scope
from snowline_marketing.dryrun import DEFAULT_DRY_RUN_VERSION_ID, dry_run, render_text
from snowline_marketing.engine import StallReason
from snowline_marketing.ledger import DeliveryLedger, DeliveryOutcome
from snowline_marketing.models import ConsumerCursor
from snowline_marketing.policy_cache import PolicyCache
from snowline_marketing.sources import fixture_files

TURTLESEDGE_BODY = (POLICY_FIXTURES_DIR / "turtlesedge.json").read_text()
PROSE_BODY = (POLICY_FIXTURES_DIR / "malformed-not-json.json").read_text()


# --- report correctness -------------------------------------------------------


def test_report_counts_match_the_engine_acceptance_expectations():
    # The same shipped capture and artifact `test_engine.py`'s
    # `test_a_full_pass_over_the_capture` proves against a real ledger: 8
    # matched, 5 ignored, 1 quarantined, nothing deduplicated on a single pass.
    report = dry_run(TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT)
    assert report.ok
    assert report.version_id == DEFAULT_DRY_RUN_VERSION_ID
    assert report.counts[DeliveryOutcome.matched] == 8
    assert report.counts[DeliveryOutcome.ignored] == 5
    assert report.counts[DeliveryOutcome.quarantined] == 1
    assert DeliveryOutcome.deduplicated not in report.counts
    assert len(report.would_mint) == 8


def test_a_custom_version_id_is_recorded_and_reported():
    report = dry_run(
        TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT, version_id="gv-candidate-7"
    )
    assert report.version_id == "gv-candidate-7"


def test_would_mint_carries_the_full_consequence_summary():
    report = dry_run(TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT)
    messaging = next(
        m
        for m in report.would_mint
        if m.policy_id == "messaging-refresh-on-marketing-impact"
    )
    assert messaging.consequence.value == "messaging_refresh"
    assert messaging.mode.value == "active"
    assert messaging.mints is True
    assert messaging.destination_scope == "turtlesedge/marketing"
    assert messaging.destination_initiative == "messaging"
    assert messaging.destination_phase is None
    assert messaging.title_template


def test_a_dry_run_mode_policy_is_reported_as_not_minting():
    # The shipped artifact's own `monthly-metrics-snapshot` policy is
    # `mode: dry_run` — the preview must say so, distinct from an
    # `approval_required` match, which IS owed work (spec §11 vs §12).
    report = dry_run(TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT)
    snapshot = next(
        m for m in report.would_mint if m.policy_id == "monthly-metrics-snapshot"
    )
    assert snapshot.mode.value == "dry_run"
    assert snapshot.mints is False
    publish = next(
        m for m in report.would_mint if m.policy_id == "app-store-listing-publish"
    )
    assert publish.mode.value == "approval_required"
    assert publish.mints is True


def test_malformed_fixture_envelopes_are_reported_not_dropped():
    report = dry_run(TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT)
    expected_names = {
        p.name for p in fixture_files(EVENT_FIXTURES_DIR) if "-malformed-" in p.name
    }
    assert {m.ref.rsplit("/", 1)[-1] for m in report.malformed} == expected_names
    assert all(m.detail for m in report.malformed)


def test_dry_run_accepts_a_mapping_body():
    report = dry_run(json.loads(TURTLESEDGE_BODY), EVENT_FIXTURES_DIR, tenant=TENANT)
    assert report.ok
    assert report.counts[DeliveryOutcome.matched] == 8


def test_dry_run_accepts_a_bytes_body():
    report = dry_run(
        TURTLESEDGE_BODY.encode("utf-8"), EVENT_FIXTURES_DIR, tenant=TENANT
    )
    assert report.ok
    assert report.counts[DeliveryOutcome.matched] == 8


def test_an_empty_fixtures_directory_is_a_no_op_preview(tmp_path):
    # Mirrors `test_the_handler_does_not_resolve_for_an_empty_pass`: a pass
    # with nothing to consume never resolves the candidate at all.
    report = dry_run(TURTLESEDGE_BODY, tmp_path, tenant=TENANT)
    assert report.ok
    assert report.events == ()
    assert report.counts == {}
    assert report.malformed == ()


# --- stalls: a broken candidate reports itself broken -------------------------


def test_a_malformed_candidate_body_reports_stalled():
    report = dry_run(
        PROSE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT, version_id="gv-prose"
    )
    assert not report.ok
    assert report.stalled is not None
    assert report.stalled.reason is StallReason.policy_quarantined
    assert report.stalled.version_id == "gv-prose"
    assert "not_json" in report.stalled.detail
    assert report.events == ()
    assert report.counts == {}
    assert report.would_mint == ()


def test_a_tenant_mismatched_candidate_reports_stalled():
    other = json.loads(TURTLESEDGE_BODY)
    other["tenant"] = "someone-else"
    report = dry_run(other, EVENT_FIXTURES_DIR, tenant=TENANT)
    assert not report.ok
    assert report.stalled is not None
    assert report.stalled.reason is StallReason.policy_quarantined
    assert "tenant_mismatch" in report.stalled.detail


# --- dedup within a dry-run mirrors production --------------------------------


def _duplicate_event_capture(tmp_path) -> None:
    """A tiny two-file capture, both files the SAME event — the "captured
    twice" scenario a flaky producer or a re-exported capture can produce."""
    from snowline_marketing.events import SCHEMA_VERSION

    envelope = {
        "schema_version": SCHEMA_VERSION,
        "event_id": "pm-evt-dup-0001",
        "event_type": "item_completed",
        "tenant": TENANT,
        "occurred_at": "2026-07-20T12:00:00+00:00",
        "subject": {"kind": "work_item", "id": "dup-item"},
        "payload": {"scope": "turtlesedge/turtletracks"},
    }
    (tmp_path / "0010-first.json").write_text(json.dumps(envelope))
    (tmp_path / "0020-again.json").write_text(json.dumps(envelope))


_MATCH_ALL_COMPLETED_BODY = {
    "schema_version": 1,
    "tenant": TENANT,
    "policies": [
        {
            "policy_id": "review-on-completion",
            "event_types": ["item_completed"],
            "consequence": "review_sweep",
            "destination": {"scope": "turtlesedge/marketing"},
            "title_template": "Review",
            "body_template": "Body.",
        }
    ],
}

_NO_POLICIES_BODY = {"schema_version": 1, "tenant": TENANT, "policies": []}


def test_a_repeat_ignored_event_is_deduplicated_within_a_dry_run(tmp_path):
    # The event-level convergence engine.py's module docstring promises:
    # "ignored"/"quarantined" rows have no second step, so a repeat delivery
    # is ALWAYS `deduplicated` — no minting layer required to observe it,
    # unlike a matched row (see the test below). A tenant with no policies
    # ignores every event, which is the simplest way to hit this path.
    _duplicate_event_capture(tmp_path)
    report = dry_run(_NO_POLICIES_BODY, tmp_path, tenant=TENANT)
    assert report.ok
    assert report.counts == {
        DeliveryOutcome.ignored: 1,
        DeliveryOutcome.deduplicated: 1,
    }
    outcomes = [d.outcome for event in report.events for d in event.deliveries]
    assert outcomes == [DeliveryOutcome.ignored, DeliveryOutcome.deduplicated]
    first_delivery, second_delivery = (
        d for event in report.events for d in event.deliveries
    )
    assert first_delivery.dedup_key == second_delivery.dedup_key


def test_a_repeat_unminted_match_is_re_owed_not_deduplicated(tmp_path):
    # The OTHER half of spec §4's "recoverably convergent" (engine.py's module
    # docstring): a `matched` row with no item ref is a mint that never
    # happened, so a repeat delivery re-owes the SAME consequence rather than
    # reporting `deduplicated` — and a dry-run mints nothing, ever, so this is
    # the ONLY way a matched policy's repeat delivery behaves inside one. That
    # this differs from the ignored-event case above is not a divergence from
    # production; it is the identical rule ("only a row that shows the work
    # was produced answers a repeat with `deduplicated`"), applied to a store
    # nothing ever advances to `created`.
    _duplicate_event_capture(tmp_path)
    report = dry_run(_MATCH_ALL_COMPLETED_BODY, tmp_path, tenant=TENANT)
    assert report.ok
    assert report.counts == {DeliveryOutcome.matched: 2}
    outcomes = [d.outcome for event in report.events for d in event.deliveries]
    assert outcomes == [DeliveryOutcome.matched, DeliveryOutcome.matched]
    # Both deliveries claim the SAME ledger row and both re-owe the mint — the
    # convergence is on the KEY, not on the delivery being silently dropped.
    first_delivery, second_delivery = (
        d for event in report.events for d in event.deliveries
    )
    assert first_delivery.dedup_key == second_delivery.dedup_key
    assert first_delivery.would_mint is not None
    assert second_delivery.would_mint is not None


# --- no trace ------------------------------------------------------------------


def test_a_dry_run_leaves_the_real_tables_untouched(migrated_db):
    dry_run(TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT)
    assert DeliveryLedger().list_for_tenant(TENANT) == []
    assert PolicyCache().list_for_tenant(TENANT) == []
    with session_scope() as session:
        count = session.execute(
            sa.select(sa.func.count()).select_from(ConsumerCursor)
        ).scalar()
    assert count == 0


def test_a_stalled_dry_run_also_leaves_the_real_tables_untouched(migrated_db):
    dry_run(PROSE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT, version_id="gv-prose")
    assert DeliveryLedger().list_for_tenant(TENANT) == []
    assert PolicyCache().list_for_tenant(TENANT) == []


# --- render_text ---------------------------------------------------------------


def test_render_text_contains_the_counts_and_a_would_mint_line():
    report = dry_run(TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT)
    text = render_text(report)
    assert "matched: 8" in text
    assert "ignored: 5" in text
    assert "quarantined: 1" in text
    assert "would mint:" in text


def test_render_text_reports_a_stall():
    report = dry_run(
        PROSE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT, version_id="gv-prose"
    )
    text = render_text(report)
    assert "STALLED" in text
    assert "gv-prose" in text

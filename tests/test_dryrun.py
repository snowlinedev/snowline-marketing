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

import pytest
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
    # 7, not 8: the artifact's `monthly-metrics-snapshot` policy is
    # mode=dry_run — evaluated and visible per delivery, but the headline
    # must not count a mint production would never perform.
    assert len(report.would_mint) == 7


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
    assert messaging.destination.scope == "turtlesedge/marketing"
    assert messaging.destination.initiative == "messaging"
    assert messaging.destination.phase is None
    assert messaging.entry.title_template


def test_a_dry_run_mode_policy_is_reported_as_not_minting():
    # The shipped artifact's own `monthly-metrics-snapshot` policy is
    # `mode: dry_run` — visible per DELIVERY with its mints flag, but
    # EXCLUDED from the would_mint headline: production would mint nothing
    # for it, and the headline must not overstate a rollout. An
    # `approval_required` match IS owed work and stays in (spec §11 vs §12).
    report = dry_run(TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT)
    per_delivery = [
        d.consequence
        for event in report.events
        for d in event.deliveries
        if d.consequence is not None
    ]
    snapshot = next(
        m for m in per_delivery if m.policy_id == "monthly-metrics-snapshot"
    )
    assert snapshot.mode.value == "dry_run"
    assert snapshot.mints is False
    assert all(m.policy_id != "monthly-metrics-snapshot" for m in report.would_mint)
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
    from conftest import make_envelope

    from snowline_marketing.events import EventType

    envelope = make_envelope(
        EventType.item_completed,
        event_id="pm-evt-dup-0001",
        subject={"kind": "work_item", "id": "dup-item"},
    )
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
    assert first_delivery.record.dedup_key == second_delivery.record.dedup_key


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
    assert first_delivery.record.dedup_key == second_delivery.record.dedup_key
    assert first_delivery.consequence is not None
    assert second_delivery.consequence is not None


def test_a_re_owed_match_appears_once_in_the_would_mint_headline(tmp_path):
    # The headline's dedup, pinned: the two matched deliveries above re-owe
    # the SAME ledger row's mint (they share its dedup_key), and production,
    # with minting doing its job, mints ONCE — so the §11 headline must list
    # the consequence once, not twice, however many times the capture
    # re-delivered the event.
    _duplicate_event_capture(tmp_path)
    report = dry_run(_MATCH_ALL_COMPLETED_BODY, tmp_path, tenant=TENANT)
    assert report.ok
    assert report.counts == {DeliveryOutcome.matched: 2}
    (would_mint,) = report.would_mint
    assert would_mint.policy_id == "review-on-completion"


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


def test_a_broken_capture_is_a_failed_report_not_a_clean_empty_one(tmp_path):
    # Mixed-width prefixes make fixture_files raise; run_intake records it as
    # a while_reading failure — the report must surface it, or a broken
    # capture reads as "this policy matches nothing".
    (tmp_path / "100-a.json").write_text("{}")
    (tmp_path / "0002-b.json").write_text("{}")
    report = dry_run(TURTLESEDGE_BODY, tmp_path, tenant=TENANT)
    assert not report.ok
    assert report.pass_failure is not None
    assert report.pass_failure.while_reading
    assert "mix widths" in report.pass_failure.error
    assert "FAILED" in render_text(report)


def test_a_broken_draft_stalls_even_against_an_empty_capture(tmp_path):
    # Eager classification: the pre-flight must never say "fine" about a
    # draft that would stall production on its first real event, however
    # empty the capture is.
    report = dry_run("this is not json", tmp_path, tenant=TENANT)
    assert not report.ok
    assert report.stalled is not None
    assert "not_json" in report.stalled.detail


def test_a_missing_fixtures_directory_is_refused(tmp_path):
    with pytest.raises(ValueError, match="does not exist"):
        dry_run(TURTLESEDGE_BODY, tmp_path / "no-such-capture", tenant=TENANT)


def test_a_fixtures_path_that_is_a_file_is_refused_distinctly(tmp_path):
    # A fixture FILE passed where its directory belongs is a different mistake
    # from a typo'd path, and the message must say which one was made.
    fixture = tmp_path / "0001-event.json"
    fixture.write_text("{}")
    with pytest.raises(ValueError, match="exists but is not a directory"):
        dry_run(TURTLESEDGE_BODY, fixture, tenant=TENANT)


def test_an_unserializable_draft_mapping_stalls_instead_of_raising(tmp_path):
    import datetime

    report = dry_run(
        {"schema_version": 1, "when": datetime.datetime(2026, 8, 1)},
        tmp_path,
        tenant=TENANT,
    )
    assert not report.ok
    assert report.stalled is not None
    assert report.stalled.reason is StallReason.policy_quarantined
    # The REAL exception text rides through — json.dumps's TypeError names the
    # offending type, which is what the operator needs to fix the draft.
    assert "not JSON-serializable" in report.stalled.detail
    assert "datetime" in report.stalled.detail


def test_counts_are_derived_from_events():
    report = dry_run(TURTLESEDGE_BODY, EVENT_FIXTURES_DIR, tenant=TENANT)
    assert sum(report.counts.values()) == sum(len(e.deliveries) for e in report.events)

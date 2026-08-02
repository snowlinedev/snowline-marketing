"""The policy cache (spec §4, "Policy cache").

DB-backed, so these ride the `migrated_db` fixture and skip cleanly when
Postgres is unreachable — the cache's whole job is remembering what a
governance version contained, so there is no honest in-memory substitute.

What is being pinned here: the exact VERSION ID is the key (contract
requirement — the ledger records it), a quarantined version persists WITH its
reason, and a valid one persists with none. The reason/outcome pairing is a
database CHECK rather than a convention, because a quarantined row with no
reason is an operator staring at a broken policy with nothing to fix.
"""

from __future__ import annotations

import sqlalchemy as sa
from conftest import POLICY_FIXTURES_DIR, TENANT

from snowline_marketing.db import session_scope
from snowline_marketing.models import CachedPolicySet as CachedPolicySetRow
from snowline_marketing.policies import (
    MalformedPolicyReason,
    MalformedPolicySet,
    PolicySet,
)
from snowline_marketing.policy_cache import ParseOutcome, PolicyCache
from snowline_marketing.policy_source import ResolvedPolicySet

VALID_BODY = (POLICY_FIXTURES_DIR / "turtlesedge.json").read_text()
PROSE_BODY = (POLICY_FIXTURES_DIR / "malformed-not-json.json").read_text()
VERSION_ID = "gv-7f3a91c4"


def _resolved(version_id: str, body: str, tenant: str = TENANT) -> ResolvedPolicySet:
    return ResolvedPolicySet(tenant=tenant, version_id=version_id, body=body)


def _store(version_id: str, body: str, tenant: str = TENANT) -> PolicyCache:
    cache = PolicyCache()
    cache.put(_resolved(version_id, body, tenant))
    return cache


def test_unknown_version_reads_as_none(migrated_db):
    # A never-fetched version is a cache MISS, not an error and not an empty
    # policy set.
    assert PolicyCache().get("gv-never-fetched") is None


def test_a_valid_version_round_trips(migrated_db):
    cache = _store(VERSION_ID, VALID_BODY)

    # Read back through a FRESH cache instance: the point of the row is
    # surviving the process that wrote it.
    row = PolicyCache().get(VERSION_ID)
    assert row is not None
    assert row.version_id == VERSION_ID
    assert row.tenant == TENANT
    assert row.outcome is ParseOutcome.valid
    assert row.quarantine_reason is None
    assert row.quarantine_detail is None
    assert row.fetched_at is not None
    assert cache.get(VERSION_ID) == row


def test_the_body_is_stored_verbatim(migrated_db):
    # Text, not JSONB (see models.py): the cached bytes must stay diffable
    # against the artifact in governance, so no key reordering, no number
    # renormalization, not even a reserialization.
    _store(VERSION_ID, VALID_BODY)
    row = PolicyCache().get(VERSION_ID)
    assert row is not None
    assert row.body == VALID_BODY


def test_a_cached_body_re_parses_to_the_policy_set(migrated_db):
    # The engine's read path: the row stores text plus an audit classification;
    # the model is re-derived against today's parser.
    _store(VERSION_ID, VALID_BODY)
    row = PolicyCache().get(VERSION_ID)
    assert row is not None
    parsed = row.parse()
    assert isinstance(parsed, PolicySet)
    assert parsed.tenant == TENANT
    assert parsed.entry("app-store-listing-publish") is not None


def test_a_quarantined_version_persists_with_its_reason(migrated_db):
    # The loud path (spec §6): the version is remembered, classified, and
    # explained — never silently absent, which would read as "no policies".
    _store("gv-prose", PROSE_BODY)
    row = PolicyCache().get("gv-prose")
    assert row is not None
    assert row.outcome is ParseOutcome.quarantined
    assert row.quarantine_reason == MalformedPolicyReason.not_json.value
    assert row.quarantine_detail
    assert row.body == PROSE_BODY


def test_a_quarantined_body_re_parses_to_the_malformed_result(migrated_db):
    _store("gv-prose", PROSE_BODY)
    row = PolicyCache().get("gv-prose")
    assert row is not None
    parsed = row.parse()
    assert isinstance(parsed, MalformedPolicySet)
    # The version id rides through, so a re-parse still names the governance
    # version an operator has to revise.
    assert parsed.version_id == "gv-prose"


def test_put_upserts_rather_than_duplicating(migrated_db):
    # A version id is an immutable content address, so re-fetching one is the
    # steady state of every sweep — it must not accumulate rows.
    _store(VERSION_ID, VALID_BODY)
    _store(VERSION_ID, VALID_BODY)
    with session_scope() as session:
        count = session.execute(
            sa.text("SELECT count(*) FROM policy_cache WHERE version_id = :v"),
            {"v": VERSION_ID},
        ).scalar()
        assert count == 1


def test_a_re_fetch_moves_fetched_at(migrated_db):
    # "When did we last see this version?" is the staleness question about the
    # policy sweep itself; an upsert that forgot the column would freeze it at
    # the first sighting forever.
    _store(VERSION_ID, VALID_BODY)
    first = PolicyCache().get(VERSION_ID)
    _store(VERSION_ID, VALID_BODY)
    second = PolicyCache().get(VERSION_ID)
    assert first is not None and second is not None
    # Distinct transactions, so `now()` (transaction start) genuinely advances.
    assert second.fetched_at > first.fetched_at


def test_re_classification_clears_a_stale_quarantine_reason(migrated_db):
    # Not a state the immutable-version-id contract should produce, but the
    # upsert overwrites every column rather than only some: a leftover reason
    # would make a healthy policy read as broken on the operator surface.
    cache = PolicyCache()
    cache.put(_resolved(VERSION_ID, PROSE_BODY))
    cache.put(_resolved(VERSION_ID, VALID_BODY))
    row = cache.get(VERSION_ID)
    assert row is not None
    assert row.outcome is ParseOutcome.valid
    assert row.quarantine_reason is None
    assert row.quarantine_detail is None


def test_versions_do_not_share_a_row(migrated_db):
    # The ledger references old version ids forever, so a superseded version
    # must stay readable alongside its successor.
    _store("gv-0001", VALID_BODY)
    _store("gv-0002", PROSE_BODY)
    cache = PolicyCache()
    first = cache.get("gv-0001")
    second = cache.get("gv-0002")
    assert first is not None and first.outcome is ParseOutcome.valid
    assert second is not None and second.outcome is ParseOutcome.quarantined


def test_tenants_are_listed_independently(migrated_db):
    import json

    _store("gv-0001", VALID_BODY, tenant=TENANT)
    _store("gv-0002", PROSE_BODY, tenant=TENANT)
    # A genuinely-valid body FOR the second tenant — the same body under a
    # different tenant would (correctly) quarantine as tenant_mismatch.
    other = json.loads(VALID_BODY)
    other["tenant"] = "snowlinedev"
    _store("gv-0003", json.dumps(other), tenant="snowlinedev")
    cache = PolicyCache()
    assert {r.version_id for r in cache.list_for_tenant(TENANT)} == {
        "gv-0001",
        "gv-0002",
    }
    assert {r.version_id for r in cache.list_for_tenant("snowlinedev")} == {"gv-0003"}
    assert cache.list_for_tenant("nobody") == []


def test_a_quarantined_row_cannot_lose_its_reason(migrated_db):
    # The invariant is the DATABASE's, not every future writer's: a
    # quarantined row with no reason is unactionable, so it must not be
    # writable at all.
    with session_scope() as session:
        statement = sa.insert(CachedPolicySetRow).values(
            version_id="gv-bad-write",
            tenant=TENANT,
            body=PROSE_BODY,
            parse_outcome=ParseOutcome.quarantined.value,
            quarantine_reason=None,
        )
        try:
            session.execute(statement)
        except sa.exc.IntegrityError:
            pass
        else:
            raise AssertionError(
                "ck_policy_cache_quarantine_reason did not reject a quarantined "
                "row with no reason"
            )


def test_a_valid_row_cannot_carry_a_reason(migrated_db):
    with session_scope() as session:
        statement = sa.insert(CachedPolicySetRow).values(
            version_id="gv-bad-write-2",
            tenant=TENANT,
            body=VALID_BODY,
            parse_outcome=ParseOutcome.valid.value,
            quarantine_reason="not_json",
        )
        try:
            session.execute(statement)
        except sa.exc.IntegrityError:
            pass
        else:
            raise AssertionError(
                "ck_policy_cache_quarantine_reason did not reject a valid row "
                "carrying a quarantine reason"
            )


def test_put_returns_the_classification(migrated_db):
    # put owns parsing (tenant cross-checked) and hands the result back — the
    # caller evaluates exactly what the row records, by construction.
    cache = PolicyCache()
    valid = cache.put(_resolved(VERSION_ID, VALID_BODY))
    assert isinstance(valid, PolicySet)
    quarantined = cache.put(_resolved("gv-prose", PROSE_BODY))
    assert isinstance(quarantined, MalformedPolicySet)


def test_a_tenant_mismatched_body_quarantines(migrated_db):
    # VALID_BODY declares turtlesedge; resolving it FOR another tenant is a
    # misregistered artifact — quarantined under the REQUESTED tenant, so the
    # operator listing for that tenant shows the problem, and one tenant's
    # rules never cache as another's valid policy.
    cache = PolicyCache()
    parsed = cache.put(_resolved("gv-cross", VALID_BODY, tenant="snowlinedev"))
    assert isinstance(parsed, MalformedPolicySet)
    assert parsed.reason is MalformedPolicyReason.tenant_mismatch
    row = cache.get("gv-cross")
    assert row is not None
    assert row.tenant == "snowlinedev"
    assert row.outcome is ParseOutcome.quarantined
    assert row.quarantine_reason == MalformedPolicyReason.tenant_mismatch.value
    assert {r.version_id for r in cache.list_for_tenant("snowlinedev")} == {"gv-cross"}
    # The READ path applies the same cross-check: re-parsing the row must not
    # hand the engine a valid PolicySet for the wrong tenant while the row's
    # own audit column says quarantined.
    reparsed = row.parse()
    assert isinstance(reparsed, MalformedPolicySet)
    assert reparsed.reason is MalformedPolicyReason.tenant_mismatch


def test_a_quarantined_row_needs_a_detail_too(migrated_db):
    # The CHECK enforces the whole pairing, not just the reason column — a
    # quarantined row whose detail is missing gives the operator a verdict
    # with nothing to fix.
    with session_scope() as session:
        statement = sa.insert(CachedPolicySetRow).values(
            version_id="gv-bad-write-3",
            tenant=TENANT,
            body=PROSE_BODY,
            parse_outcome=ParseOutcome.quarantined.value,
            quarantine_reason="not_json",
            quarantine_detail=None,
        )
        try:
            session.execute(statement)
        except sa.exc.IntegrityError:
            pass
        else:
            raise AssertionError(
                "ck_policy_cache_quarantine_reason did not reject a quarantined "
                "row with no detail"
            )


def test_a_cross_tenant_id_collision_is_refused_loudly(migrated_db, caplog):
    # The refusal must be VISIBLE, not merely effective: a silently-unchanged
    # row leaves an operator wondering why their dry-run's policy text is
    # someone else's. (The guard originally tested `rowcount == 0`, which
    # psycopg reports as -1 for an upsert — so the warning never fired.)
    import json
    import logging

    cache = PolicyCache()
    cache.put(_resolved("pv-collide", VALID_BODY, tenant=TENANT))
    other = json.loads(VALID_BODY)
    other["tenant"] = "snowlinedev"
    with caplog.at_level(logging.WARNING, logger="snowline_marketing.policy_cache"):
        cache.put(_resolved("pv-collide", json.dumps(other), tenant="snowlinedev"))
    assert [r for r in caplog.records if r.levelno == logging.WARNING]


def test_a_same_tenant_re_fetch_is_not_reported_as_refused(migrated_db, caplog):
    # The other half: the ordinary steady state of every sweep must stay quiet.
    import logging

    cache = PolicyCache()
    cache.put(_resolved(VERSION_ID, VALID_BODY))
    with caplog.at_level(logging.WARNING, logger="snowline_marketing.policy_cache"):
        cache.put(_resolved(VERSION_ID, VALID_BODY))
    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []


def test_a_cross_tenant_id_collision_is_refused(migrated_db):
    # Version ids are caller-chosen on the dry-run/fixtures path, so two
    # tenants CAN reuse a readable id. The second put must not rewrite the
    # first tenant's row — the version would vanish from its listing and any
    # ledger row recording it would join to the other tenant's policy text.
    import json

    cache = PolicyCache()
    cache.put(_resolved("pv-0001", VALID_BODY, tenant=TENANT))
    other = json.loads(VALID_BODY)
    other["tenant"] = "snowlinedev"
    cache.put(_resolved("pv-0001", json.dumps(other), tenant="snowlinedev"))

    row = cache.get("pv-0001")
    assert row is not None
    assert row.tenant == TENANT  # first writer keeps the row
    assert row.body == VALID_BODY

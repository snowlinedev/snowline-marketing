"""Marketing test harness.

`_marketing_stays_disabled` is autouse: MARKETING_ENABLED must NEVER be true
inside the suite (spec §2 — off by default; pattern:
SNOWLINE_SHADOW_TURNS_ENABLED). There is no intake/policy loop yet to
accidentally start, but the gate is pinned now so future engine tests
inherit a safe default without each one having to remember to set it.

`event_fixtures_dir` points at the captured event envelopes in
`tests/fixtures/events/` — the shipped v1 stream (every event type, plus
malformed cases), shared by the source, intake-loop and envelope tests so they
all assert against the SAME capture rather than each inventing its own.

`policy_fixtures_dir` points at `tests/fixtures/policies/` — Turtle's Edge's
policy-set document (every consequence type, both the default and custom
dedup templates) plus one file per malformed class. Unlike the event fixtures
these carry NO `NNNN-` prefix: events are a STREAM whose file names are the
cursor's resume tokens, while each policy file is a standalone artifact body
resolved by name. A numeric prefix here would imply an ordering that does not
exist.

`migrated_db` mirrors the house plugin idiom: a disposable Postgres database,
migrated with `alembic upgrade head` (exercising the migration chain), that
`pytest.skip`s with a clear message when Postgres is unreachable (or
reachable but not provisionable by this role) — so the
stub-based / registration / config tests that don't need a DB still run in an
environment with no Postgres (e.g. plain CI with no service container).
"""

from __future__ import annotations

import copy
import os
from pathlib import Path

import pytest

# The shipped capture (spec §5: fixtures mode is a first-class dev/CI surface).
EVENT_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "events"

# The shipped policy-set documents (spec §6). Fixtures-first applies to
# policies exactly as it does to events: the deterministic core is built and
# tested against these, with the gateway an integration point rather than a
# build prerequisite.
POLICY_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "policies"

# Point marketing's DB layer at the disposable test database BEFORE any
# marketing module builds its (lazy) engine.
TEST_DB_URL = os.environ.get(
    "MARKETING_TEST_DATABASE_URL",
    "postgresql+psycopg:///snowline_marketing_test",
)
os.environ["MARKETING_DATABASE_URL"] = TEST_DB_URL

import sqlalchemy as sa  # noqa: E402
from alembic import command  # noqa: E402

# The shared programmatic Alembic config (script location + URL sourced in
# one place); safe to import before the fixtures run — the DB layer is lazy.
from snowline_marketing.db import alembic_config, reset_engine  # noqa: E402

# The tenant/scope slugs every test and fixture speaks — the REAL Turtle's
# Edge slugs, so a captured fixture and a hand-built envelope can't drift
# apart on identity.
TENANT = "turtlesedge"
SCOPE = "turtlesedge/turtletracks"


# The per-type shape each event REQUIRES (spec §5 subject refs + §6 predicate
# surface). Keyed by every EventType member — test_events asserts the
# coverage, so a new event type cannot be added without landing here. Lives in
# conftest because BOTH test_events (contract tests) and test_intake (a
# source-agnostic loop test) build envelopes from it — one source of truth for
# the minimal-valid shape.
def _type_specific() -> dict:
    from snowline_marketing.events import EventType

    return {
        EventType.item_completed: {},
        EventType.item_reopened: {},
        EventType.item_abandoned: {},
        EventType.item_rescoped: {
            "payload": {"scope": SCOPE, "details": {"from_scope": "turtlesedge/legacy"}}
        },
        EventType.initiative_phase_completed: {
            "subject": {"kind": "initiative", "id": "8ad41b77", "phase": "build"},
            "payload": {
                "scope": SCOPE,
                "initiative": "summer-release",
                "phase": "build",
            },
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


TYPE_SPECIFIC = _type_specific()


def make_envelope(event_type, **overrides) -> dict:
    """A minimal VALID envelope dict for `event_type` — only what that type
    requires, so a test that mutates one field is testing that field.

    The type-specific shape is DEEP-COPIED in: tests mutate the envelopes they
    build (popping a nested key is the natural way to probe a required-field
    path), and a by-reference share would let one test's mutation contaminate
    `TYPE_SPECIFIC` for every later test in the session."""
    from snowline_marketing.events import SCHEMA_VERSION

    base: dict = {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"pm-evt-{event_type.value}",
        "event_type": event_type.value,
        "tenant": TENANT,
        "occurred_at": "2026-07-20T12:00:00+00:00",
        "subject": {"kind": "work_item", "id": "3f1c9a20"},
        "payload": {"scope": SCOPE},
    }
    base.update(copy.deepcopy(TYPE_SPECIFIC[event_type]))
    for key, value in overrides.items():
        base[key] = value
    return base


@pytest.fixture(autouse=True)
def _plugin_tables_do_not_leak(request):
    """Any DB-backed test leaves EVERY plugin-owned table empty behind it.

    Rows in these tables key on ids tests naturally reuse (a source_key, a
    readable governance version id like 'pv-0001'), so a row leaked by one
    test silently becomes another test's resume point or cache hit — an
    execution-order-dependent flake in exactly the isolation area this suite
    exists to nail down. Driven off `Base.metadata` (reverse dependency
    order), so a new §4 table — delivery ledger, provenance ledger,
    quarantine — is covered the day its model lands, instead of requiring a
    per-table fixture copy someone forgets."""
    yield
    if "migrated_db" not in request.fixturenames:
        return
    import sqlalchemy as sa_

    from snowline_marketing.db import session_scope
    from snowline_marketing.models import Base

    with session_scope() as session:
        for table in reversed(Base.metadata.sorted_tables):
            session.execute(sa_.delete(table))


@pytest.fixture(autouse=True)
def _marketing_stays_disabled(monkeypatch):
    """MARKETING_ENABLED must NEVER be true inside the suite: a dev shell's
    `export MARKETING_ENABLED=1` (natural while working on a later intake/
    policy-engine item) must not let a full-lifespan test start real intake
    evaluation mid-test. Symmetric to the platform's
    SNOWLINE_SHADOW_TURNS_ENABLED pin."""
    monkeypatch.setenv("MARKETING_ENABLED", "0")


@pytest.fixture
def event_fixtures_dir() -> Path:
    """The shipped event capture directory (see module docstring)."""
    return EVENT_FIXTURES_DIR


@pytest.fixture
def policy_fixtures_dir() -> Path:
    """The shipped policy-set fixture directory (see module docstring)."""
    return POLICY_FIXTURES_DIR


def _db_name(url: str) -> str:
    return sa.make_url(url).database


def _maintenance_url(url: str) -> str:
    return str(sa.make_url(url).set(database="postgres"))


def _postgres_reachable() -> bool:
    try:
        eng = sa.create_engine(
            _maintenance_url(TEST_DB_URL), isolation_level="AUTOCOMMIT"
        )
        with eng.connect():
            pass
        eng.dispose()
        return True
    except Exception:
        return False


def create_database(url: str) -> None:
    name = _db_name(url)
    eng = sa.create_engine(_maintenance_url(url), isolation_level="AUTOCOMMIT")
    with eng.connect() as conn:
        exists = conn.execute(
            sa.text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": name}
        ).scalar()
        if not exists:
            conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    eng.dispose()


def drop_database(url: str) -> None:
    name = _db_name(url)
    eng = sa.create_engine(_maintenance_url(url), isolation_level="AUTOCOMMIT")
    with eng.connect() as conn:
        conn.execute(
            sa.text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :n AND pid <> pg_backend_pid()"
            ),
            {"n": name},
        )
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}"'))
    eng.dispose()


@pytest.fixture(scope="session")
def migrated_db() -> str:
    """A freshly created + migrated marketing test database for the session."""
    if not _postgres_reachable():
        pytest.skip(
            "Postgres not reachable at "
            f"{_maintenance_url(TEST_DB_URL)!r} — DB-backed tests skipped"
        )
    try:
        drop_database(TEST_DB_URL)
        create_database(TEST_DB_URL)
    except sa.exc.DBAPIError as exc:
        # Reachable is not provisionable: a locked-down role can connect to
        # the maintenance DB yet lack CREATEDB (or the right to terminate
        # other sessions' backends). The fixture's contract is a clean skip,
        # not a fixture ERROR that reds the suite.
        pytest.skip(f"cannot provision disposable test database: {exc}")
    reset_engine()
    command.upgrade(alembic_config(), "head")
    yield TEST_DB_URL
    reset_engine()
    drop_database(TEST_DB_URL)

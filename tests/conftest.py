"""Marketing test harness.

`_marketing_stays_disabled` is autouse: MARKETING_ENABLED must NEVER be true
inside the suite (spec §2 — off by default; pattern:
SNOWLINE_SHADOW_TURNS_ENABLED). There is no intake/policy loop yet to
accidentally start, but the gate is pinned now so future engine tests
inherit a safe default without each one having to remember to set it.

`migrated_db` mirrors the house plugin idiom: a disposable Postgres database,
migrated with `alembic upgrade head` (exercising the migration chain), that
`pytest.skip`s with a clear message when Postgres is unreachable — so the
stub-based / registration / config tests that don't need a DB still run in an
environment with no Postgres (e.g. plain CI with no service container).
"""

from __future__ import annotations

import os

import pytest

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


@pytest.fixture(autouse=True)
def _marketing_stays_disabled(monkeypatch):
    """MARKETING_ENABLED must NEVER be true inside the suite: a dev shell's
    `export MARKETING_ENABLED=1` (natural while working on a later intake/
    policy-engine item) must not let a full-lifespan test start real intake
    evaluation mid-test. Symmetric to the platform's
    SNOWLINE_SHADOW_TURNS_ENABLED pin."""
    monkeypatch.setenv("MARKETING_ENABLED", "0")


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
    drop_database(TEST_DB_URL)
    create_database(TEST_DB_URL)
    reset_engine()
    command.upgrade(alembic_config(), "head")
    yield TEST_DB_URL
    reset_engine()
    drop_database(TEST_DB_URL)

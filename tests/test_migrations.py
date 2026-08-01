"""Alembic wiring — the migration chain has exactly one head.

Uses `ScriptDirectory` directly (no DB connection needed) so this test runs
without a live Postgres, per the house convention. A DB-backed `migrated_db`
fixture (see conftest.py) additionally exercises `alembic upgrade head`
against a real database when Postgres is reachable, skipping cleanly when it
is not.
"""

from __future__ import annotations

from alembic.script import ScriptDirectory
from conftest import alembic_config


def test_exactly_one_head():
    script = ScriptDirectory.from_config(alembic_config())
    heads = script.get_heads()
    assert len(heads) == 1, f"expected exactly one alembic head, got {heads!r}"


def test_chain_is_linear_from_genesis():
    # The history is a single unbranched chain: exactly one root (the genesis
    # baseline), and every revision has at most ONE down_revision (a tuple
    # would be an alembic merge revision — a fork that got merged back, which
    # applies siblings in unspecified order). Single-root + single-head alone
    # would still admit that shape; this walks every revision to rule it out.
    script = ScriptDirectory.from_config(alembic_config())
    revisions = list(script.walk_revisions())
    roots = [r.revision for r in revisions if r.down_revision is None]
    assert roots == ["5cddd792fe7d"]
    forks = [r.revision for r in revisions if isinstance(r.down_revision, tuple)]
    assert forks == [], f"merge revisions found (branched history): {forks!r}"


def test_alembic_config_survives_percent_in_url():
    # Alembic's Config rides configparser interpolation: a percent-encoded
    # credential (e.g. a password with `@` as `%40`) raised ValueError inside
    # set_main_option before db.alembic_config escaped the URL for storage.
    # Readers must get the ORIGINAL url back (configparser collapses %% -> %).
    url = "postgresql+psycopg://user:p%40ss@localhost/db"
    cfg = alembic_config(url)
    assert cfg.get_main_option("sqlalchemy.url") == url


def test_migration_chain_applies_cleanly(migrated_db):
    """DB-backed: `alembic upgrade head` (run by the `migrated_db` fixture)
    succeeds against a real database. Skips when Postgres is unreachable."""
    # If we get here, the migrated_db fixture already ran `upgrade head`
    # without raising — this test exists to give that a visible, named
    # assertion in the suite.
    assert migrated_db

"""Marketing's DB layer — engine, sessionmaker, and `session_scope()`.

Marketing has its OWN database. Mirrors the house plugin pattern: the
engine/sessionmaker are built lazily on first use, not at import time, so the
database URL is read when a session is actually opened — which lets tests
point at a disposable database and avoids connecting just by importing the
package.

The models so far are the intake `ConsumerCursor`, the `CachedPolicySet` policy
cache, the `DeliveryLedgerEntry` delivery ledger, the `DeliverableProvenanceEntry`
(+ `DeliverableSourceVersion`) provenance ledger and the
`CompletionQuarantineEntry` completion quarantine (spec §4); the malformed-event
quarantine arrives with the operator surfaces that read it (§11). Everything
here is model-agnostic — it owns connections, not schema.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from alembic.config import Config as AlembicConfig
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from snowline_marketing.config import database_url

_engine: Engine | None = None
_sessionmaker: sessionmaker[Session] | None = None

MIGRATIONS = Path(__file__).resolve().parent / "migrations"


def alembic_config(url: str | None = None) -> AlembicConfig:
    """One Alembic `Config` for every programmatic caller — the app's
    boot-migrate and the test harness source the script location and DB URL
    here, in exactly one place (alembic.ini repeats them only for the CLI).
    `url` defaults to the live `database_url()`."""
    cfg = AlembicConfig()
    cfg.set_main_option("script_location", str(MIGRATIONS))
    # Alembic's Config rides configparser, whose interpolation treats a bare
    # `%` as syntax — a percent-encoded credential (e.g. an `@` in a password
    # as `%40`) would raise ValueError right here, killing the lifespan's
    # boot-migrate before the service comes up. Escape for storage; readers
    # get the original URL back (configparser collapses `%%` to `%`).
    cfg.set_main_option("sqlalchemy.url", (url or database_url()).replace("%", "%%"))
    return cfg


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(database_url(), future=True)
    return _engine


def get_sessionmaker() -> sessionmaker[Session]:
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = sessionmaker(
            bind=get_engine(),
            expire_on_commit=False,
            future=True,
        )
    return _sessionmaker


def reset_engine() -> None:
    """Drop the cached engine/sessionmaker (used by tests after switching URL)."""
    global _engine, _sessionmaker
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _sessionmaker = None


@contextmanager
def session_scope() -> Iterator[Session]:
    session = get_sessionmaker()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

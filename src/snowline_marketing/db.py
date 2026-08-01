"""Marketing's DB layer — engine, sessionmaker, and `session_scope()`.

Marketing has its OWN database. Mirrors the house plugin pattern: the
engine/sessionmaker are built lazily on first use, not at import time, so the
database URL is read when a session is actually opened — which lets tests
point at a disposable database and avoids connecting just by importing the
package.

No models are defined yet (spec §13 stage 1 is scaffold-only — no ledger
tables); this module exists so the alembic env and the app lifespan's
boot-migrate have something real to import.
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
    cfg.set_main_option("sqlalchemy.url", url or database_url())
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

"""The marketing plugin app — v1 skeleton (spec §13 stage 1: service skeleton
+ registration + health).

A FastAPI app that:
  - exposes `/health` (the platform supervisor polls this),
  - REGISTERS with the platform via a lifespan-long registration HEARTBEAT:
    the manifest is POSTed at boot and re-asserted every interval, so a
    platform restart (in-memory registry, boots empty) self-heals within one
    beat instead of requiring this plugin to also be kickstarted. Each beat
    is best-effort, so a briefly-down platform doesn't crash the plugin.

Marketing has its OWN database; like the house plugin convention it boot-
migrates to the latest Alembic head in the lifespan, so a schema change
deploys on a plain restart. There is no schema yet (no delivery ledger, no
deliverable provenance ledger — later items, spec §4); the migration chain
is baselined empty so it exists.

This is scaffold-only: no fixtures intake loop, no policy engine, no ledger
tables, no audit surfaces yet. Those are separate follow-up items (spec §13
stages 2+). `MARKETING_ENABLED` (off by default, spec §2) gates the future
intake/evaluation loops; nothing in this phase does anything gated by it yet
beyond existing as a config knob surfaced on /health.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI

from snowline_marketing import config, registration

log = logging.getLogger("snowline_marketing.app")


class _HeartbeatHttpxLogFilter(logging.Filter):
    """Drops httpx's per-request INFO line for the registration heartbeat's
    `POST <platform>/plugins` (one line per beat, forever) while letting every
    OTHER httpx request trace through — including a POST to some other host's
    `/plugins` path. The platform URL is read per record, not captured, so the
    filter can stay a module-level singleton (`addFilter` is idempotent for
    the same object) and still track an env change."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if "POST" not in msg:
            return True
        return f"{config.platform_url()}/plugins" not in msg


_HEARTBEAT_HTTPX_FILTER = _HeartbeatHttpxLogFilter()


def _migrate_to_head() -> None:
    """Bring the marketing DB to the latest Alembic head — services in this
    codebase family boot-migrate in their lifespan, so a schema change
    deploys on a plain restart."""
    from alembic import command

    from snowline_marketing.db import alembic_config

    command.upgrade(alembic_config(), "head")


def create_app(
    *,
    migrate_on_startup: bool = True,
    register_on_startup: bool = True,
) -> FastAPI:
    """Build the marketing app. `migrate_on_startup=False` skips the boot-
    migrate (tests provision their own schema, or need none yet);
    `register_on_startup=False` skips the platform registration heartbeat
    entirely (tests assert registration separately, against a stubbed
    platform)."""
    if register_on_startup:
        # httpx logs every request at INFO — with the registration heartbeat
        # that is one line per beat forever. Installed only when the heartbeat
        # will actually run; other httpx traffic still traces through.
        logging.getLogger("httpx").addFilter(_HEARTBEAT_HTTPX_FILTER)

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        if migrate_on_startup:
            _migrate_to_head()
        # The task group is UNCONDITIONAL (the house lifespan shape): only
        # the start_soon call is gated, so a future lifespan-long loop
        # can't silently never-start because it forgot to extend a flag
        # disjunction, and there is exactly ONE yield/teardown path shared
        # by production and the test factory. An empty group is free.
        async with anyio.create_task_group() as tg:
            if register_on_startup:
                # The registration HEARTBEAT: first beat immediately (the
                # boot registration), then a re-assert every interval so a
                # platform restart — whose in-memory registry boots empty —
                # heals without this plugin being kickstarted too. Each beat
                # is best-effort and runs off the event loop. Riding the task
                # group means boot never blocks on a slow/down platform —
                # deliberately, /health can come up BEFORE the first beat
                # completes (the gateway self-heals within a beat).
                tg.start_soon(registration.registration_heartbeat)
            yield
            tg.cancel_scope.cancel()

    app = FastAPI(title="Snowline Marketing", lifespan=_lifespan)

    @app.get("/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "plugin": registration.PLUGIN_NAME,
            "enabled": config.marketing_enabled(),
        }

    return app


app = create_app()

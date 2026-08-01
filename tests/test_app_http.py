"""The marketing app's HTTP surface (skeleton phase): /health, and the
lifespan's registration heartbeat. No DB needed —
`migrate_on_startup=False` skips the boot-migrate, matching the house
test-factory idiom.
"""

from __future__ import annotations

import anyio
import httpx

from snowline_marketing.app import create_app


def _app(**kwargs):
    kwargs.setdefault("migrate_on_startup", False)
    kwargs.setdefault("register_on_startup", False)
    return create_app(**kwargs)


def _http(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://marketing",
        timeout=httpx.Timeout(30.0),
    )


def test_health_returns_200():
    app = _app()

    async def go() -> dict:
        async with app.router.lifespan_context(app):
            async with _http(app) as http:
                r = await http.get("http://marketing/health")
                return {"status": r.status_code, "body": r.json()}

    res = anyio.run(go)
    assert res["status"] == 200
    assert res["body"]["plugin"] == "marketing"
    assert res["body"]["status"] == "ok"


def test_health_reports_marketing_enabled_flag(monkeypatch):
    """LIFESPAN-FREE on purpose: this is the one test that overrides the
    conftest autouse pin (MARKETING_ENABLED never true inside the suite), so
    it must not enter the lifespan — once the intake/evaluation loops ride
    the lifespan task group (stage 2), a full-lifespan run here would start
    them for real mid-suite, exactly what the pin exists to prevent.
    ASGITransport serves requests without the lifespan running."""
    monkeypatch.setenv("MARKETING_ENABLED", "1")
    app = _app()

    async def go() -> dict:
        async with _http(app) as http:
            r = await http.get("http://marketing/health")
            return r.json()

    assert anyio.run(go)["enabled"] is True


def test_health_enabled_false_by_default(monkeypatch):
    monkeypatch.delenv("MARKETING_ENABLED", raising=False)
    app = _app()

    async def go() -> dict:
        async with app.router.lifespan_context(app):
            async with _http(app) as http:
                r = await http.get("http://marketing/health")
                return r.json()

    assert anyio.run(go)["enabled"] is False


def test_lifespan_starts_and_cancels_registration_heartbeat(monkeypatch):
    """`create_app(register_on_startup=True)`'s lifespan starts the
    registration heartbeat (it fires at least once) and cancels it cleanly on
    shutdown (the lifespan exits without hanging or raising). The heartbeat
    itself is monkeypatched so no real HTTP is attempted."""
    from snowline_marketing import registration

    calls = {"beats": 0}

    async def fake_heartbeat(*args, **kwargs):
        try:
            while True:
                calls["beats"] += 1
                await anyio.sleep(0.01)
        finally:
            pass

    monkeypatch.setattr(registration, "registration_heartbeat", fake_heartbeat)

    app = _app(register_on_startup=True)

    async def go() -> None:
        async with app.router.lifespan_context(app):
            with anyio.fail_after(5):
                while calls["beats"] < 1:
                    await anyio.sleep(0.01)
        # Exiting the lifespan context = the task group cancelled cleanly.

    anyio.run(go)
    assert calls["beats"] >= 1


def test_main_serves_on_configured_bind(monkeypatch):
    """`python -m snowline_marketing` binds MARKETING_BIND_HOST/
    MARKETING_BIND_PORT — the knobs must actually reach uvicorn, or a
    tailnet deploy silently binds loopback while advertising a tailnet
    base_url."""
    import uvicorn

    from snowline_marketing.__main__ import main

    seen = {}
    monkeypatch.setattr(uvicorn, "run", lambda app, **kw: seen.update(kw, app=app))
    monkeypatch.setenv("MARKETING_BIND_HOST", "127.0.0.2")
    monkeypatch.setenv("MARKETING_BIND_PORT", "9999")
    main()
    assert seen["app"] == "snowline_marketing.app:app"
    assert seen["host"] == "127.0.0.2"
    assert seen["port"] == 9999


def test_lifespan_skips_registration_when_disabled(monkeypatch):
    from snowline_marketing import registration

    calls = {"beats": 0}

    async def fake_heartbeat(*args, **kwargs):
        calls["beats"] += 1

    monkeypatch.setattr(registration, "registration_heartbeat", fake_heartbeat)

    app = _app(register_on_startup=False)

    async def go() -> None:
        async with app.router.lifespan_context(app):
            await anyio.sleep(0.05)

    anyio.run(go)
    assert calls["beats"] == 0

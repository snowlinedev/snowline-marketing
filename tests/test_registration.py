"""Plugin registration — posts the right manifest, and is best-effort/robust.

Stubs the platform HTTP with an `httpx.MockTransport`, so no platform runs.
No DB needed.
"""

from __future__ import annotations

import anyio
import httpx

from snowline_marketing import registration


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _run_heartbeat_until(handler, *, beats: int) -> int:
    """Run the heartbeat loop (tiny interval, stubbed platform) until `beats`
    POSTs have landed, then cancel it — how the app lifespan tears it down."""
    count = 0

    def counting_handler(request: httpx.Request) -> httpx.Response:
        nonlocal count
        count += 1
        return handler(request)

    async def main():
        async with anyio.create_task_group() as tg:

            async def _beat():
                await registration.registration_heartbeat(
                    "http://platform.example",
                    interval=0.01,
                    client=_client(counting_handler),
                )

            tg.start_soon(_beat)
            with anyio.fail_after(5):
                while count < beats:
                    await anyio.sleep(0.005)
            tg.cancel_scope.cancel()

    anyio.run(main)
    return count


def test_manifest_shape():
    m = registration.build_manifest(base_url="http://marketing.example:8805")
    assert m["name"] == "marketing"
    assert m["base_url"] == "http://marketing.example:8805"
    assert m["mcp_path"] == "/mcp"
    assert m["health_path"] == "/health"
    # Marketing's only surface for now: /mcp -> main.
    assert m["surfaces"] == {"/mcp": "main"}


def test_register_success():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/plugins"
        return httpx.Response(201, json={"ok": True})

    assert (
        registration.register_with_platform(
            platform_url="http://platform", client=_client(handler)
        )
        is True
    )


def test_register_409_is_idempotent_success():
    assert (
        registration.register_with_platform(
            platform_url="http://platform",
            client=_client(lambda r: httpx.Response(409)),
        )
        is True
    )


def test_register_transport_error_is_swallowed():
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("platform down", request=request)

    # Never raises — a down platform must not crash boot.
    assert (
        registration.register_with_platform(
            platform_url="http://platform", client=_client(boom)
        )
        is False
    )


def test_register_malformed_platform_url_is_swallowed():
    # httpx.InvalidURL is NOT an httpx.HTTPError — the never-raises contract
    # must cover it too (a typo'd SNOWLINE_PLATFORM_URL must not crash boot,
    # and must not stack-trace every heartbeat). The URL fails to parse, so
    # no network is attempted.
    assert (
        registration.register_with_platform(platform_url="http://127.0.0.1:notaport")
        is False
    )


def test_register_non_2xx_is_failure():
    assert (
        registration.register_with_platform(
            platform_url="http://platform",
            client=_client(lambda r: httpx.Response(500)),
        )
        is False
    )


def test_heartbeat_reasserts_registration_every_beat():
    # The self-healing property: the loop keeps re-POSTing, so a platform
    # whose in-memory registry was wiped gets the manifest again on the next
    # beat.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"name": "marketing", "outcome": "unchanged"})

    assert _run_heartbeat_until(handler, beats=3) >= 3


def test_heartbeat_outlives_failed_beats():
    # A down platform (transport error) and a server error must not kill the
    # loop — the beat after a failure still fires.
    responses = iter(
        [
            httpx.Response(201, json={}),
            "raise",
            httpx.Response(500, json={}),
            httpx.Response(200, json={}),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        step = next(responses, None) or httpx.Response(200, json={})
        if step == "raise":
            raise httpx.ConnectError("platform is down")
        return step

    assert _run_heartbeat_until(handler, beats=4) >= 4

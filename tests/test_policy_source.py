"""Resolving a tenant's current policy version (spec §6).

Stubs governance's HTTP with an `httpx.MockTransport`, so no governance runs.
No DB needed. The contract under test on every path: `resolve` NEVER raises,
and the ONE distinction that must not blur is `not_found` (an answer: this
tenant has no policies) versus `unavailable` / `malformed_response` (no answer
at all). Rounding the second down to the first is the silent match-none spec
§6 forbids.
"""

from __future__ import annotations

import httpx
import pytest
from conftest import POLICY_FIXTURES_DIR, TENANT

from snowline_marketing.policies import PolicySet, parse_policy_set
from snowline_marketing.policy_source import (
    POLICY_SET_PATH,
    GatewayPolicyProvider,
    InMemoryPolicyProvider,
    PolicyResolutionError,
    ResolutionFailure,
    ResolvedPolicySet,
)

BODY = (POLICY_FIXTURES_DIR / "turtlesedge.json").read_text()
VERSION_ID = "gv-7f3a91c4"


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _version_payload(**overrides) -> dict:
    # Governance's verified `get_artifact_version` shape (see policy_source's
    # module docstring): `id` is the VERSION id, body rides `body_snapshot`.
    payload = {
        "id": VERSION_ID,
        "artifact_id": "b964d217",
        "title": "Turtle's Edge marketing policies",
        "status": "current",
        "body_snapshot": BODY,
    }
    payload.update(overrides)
    return payload


# --- the fixture provider --------------------------------------------------


def test_in_memory_provider_round_trips():
    provider = InMemoryPolicyProvider()
    stored = provider.put(TENANT, VERSION_ID, BODY, artifact_id="b964d217")
    resolved = provider.resolve(TENANT)
    assert resolved == stored
    assert isinstance(resolved, ResolvedPolicySet)
    assert resolved.version_id == VERSION_ID
    assert resolved.body == BODY
    assert resolved.artifact_id == "b964d217"


def test_in_memory_provider_body_parses():
    # The seam's whole point: the deterministic core is exercisable end to end
    # with no gateway in sight.
    provider = InMemoryPolicyProvider()
    provider.put(TENANT, VERSION_ID, BODY)
    resolved = provider.resolve(TENANT)
    assert isinstance(resolved, ResolvedPolicySet)
    parsed = parse_policy_set(resolved.body, version_id=resolved.version_id)
    assert isinstance(parsed, PolicySet)
    assert parsed.tenant == TENANT


def test_in_memory_provider_unknown_tenant_is_not_found():
    result = InMemoryPolicyProvider().resolve("someone-else")
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.not_found
    assert result.tenant == "someone-else"


def test_in_memory_provider_replaces_on_put():
    provider = InMemoryPolicyProvider()
    provider.put(TENANT, "gv-old", BODY)
    provider.put(TENANT, "gv-new", BODY)
    resolved = provider.resolve(TENANT)
    assert isinstance(resolved, ResolvedPolicySet)
    assert resolved.version_id == "gv-new"


def test_in_memory_provider_tenants_are_independent():
    provider = InMemoryPolicyProvider()
    provider.put(TENANT, "gv-a", BODY)
    provider.put("snowlinedev", "gv-b", BODY)
    a = provider.resolve(TENANT)
    b = provider.resolve("snowlinedev")
    assert isinstance(a, ResolvedPolicySet) and a.version_id == "gv-a"
    assert isinstance(b, ResolvedPolicySet) and b.version_id == "gv-b"


# --- the gateway client ----------------------------------------------------


def test_gateway_resolves_the_current_version():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == POLICY_SET_PATH
        assert request.url.params["tenant"] == TENANT
        return httpx.Response(200, json=_version_payload())

    provider = GatewayPolicyProvider(
        "http://governance.example", client=_client(handler)
    )
    resolved = provider.resolve(TENANT)
    assert isinstance(resolved, ResolvedPolicySet), resolved
    assert resolved.version_id == VERSION_ID
    assert resolved.artifact_id == "b964d217"
    # Verbatim: the cached bytes must be diffable against the artifact.
    assert resolved.body == BODY


def test_gateway_url_is_normalized():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json=_version_payload())

    GatewayPolicyProvider(
        "http://governance.example/", client=_client(handler)
    ).resolve(TENANT)
    # A trailing slash on the configured URL must not produce `//marketing/...`.
    assert seen == [f"http://governance.example{POLICY_SET_PATH}?tenant={TENANT}"]


def test_gateway_tenant_identified_404_is_not_found():
    # The no-artifact answer must IDENTIFY ITSELF (a JSON body naming the
    # tenant) — that is part of the assumed route contract, because a bare
    # 404 is indistinguishable from the route not existing at all.
    provider = GatewayPolicyProvider(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(404, json={"tenant": TENANT})),
    )
    result = provider.resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.not_found
    assert result.status_code == 404


@pytest.mark.parametrize(
    "response",
    [
        # FastAPI's framework 404 for an unknown route — the missing-route
        # case this disambiguation exists for.
        httpx.Response(404, json={"detail": "Not Found"}),
        # A 404 that names a DIFFERENT tenant is answering some other question.
        httpx.Response(404, json={"tenant": "someone-else"}),
        # A non-JSON 404 (a proxy error page).
        httpx.Response(404, text="<html>404</html>"),
    ],
)
def test_a_bare_404_is_unavailable_not_no_policies(response):
    # Misreading a missing ROUTE as "no policies" would be a fleet-wide
    # silent match-none — the caller must stall visibly instead.
    provider = GatewayPolicyProvider(
        "http://governance.example", client=_client(lambda r: response)
    )
    result = provider.resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.unavailable
    assert result.status_code == 404


@pytest.mark.parametrize("status", [403, 500, 502, 503])
def test_gateway_other_error_statuses_are_unavailable(status):
    # NOT `not_found`: we did not learn the tenant's rules, so the caller must
    # stall rather than evaluate against nothing.
    provider = GatewayPolicyProvider(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(status)),
    )
    result = provider.resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.unavailable
    assert result.status_code == status


def test_gateway_transport_error_is_unavailable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    result = GatewayPolicyProvider(
        "http://governance.example", client=_client(handler)
    ).resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.unavailable
    assert result.status_code is None
    assert "connection refused" in result.detail


@pytest.mark.parametrize("bad_url", ["not-a-url", "http:/governance.example", "://x"])
def test_a_typod_governance_url_does_not_escape(bad_url):
    # The never-raises contract has to hold for the CONFIG TYPO above all,
    # because that is the failure an operator actually hits. These never reach
    # the transport: httpx rejects them first, and (in this version) does so
    # with a bare ValueError from urllib rather than anything in its own
    # exception hierarchy. The client is stubbed, so no socket is involved
    # either way.
    result = GatewayPolicyProvider(
        bad_url, client=_client(lambda r: httpx.Response(200))
    ).resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.unavailable


def test_gateway_non_json_200_is_malformed_response():
    provider = GatewayPolicyProvider(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(200, text="<html>proxy error</html>")),
    )
    result = provider.resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.malformed_response


def test_gateway_non_object_200_is_malformed_response():
    provider = GatewayPolicyProvider(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(200, json=[1, 2, 3])),
    )
    result = provider.resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.malformed_response


@pytest.mark.parametrize("missing", ["id", "body_snapshot"])
def test_gateway_payload_missing_a_required_key_is_malformed_response(missing):
    payload = _version_payload()
    del payload[missing]
    provider = GatewayPolicyProvider(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(200, json=payload)),
    )
    result = provider.resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.malformed_response
    assert missing in result.detail


def test_gateway_will_not_accept_an_empty_version_id():
    # The version id is the one fact the ledger contract requires; a blank one
    # is worse than no answer, because it would be recorded as if it were real.
    provider = GatewayPolicyProvider(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(200, json=_version_payload(id="  "))),
    )
    result = provider.resolve(TENANT)
    assert isinstance(result, PolicyResolutionError)
    assert result.failure is ResolutionFailure.malformed_response


def test_gateway_keeps_a_prose_body_intact():
    # A policy artifact revised to prose still RESOLVES — resolution succeeded,
    # and it is the parser that quarantines. Conflating the two would report a
    # governance outage every time an operator broke their own policy.
    prose = "# Turtle's Edge marketing policies\n\nNo longer JSON.\n"
    provider = GatewayPolicyProvider(
        "http://governance.example",
        client=_client(
            lambda r: httpx.Response(200, json=_version_payload(body_snapshot=prose))
        ),
    )
    resolved = provider.resolve(TENANT)
    assert isinstance(resolved, ResolvedPolicySet)
    assert resolved.body == prose


def test_gateway_defaults_to_configured_governance_url(monkeypatch):
    monkeypatch.setenv("MARKETING_GOVERNANCE_URL", "http://gov.example:9999/")
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.host)
        return httpx.Response(200, json=_version_payload())

    GatewayPolicyProvider(client=_client(handler)).resolve(TENANT)
    assert seen == ["gov.example"]


def test_an_empty_body_resolves_for_the_parser_to_quarantine():
    # An operator-authored empty body is an artifact state, not a transport
    # failure: it must flow through with its version id so the parser
    # quarantines it and the §11 listing shows the broken version.
    provider = GatewayPolicyProvider(
        "http://governance.example",
        client=_client(
            lambda r: httpx.Response(200, json=_version_payload(body_snapshot=""))
        ),
    )
    resolved = provider.resolve(TENANT)
    assert isinstance(resolved, ResolvedPolicySet), resolved
    assert resolved.body == ""
    assert resolved.version_id == VERSION_ID

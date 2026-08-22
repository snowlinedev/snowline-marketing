"""Resolving an artifact's CURRENT version — the poll half of §8.

Stubs governance's HTTP with an `httpx.MockTransport`, so no governance runs.
No DB needed. The contract under test on every path: `resolve` NEVER raises,
and the distinction that must not blur is between an ANSWER that leaves nothing
to compare (`not_found`, `no_current_version`) and the ABSENCE of an answer
(`unavailable`, `malformed_response`). Rounding the second into "the artifact is
gone" would silently exempt every deliverable in the fleet from staleness —
which is the same failure `test_policy_source.py` guards on the policy side, and
the reason both clients share `thin_client.identified_404`.
"""

from __future__ import annotations

import httpx
import pytest

from snowline_marketing.artifact_versions import (
    ARTIFACT_PATH,
    ArtifactVersion,
    ArtifactVersionError,
    GatewayArtifactVersions,
    InMemoryArtifactVersions,
    VersionFailure,
    resolve_all,
)

ARTIFACT = "b964d217"
VERSION = "av-3c81f9d2"


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _artifact_payload(**overrides) -> dict:
    # Governance's VERIFIED `get_artifact` shape (see artifact_versions' module
    # docstring): `id` is the ARTIFACT id, `current_version.id` is the VERSION
    # id, `current_version.milestone` is the canonical stamp.
    payload = {
        "id": ARTIFACT,
        "doc_kind": "reference",
        "maturity": "canonical",
        "current_version": {
            "id": VERSION,
            "title": "Turtle's Edge messaging",
            "status": "current",
            "milestone": "v1.4",
            "has_snapshot": True,
        },
        "leaves": [],
        "version_count": 3,
    }
    payload.update(overrides)
    return payload


# --- the fixture provider ----------------------------------------------------


def test_in_memory_provider_round_trips():
    versions = InMemoryArtifactVersions()
    stored = versions.put(ARTIFACT, VERSION, milestone="v1.4")
    resolved = versions.resolve(ARTIFACT)
    assert resolved == stored
    assert isinstance(resolved, ArtifactVersion)
    assert (resolved.artifact_id, resolved.version_id, resolved.milestone) == (
        ARTIFACT,
        VERSION,
        "v1.4",
    )


def test_in_memory_provider_revises_on_put():
    # "Governance revised this artifact" — the event that makes every
    # deliverable citing it stale.
    versions = InMemoryArtifactVersions()
    versions.put(ARTIFACT, VERSION)
    versions.put(ARTIFACT, "av-99", milestone="v1.5")
    resolved = versions.resolve(ARTIFACT)
    assert isinstance(resolved, ArtifactVersion)
    assert (resolved.version_id, resolved.milestone) == ("av-99", "v1.5")


def test_in_memory_provider_unknown_artifact_is_not_found():
    result = InMemoryArtifactVersions().resolve("nope")
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.not_found
    assert result.artifact_id == "nope"


def test_resolve_all_asks_once_per_distinct_artifact_in_id_order():
    asked: list[str] = []

    class Counting:
        def __init__(self, inner):
            self.inner = inner

        def resolve(self, artifact_id):
            asked.append(artifact_id)
            return self.inner.resolve(artifact_id)

    inner = InMemoryArtifactVersions()
    inner.put("b964d217", VERSION)
    inner.put("9f21ac04", "av-77b0e315")
    resolved = resolve_all(
        Counting(inner), ["b964d217", "9f21ac04", "b964d217", "b964d217"]
    )
    # One read per artifact per sweep (so two deliverables can never compare
    # against different reads of the same artifact), in a deterministic order.
    assert asked == ["9f21ac04", "b964d217"]
    assert set(resolved) == {"9f21ac04", "b964d217"}


def test_resolve_all_keeps_failures_keyed_by_artifact():
    resolved = resolve_all(InMemoryArtifactVersions(), ["gone"])
    assert isinstance(resolved["gone"], ArtifactVersionError)


# --- the gateway client ------------------------------------------------------


def test_gateway_resolves_the_current_version():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == ARTIFACT_PATH
        assert request.url.params["artifact_id"] == ARTIFACT
        return httpx.Response(200, json=_artifact_payload())

    resolved = GatewayArtifactVersions(
        "http://governance.example", client=_client(handler)
    ).resolve(ARTIFACT)
    assert isinstance(resolved, ArtifactVersion), resolved
    assert resolved.version_id == VERSION
    assert resolved.milestone == "v1.4"


def test_gateway_url_is_normalized():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json=_artifact_payload())

    GatewayArtifactVersions(
        "http://governance.example/", client=_client(handler)
    ).resolve(ARTIFACT)
    assert seen == [f"http://governance.example{ARTIFACT_PATH}?artifact_id={ARTIFACT}"]


def test_an_unstamped_current_version_resolves_without_a_milestone():
    # §8 works without stamps — version inequality alone is v1's trigger — so a
    # null milestone is an ordinary state, never a reason to reject the read.
    payload = _artifact_payload()
    payload["current_version"] = dict(payload["current_version"], milestone=None)
    resolved = GatewayArtifactVersions(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(200, json=payload)),
    ).resolve(ARTIFACT)
    assert isinstance(resolved, ArtifactVersion), resolved
    assert resolved.milestone is None


def test_gateway_identified_404_is_not_found():
    result = GatewayArtifactVersions(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(404, json={"artifact_id": ARTIFACT})),
    ).resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.not_found
    assert result.status_code == 404


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(404, json={"detail": "Not Found"}),
        httpx.Response(404, json={"artifact_id": "some-other-artifact"}),
        httpx.Response(404, text="<html>404</html>"),
    ],
)
def test_a_bare_404_is_unavailable_not_a_missing_artifact(response):
    # A missing ROUTE read as "no such artifact" would skip every deliverable in
    # the fleet and report a quiet, successful sweep.
    result = GatewayArtifactVersions(
        "http://governance.example", client=_client(lambda r: response)
    ).resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.unavailable


@pytest.mark.parametrize("status", [403, 500, 502, 503])
def test_other_error_statuses_are_unavailable(status):
    result = GatewayArtifactVersions(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(status)),
    ).resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.unavailable
    assert result.status_code == status


def test_transport_error_is_unavailable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    result = GatewayArtifactVersions(
        "http://governance.example", client=_client(handler)
    ).resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.unavailable
    assert "connection refused" in result.detail


@pytest.mark.parametrize("bad_url", ["not-a-url", "http:/governance.example", "://x"])
def test_a_typod_governance_url_does_not_escape(bad_url):
    # The never-raises contract has to hold for the CONFIG TYPO above all —
    # the failure an operator actually hits.
    result = GatewayArtifactVersions(
        bad_url, client=_client(lambda r: httpx.Response(200))
    ).resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.unavailable


def test_a_null_current_version_is_its_own_answer():
    # Governance ANSWERED: the artifact exists and has no canonical leaf
    # (every version milestone-ineligible). Nothing to compare, and a different
    # operator fix from "governance is down" — so a distinct failure value.
    result = GatewayArtifactVersions(
        "http://governance.example",
        client=_client(
            lambda r: httpx.Response(200, json=_artifact_payload(current_version=None))
        ),
    ).resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.no_current_version


@pytest.mark.parametrize(
    "payload",
    [
        # A body answering about a DIFFERENT artifact (a cache, a route that
        # ignores its query parameter): comparing against it would cite two
        # unrelated facts as though they had been compared.
        _artifact_payload(id="another-artifact"),
        # No artifact id at all.
        {"current_version": {"id": VERSION}},
        # `current_version` is not an object.
        _artifact_payload(current_version="av-3c81f9d2"),
        # The one fact a finding must cite verbatim is missing.
        _artifact_payload(current_version={"title": "no id here"}),
        _artifact_payload(current_version={"id": "   "}),
    ],
)
def test_a_200_that_is_not_an_artifact_record_is_malformed(payload):
    result = GatewayArtifactVersions(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(200, json=payload)),
    ).resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.malformed_response


@pytest.mark.parametrize("body", ["<html>proxy error</html>", "[1, 2, 3]"])
def test_a_non_object_200_is_malformed(body):
    result = GatewayArtifactVersions(
        "http://governance.example",
        client=_client(lambda r: httpx.Response(200, text=body)),
    ).resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.malformed_response


def test_gateway_defaults_to_the_configured_governance_url(monkeypatch):
    monkeypatch.setenv("MARKETING_GOVERNANCE_URL", "http://gov.example:9999/")
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.host)
        return httpx.Response(200, json=_artifact_payload())

    GatewayArtifactVersions(client=_client(handler)).resolve(ARTIFACT)
    assert seen == ["gov.example"]


def test_a_broken_ssl_env_does_not_escape(monkeypatch):
    # httpx builds the SSL context eagerly in the Client constructor, so a
    # broken SSL_CERT_FILE raises FileNotFoundError (an OSError, outside the
    # httpx hierarchy) from the lazy construction — it must come back as
    # 'unavailable', not crash the sweep.
    def boom(*args, **kwargs):
        raise FileNotFoundError("/nonexistent/ca-bundle.pem")

    monkeypatch.setattr(httpx, "Client", boom)
    result = GatewayArtifactVersions("http://governance.example").resolve(ARTIFACT)
    assert isinstance(result, ArtifactVersionError)
    assert result.failure is VersionFailure.unavailable
    assert "ca-bundle" in result.detail

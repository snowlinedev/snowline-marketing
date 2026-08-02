"""Where a tenant's CURRENT policy set comes from (spec §6).

Spec §6: policies live as governance artifacts, one per tenant org scope; "the
plugin resolves the current version through the gateway, caches it, and records
the evaluated version id on every ledger row". This module is that resolution
step and nothing else — it hands back a body plus the governance artifact
VERSION ID it came from. Parsing is `policies.parse_policy_set`; persistence is
`policy_cache.PolicyCache`; matching is the evaluation engine. The version id
is the load-bearing output: it is the cache key AND the value the delivery
ledger records (spec §4, contract requirement).

`PolicyProvider` is a PROTOCOL with two implementations because the
deterministic core must be buildable and testable with no gateway in sight
(spec §5's fixtures-first posture applies to policies exactly as it does to
events): `InMemoryPolicyProvider` for tests, captured policy fixtures and the
§11 dry-run; `GatewayPolicyProvider` for the live service.

**The live client rides a DOCUMENTED ASSUMPTION, and this is deliberate.**
What was verified in the sibling repos (2026-08-01):

- There is NO plugin-to-plugin REST convention in Snowline, by design — the
  platform's architecture spec makes the agent the integration runtime, so
  "plugin↔plugin coupling is ~nil and the platform never needs to route
  between plugins". The gateway proxies MCP tool traffic, not REST.
- Governance's artifact surface is MCP-TOOL-ONLY. Its FastAPI app registers
  `/health`, a dashboard `/ui-api` router, and two mounted MCP servers; there
  is no artifact/decision REST route anywhere, and `gateway.md` lists
  server-side cross-plugin tool placement as explicitly deferred.
- The one established plugin→service HTTP idiom is the vendored thin client:
  PM and governance each carry their own `HttpScopeClient`/
  `HttpMilestoneClient` — `(url: str | None = None, *, client: httpx.Client |
  None = None, timeout: float = 10.0)`, base URL from a shared unprefixed env
  var, NO per-request secret (trust is source-IP network position on the
  tailnet). `GatewayPolicyProvider` is that idiom, applied to governance.
- The RESPONSE shape below is not invented: it is governance's real
  `get_artifact_version` payload (`id`, `artifact_id`, `body_snapshot` — read
  from `snowline_governance/artifacts.py`). Only the ROUTE is assumed.

So exactly one thing here is unverifiable today: the HTTP path. It is a single
module constant, and the response mapping is one small function. When
governance grows the read surface — or when this plugin instead speaks MCP
through the gateway — a new `PolicyProvider` implementation lands beside these
two and nothing above the protocol changes. That is the whole point of the
seam; the engine, the cache and the ledger are testable now regardless.

Failure is TYPED, never raised, and the distinction that matters is
`not_found` vs `unavailable`:

- `not_found` — the tenant has no policy artifact. A real, evaluable answer:
  no policies, every event audits as `ignored` (spec §14), nothing is minted.
- `unavailable` / `malformed_response` — we do not KNOW what the tenant's
  policies are. The caller must never round this down to "no policies": that
  is the silent match-none §6 forbids. A transport blip must stall evaluation
  visibly, not quietly stop minting work.
"""

from __future__ import annotations

import enum
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

import httpx

from snowline_marketing import classify, config

log = logging.getLogger("snowline_marketing.policy_source")

# ASSUMED (see module docstring): the governance read that answers "which
# artifact version currently holds this tenant's marketing policy set, and what
# is in it". One constant, one query parameter, plus ONE response-shape
# requirement: the no-artifact answer must be a 404 whose JSON body names the
# tenant it is answering for ({"tenant": ...}), because a bare framework 404
# is indistinguishable from this route not existing at all — and that
# ambiguity, misread as "no policies", would be a fleet-wide silent
# match-none. That is the whole surface area of the assumption.
POLICY_SET_PATH = "/marketing/policy-set"

# VERIFIED: governance's `get_artifact_version` payload keys. `id` is the
# artifact VERSION id (not the artifact id, which comes back separately) and
# `body_snapshot` is the version's body as text.
_VERSION_ID_KEY = "id"
_ARTIFACT_ID_KEY = "artifact_id"
_BODY_KEY = "body_snapshot"


@dataclass(frozen=True)
class ResolvedPolicySet:
    """One tenant's current policy artifact version, unparsed.

    `body` is TEXT, verbatim, all the way from governance to the cache column:
    governance stores artifact bodies as text, `parse_policy_set` accepts text,
    and the cache keeps text — so no decode/re-encode ever stands between the
    artifact an operator reads in governance and the bytes this plugin
    evaluated. A body that is not even JSON (an artifact revised to prose) is a
    real case, and it has to survive the trip intact to be quarantined
    honestly.

    `version_id` is the governance artifact VERSION id: immutable, the cache
    key, and the value every ledger row records (spec §4)."""

    tenant: str
    version_id: str
    body: str
    # The owning artifact's id, when the source knew it. Audit only — an
    # operator asked to fix a quarantined policy needs to know which artifact
    # to revise, and the version id alone does not say.
    artifact_id: str | None = None


class ResolutionFailure(enum.StrEnum):
    """Why the current policy set could not be resolved.

    Three values, and the first is categorically unlike the other two: see the
    module docstring on `not_found` (an answer) vs the rest (an absence of
    one)."""

    # The tenant has no policy-set artifact. Evaluable: no policies.
    not_found = "not_found"
    # Governance could not be reached, or answered with something other than a
    # policy set (any non-200/404 status included). NOT "no policies".
    unavailable = "unavailable"
    # Governance answered 200 with a payload that is not a policy artifact
    # version — a route that moved, a proxy error page, a shape change.
    malformed_response = "malformed_response"


@dataclass(frozen=True)
class PolicyResolutionError:
    """A failed resolution, as a RESULT. Mirrors `MalformedEnvelope` /
    `MalformedPolicySet`: expected input classes are returned and explained,
    never raised through, so the caller's control flow is the same on every
    path."""

    tenant: str
    failure: ResolutionFailure
    detail: str
    # The HTTP status when there was one — the first thing an operator wants
    # when governance is answering but not with what we asked for.
    status_code: int | None = None


# What a caller gets back for one tenant: resolved, or explained.
ResolutionResult = ResolvedPolicySet | PolicyResolutionError


class PolicyProvider(Protocol):
    """The only thing the evaluation engine will know about where policies come
    from — the seam that keeps the deterministic core buildable without a live
    gateway.

    `resolve` takes the TENANT ORG SCOPE (spec §6: one policy-set artifact per
    tenant) and NEVER raises: everything it cannot do comes back as a
    `PolicyResolutionError`."""

    def resolve(self, tenant: str) -> ResolutionResult: ...


class InMemoryPolicyProvider:
    """A provider over policy sets held in the process.

    Not a mock: this is the fixtures-mode provider (spec §5 makes fixtures a
    first-class dev/CI surface, and §11's dry-run — "evaluate a policy version
    against captured fixtures" — needs to point the engine at a version that is
    NOT the tenant's current one). Tests use it for the same reason.

    Version ids are the caller's to choose, and they are what a ledger row will
    record, so a fixture-driven dry-run produces a traceable audit trail rather
    than a blank."""

    def __init__(self, sets: Mapping[str, ResolvedPolicySet] | None = None) -> None:
        self._sets: dict[str, ResolvedPolicySet] = dict(sets or {})

    def put(
        self,
        tenant: str,
        version_id: str,
        body: str,
        *,
        artifact_id: str | None = None,
    ) -> ResolvedPolicySet:
        """Install (or replace) a tenant's current policy set. Returns what a
        later `resolve` will hand back, so a caller can assert on it directly."""
        resolved = ResolvedPolicySet(
            tenant=tenant,
            version_id=version_id,
            body=body,
            artifact_id=artifact_id,
        )
        self._sets[tenant] = resolved
        return resolved

    def resolve(self, tenant: str) -> ResolutionResult:
        resolved = self._sets.get(tenant)
        if resolved is None:
            # `not_found`, not an empty set: a tenant nobody configured has no
            # policy artifact, which is exactly what governance would say.
            return PolicyResolutionError(
                tenant=tenant,
                failure=ResolutionFailure.not_found,
                detail=f"no policy set configured for tenant {tenant!r}",
            )
        return resolved


class GatewayPolicyProvider:
    """The live provider — governance over HTTP (see the module docstring for
    what is verified and what is assumed).

    Constructor mirrors the house vendored-client shape exactly
    (`HttpScopeClient` / `HttpMilestoneClient`): optional base URL defaulting to
    config, an injectable `httpx.Client` so tests stub the transport, and a
    timeout. No auth header — behind the platform trust gate the request rides
    the tailnet and there is no per-request secret (governance decision
    `35546152`, verified in the platform's `trust.py`)."""

    def __init__(
        self,
        governance_url: str | None = None,
        *,
        client: httpx.Client | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._governance_url = (governance_url or config.governance_url()).rstrip("/")
        self._client = client
        self._timeout = timeout

    def resolve(self, tenant: str) -> ResolutionResult:
        url = f"{self._governance_url}{POLICY_SET_PATH}"
        params = {"tenant": tenant}
        try:
            if self._client is None:
                # One long-lived client for the provider's lifetime, built
                # lazily INSIDE the never-raises guard (construction can fail
                # on a broken SSL_CERT_FILE). A one-shot httpx.get per tenant
                # would re-handshake TCP on every sweep cycle — sustained
                # connection churn against governance for no benefit. The
                # provider is driven by the single-threaded policy sweep;
                # nothing here is locked.
                self._client = httpx.Client(timeout=self._timeout)
            resp = self._client.get(url, params=params, timeout=self._timeout)
        except (httpx.HTTPError, httpx.InvalidURL, ValueError, OSError) as exc:
            # A config error must land here as a typed result, not escape the
            # never-raises contract — and it takes all four arms to guarantee
            # that. `InvalidURL` is not an `HTTPError` (it subclasses
            # Exception directly); a base URL malformed enough that the
            # request never reaches the transport (`http:/gov.example`, a
            # missing scheme) surfaces as a bare `ValueError` from urllib; and
            # the lazy `httpx.Client()` construction above raises `OSError`
            # (FileNotFoundError) on a broken SSL_CERT_FILE — httpx builds the
            # SSL context eagerly in the constructor. The config typo is
            # precisely the failure an operator hits, so it is the one this
            # must not crash on.
            log.warning("policy resolution for %r failed at %s: %s", tenant, url, exc)
            return PolicyResolutionError(
                tenant=tenant,
                failure=ResolutionFailure.unavailable,
                detail=str(exc),
            )
        if resp.status_code == httpx.codes.NOT_FOUND:
            # A 404 is AMBIGUOUS: "this tenant has no policy artifact" (an
            # evaluable answer) and "this ROUTE does not exist on this
            # governance build" (we learned nothing) share a status code — and
            # the route is exactly the assumed, unverified part of this
            # client. Conflating them converts a missing route into a fleet-
            # wide silent match-none. So the route CONTRACT requires the
            # no-artifact answer to identify itself: a JSON body naming the
            # tenant it is answering for. A bare framework 404 (FastAPI's
            # {"detail": "Not Found"}) is treated as unavailable — stall
            # visibly, let the operator find the deployment problem.
            payload, _decode_failure = classify.decode_json_object(resp.content)
            if payload is not None and payload.get("tenant") == tenant:
                return PolicyResolutionError(
                    tenant=tenant,
                    failure=ResolutionFailure.not_found,
                    detail=(
                        f"governance has no policy-set artifact for tenant {tenant!r}"
                    ),
                    status_code=resp.status_code,
                )
            return PolicyResolutionError(
                tenant=tenant,
                failure=ResolutionFailure.unavailable,
                detail=(
                    f"404 from {url} without a tenant-identifying body — "
                    "indistinguishable from a missing route; NOT treated as "
                    "'no policies'"
                ),
                status_code=resp.status_code,
            )
        if not resp.is_success:
            # Everything else — 403, 500, a gateway's 502 — is UNAVAILABLE, not
            # "no policies". We did not learn the tenant's rules; the caller
            # must stall rather than evaluate against nothing.
            return PolicyResolutionError(
                tenant=tenant,
                failure=ResolutionFailure.unavailable,
                detail=f"governance returned {resp.status_code} for {url}",
                status_code=resp.status_code,
            )
        return _read_version_payload(tenant, resp)


def _read_version_payload(tenant: str, resp: httpx.Response) -> ResolutionResult:
    """Map governance's artifact-version payload onto `ResolvedPolicySet`.

    Strict: a 200 whose payload is not an artifact version is
    `malformed_response`, never a best-effort partial. A missing version id is
    the worst possible thing to paper over — the ledger would record the
    evaluated version as unknown, which is the one fact the contract requires
    it to have."""
    # The decode skeleton is classify.py's, same as both parsers — a third
    # hand-rolled decoder here would silently miss the next hardening fix
    # (httpx's resp.json() is json.loads(resp.content) underneath, so this is
    # a drop-in).
    payload, decode_failure = classify.decode_json_object(resp.content)
    if decode_failure is not None:
        failure, detail = decode_failure
        return PolicyResolutionError(
            tenant=tenant,
            failure=ResolutionFailure.malformed_response,
            detail=f"governance response: {failure.value} — {detail}",
            status_code=resp.status_code,
        )
    version_id = payload.get(_VERSION_ID_KEY)
    body = payload.get(_BODY_KEY)
    # The version id must be a real id — without it the ledger cannot record
    # what was evaluated. The BODY need only be a string: an EMPTY body is an
    # operator-authored artifact state (revised to nothing), and it must flow
    # THROUGH resolution so the parser quarantines it against its version id —
    # classifying it as a transport failure here would hide the broken version
    # from the §11 quarantine listing and send the operator debugging a
    # governance problem that does not exist.
    if not isinstance(version_id, str) or not version_id.strip():
        return PolicyResolutionError(
            tenant=tenant,
            failure=ResolutionFailure.malformed_response,
            detail=f"policy-set payload is missing or empty at {_VERSION_ID_KEY!r}",
            status_code=resp.status_code,
        )
    if not isinstance(body, str):
        return PolicyResolutionError(
            tenant=tenant,
            failure=ResolutionFailure.malformed_response,
            detail=(
                f"policy-set payload is missing {_BODY_KEY!r} or it is not a string"
            ),
            status_code=resp.status_code,
        )
    artifact_id = payload.get(_ARTIFACT_ID_KEY)
    return ResolvedPolicySet(
        tenant=tenant,
        version_id=version_id.strip(),
        # NOT stripped: the body is kept verbatim (see `ResolvedPolicySet`).
        body=body,
        artifact_id=artifact_id if isinstance(artifact_id, str) else None,
    )

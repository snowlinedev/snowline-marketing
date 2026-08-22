"""Which version of a source artifact is CURRENT — the poll half of §8.

Spec §5, settled: "Governance is **polled, not evented**, at v1: a scheduled
sweep compares artifact leaves/version ids against the deliverable provenance
ledger and the policy cache watermark. Staleness is not latency-sensitive; no
second event spine gets built before PM's exists." This module is that poll and
nothing else: given a governance artifact id it hands back the version id that
is current NOW — plus the milestone stamp when the current version carries one
(Snowline#141's release boundary) — or a typed failure. Comparing that against
what a deliverable recorded is `staleness.py`'s job; nothing here reads a
deliverable row, and nothing here decides what a difference means.

It is `policy_source.py` one artifact-shaped step to the side, deliberately so:
same protocol-with-two-implementations seam (`InMemoryArtifactVersions` for the
fixtures-first flow and tests, `GatewayArtifactVersions` for the live service),
same vendored-thin-client idiom, same never-raises typed-failure contract, and
the same identified-404 discipline (`thin_client.identified_404`). A reader who
has understood one has understood both, and the shared rule lives in exactly one
place.

**The live client rides a DOCUMENTED ASSUMPTION, exactly one.** What was
verified in the sibling repo (`snowline-platform/governance`, 2026-08-22):

- Governance's artifact surface is MCP-TOOL-ONLY — the same finding
  `policy_source.py` records, unchanged: its FastAPI app registers `/health`, a
  dashboard `/ui-api` router and two mounted MCP servers, and there is no
  artifact REST route. There is no plugin-to-plugin REST convention in Snowline
  by design; the gateway proxies MCP tool traffic.
- The RESPONSE shape below is not invented: it is governance's real
  `get_artifact` payload (`snowline_governance/artifacts.py`, `_artifact_dict` /
  `_version_dict`). The artifact's own id rides `id`; `current_version` is a
  version record whose `id` is the VERSION id and whose `milestone` is the
  canonical milestone stamp the write path resolved (`None` on an unstamped
  version). `current_version` is the leaf of the ELIGIBLE subgraph on a
  milestone-aware read — that is, the version governance itself calls current.
- `current_version` can legitimately be NULL: an artifact whose every version is
  milestone-ineligible has no canonical leaf (governance §6.1.3, which surfaces
  competition rather than tie-breaking silently). That is a real answer and it
  is NOT a version id, so it gets its own failure value rather than being
  rounded into one of the others.

So exactly one thing is unverifiable today: the HTTP path. It is a single module
constant, and the response mapping is one small function. When governance grows
the read surface — or when this plugin speaks MCP through the gateway — a new
`ArtifactVersionProvider` lands beside these two and nothing above the protocol
changes.

**Failure is TYPED, never raised, and "unavailable" must never be read as an
answer.** The sweep's whole value is that it says something true about
staleness, and there are exactly two ways to make it lie:

- `unavailable` / `malformed_response` — governance could not be reached, or
  answered with something that is not an artifact. We do not KNOW the current
  version. Reading that as "not stale" hides drift silently; reading it as
  "stale" mints work against evidence nobody has. The sweep must STALL for that
  artifact (`staleness.py` skips every deliverable citing it and reports why),
  which is the same posture `engine.py` takes when policies cannot be resolved.
- `not_found` / `no_current_version` — governance ANSWERED, and the answer is
  that there is no current version to compare against (the artifact was never
  registered, was retired, or has no canonical leaf). Unlike policy resolution's
  `not_found` — which is evaluable, meaning "this tenant has no rules" — there
  is nothing evaluable here: a deliverable citing an artifact governance cannot
  name a current version for can be judged neither fresh nor stale. It is
  reported and skipped, with a different sentence for the operator, because the
  fix is different (revise the artifact ref, or governance's stamps) and it will
  not heal by retrying.
"""

from __future__ import annotations

import enum
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

import httpx

from snowline_marketing import classify, config, thin_client

log = logging.getLogger("snowline_marketing.artifact_versions")

# ASSUMED (see module docstring): the governance read that answers "which
# version of this artifact is current, and what milestone is it stamped with".
# One constant, one query parameter, plus ONE response-shape requirement: the
# no-such-artifact answer must be a 404 whose JSON body names the artifact it is
# answering for ({"artifact_id": ...}), because a bare framework 404 is
# indistinguishable from this route not existing on this governance build — and
# that ambiguity, misread as "the artifact is gone", would silently exempt every
# deliverable in the fleet from staleness. That is the whole surface area of the
# assumption.
ARTIFACT_PATH = "/marketing/artifact"

# VERIFIED: governance's `get_artifact` payload keys (`_artifact_dict` /
# `_version_dict` in snowline_governance/artifacts.py). `id` at the top level is
# the ARTIFACT id; `current_version.id` is the VERSION id; `current_version.
# milestone` is the canonical stamp, None on an unstamped version.
_ARTIFACT_ID_KEY = "id"
_CURRENT_VERSION_KEY = "current_version"
_VERSION_ID_KEY = "id"
_MILESTONE_KEY = "milestone"


@dataclass(frozen=True)
class ArtifactVersion:
    """One artifact's current version, as governance reports it.

    `version_id` is the fact §8's comparison turns on and §14 requires a finding
    to cite verbatim, so it is carried as governance spelled it — never
    normalized, never shortened.

    `milestone` is the release-boundary stamp (Snowline#141): it REFINES a
    finding's story ("the listing reflects the v1-stamped feature list, but a
    v2-stamped version now exists") and is deliberately not a trigger of its
    own — version inequality alone is v1's trigger, so the sweep works against
    artifacts nobody has stamped yet."""

    artifact_id: str
    version_id: str
    milestone: str | None = None


class VersionFailure(enum.StrEnum):
    """Why an artifact's current version could not be established.

    Two categorically different pairs, and the module docstring is where the
    distinction is argued: `not_found`/`no_current_version` are ANSWERS that
    happen to leave nothing to compare, while `unavailable`/`malformed_response`
    are the absence of an answer. Both make the sweep skip the deliverable — the
    difference is the sentence the operator reads and whether retrying helps."""

    # Governance has no such artifact (never registered, or the ref is wrong).
    not_found = "not_found"
    # The artifact exists and governance names no current version for it — every
    # version is milestone-ineligible, so there is no canonical leaf (governance
    # §6.1.3). Nothing to compare, and an operator has to stamp or resolve it.
    no_current_version = "no_current_version"
    # Governance could not be reached, or answered with any non-200/404 status.
    # NOT "the artifact is gone", NOT "nothing changed".
    unavailable = "unavailable"
    # A 200 whose payload is not an artifact record — a route that moved, a
    # proxy error page, a shape change.
    malformed_response = "malformed_response"


@dataclass(frozen=True)
class ArtifactVersionError:
    """A failed resolution, as a RESULT.

    Mirrors `policy_source.PolicyResolutionError` field for field, for the same
    reason: expected input classes are returned and explained, never raised
    through, so every caller's control flow is the same on every path."""

    artifact_id: str
    failure: VersionFailure
    detail: str
    # The HTTP status when there was one — the first thing an operator wants
    # when governance is answering but not with what we asked for.
    status_code: int | None = None


# What a caller gets back for one artifact: resolved, or explained.
VersionResolution = ArtifactVersion | ArtifactVersionError


class ArtifactVersionProvider(Protocol):
    """The only thing the staleness sweep knows about where current versions
    come from.

    `resolve` takes a governance artifact id — the same id a deliverable's
    provenance recorded (`deliverables.SourceVersion.artifact_id`) — and NEVER
    raises: everything it cannot do comes back as an `ArtifactVersionError`."""

    def resolve(self, artifact_id: str) -> VersionResolution: ...


def resolve_all(
    provider: ArtifactVersionProvider, artifact_ids: Iterable[str]
) -> dict[str, VersionResolution]:
    """Resolve each DISTINCT artifact id exactly once, in id order.

    The batching the sweep needs and the reason it is a free function rather
    than a protocol method: one tenant's deliverables cite the same few
    artifacts over and over (a canonical messaging doc, a listing doc), so a
    per-deliverable resolve would be N round trips against governance for a
    handful of distinct answers — and two deliverables comparing against
    DIFFERENT reads of the same artifact, if a revision landed mid-sweep, would
    make one sweep's findings disagree with each other. One read per artifact
    per sweep is both cheaper and more honest.

    Sorted, so a sweep's governance traffic (and any log it produces) is the
    same sequence for the same inputs — determinism is the property §6 claims
    for the whole machine, and it is cheapest to keep by never leaving an order
    to chance. Every implementation of the protocol gets this behaviour for
    free; none of them has to remember to dedup."""
    return {
        artifact_id: provider.resolve(artifact_id)
        for artifact_id in sorted(set(artifact_ids))
    }


class InMemoryArtifactVersions:
    """Current versions held in the process.

    Not a mock: this is the fixtures-first provider (spec §5 makes fixtures a
    first-class dev/CI surface), the same role `InMemoryPolicyProvider` plays for
    policies and `InMemoryWorkItemSink` for PM. The whole §8 sweep — comparison,
    finding synthesis, minting, dedup across sweeps — is developed and tested
    against it, so the behaviour the suite proves is the behaviour the live
    provider inherits.

    An artifact nobody `put` resolves as `not_found`, which is exactly what
    governance would say. Failure injection deliberately lives in the TESTS (a
    wrapper that answers `unavailable`, as `test_engine.DownProvider` does for
    policies) rather than here: a provider that can be told to break is a
    provider whose ordinary path is one flag away from being untested."""

    def __init__(self, versions: Mapping[str, ArtifactVersion] | None = None) -> None:
        self._versions: dict[str, ArtifactVersion] = dict(versions or {})

    def put(
        self, artifact_id: str, version_id: str, *, milestone: str | None = None
    ) -> ArtifactVersion:
        """Install (or REVISE) an artifact's current version. Returns what a
        later `resolve` will hand back, so a caller can assert on it directly.

        Calling it again with a new version id is how a test says "governance
        revised this artifact" — the event that makes every deliverable citing
        it stale."""
        current = ArtifactVersion(
            artifact_id=artifact_id, version_id=version_id, milestone=milestone
        )
        self._versions[artifact_id] = current
        return current

    def resolve(self, artifact_id: str) -> VersionResolution:
        current = self._versions.get(artifact_id)
        if current is None:
            return ArtifactVersionError(
                artifact_id=artifact_id,
                failure=VersionFailure.not_found,
                detail=f"no artifact {artifact_id!r} in governance",
            )
        return current


class GatewayArtifactVersions:
    """The live provider — governance over HTTP (see the module docstring for
    what is verified and what is assumed).

    Constructor mirrors the house vendored-client shape exactly
    (`policy_source.GatewayPolicyProvider`, and PM's/governance's own
    `HttpScopeClient`/`HttpMilestoneClient` before it): optional base URL
    defaulting to config, an injectable `httpx.Client` so tests stub the
    transport, and a timeout. No auth header — behind the platform trust gate the
    request rides the tailnet and there is no per-request secret (governance
    decision `35546152`)."""

    def __init__(
        self,
        governance_url: str | None = None,
        *,
        client: httpx.Client | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._governance_url = (governance_url or config.governance_url()).rstrip("/")
        self._http = thin_client.GuardedGetter(client=client, timeout=timeout)

    def resolve(self, artifact_id: str) -> VersionResolution:
        url = f"{self._governance_url}{ARTIFACT_PATH}"
        resp = self._http.get(url, params={"artifact_id": artifact_id})
        if isinstance(resp, thin_client.RequestFailed):
            log.warning(
                "artifact version resolution for %r failed at %s: %s",
                artifact_id,
                url,
                resp.detail,
            )
            return ArtifactVersionError(
                artifact_id=artifact_id,
                failure=VersionFailure.unavailable,
                detail=resp.detail,
            )
        if resp.status_code == httpx.codes.NOT_FOUND:
            # A 404 is AMBIGUOUS — "governance has no such artifact" and "this
            # ROUTE does not exist on this build" share a status code, and the
            # route is exactly the assumed part of this client. Conflating them
            # would turn a missing route into a fleet-wide "every artifact is
            # gone", which the sweep would report as every deliverable skipped:
            # silent, total, and indistinguishable from a quiet system. So the
            # route CONTRACT requires the no-artifact answer to identify itself
            # (`thin_client.identified_404`, the discipline every thin client
            # here shares); a bare framework 404 is UNAVAILABLE.
            if thin_client.identified_404(resp, "artifact_id", artifact_id):
                return ArtifactVersionError(
                    artifact_id=artifact_id,
                    failure=VersionFailure.not_found,
                    detail=f"governance has no artifact {artifact_id!r}",
                    status_code=resp.status_code,
                )
            return ArtifactVersionError(
                artifact_id=artifact_id,
                failure=VersionFailure.unavailable,
                detail=(
                    f"404 from {url} without an artifact-identifying body — "
                    "indistinguishable from a missing route; NOT treated as "
                    "'no such artifact'"
                ),
                status_code=resp.status_code,
            )
        if not resp.is_success:
            return ArtifactVersionError(
                artifact_id=artifact_id,
                failure=VersionFailure.unavailable,
                detail=f"governance returned {resp.status_code} for {url}",
                status_code=resp.status_code,
            )
        return _read_artifact_payload(artifact_id, resp)


def _read_artifact_payload(artifact_id: str, resp: httpx.Response) -> VersionResolution:
    """Map governance's artifact payload onto `ArtifactVersion`.

    Strict, for `policy_source._read_version_payload`'s reason: a 200 whose
    payload is not an artifact record is `malformed_response`, never a
    best-effort partial. A missing version id is the worst thing to paper over
    here — every finding must cite the exact version it compared against (§14),
    and a finding citing a guess is worse than no finding at all."""
    # The decode skeleton is classify.py's, shared with both parsers and the
    # policy client, so a hardening fix lands everywhere at once.
    payload, decode_failure = classify.decode_json_object(resp.content)
    if decode_failure is not None:
        failure, detail = decode_failure
        return ArtifactVersionError(
            artifact_id=artifact_id,
            failure=VersionFailure.malformed_response,
            detail=f"governance response: {failure.value} — {detail}",
            status_code=resp.status_code,
        )
    answered = classify.best_effort_str(payload, _ARTIFACT_ID_KEY)
    if answered != artifact_id:
        # The payload must answer the question that was ASKED. A body naming a
        # different artifact (a proxy serving a cached response, a route that
        # ignores its query parameter) would otherwise compare one deliverable's
        # recorded version against another artifact's current one — a finding
        # that cites two unrelated facts and reads as though it had compared
        # them.
        return ArtifactVersionError(
            artifact_id=artifact_id,
            failure=VersionFailure.malformed_response,
            detail=(
                f"artifact payload names {answered!r} at {_ARTIFACT_ID_KEY!r}, "
                f"but {artifact_id!r} was asked for"
            ),
            status_code=resp.status_code,
        )
    current = payload.get(_CURRENT_VERSION_KEY)
    if current is None:
        # VERIFIED-possible (module docstring): an artifact with no canonical
        # leaf. An ANSWER, and a different operator problem from a broken
        # response — so it gets its own failure value rather than being reported
        # as governance misbehaving.
        return ArtifactVersionError(
            artifact_id=artifact_id,
            failure=VersionFailure.no_current_version,
            detail=(
                f"governance holds artifact {artifact_id!r} but names no current "
                "version for it — nothing to compare a deliverable against"
            ),
            status_code=resp.status_code,
        )
    if not isinstance(current, Mapping):
        return ArtifactVersionError(
            artifact_id=artifact_id,
            failure=VersionFailure.malformed_response,
            detail=(f"artifact payload's {_CURRENT_VERSION_KEY!r} is not an object"),
            status_code=resp.status_code,
        )
    version_id = classify.best_effort_str(current, _VERSION_ID_KEY)
    if version_id is None:
        return ArtifactVersionError(
            artifact_id=artifact_id,
            failure=VersionFailure.malformed_response,
            detail=(
                f"artifact payload is missing or empty at "
                f"{_CURRENT_VERSION_KEY}.{_VERSION_ID_KEY}"
            ),
            status_code=resp.status_code,
        )
    return ArtifactVersion(
        artifact_id=artifact_id,
        version_id=version_id,
        # An unstamped version reports `milestone: null`, which is an ordinary
        # state (§8 works without stamps) — so a non-string or blank value is
        # simply absent rather than a reason to reject the whole read. The
        # same `best_effort_str` read as every identifying string here, so a
        # whitespace-padded stamp cannot disagree with the STRIPPED one
        # provenance validation recorded and fake a boundary move.
        milestone=classify.best_effort_str(current, _MILESTONE_KEY),
    )

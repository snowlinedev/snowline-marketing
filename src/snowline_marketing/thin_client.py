"""What the vendored thin HTTP clients share — the identified-404 check and
the guarded GET skeleton.

`policy_source.GatewayPolicyProvider` and `work_sink.PMWorkItemSink` are the
same house idiom (an assumed route on a sibling service, a typed-result
`submit`/`resolve` that never raises), and they inherit the same ambiguity
from it: a 404 can mean "the service refuses this exact request" or "this
ROUTE does not exist on this build" — and the route is precisely the assumed,
unverified part of each client. Both therefore hold the same discipline: a
REAL refusal must identify itself by echoing back the thing it refused (the
tenant asked about, the destination scope submitted), and a bare framework
404 is treated as transient/unavailable, which fails visibly instead of
turning a missing route into a fleet-wide silent match-none or dead-letter.

That check lives HERE, once, so the clients cannot drift on the one rule
their 404 handling depends on. A new module rather than `classify.py` because
classify's charter is the never-raises JSON/model classification skeleton —
it knows nothing of HTTP and should stay that way; this is the thin clients'
own shared vocabulary, and the next shared client rule belongs beside it.

`GuardedGetter` is the second shared rule: the lazy-client-plus-four-arm-
except GET skeleton that `policy_source.GatewayPolicyProvider` and
`artifact_versions.GatewayArtifactVersions` both ride, extracted so a
hardening fix to the exception arms or the client construction lands in one
place. `work_sink.PMWorkItemSink` deliberately does NOT use it: a POST's
failure taxonomy is different in kind — it must distinguish "provably never
sent" from "sent, fate unknown", because PM may have minted — while a GET
that failed anywhere is simply an answer we do not have.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import httpx

from snowline_marketing import classify


@dataclass(frozen=True)
class RequestFailed:
    """The request never produced a response — transport failure, malformed
    URL, or the lazy client construction itself failing. The caller maps this
    onto its OWN typed `unavailable` result; `detail` is the exception,
    stringified, which is what an operator debugging a config typo reads."""

    detail: str


class GuardedGetter:
    """The shared GET skeleton of the vendored read clients.

    One long-lived `httpx.Client` for the getter's lifetime, built lazily
    INSIDE the never-raises guard: construction itself can fail on a broken
    SSL_CERT_FILE (httpx builds the SSL context eagerly), and a one-shot
    request per call would re-handshake TCP on every sweep for no benefit.
    Callers are driven by single-threaded sweeps; nothing here is locked.

    The four except arms are the never-raises contract, and it takes all four:
    `InvalidURL` is not an `HTTPError` (it subclasses Exception directly); a
    base URL malformed enough that the request never reaches the transport
    (`http:/gov.example`, a missing scheme) surfaces as a bare `ValueError`
    from urllib; and the lazy construction raises `OSError` on a broken cert
    file. A config typo is precisely the failure an operator hits, so it is
    the one this must not crash on — it lands as a `RequestFailed`, never an
    escape."""

    def __init__(
        self, *, client: httpx.Client | None = None, timeout: float = 10.0
    ) -> None:
        self._client = client
        self._timeout = timeout

    def get(
        self, url: str, *, params: Mapping[str, str]
    ) -> httpx.Response | RequestFailed:
        try:
            if self._client is None:
                self._client = httpx.Client(timeout=self._timeout)
            return self._client.get(url, params=params, timeout=self._timeout)
        except (httpx.HTTPError, httpx.InvalidURL, ValueError, OSError) as exc:
            return RequestFailed(detail=str(exc))


def identified_404(response: httpx.Response, field: str, expected: str) -> bool:
    """Whether a 404 response IDENTIFIES ITSELF as a genuine refusal: a JSON
    object body whose `field` names exactly the thing the caller asked about
    (`expected`). Anything else — a non-JSON body, a framework's bare
    {"detail": "Not Found"}, a body answering some other question — is
    indistinguishable from the route not existing, and the caller must treat
    it as transient rather than as an answer."""
    payload, _decode_failure = classify.decode_json_object(response.content)
    return payload is not None and payload.get(field) == expected

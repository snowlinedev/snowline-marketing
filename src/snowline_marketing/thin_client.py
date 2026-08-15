"""What the vendored thin HTTP clients share — today, the identified-404 check.

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

That check lives HERE, once, so the two clients cannot drift on the one rule
their 404 handling depends on. A new module rather than `classify.py` because
classify's charter is the never-raises JSON/model classification skeleton —
it knows nothing of HTTP and should stay that way; this is the thin clients'
own shared vocabulary, and the next shared client rule belongs beside it.
"""

from __future__ import annotations

import httpx

from snowline_marketing import classify


def identified_404(response: httpx.Response, field: str, expected: str) -> bool:
    """Whether a 404 response IDENTIFIES ITSELF as a genuine refusal: a JSON
    object body whose `field` names exactly the thing the caller asked about
    (`expected`). Anything else — a non-JSON body, a framework's bare
    {"detail": "Not Found"}, a body answering some other question — is
    indistinguishable from the route not existing, and the caller must treat
    it as transient rather than as an answer."""
    payload, _decode_failure = classify.decode_json_object(response.content)
    return payload is not None and payload.get(field) == expected

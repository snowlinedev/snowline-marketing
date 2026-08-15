"""Where minted work goes — PM's work-item surface, behind a seam (spec §7).

Spec §7: follow-through is "minted through PM's surface (gateway), landing on
the canonical roadmap", and "no standalone GitHub marketing issues — GitHub
involvement stays PM mirroring". This module is that one call and nothing else:
a fully-rendered `MintRequest` in, a typed `SinkResult` out. It renders nothing
(that is `rendering.py`), decides nothing (that is `minting.py`), and touches no
ledger row — which is what lets the minting pass's convergence logic be tested
against a sink that cannot fail, and this client's status mapping be tested
against a stub transport with no ledger in sight.

`WorkItemSink` is the protocol, with the same two implementations every seam in
this plugin has (`policy_source.py`, `cursors.py`, `ledger.py`):
`InMemoryWorkItemSink` for fixtures-first development and tests, and
`PMWorkItemSink` for the live service.

**What was VERIFIED about PM's surface (read from snowline-pm, 2026-08-15):**

- `create_work_item(title, primary_scope, body=None, external_links=None,
  affects=None, work_kind="implementation", human_owned=False,
  recommended_model=None, milestone=None, origin="human_directed",
  spec_id=None)` — the MCP tool's real signature. The payload below mirrors
  those names exactly where it uses them at all.
- `origin` is one of `WORK_ORIGINS = ("human_directed", "ai_generated")`. There
  is no "plugin" origin, and an unknown value raises PM-side. Policy-minted
  work is therefore `ai_generated`: it was produced by an automated rule with
  no human directing this particular item, which is exactly what the flag
  distinguishes (PM uses it to pick the credential a mirrored issue is filed
  under).
- `human_owned` is a flag with the same meaning marketing wants: work that is
  the user's to do rather than an agent's to pick up. The policy's flag maps
  straight through.
- The destination is `primary_scope`, a SLUG, and it is a soft ref — PM
  validates its grammar and does not resolve it against the platform at
  capture time.
- `create_work_item` returns a dict whose `id` is the work item's UUID string.
  PM work items have no short-hash ref; `id` is the ref this plugin records.
- There is NO initiative/phase parameter on create. Placement into an
  initiative is a SEPARATE tool (`add_to_initiative(item_id, initiative,
  phase)`); a newly created item lands in PM's unpositioned placement queue.
- `WORK_KINDS = ("implementation", "planning")` — a closed, PM-owned
  vocabulary. This plugin sends none and takes PM's default rather than
  inventing a marketing kind in a plugin that must hold no
  organization-specific vocabulary (spec §1).
- There is NO musher-dispatch field on the work item or on `create_work_item`
  today: it is snowline-pm #65 (the per-item dispatch flag) with #66 (the
  provider endpoints) behind it. Both are open.
- PM exposes NO write-capable REST route. `create_work_item` is reachable only
  as an MCP tool (PM's app mounts `/mcp`), and the platform gateway proxies MCP
  tool traffic rather than routing REST between plugins.

**What is ASSUMED, and why it is exactly one thing.** Same posture as
`policy_source.GatewayPolicyProvider`, for the same verified reason (there is no
plugin-to-plugin REST convention in Snowline — the agent is the integration
runtime): the ROUTE is a single documented constant, `MINT_PATH`, and everything
else is derived from the verified surface above. When PM grows a plugin-facing
write route — or when this plugin speaks MCP through the gateway — a new
`WorkItemSink` lands beside these two and nothing above the protocol changes.

The assumption has a stated CONTRACT, because a wrong guess must fail visibly
rather than plausibly:

- The endpoint accepts the composite request (destination + placement) as ONE
  call. The plugin deliberately does not orchestrate `create_work_item` then
  `add_to_initiative` itself: two calls across a network boundary are not
  atomic, and a crash between them would leave a confirmed `created` ledger row
  pointing at an item stranded in PM's placement queue — a silent, per-item
  drift the ledger could never detect. One call, or the mint did not happen.
- A PERMANENT rejection identifies itself. A 404 whose JSON body names the
  destination scope it refused is "PM will not accept this request"; a bare 404
  is indistinguishable from this route not existing on this PM build and is
  treated as transient. Conflating them would turn a missing route into a
  fleet-wide dead-letter of perfectly good work — the same ambiguity, and the
  same discipline, as `policy_source`'s 404 split.

**The musher-dispatch flag rides the payload.** The plugin sets a flag; PM's
watcher routes to musher and the plugin never calls musher directly (spec §3/§7).
Until #65 lands there is no field to set, so `musher_dispatch` is carried as a
documented extension key. The compatibility posture is that PM ignores an
unknown field — and the honest note is that a strict tool schema would instead
REJECT it, which surfaces as a `failed` row naming the reason rather than as
silence. Either way the intent is never lost: `rendering.py` writes the dispatch
request into the minted item's provenance block, which is durable and readable
by a human whatever the API does with the key.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from snowline_marketing import classify, config, thin_client

log = logging.getLogger("snowline_marketing.work_sink")

# ASSUMED (see module docstring): the PM write that mints one work item on the
# canonical roadmap, including its placement. One constant, one contract.
MINT_PATH = "/marketing/work-items"

# VERIFIED: PM's work-item origin vocabulary is `("human_directed",
# "ai_generated")` and an unknown value raises PM-side. Policy-minted work is
# `ai_generated` — see the module docstring.
ORIGIN_AI_GENERATED = "ai_generated"

# VERIFIED: `create_work_item` returns the item's UUID string under this key.
_ITEM_ID_KEY = "id"

# The statuses that mean "this request will never be accepted as it stands".
# Everything else non-2xx is treated as transient, because re-delivery is a
# cheap retry while a `failed` row costs an operator a replay: 401/403 is a
# deployment fix away, 429 is a wait, 5xx is PM's problem, and a bare 404 is
# most likely this module's assumed route.
_PERMANENT_STATUSES = frozenset(
    {
        httpx.codes.BAD_REQUEST,
        httpx.codes.CONFLICT,
        httpx.codes.UNPROCESSABLE_ENTITY,
    }
)

# The transport failures that PROVABLY happened before anything was sent — no
# connection, no usable URL, a request this process could not even build. Those
# are the only ones safe to re-owe. Every OTHER httpx error (a read timeout
# above all) happens after the request was written, so PM may have minted and
# the answer is `SinkIndeterminate`, not `SinkUnavailable`. Listed explicitly
# rather than caught as a base class, because the default has to be the
# conservative one: a new httpx error type nobody classified must land on
# "hold the claim", never on "re-mint".
#
# Placement verified against httpx 0.28.1's hierarchy: `ConnectError`
# (NetworkError) and `ConnectTimeout` (TimeoutException) are both raised while
# ESTABLISHING the connection, and `PoolTimeout` (TimeoutException) is raised
# while WAITING for a connection from the pool — in all three cases no request
# was written, so re-owing cannot duplicate. `ReadTimeout`/`WriteTimeout`
# share `PoolTimeout`'s base class and are deliberately NOT here: they fire
# after (or during) the send.
_NEVER_SENT_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    httpx.UnsupportedProtocol,
    httpx.InvalidURL,
    httpx.LocalProtocolError,
)


@dataclass(frozen=True)
class MintRequest:
    """One fully-rendered work item, ready to submit (spec §7).

    Every string here is FINAL: templates are already rendered, the provenance
    block is already appended to `body`, and no implementation of this protocol
    may reinterpret them. That is deliberate — rendering is deterministic and
    belongs to one module (`rendering.py`), so two sinks cannot mint two
    different items from one match.

    The fields split three ways, and the split is the honest map of what is
    known about PM's surface (module docstring): `title`, `body`, `scope`,
    `human_owned` and `origin` mirror `create_work_item`'s verified parameters;
    `initiative`/`phase` are the destination PM takes through a SEPARATE tool
    today; `owner` and `musher_dispatch` have no PM parameter at all yet.
    Nothing is dropped for lacking a home — what the API cannot carry, the
    provenance block states in the item body."""

    tenant: str
    # PM's `primary_scope`: the destination scope slug from the policy entry.
    scope: str
    title: str
    # The rendered body INCLUDING §7's provenance block (`rendering.py`).
    body: str
    # PM's `human_owned`, straight through from the policy entry.
    human_owned: bool
    # The policy's musher-dispatch opt-in (spec §7). No PM field exists yet
    # (snowline-pm #65) — see the module docstring for the compatibility
    # posture and why the flag is also written into the body.
    musher_dispatch: bool
    # The delivery this mint belongs to, carried for provenance and for the
    # idempotency key PM does not offer yet: a mint whose response was lost is
    # ambiguous (see `SinkIndeterminate`), and a PM-side key on this exact
    # string is what would close that window. Sent so the contract is stated
    # now rather than retrofitted after the first duplicate.
    dedup_key: str
    policy_id: str
    policy_version_id: str
    event_id: str
    initiative: str | None = None
    phase: str | None = None
    # The rendered `owner_template` (spec §6's "ownership template"), when the
    # policy declares one. PM's create surface has no assignee parameter, so
    # this rides the payload as an extension key and appears in the provenance
    # block; it is never silently discarded.
    owner: str | None = None
    origin: str = ORIGIN_AI_GENERATED

    def payload(self) -> dict[str, Any]:
        """The request body: `create_work_item`'s verified parameter names for
        everything PM already takes, plus the documented extension keys for
        what it does not (see the module docstring).

        Built here rather than in the client so that the ONE thing a future
        MCP-speaking or REST-speaking sink must agree on — the wire shape —
        lives with the request it describes, and so a test can assert on it
        without a transport."""
        body: dict[str, Any] = {
            # VERIFIED parameter names.
            "title": self.title,
            "body": self.body,
            "primary_scope": self.scope,
            "human_owned": self.human_owned,
            "origin": self.origin,
            # EXTENSION keys: real destinations and real intent that PM's
            # create tool has no parameter for today.
            "initiative": self.initiative,
            "phase": self.phase,
            "owner": self.owner,
            "musher_dispatch": self.musher_dispatch,
            # Provenance / idempotency.
            "tenant": self.tenant,
            "dedup_key": self.dedup_key,
            "policy_id": self.policy_id,
            "policy_version_id": self.policy_version_id,
            "event_id": self.event_id,
        }
        # Nulls are dropped rather than sent: a strict schema is likelier to
        # accept an absent optional than an explicit null, and "the plugin did
        # not say" is the truthful encoding of a policy that declared nothing.
        return {key: value for key, value in body.items() if value is not None}


@dataclass(frozen=True)
class SinkCreated:
    """PM minted the item and named it. `item_ref` is what the ledger row
    records as `created_item_ref` (spec §4) — opaque here: PM says it is the
    work item's UUID string, and this plugin only has to be able to point at
    it."""

    item_ref: str
    detail: str | None = None


@dataclass(frozen=True)
class SinkRejected:
    """PM refused, permanently — the request will never be accepted as it
    stands (a bad destination scope, a payload PM's schema will not take).

    Terminal: the minting pass writes a `failed` row carrying `reason`, which is
    §11's dead-letter input. Never retried by re-delivery, because retrying a
    permanent refusal forever is how a pipeline hides a broken policy."""

    reason: str
    status_code: int | None = None


@dataclass(frozen=True)
class SinkUnavailable:
    """PM could not be reached AND the request provably never arrived.

    The narrow case, and narrow on purpose: a connect failure, a bad base URL, a
    request refused before it was sent. The minting pass releases its claim and
    the delivery re-owes, so re-delivery is the retry loop — no backoff timer
    lives at this layer (spec §11 owns retry policy)."""

    detail: str
    status_code: int | None = None


@dataclass(frozen=True)
class SinkIndeterminate:
    """The request may have reached PM, and we did not learn what it did.

    A read timeout after the body was sent, a 5xx from something that may sit in
    front of PM, a 2xx whose payload named no item. The distinction from
    `SinkUnavailable` is a TYPE and not a flag because the two demand OPPOSITE
    actions: an unavailable request is safe to re-owe, and an indeterminate one
    is not — releasing the claim would re-mint an item that may already exist,
    and that is a SILENT double-mint, which spec §7 forbids exactly as firmly as
    it forbids silent loss.

    So the minting pass HOLDS the claim: the row stays `claimed`, no work is
    lost, nothing is duplicated, and the row surfaces on §11's reconciliation
    list where a human (or, later, a PM-side idempotency key on `dedup_key`)
    settles it. Visible and stuck beats invisible and wrong."""

    detail: str
    status_code: int | None = None


# What one submit produced. A union rather than a result-with-flags, so a caller
# cannot handle three cases and silently fall through the fourth.
SinkResult = SinkCreated | SinkRejected | SinkUnavailable | SinkIndeterminate


class WorkItemSink(Protocol):
    """The only thing the minting layer knows about where work items go.

    `submit` NEVER raises: every failure is one of the typed results above,
    because the minting pass has a LEDGER ROW claimed when it calls this, and an
    exception escaping here would leave that claim held with no record of why
    (`policy_source.PolicyProvider` holds the same contract for the same
    reason)."""

    def submit(self, request: MintRequest) -> SinkResult: ...


def _deterministic_ref(request: MintRequest) -> str:
    """A stable fake item ref for `InMemoryWorkItemSink`.

    Derived from the delivery's identity rather than a counter, so the ref a
    fixtures run produces does not depend on how many mints preceded it: a test
    asserting on a ledger row's `created_item_ref` asserts on the same string
    whatever order the suite runs in, and a re-run of a capture reproduces the
    same refs."""
    identity = f"{request.tenant}\0{request.dedup_key}".encode()
    return f"mem-item-{hashlib.sha256(identity).hexdigest()[:12]}"


class InMemoryWorkItemSink:
    """A work-item sink held in the process — the fixtures-first sink (spec §5).

    Not a mock: this is the sink the whole deterministic core is developed and
    tested against, the way `InMemoryPolicyProvider` is the provider and
    `InMemoryDeliveryLedger` is the ledger. It records every request in order
    (`requests`) and hands back a deterministic ref, so an end-to-end run over
    the shipped capture proves "one item per matched delivery, across repeated
    passes" without PM existing.

    Failure injection is the `responder` and nothing else — ONE mechanism, so a
    test's intent is readable in one place. It is a plain callable over the
    request, which covers every case the minting pass has to survive: a fixed
    rejection, an unavailability that heals on the second call, an
    indeterminate response, or a decision that depends on the request. The
    request is recorded BEFORE the responder runs, so a test can assert on what
    was submitted even when the submission "failed"."""

    def __init__(
        self, responder: Callable[[MintRequest], SinkResult] | None = None
    ) -> None:
        self.requests: list[MintRequest] = []
        self._responder = responder

    def submit(self, request: MintRequest) -> SinkResult:
        self.requests.append(request)
        if self._responder is None:
            return SinkCreated(item_ref=_deterministic_ref(request))
        return self._responder(request)

    @property
    def created_refs(self) -> list[str]:
        """The refs this sink would have handed back, in submission order — the
        readable form of "what did this run mint?"."""
        return [_deterministic_ref(request) for request in self.requests]

    @property
    def dedup_keys(self) -> list[str]:
        """Every submitted delivery key, in order. The direct assertion for
        "minted exactly once": a key appearing twice is a double mint, whatever
        the ledger says about it."""
        return [request.dedup_key for request in self.requests]


class PMWorkItemSink:
    """The live sink — PM over HTTP (see the module docstring for what is
    verified and what is assumed).

    Constructor mirrors the house vendored-client shape exactly
    (`policy_source.GatewayPolicyProvider`, PM's own `HttpScopeClient`): an
    optional base URL defaulting to config, an injectable `httpx.Client` so
    tests stub the transport, and a timeout. No auth header — behind the
    platform trust gate the request rides the tailnet and there is no
    per-request secret (governance decision `35546152`)."""

    def __init__(
        self,
        pm_url: str | None = None,
        *,
        client: httpx.Client | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._pm_url = (pm_url or config.pm_url()).rstrip("/")
        self._url = f"{self._pm_url}{MINT_PATH}"
        self._client = client
        self._timeout = timeout

    def submit(self, request: MintRequest) -> SinkResult:
        """Mint one work item. Never raises (see `WorkItemSink`).

        The mapping is where this client earns its keep, and every arm exists
        because getting it wrong has a specific cost: a permanent failure
        treated as transient re-owes forever, a transient failure treated as
        permanent dead-letters good work, and an AMBIGUOUS failure treated as
        either one duplicates or loses an item."""
        try:
            if self._client is None:
                # One long-lived client, built lazily INSIDE the never-raises
                # guard: httpx builds its SSL context in the constructor, so a
                # broken SSL_CERT_FILE raises OSError here rather than at
                # request time (same reasoning, and the same four-arm except,
                # as `policy_source.GatewayPolicyProvider.resolve`).
                self._client = httpx.Client(timeout=self._timeout)
            response = self._client.post(
                self._url, json=request.payload(), timeout=self._timeout
            )
        except _NEVER_SENT_ERRORS as exc:
            # The request PROVABLY never arrived: no connection was
            # established, or the URL never produced one. Safe to re-owe.
            log.warning("mint for %r could not reach PM: %s", request.dedup_key, exc)
            return SinkUnavailable(detail=f"{type(exc).__name__}: {exc}")
        except (ValueError, OSError) as exc:
            # A malformed base URL surfacing as a bare ValueError out of
            # urllib, or the lazy client construction failing on a broken cert
            # file. Config errors, and the request never left this process.
            log.warning("mint for %r failed before sending: %s", request.dedup_key, exc)
            return SinkUnavailable(detail=f"{type(exc).__name__}: {exc}")
        except httpx.HTTPError as exc:
            # Everything else transport-shaped — a read timeout above all —
            # happens AFTER the request was written. PM may have minted the
            # item and we will never know from here.
            log.warning(
                "mint for %r failed after sending (fate unknown): %s",
                request.dedup_key,
                exc,
            )
            return SinkIndeterminate(detail=f"{type(exc).__name__}: {exc}")
        return self._read_response(request, response)

    def _read_response(
        self, request: MintRequest, response: httpx.Response
    ) -> SinkResult:
        if response.is_success:
            payload, decode_failure = classify.decode_json_object(response.content)
            if decode_failure is not None:
                failure, detail = decode_failure
                # A 2xx we cannot read is the ambiguous case, not a rejection:
                # PM most likely minted something, and this plugin cannot name
                # it. Holding the claim keeps the item findable by a human
                # instead of minting a second one.
                return SinkIndeterminate(
                    detail=(
                        f"PM answered {response.status_code} with a body that is "
                        f"not JSON ({failure.value}: {detail}) — the item may "
                        "exist and cannot be named"
                    ),
                    status_code=response.status_code,
                )
            item_ref = payload.get(_ITEM_ID_KEY)
            if not isinstance(item_ref, str) or not item_ref.strip():
                return SinkIndeterminate(
                    detail=(
                        f"PM answered {response.status_code} without a string "
                        f"{_ITEM_ID_KEY!r} — an item was probably created and "
                        "cannot be pointed at; refusing to invent a ref"
                    ),
                    status_code=response.status_code,
                )
            return SinkCreated(item_ref=item_ref.strip())
        if response.status_code == httpx.codes.NOT_FOUND:
            # A 404 is AMBIGUOUS in exactly the way `policy_source`'s is: "PM
            # refuses this destination" and "this ROUTE does not exist on this
            # PM build" share a status code, and the route is the assumed part
            # of this client. So the contract requires a real rejection to
            # identify itself by naming the scope it refused
            # (`thin_client.identified_404`, the discipline both thin clients
            # share); a bare framework 404 is transient, which fails visibly
            # (nothing mints, rows re-owe) instead of dead-lettering every
            # mint in the fleet.
            if thin_client.identified_404(response, "primary_scope", request.scope):
                # Decoded again only for the refusal's own words — the shared
                # helper answers the yes/no, and the body is small.
                payload, _decode_failure = classify.decode_json_object(response.content)
                detail = payload.get("detail") if payload is not None else None
                return SinkRejected(
                    reason=(
                        f"PM rejected destination scope {request.scope!r}: "
                        f"{detail or 'not found'}"
                    ),
                    status_code=response.status_code,
                )
            return SinkUnavailable(
                detail=(
                    f"404 from {self._url} without a scope-identifying body — "
                    "indistinguishable from a missing route; NOT treated as a "
                    "rejection"
                ),
                status_code=response.status_code,
            )
        if response.status_code in _PERMANENT_STATUSES:
            return SinkRejected(
                reason=(
                    f"PM returned {response.status_code} for {self._url}: "
                    f"{response.text[:500]}"
                ),
                status_code=response.status_code,
            )
        if response.status_code >= httpx.codes.INTERNAL_SERVER_ERROR:
            # A 5xx can be raised before OR after PM committed the item (a
            # crash in a response handler, a proxy losing the answer), so it is
            # the ambiguous case rather than the retryable one.
            return SinkIndeterminate(
                detail=f"PM returned {response.status_code} for {self._url}",
                status_code=response.status_code,
            )
        return SinkUnavailable(
            detail=f"PM returned {response.status_code} for {self._url}",
            status_code=response.status_code,
        )

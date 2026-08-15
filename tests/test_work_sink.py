"""The work-item sink: the payload PM gets, and how its answers are read.

Stubs PM's HTTP with an `httpx.MockTransport`, so no PM runs and no DB is
needed. The contract under test on every path: `submit` NEVER raises (the
minting pass holds a claimed ledger row when it calls this), and the FOUR
answers stay distinct — because each one commands a different, irreversible
thing of that claim. Created confirms it, rejected dead-letters it, unavailable
releases it back into re-delivery, and indeterminate HOLDS it. Collapsing the
last two is how a plugin either duplicates a work item or loses one silently.
"""

from __future__ import annotations

import httpx
import pytest
from conftest import TENANT

from snowline_marketing.work_sink import (
    MINT_PATH,
    ORIGIN_AI_GENERATED,
    InMemoryWorkItemSink,
    MintRequest,
    PMWorkItemSink,
    SinkCreated,
    SinkIndeterminate,
    SinkRejected,
    SinkUnavailable,
)

PM_URL = "http://pm.test"
ITEM_ID = "7c9f2b1e-3a44-4f7d-9a55-1b2c3d4e5f60"


def request(**overrides) -> MintRequest:
    values = {
        "tenant": TENANT,
        "scope": "turtlesedge/marketing",
        "title": "Regenerate the App Store listing for v1.4",
        "body": "Body plus provenance.",
        "human_owned": False,
        "musher_dispatch": False,
        "dedup_key": f"p:{TENANT}:listing-regeneration:v1.4",
        "policy_id": "listing-regeneration",
        "policy_version_id": "gv-7f3a91c4",
        "event_id": "pm-evt-0000110",
    }
    values.update(overrides)
    return MintRequest(**values)


def _sink(handler) -> PMWorkItemSink:
    return PMWorkItemSink(
        PM_URL, client=httpx.Client(transport=httpx.MockTransport(handler))
    )


def _answers(status: int, payload: dict | None = None, text: str | None = None):
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == MINT_PATH
        if text is not None:
            return httpx.Response(status, text=text)
        return httpx.Response(status, json=payload if payload is not None else {})

    return handler


# --- the payload -------------------------------------------------------------


def test_the_payload_mirrors_pms_verified_parameter_names():
    payload = request(
        human_owned=True, initiative="app-store", phase="release"
    ).payload()
    # VERIFIED from snowline-pm's `create_work_item` signature.
    assert payload["title"]
    assert payload["body"]
    assert payload["primary_scope"] == "turtlesedge/marketing"
    assert payload["human_owned"] is True
    # PM's WORK_ORIGINS is ("human_directed", "ai_generated") — policy-minted
    # work is the second, and there is no third value to invent.
    assert payload["origin"] == ORIGIN_AI_GENERATED
    # EXTENSION keys: real destinations PM's create tool has no parameter for.
    assert payload["initiative"] == "app-store"
    assert payload["phase"] == "release"
    # PM's work-kind vocabulary is closed and PM-owned; the plugin sends none
    # and takes PM's default rather than inventing a marketing kind.
    assert "work_kind" not in payload


def test_the_payload_carries_the_dispatch_flag_and_the_delivery_key():
    payload = request(musher_dispatch=True).payload()
    # No PM field exists yet (snowline-pm #65): carried so the intent reaches
    # PM the day it does, and stated in the item body meanwhile.
    assert payload["musher_dispatch"] is True
    # The idempotency handle a PM-side key would use to close the ambiguous
    # timeout window.
    assert payload["dedup_key"] == f"p:{TENANT}:listing-regeneration:v1.4"
    assert payload["policy_version_id"] == "gv-7f3a91c4"
    assert payload["event_id"] == "pm-evt-0000110"


def test_absent_optionals_are_omitted_rather_than_sent_as_null():
    payload = request().payload()
    assert "initiative" not in payload
    assert "phase" not in payload
    assert "owner" not in payload


# --- the in-memory sink ------------------------------------------------------


def test_the_in_memory_sink_records_and_hands_back_deterministic_refs():
    sink = InMemoryWorkItemSink()
    first = sink.submit(request())
    second = InMemoryWorkItemSink().submit(request())
    assert isinstance(first, SinkCreated)
    # Derived from the delivery's identity, not a counter: the same delivery
    # produces the same ref whatever ran before it.
    assert first == second
    assert sink.dedup_keys == [f"p:{TENANT}:listing-regeneration:v1.4"]


def test_the_in_memory_sink_records_the_request_even_when_it_fails():
    # A test asserting on WHAT was submitted must work on the failure paths too
    # — that is where the interesting assertions are.
    sink = InMemoryWorkItemSink(lambda req: SinkRejected(reason="no such scope"))
    assert isinstance(sink.submit(request()), SinkRejected)
    assert len(sink.requests) == 1


def test_the_responder_is_the_failure_injection():
    calls: list[str] = []

    def responder(req: MintRequest):
        calls.append(req.dedup_key)
        if len(calls) == 1:
            return SinkUnavailable(detail="PM restarting")
        return SinkCreated(item_ref=ITEM_ID)

    sink = InMemoryWorkItemSink(responder)
    assert isinstance(sink.submit(request()), SinkUnavailable)
    assert isinstance(sink.submit(request()), SinkCreated)


# --- the live client's status mapping ----------------------------------------


def test_a_created_item_is_read_from_pms_verified_id_key():
    sink = _sink(_answers(201, {"id": ITEM_ID, "state": "captured"}))
    result = sink.submit(request())
    assert isinstance(result, SinkCreated)
    assert result.item_ref == ITEM_ID


def test_a_success_without_an_item_id_is_indeterminate_not_created():
    # PM probably minted and this plugin cannot name it. Inventing a ref would
    # put an unfindable string on a `created` ledger row; calling it a rejection
    # would dead-letter work that exists.
    sink = _sink(_answers(200, {"state": "captured"}))
    result = sink.submit(request())
    assert isinstance(result, SinkIndeterminate)
    assert "id" in result.detail


def test_a_success_that_is_not_json_is_indeterminate():
    sink = _sink(_answers(200, text="<html>proxy</html>"))
    assert isinstance(sink.submit(request()), SinkIndeterminate)


@pytest.mark.parametrize("status", [400, 409, 422])
def test_a_refused_payload_is_a_permanent_rejection(status):
    sink = _sink(_answers(status, {"detail": "unknown field 'musher_dispatch'"}))
    result = sink.submit(request())
    assert isinstance(result, SinkRejected)
    assert result.status_code == status
    # The compatibility posture stated in work_sink's docstring: if PM's schema
    # is strict, the unknown extension key surfaces as a NAMED rejection on a
    # failed row — never as silence.
    assert "musher_dispatch" in result.reason


def test_a_404_naming_the_refused_scope_is_a_rejection():
    sink = _sink(
        _answers(
            404,
            {"primary_scope": "turtlesedge/marketing", "detail": "no such scope"},
        )
    )
    result = sink.submit(request())
    assert isinstance(result, SinkRejected)
    assert "turtlesedge/marketing" in result.reason


def test_a_bare_404_is_transient_because_the_route_is_the_assumed_part():
    # The same discipline as `policy_source`'s 404 split: a missing route must
    # not dead-letter every mint in the fleet.
    sink = _sink(_answers(404, {"detail": "Not Found"}))
    result = sink.submit(request())
    assert isinstance(result, SinkUnavailable)
    assert "missing route" in result.detail


@pytest.mark.parametrize("status", [401, 403, 429])
def test_a_gate_or_a_wait_is_transient(status):
    # A deployment fix or a pause away from succeeding, and answered before PM
    # ever looked at the payload: release the claim, re-owe, try next pass.
    sink = _sink(_answers(status, {"detail": "later"}))
    result = sink.submit(request())
    assert isinstance(result, SinkUnavailable)
    assert result.status_code == status


def test_a_5xx_is_indeterminate_because_pm_may_have_committed():
    sink = _sink(_answers(500, {"detail": "boom"}))
    result = sink.submit(request())
    assert isinstance(result, SinkIndeterminate)
    assert result.status_code == 500


def test_a_connect_failure_never_reached_pm_and_is_transient():
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=req)

    result = _sink(handler).submit(request())
    assert isinstance(result, SinkUnavailable)
    assert "ConnectError" in result.detail


def test_a_pool_timeout_never_sent_anything_and_is_transient():
    # PoolTimeout fires while WAITING for a connection from the pool (verified
    # against httpx 0.28.1's hierarchy) — no request was written, so it is the
    # provably-never-sent case: release the claim, re-owe, try next pass.
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.PoolTimeout("pool exhausted", request=req)

    result = _sink(handler).submit(request())
    assert isinstance(result, SinkUnavailable)
    assert "PoolTimeout" in result.detail


def test_a_read_timeout_is_indeterminate_because_the_request_was_sent():
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=req)

    result = _sink(handler).submit(request())
    assert isinstance(result, SinkIndeterminate)
    assert "ReadTimeout" in result.detail


def test_a_malformed_base_url_is_a_typed_result_not_a_crash():
    # The config typo is the failure an operator actually hits; it must land as
    # a result like everything else (same four-arm guard as policy_source).
    result = PMWorkItemSink("http:/pm.example").submit(request())
    assert isinstance(result, SinkUnavailable)

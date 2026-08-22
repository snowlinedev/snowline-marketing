"""The deliverable provenance payload contract (spec §8).

What is pinned here is the classification, because everything the watch does
hangs off which of three answers this module gives: a readable declaration
(deliverable rows), nothing declared (quarantine, reason `provenance_missing`),
or a declaration that could not be read (quarantine, reason
`provenance_malformed`, detail naming the defect). The third answer is the one
worth the most tests — a malformed payload silently treated as ABSENT would file
a producer bug under "the operator forgot" and send someone looking for a human
who did nothing wrong.

Envelopes are built through `conftest.make_envelope` and parsed through the real
`events.parse_envelope`, so every payload under test is one an intake pass could
actually hand the watch.
"""

from __future__ import annotations

import pytest
from conftest import SCOPE, make_envelope

from snowline_marketing.events import EventEnvelope, EventType, parse_envelope
from snowline_marketing.provenance import (
    PROVENANCE_DETAILS_KEY,
    DeliverableProvenance,
    MissingProvenance,
    ProvenanceReason,
    parse_provenance,
)

FULL_DECLARATION = {
    "schema_version": 1,
    "deliverables": [
        {
            "channel": "app_store",
            "deliverable_class": "store_listing",
            "source_artifact_versions": [
                {
                    "artifact_id": "b964d217",
                    "version_id": "av-3c81f9d2",
                    "milestone": "v1.4",
                },
                {"artifact_id": "9f21ac04", "version_id": "av-77b0e315"},
            ],
            "external_url": "https://apps.apple.com/app/turtletracks/id6470000000",
        },
        {
            "channel": "app_store",
            "deliverable_class": "screenshot_set",
            "source_artifact_versions": [
                {"artifact_id": "9f21ac04", "version_id": "av-77b0e315"}
            ],
        },
    ],
}


def completion(details: dict | None = None) -> EventEnvelope:
    """A valid `item_completed` envelope carrying `details`."""
    raw = make_envelope(
        EventType.item_completed,
        payload={"scope": SCOPE, "details": details or {}},
    )
    parsed = parse_envelope(raw)
    assert isinstance(parsed, EventEnvelope), getattr(parsed, "detail", "")
    return parsed


def declaring(document: object) -> EventEnvelope:
    return completion({PROVENANCE_DETAILS_KEY: document})


def missing(document: object) -> MissingProvenance:
    parsed = parse_provenance(declaring(document))
    assert isinstance(parsed, MissingProvenance), parsed
    return parsed


# --- a readable declaration ---------------------------------------------------


def test_a_full_declaration_carries_every_fact_the_ledger_stores():
    parsed = parse_provenance(declaring(FULL_DECLARATION))
    assert isinstance(parsed, DeliverableProvenance)
    listing, screenshots = parsed.deliverables
    assert listing.identity == ("app_store", "store_listing")
    assert screenshots.identity == ("app_store", "screenshot_set")
    # Multiple deliverables per completion is the normal case, not an edge one:
    # one completion produced a listing update AND a screenshot set (§12).
    assert len(parsed.deliverables) == 2
    assert [
        (version.artifact_id, version.version_id, version.milestone)
        for version in listing.source_artifact_versions
    ] == [("b964d217", "av-3c81f9d2", "v1.4"), ("9f21ac04", "av-77b0e315", None)]
    assert listing.external_url.endswith("id6470000000")
    # Both optional facts are genuinely optional: §13 says the sweep works
    # without milestone stamps, and a deliverable with no public URL yet is a
    # true record rather than a quarantine case.
    assert screenshots.external_url is None
    assert screenshots.source_artifact_versions[0].milestone is None


def test_a_declaration_is_frozen():
    parsed = parse_provenance(declaring(FULL_DECLARATION))
    with pytest.raises(Exception):
        parsed.deliverables[0].channel = "website"


# --- nothing declared: spec §8's quarantine case ------------------------------


def test_a_completion_with_no_declaration_is_absent_not_malformed():
    parsed = parse_provenance(completion({"title": "Prepare the v1.4 announcement"}))
    assert isinstance(parsed, MissingProvenance)
    assert parsed.reason is ProvenanceReason.absent
    assert parsed.is_absent
    assert PROVENANCE_DETAILS_KEY in parsed.detail


def test_a_completion_with_no_details_at_all_is_absent():
    parsed = parse_provenance(completion())
    assert isinstance(parsed, MissingProvenance)
    assert parsed.is_absent


def test_an_explicit_null_declaration_is_absent_and_says_so():
    # A producer that writes the key with no value has declared nothing. Treated
    # as absent — the resolvable case — but the detail says the key was there, so
    # an operator is not left hunting for a producer that never wrote it.
    parsed = missing(None)
    assert parsed.reason is ProvenanceReason.absent
    assert "present and null" in parsed.detail


# --- a declaration that could not be read -------------------------------------


def test_a_non_object_declaration_is_malformed_not_absent():
    for document in (["app_store"], "store_listing", 7):
        parsed = missing(document)
        assert parsed.reason is ProvenanceReason.not_an_object
        assert not parsed.is_absent
        assert "must be a JSON object" in parsed.detail


def test_a_version_skewed_declaration_quarantines_rather_than_being_guessed_at():
    parsed = missing({**FULL_DECLARATION, "schema_version": 2})
    assert parsed.reason is ProvenanceReason.invalid_document
    assert "schema_version" in parsed.detail


def test_an_unknown_field_is_a_defect_not_a_silent_drop():
    # `extra="forbid"`, for `events.py`'s reason: a producer that grew a field we
    # silently dropped is exactly the drift quarantine exists to surface.
    document = {
        **FULL_DECLARATION,
        "deliverables": [
            {**FULL_DECLARATION["deliverables"][0], "locale": "en-GB"},
        ],
    }
    parsed = missing(document)
    assert parsed.reason is ProvenanceReason.invalid_document
    assert "locale" in parsed.detail


def test_a_declaration_of_no_deliverables_is_malformed():
    # A completion claiming to have produced nothing is not the same as one that
    # declared nothing: the first is a broken payload, the second is spec §8's
    # ordinary quarantine case, and they need different reasons.
    parsed = missing({"schema_version": 1, "deliverables": []})
    assert parsed.reason is ProvenanceReason.invalid_document
    assert "deliverables" in parsed.detail


def test_a_deliverable_with_no_source_versions_is_malformed():
    # The row would be one §8's sweep can never evaluate — recorded,
    # unfalsifiable, and silently exempt from the staleness the ledger exists to
    # make visible.
    parsed = missing(
        {
            "schema_version": 1,
            "deliverables": [
                {
                    "channel": "website",
                    "deliverable_class": "positioning_copy",
                    "source_artifact_versions": [],
                }
            ],
        }
    )
    assert parsed.reason is ProvenanceReason.invalid_document
    assert "source_artifact_versions" in parsed.detail


def test_one_artifact_cited_at_two_versions_is_malformed():
    # The sweep's only question is "is the version this deliverable reflects
    # still current?", and a deliverable citing A at both v1 and v2 has no single
    # answer. Refused at the payload, and keyed out at the row (`deliverables`).
    parsed = missing(
        {
            "schema_version": 1,
            "deliverables": [
                {
                    "channel": "app_store",
                    "deliverable_class": "store_listing",
                    "source_artifact_versions": [
                        {"artifact_id": "b964d217", "version_id": "av-1"},
                        {"artifact_id": "b964d217", "version_id": "av-2"},
                    ],
                }
            ],
        }
    )
    assert parsed.reason is ProvenanceReason.invalid_document
    assert "b964d217" in parsed.detail


def test_two_deliverables_sharing_a_channel_and_class_are_malformed():
    # That pair under the producing item IS the ledger's natural key, so the
    # second would silently overwrite the first. The producer says which it means
    # by giving them distinct classes.
    one = FULL_DECLARATION["deliverables"][0]
    parsed = missing({"schema_version": 1, "deliverables": [one, one]})
    assert parsed.reason is ProvenanceReason.invalid_document
    assert "app_store/store_listing" in parsed.detail


def test_blank_identifiers_are_refused():
    # `NonEmptyStr`, for `events.py`'s reason: a whitespace artifact id would
    # pass a bare `str` and then key an association row nothing can join to.
    parsed = missing(
        {
            "schema_version": 1,
            "deliverables": [
                {
                    "channel": "  ",
                    "deliverable_class": "store_listing",
                    "source_artifact_versions": [
                        {"artifact_id": "b964d217", "version_id": "av-1"}
                    ],
                }
            ],
        }
    )
    assert parsed.reason is ProvenanceReason.invalid_document
    assert "channel" in parsed.detail


def test_a_json_encoded_declaration_is_not_quietly_decoded():
    # A producer that JSON-encoded the sub-document into a string is not carrying
    # an object. Decoding it here would hide exactly the drift `extra="forbid"`
    # and the schema version exist to surface.
    import json

    parsed = missing(json.dumps(FULL_DECLARATION))
    assert parsed.reason is ProvenanceReason.not_an_object


def test_classification_never_raises_whatever_the_payload_says():
    # The house never-raises contract (`events.parse_envelope`,
    # `policies.parse_policy_set`): malformed input is an expected input CLASS,
    # and the intake loop calls the watch outside any try.
    for document in (
        {},
        {"schema_version": 1},
        {"deliverables": [{}]},
        {"schema_version": 1, "deliverables": [{"channel": None}]},
        {"schema_version": "one", "deliverables": "all of them"},
        True,
        [[["nested"]]],
    ):
        assert isinstance(missing(document), MissingProvenance)

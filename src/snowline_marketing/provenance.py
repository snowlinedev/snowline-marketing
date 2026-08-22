"""The deliverable provenance payload — what a completion DECLARES it produced
(spec §8).

Spec §8 settles provenance by **watch-and-quarantine**: completing a
marketing-minted item is watched on the same PM event stream, "a completion
carrying a provenance payload (channel, deliverable class, source artifact
versions, URL) upserts the deliverable ledger; a completion without one lands
in quarantine". This module owns the SHAPE of that payload and the never-raises
classification of it. Where it rides, what joins a completion to a minted item,
and which store the answer lands in are `watch.py`'s; what the rows look like is
`deliverables.py`'s and `quarantine.py`'s.

**Where it rides: `payload.details.deliverable_provenance`.** `events.py` keeps
`details` deliberately unschema'd — "per-type variation lives in the free-form
`details` map, which no predicate reads... leaving room for facts a consequence
template wants to quote" — and this is exactly such a fact: it is carried BY a
completion, it is not something any §6 predicate may select on, and PM's outbox
must be able to grow it without the envelope schema changing. One documented key
under `details`, versioned INSIDE (`schema_version`), so a producer that grows
the sub-document bumps its own version and lands here deliberately rather than
half-arriving — the same evolution knob the envelope has, one level down.

**Three answers, never an exception.** `parse_provenance` returns a
`DeliverableProvenance` or a `MissingProvenance`, mirroring
`events.parse_envelope` and `policies.parse_policy_set`. The distinction the
watch acts on is INSIDE `MissingProvenance.reason`: `absent` is spec §8's
"completion without one" (quarantine, reason `provenance_missing`), and every
other reason is a payload that TRIED to declare provenance and could not be
read (quarantine, reason `provenance_malformed`, with a detail naming the
defect). What must never happen is the third thing: a malformed payload treated
as absent, which would file a producer bug under "the operator forgot" and send
someone looking for a human who did nothing wrong.

**Multiple deliverables per completion are the normal case.** One completion
may have produced a listing update AND a screenshot set (§12's App Store
adapter pushes both), so the document is a LIST and each entry is one
deliverable instance — one row in the deliverable ledger.

Constraints this module encodes, and why:

- **Every deliverable names at least one source artifact version.** The whole
  point of the ledger is §8's staleness sweep, which compares "source artifact
  current version vs the version recorded in deliverable provenance". A
  deliverable citing no source version is a row the sweep can never evaluate —
  recorded, unfalsifiable, and silently exempt from the staleness it exists to
  make visible. So it is a malformed payload, not an empty one.
- **One version per artifact per deliverable.** A deliverable claiming to
  reflect artifact A at both v1 and v2 cannot answer the sweep's only question
  ("is the version this deliverable reflects still current?"), so the ambiguity
  is refused at the payload rather than stored and guessed at later. It is also
  what lets the ledger's association row key on the artifact id
  (`deliverables.py`).
- **No two deliverables in one payload share (channel, deliverable class).**
  That pair, under the producing item, IS the deliverable ledger's natural key
  (`deliverables.py`) — two entries claiming it are one completion asking for
  two different answers in one row, and last-one-wins would silently drop the
  other. The producer says which it means by giving them distinct classes.
- **The milestone stamp is optional.** Spec §13: Snowline#141 (milestone stamp
  on ArtifactVersion) is a dependency whose absence the sweep survives ("works
  without stamps (version compare only), better with"), so requiring one here
  would make today's honest payload malformed.
- **`external_url` is optional.** A deliverable the operator cannot open is
  worse to read but is still a true record of what was produced, and quarantine
  is for provenance that is MISSING — pushing an otherwise-complete declaration
  into it because a screenshot set has no public URL yet would teach operators
  that the quarantine queue is noise.
- **`produced_at` is NOT in the payload.** The completion event's `occurred_at`
  is when the producing work was completed, which is the fact the ledger records
  and the sweep compares against release boundaries. A producer-declared
  timestamp would be an unverifiable second answer to a question the envelope
  already answers, and the sub-document is versioned if that ever has to change.
- Every model is frozen and `extra="forbid"`, for the same reasons as
  `events.py` and `policies.py`: a provenance declaration is a record of
  something that already happened, and a field silently dropped is a source
  version the sweep will never compare.
"""

from __future__ import annotations

import enum
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)

from snowline_marketing import classify
from snowline_marketing.events import EventEnvelope

# The `details` key the sub-document rides under (see the module docstring).
# A constant because it is the contract between PM's producer and this plugin,
# and because §11's operator surface has to be able to say where to put it.
PROVENANCE_DETAILS_KEY = "deliverable_provenance"

# The sub-document's own version, independent of the envelope's and of the
# policy body's. Bumped only when THIS shape changes incompatibly; a payload
# declaring any other version quarantines rather than being best-effort read,
# so a producer that got ahead of this deploy finds out from the quarantine
# queue instead of from a deliverable row missing half its facts.
PROVENANCE_SCHEMA_VERSION = 1

# Same rule as `events.NonEmptyStr` / `policies.NonEmptyStr`: identifiers and
# URLs are refs, not prose. An all-whitespace artifact id would pass a bare
# `str` and then key an association row nothing can ever join to.
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class _Model(BaseModel):
    """Shared model config: frozen (a declaration of what was produced is a
    record of something that already happened) and `extra="forbid"` (a field
    silently dropped is a source version the §8 sweep will never compare)."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceArtifactVersion(_Model):
    """One governance artifact version a deliverable was produced FROM.

    `milestone` is Snowline#141's release stamp, carried when the producer knows
    it — spec §8's "the milestone stamp gives the release boundary" — and
    optional because the sweep is specified to work without it (§13)."""

    artifact_id: NonEmptyStr
    version_id: NonEmptyStr
    milestone: NonEmptyStr | None = None


class DeclaredDeliverable(_Model):
    """One deliverable instance, as the completion declares it — one row in the
    deliverable provenance ledger (spec §4).

    `channel` and `deliverable_class` are OPEN strings, not enums, for the same
    reason `policies.PolicyEntry` keeps them open: channels grow with §12's
    adapters and deliverable classes are tenant vocabulary. A closed enum here
    would make every new tenant class a plugin release."""

    channel: NonEmptyStr
    deliverable_class: NonEmptyStr
    # Tuples, not lists: the model is frozen, and a mutable default would leave
    # exactly one editable collection on an otherwise-immutable record.
    source_artifact_versions: Annotated[
        tuple[SourceArtifactVersion, ...], Field(min_length=1)
    ]
    external_url: NonEmptyStr | None = None

    @property
    def identity(self) -> tuple[str, str]:
        """The pair that identifies this deliverable under its producing item —
        the deliverable ledger's natural key minus the tenant and the item ref
        (`deliverables.py`)."""
        return (self.channel, self.deliverable_class)

    @model_validator(mode="after")
    def _one_version_per_artifact(self) -> DeclaredDeliverable:
        counts = Counter(
            version.artifact_id for version in self.source_artifact_versions
        )
        duplicated = sorted(name for name, count in counts.items() if count > 1)
        if duplicated:
            raise ValueError(
                "a deliverable may cite each source artifact at exactly one "
                f"version; {', '.join(repr(a) for a in duplicated)} appears "
                "more than once — the staleness sweep would have no single "
                "version to compare"
            )
        return self


class DeliverableProvenance(_Model):
    """What one completion declares it produced (spec §8)."""

    # A Literal, not an int compared later: a producer speaking another version
    # fails validation at the same seam as any other shape violation, so version
    # skew reaches quarantine through one path (same posture as
    # `events.EventEnvelope.schema_version`).
    schema_version: Literal[1]
    deliverables: Annotated[tuple[DeclaredDeliverable, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def _deliverable_identities_are_distinct(self) -> DeliverableProvenance:
        counts = Counter(deliverable.identity for deliverable in self.deliverables)
        duplicated = sorted(name for name, count in counts.items() if count > 1)
        if duplicated:
            pairs = ", ".join(f"{channel}/{klass}" for channel, klass in duplicated)
            raise ValueError(
                f"two deliverables share the same channel/class identity ({pairs}) "
                "— that pair is one row of the deliverable ledger under the "
                "producing item, so the second would silently overwrite the first"
            )
        return self


class ProvenanceReason(enum.StrEnum):
    """Why a completion carries no readable provenance — the operator-visible
    reason on the quarantine row (spec §4/§11).

    Coarse by design, exactly like `events.MalformedReason`: the actionable
    specifics live in `MissingProvenance.detail`, which names the offending
    field. What the WATCH branches on is only whether the reason is `absent`,
    because that is the difference between "nobody declared anything" and "a
    declaration was attempted and is broken" — two different people to talk to
    (`quarantine.QuarantineReason`)."""

    # The completion declared no provenance at all — spec §8's "a completion
    # without one". Not an error on anyone's part yet: it is the case the
    # quarantine's resolve verb exists for.
    absent = "absent"
    # The key is there and is not a JSON object (a list, a string, a number) —
    # a producer writing something else under a reserved key.
    not_an_object = "not_an_object"
    # An object that does not satisfy the sub-document contract.
    invalid_document = "invalid_document"


@dataclass(frozen=True)
class MissingProvenance:
    """A completion whose provenance could not be read.

    A RESULT, not an error (the house posture: `events.MalformedEnvelope`,
    `policies.MalformedPolicySet`, `rendering.RenderFailure`): the watch turns
    it into a quarantine row and the pass carries on. The raw payload is NOT
    carried here — the quarantine store keeps the whole completion event
    (`quarantine.py`), which is strictly more than this could hold and is what
    the resolve verb reads."""

    reason: ProvenanceReason
    detail: str

    @property
    def is_absent(self) -> bool:
        """Whether nothing was declared at all, as opposed to a declaration
        that could not be read. The one distinction the watch branches on."""
        return self.reason is ProvenanceReason.absent


# What reading one completion's provenance produced: a declaration, or the
# reason there is none. A union rather than an optional-with-a-reason field, so
# a caller cannot use one where the other belongs.
ParsedProvenance = DeliverableProvenance | MissingProvenance


def parse_provenance(envelope: EventEnvelope) -> ParsedProvenance:
    """Read `envelope`'s deliverable provenance declaration (spec §8).

    Never raises. Takes the whole envelope rather than the sub-document because
    the KEY is part of this module's contract — a caller that had to reach into
    `payload.details` itself would be free to spell it differently, and the one
    place a producer and this plugin have to agree would be spread across
    callers.

    Text is NOT decoded here: a producer that JSON-encoded the sub-document into
    a string is not carrying an object, and quietly decoding it would hide the
    drift `extra="forbid"` and the schema version exist to surface. That is why
    this does not ride `classify.decode_json_object` — the envelope arrived as
    JSON and its `details` values are already decoded, so anything but a mapping
    here is a producer bug, not an encoding one."""
    details = envelope.payload.details
    raw = details.get(PROVENANCE_DETAILS_KEY)
    if raw is None:
        return MissingProvenance(
            reason=ProvenanceReason.absent,
            detail=(
                f"completion carries no {PROVENANCE_DETAILS_KEY!r} declaration"
                + (
                    " (the key is present and null)"
                    if PROVENANCE_DETAILS_KEY in details
                    else ""
                )
                + " — attach one to resolve this row (spec §8)"
            ),
        )
    if not isinstance(raw, Mapping):
        return MissingProvenance(
            reason=ProvenanceReason.not_an_object,
            detail=(
                f"payload.details.{PROVENANCE_DETAILS_KEY} must be a JSON object, "
                f"got {type(raw).__name__}"
            ),
        )
    try:
        return DeliverableProvenance.model_validate(dict(raw))
    except ValidationError as exc:
        return MissingProvenance(
            reason=ProvenanceReason.invalid_document,
            detail=(
                f"payload.details.{PROVENANCE_DETAILS_KEY}: "
                # Scalars quoted, like `policies.parse_policy_set` and unlike
                # `events.parse_envelope`: a provenance payload is attached by a
                # person or an agent finishing the work, so the operator's
                # question is "which of my values is wrong?" rather than "which
                # field did the producer break?".
                + classify.compact_errors(exc, quote_scalars=True)
            ),
        )

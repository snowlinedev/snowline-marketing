"""The staleness sweep — deliverable rows in, staleness findings out (spec §8).

Spec §8: "The staleness sweep compares, per channel/deliverable class: source
artifact current version vs the version recorded in deliverable provenance (the
milestone stamp from Snowline#141 gives the release boundary)... Findings mint
(deduplicated) staleness items through the same policy machinery; a finding
whose deliverable is already covered by an open minted item does not
double-file."

This module is that sweep. `deliverables.py` holds what was recorded,
`artifact_versions.py` says what is current, and everything below the comparison
— which policy fires, what it mints, where it lands, whether it mints at all —
belongs to the machinery that already exists.

**Findings are SYNTHETIC EVENTS through the same policy machinery.** The sweep
mints nothing itself. It synthesizes `semantic_signal` envelopes and drives them
through the standard intake + evaluation + minting composition
(`minting.drive_intake` over a `MintingEvaluationHandler`), so a TENANT POLICY
decides what a staleness finding produces: which consequence, which destination
scope/initiative/phase, which templates, which mode, whether it is
approval-gated. That is spec §1's rule held structurally — "no organization-
specific marketing rules in plugin code" — and it is the reason `semantic_signal`
exists as the one open-vocabulary event type in a closed vocabulary (`events.py`).
A sweep that called PM directly would be a second minting path with its own dedup,
its own provenance block and its own audit trail, and §14's criteria would then be
true of one of them.

**The signal vocabulary, and why there are two signals per finding.** Every
finding carries `deliverable-stale` — the stable kind string, the thing a tenant
writes when it wants staleness routed somewhere at all — plus a class-qualified
refinement, `deliverable-stale:<deliverable class>`, so screenshot/asset classes
can be routed to a `screenshot_review` consequence while listings go to a
`review_sweep`. The qualifier is MECHANICAL: it is the tenant's own deliverable
class (an open vocabulary by §6's schema and by the ledger's columns) with a
documented prefix, so this module names no class of its own and a tenant can
invent one tomorrow without a code change. Predicates are globs, so
`deliverable-stale:*` routes every class and `deliverable-stale:screenshot_set`
routes exactly one.

The consequence of carrying both, stated plainly because it is a tenant-visible
trade: a policy selecting the bare `deliverable-stale` matches EVERY finding,
including the class-qualified ones, so a tenant running both a general policy and
a screenshot policy gets two items for a stale screenshot set — one per matched
entry, exactly as any two overlapping entries already behave (`engine.evaluate`
emits one consequence per match). That is the price of the bare signal existing,
and the bare signal has to exist: without it `signals: ["deliverable-stale"]` —
the obvious thing an operator writes first — would match nothing at all, which is
the silent match-none §6 forbids. A tenant wanting mutually exclusive routing
qualifies every entry by class.

**Deterministic synthetic event ids are the dedup backbone.** A finding's event
id is derived — `sha256`, not Python's salted `hash` — from the deliverable's
natural key plus the CURRENT version set it was compared against
(`finding_event_id`). Everything §8's "does not double-file" asks for then falls
out of machinery that already exists, with no staleness-specific dedup anywhere:

- Re-sweeping an UNCHANGED stale state re-synthesizes the SAME event id, so the
  tenant's dedup-key template (the §4 default is `{tenant}:{policy_id}:
  {event_id}`) renders the same key, the delivery ledger's unique row is already
  `created`, and the delivery reports `deduplicated`. One open item per finding,
  however often the sweep runs.
- A NEW current version — the artifact was revised AGAIN while the item was
  still open — is a NEW event id and therefore a NEW mint. That is correct, not
  a leak: the deliverable is now stale against DIFFERENT facts, the open item
  cites versions that are themselves out of date, and §14 requires a finding to
  cite the exact versions it compared. Collapsing the two would leave an
  operator working from a citation the sweep knows is wrong.
- A tenant whose dedup template omits `{event_id}` is choosing coarser
  collapsing on purpose (say `{tenant}:{policy_id}:{entity_id}` — one open item
  per producing item, ever). The sweep does not second-guess it; the id is what
  makes the DEFAULT correct.

**The cursor is per-sweep and in-memory, deliberately.** The synthetic source is
regenerated from scratch every sweep, so it is not a durable stream and its
positions are not resume tokens: position `000003` means "the fourth finding of
this sweep", and two sweeps' fourth findings are unrelated. A PERSISTED cursor
over it would therefore silently skip the first N findings of every later sweep —
the exact "silently skipped forever" failure `sources.py` validates fixture
prefixes to prevent. The dedup that matters is the delivery ledger's, which is
durable, keyed on facts rather than on positions, and already proven. So the
sweep builds its own `InMemoryCursorStore` and does not accept one: a seam whose
only correct argument is the default is a footgun, not a seam. `source_key`
still names the sweep (`staleness:<tenant>`), because `IntakeResult` reports it
and an operator reading a stopped pass wants to know which loop stopped.

**A deliverable is compared WHOLE or not at all.** If any artifact a row cites
could not be resolved, the row is skipped and reported (`SkippedDeliverable`) —
no finding, no mint, nothing partial. Two reasons, and both bite. An unavailable
governance must never read as "not stale" (drift hidden) OR as "stale" (work
minted against evidence nobody has), which is `artifact_versions.py`'s whole
posture; and a finding synthesized from a PARTIAL current-version set would
carry an id derived from facts that are about to change, so the sweep after
recovery would mint a second item for the same staleness. Skipping is visible,
convergent, and costs one sweep's delay.

**What this module never does.** It never calls walkthrough-mcp or captures
anything: §8 delegates asset capture to the asset plugin and leaves the
marketing plugin "only tracking staleness and minting review work", so a stale
screenshot set produces a FINDING and the class-qualified signal that lets a
tenant route it — nothing more. It never writes a deliverable row (it takes the
read-only `DeliverableInventory` protocol, so it structurally cannot). It owns
no scheduler (spec §10: "the plugin owns no scheduler") — this is a library
callable, driven by tests today and by an operator surface or a driver later,
and like every other pass here it is deliberately NOT wired into the app
lifespan (`MARKETING_ENABLED`, spec §2, gates the live loops).

**Why the scope is a parameter.** A synthetic envelope must name a
`payload.scope`: routing is isolation-safe by contract (§14) and the engine
refuses an envelope without one. The deliverable ledger records no scope — its
row is about a producing ITEM and a channel, and the producing item's scope
belongs to PM, not to this plugin — so the sweep is TOLD which project scope its
findings are raised on rather than inventing one from a policy destination it
would have to re-resolve, or guessing from the tenant slug. Told is
configuration; guessed is an organization-specific rule in plugin code.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from snowline_marketing.artifact_versions import (
    ArtifactVersion,
    ArtifactVersionError,
    ArtifactVersionProvider,
    VersionFailure,
    VersionResolution,
    resolve_all,
)
from snowline_marketing.cursors import InMemoryCursorStore
from snowline_marketing.db import utc_now
from snowline_marketing.deliverables import (
    DeliverableProvenanceLedger,
    DeliverableRecord,
)
from snowline_marketing.engine import EvaluationStalled
from snowline_marketing.events import (
    SCHEMA_VERSION,
    EntityKind,
    EntityRef,
    EventEnvelope,
    EventPayload,
    EventType,
    MalformedEnvelope,
)
from snowline_marketing.ledger import MintingLedger
from snowline_marketing.minting import (
    IntakeMintResult,
    MintingEvaluationHandler,
    MintOutcome,
    MintPassReport,
    drive_intake,
)
from snowline_marketing.policy_cache import PolicyCacheStore
from snowline_marketing.policy_source import PolicyProvider
from snowline_marketing.sources import RawEvent
from snowline_marketing.work_sink import WorkItemSink

log = logging.getLogger("snowline_marketing.staleness")

# The stable staleness signal — the one a tenant policy selects to route
# staleness at all. Carried on EVERY finding (see the module docstring on why
# the bare signal has to exist alongside the class-qualified one).
STALE_SIGNAL = "deliverable-stale"

# How the class-qualified refinement is spelled. The deliverable class is TENANT
# vocabulary (`policies.PolicyEntry.deliverable_classes` is deliberately an open
# string list, and the ledger's column is too), so this module contributes the
# separator and nothing else — no class name appears in plugin code.
CLASS_SIGNAL_SEPARATOR = ":"

# Synthetic event ids are namespaced so they are unmistakable in a ledger row, a
# minted item's provenance block, or a log line: nothing that came from PM's
# outbox looks like this.
FINDING_ID_PREFIX = "mkt-stale-"

# How much of the digest the id carries. 16 hex characters is 64 bits: the
# collision risk across every finding this plugin will ever synthesize is
# nowhere near the risk of any other failure in the system, and a full 64-char
# digest would make the ledger key — which an operator reads and quotes —
# unreadable for nothing.
_FINDING_ID_DIGEST_LENGTH = 16

# The cursor row this sweep's source would name (it never persists one — see
# the module docstring). Per tenant, because a sweep is per tenant.
SOURCE_KEY_PREFIX = "staleness:"

# How list-valued details render. One separator for every such key, so a
# template author learns it once (the same choice `rendering.py` makes).
_LIST_SEPARATOR = ", "
# How one artifact's before/after reads inside `details.comparison`.
_COMPARISON_SEPARATOR = "; "


def class_signal(deliverable_class: str) -> str:
    """The class-qualified staleness signal for one deliverable class — what a
    tenant writes in `predicates.signals` to route this class specifically (or
    globs, `deliverable-stale:*`, to route every class)."""
    return f"{STALE_SIGNAL}{CLASS_SIGNAL_SEPARATOR}{deliverable_class}"


def finding_event_id(
    *,
    tenant: str,
    item_ref: str,
    channel: str,
    deliverable_class: str,
    current_versions: Iterable[tuple[str, str]],
) -> str:
    """The deterministic event id for one finding — the dedup backbone (module
    docstring).

    Derived from the deliverable's natural key plus the CURRENT (artifact id,
    version id) pairs it was compared against, sorted, so the id is a function
    of the finding's FACTS and of nothing else: not of sweep order, not of the
    clock, not of which process ran the sweep. Two sweeps of an unchanged stale
    state produce the same id and therefore the same ledger row; a further
    revision of any cited artifact produces a different id and therefore a fresh
    mint, because the deliverable is now stale against different facts.

    `sha256`, never Python's `hash`: the builtin is salted per process, so a
    dedup key built from it would change on every restart and quietly re-mint
    everything. The fields are joined with NUL, which cannot occur in any of
    them, so no combination of ids can be re-partitioned into another finding's.

    The current versions of artifacts that are NOT stale are included too, and
    that changes nothing about when the id moves: a fresh artifact's current
    version IS its recorded version, so it cannot change without the artifact
    becoming stale. Including them keeps the digest a function of the whole
    comparison the finding cites, which is what §14 says a finding is about."""
    pairs = sorted(
        f"{artifact_id}={version_id}" for artifact_id, version_id in current_versions
    )
    identity = "\0".join((tenant, item_ref, channel, deliverable_class, *pairs))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"{FINDING_ID_PREFIX}{digest[:_FINDING_ID_DIGEST_LENGTH]}"


class DeliverableInventory(Protocol):
    """What the sweep needs from the deliverable provenance ledger: one tenant's
    rows, and nothing it could write with.

    A narrow protocol for `watch.MintedItemLookup`'s reason — the sweep must be
    unable to upsert a deliverable, and taking the narrowest surface is how that
    is stated rather than promised. Both `DeliverableProvenanceLedger` and
    `InMemoryDeliverables` satisfy it, so the sweep runs with no database in
    sight (spec §5's fixtures-first posture)."""

    def list_for_tenant(
        self, tenant: str, *, limit: int | None = None
    ) -> list[DeliverableRecord]: ...


@dataclass(frozen=True)
class VersionComparison:
    """One source artifact, as recorded versus as it stands now.

    Both sides are kept whole, always — including the milestone stamps and
    including the comparisons that came out EQUAL — because §14 requires a
    finding to cite "the exact source artifact versions and recorded deliverable
    provenance they compared", and a comparison that kept only the differences
    would be citing its conclusion rather than its evidence."""

    artifact_id: str
    recorded_version_id: str
    current_version_id: str
    recorded_milestone: str | None = None
    current_milestone: str | None = None

    @property
    def is_stale(self) -> bool:
        """Version inequality — v1's only trigger (spec §8, Snowline#141's
        posture). The milestone stamps refine the STORY a finding tells and are
        deliberately not a trigger of their own, so the sweep works unchanged
        against artifacts nobody has stamped yet."""
        return self.recorded_version_id != self.current_version_id

    @property
    def milestone_moved(self) -> bool:
        """Whether the release boundary moved too — "reflects the v1-stamped
        feature list, but a v2-stamped version now exists" (spec §8). Reported
        in a finding's details when true; never a trigger."""
        return (
            self.recorded_milestone != self.current_milestone
            and self.current_milestone is not None
        )


@dataclass(frozen=True)
class StalenessFinding:
    """One deliverable that no longer reflects its sources.

    Carries the whole `DeliverableRecord` and the whole comparison set, for
    `engine.PendingConsequence`'s reason: every caller wants a different
    projection (the envelope's details, an operator listing, a test's
    assertion), and a hand-copied subset would fall behind the day either schema
    grew.

    `observed_at` is when the SWEEP looked, not when anything changed: governance
    is polled (§5), so the moment a revision landed is not a fact this plugin
    has. It is the envelope's `occurred_at` and deliberately NOT part of the
    event id — a finding's identity is its facts, so a clock that moved between
    sweeps must not mint a second item."""

    deliverable: DeliverableRecord
    comparisons: tuple[VersionComparison, ...]
    scope: str
    observed_at: datetime

    @property
    def identity(self) -> tuple[str, str, str, str]:
        """The deliverable's natural key — what this finding is ABOUT."""
        return self.deliverable.identity

    @property
    def stale(self) -> tuple[VersionComparison, ...]:
        """The comparisons that came out unequal — the finding's cause."""
        return tuple(
            comparison for comparison in self.comparisons if comparison.is_stale
        )

    @property
    def event_id(self) -> str:
        """This finding's deterministic synthetic event id (`finding_event_id`)."""
        record = self.deliverable
        return finding_event_id(
            tenant=record.tenant,
            item_ref=record.item_ref,
            channel=record.channel,
            deliverable_class=record.deliverable_class,
            current_versions=(
                (comparison.artifact_id, comparison.current_version_id)
                for comparison in self.comparisons
            ),
        )

    @property
    def signals(self) -> tuple[str, ...]:
        """The kind signal plus its class-qualified refinement, in that order
        (module docstring). De-duplicated defensively: they can only coincide if
        a deliverable class rendered the bare signal string back, and a policy
        matching one signal twice is meaningless either way."""
        return tuple(
            dict.fromkeys(
                (STALE_SIGNAL, class_signal(self.deliverable.deliverable_class))
            )
        )

    @property
    def ref(self) -> str:
        """A human locator for the operator-facing half of the intake report —
        the deliverable this finding is about, spelled the way an operator reads
        a deliverable ledger row."""
        record = self.deliverable
        return (
            f"staleness:{record.tenant}:{record.item_ref}/{record.channel}/"
            f"{record.deliverable_class}"
        )

    @property
    def details(self) -> dict[str, str]:
        """The comparison FACTS, for `payload.details` — §14's citation.

        Every value is TEXT, because that is what a `{details.<key>}` template
        renders (`rendering.py`) and what a minted body must read as. The keys
        are stable and documented: a tenant's body template quotes them, so
        renaming one is a breaking change to every artifact that uses it.

        Absent facts are OMITTED rather than rendered empty — a body reading
        "external URL:" with nothing after it teaches a reader to distrust the
        block, and a template quoting a key this finding does not carry fails
        the mint loudly (`rendering.RenderFailure`), which is the honest
        signal."""
        record = self.deliverable
        details: dict[str, str] = {
            "deliverable_channel": record.channel,
            "deliverable_class": record.deliverable_class,
            "deliverable_event_id": record.event_id,
            "deliverable_produced_at": record.produced_at.isoformat(),
            # The artifacts whose versions moved — the finding's cause, on its
            # own, for a title that has no room for the full comparison.
            "stale_artifacts": _LIST_SEPARATOR.join(
                comparison.artifact_id for comparison in self.stale
            ),
            # Both sides of the whole compared set, verbatim (§14).
            "recorded_versions": _versions_text(
                (comparison.artifact_id, comparison.recorded_version_id)
                for comparison in self.comparisons
            ),
            "current_versions": _versions_text(
                (comparison.artifact_id, comparison.current_version_id)
                for comparison in self.comparisons
            ),
            # The one line a body template usually wants: what moved, from what,
            # to what.
            "comparison": _COMPARISON_SEPARATOR.join(
                f"{comparison.artifact_id}: recorded {comparison.recorded_version_id} "
                f"→ current {comparison.current_version_id}"
                for comparison in self.stale
            ),
            "observed_at": self.observed_at.isoformat(),
        }
        if record.external_url:
            details["deliverable_external_url"] = record.external_url
        recorded_milestones = _versions_text(
            (comparison.artifact_id, comparison.recorded_milestone)
            for comparison in self.comparisons
            if comparison.recorded_milestone
        )
        if recorded_milestones:
            details["recorded_milestones"] = recorded_milestones
        current_milestones = _versions_text(
            (comparison.artifact_id, comparison.current_milestone)
            for comparison in self.comparisons
            if comparison.current_milestone
        )
        if current_milestones:
            details["current_milestones"] = current_milestones
        moved = _LIST_SEPARATOR.join(
            comparison.artifact_id
            for comparison in self.comparisons
            if comparison.milestone_moved
        )
        if moved:
            # The Snowline#141 refinement, named separately so a template can
            # say "a v2-stamped version now exists" without re-deriving it.
            details["milestone_moved_artifacts"] = moved
        return details

    @property
    def envelope(self) -> EventEnvelope:
        """This finding as a `semantic_signal` envelope — the synthetic event the
        policy machinery consumes.

        The SUBJECT is the producing work item: the deliverable ledger keys on
        it (`deliverables.py`), a minted item's provenance block then points an
        operator at the work that produced the stale deliverable, and
        `semantic_signal` accepts a work-item subject by validation
        (`events.py`).

        `payload.milestone` is deliberately left unset even when the recorded
        versions carry stamps: a deliverable cites SEVERAL artifacts, each with
        its own stamp and its own current stamp, so any single value there would
        be one of four defensible answers presented as the answer — and
        `milestone` is a predicate field, so a wrong one silently mis-routes.
        The stamps ride `details`, where they are all visible and none is
        privileged."""
        record = self.deliverable
        return EventEnvelope(
            schema_version=SCHEMA_VERSION,
            event_id=self.event_id,
            event_type=EventType.semantic_signal,
            tenant=record.tenant,
            occurred_at=self.observed_at,
            subject=EntityRef(kind=EntityKind.work_item, id=record.item_ref),
            payload=EventPayload(
                scope=self.scope,
                signals=self.signals,
                details=self.details,
            ),
        )


def _versions_text(pairs: Iterable[tuple[str, str | None]]) -> str:
    """`artifact=value` pairs, joined — the shape both version lists and both
    milestone lists render in, so a body quoting two of them reads consistently.
    Ordered as the comparisons are (by artifact id), so the same finding always
    renders the same string."""
    return _LIST_SEPARATOR.join(
        f"{artifact_id}={value}" for artifact_id, value in pairs if value is not None
    )


@dataclass(frozen=True)
class SkippedDeliverable:
    """A deliverable the sweep refused to judge, because at least one artifact
    it cites could not be resolved.

    Compared WHOLE or not at all (module docstring): no finding was synthesized,
    nothing was minted, and nothing partial was recorded. `unresolved` carries
    the typed failures so the report says which artifacts, and whether retrying
    will help (`artifact_versions.VersionFailure`)."""

    deliverable: DeliverableRecord
    unresolved: tuple[ArtifactVersionError, ...]

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return self.deliverable.identity

    @property
    def detail(self) -> str:
        """One operator-facing sentence naming every artifact that blocked this
        deliverable and why."""
        reasons = _LIST_SEPARATOR.join(
            f"{error.artifact_id} ({error.failure.value})" for error in self.unresolved
        )
        return (
            f"deliverable {self.deliverable.channel}/"
            f"{self.deliverable.deliverable_class} of item "
            f"{self.deliverable.item_ref!r} was not compared: {reasons} — no "
            "finding synthesized, nothing minted"
        )


@dataclass(frozen=True)
class SweepReport:
    """What one sweep did, for one tenant.

    `drive` is `minting.drive_intake`'s own result, kept WHOLE rather than
    flattened, exactly as `watch.IntakeWatchResult` keeps it: the mint half's
    story (how far the synthetic stream got, whether the policies resolved,
    whether PM was reachable) is unchanged by the sweep, and re-exporting its
    fields would be a second copy to keep in step. The delegating properties
    exist so a caller that only wants "did the pass run?" need not know which
    half owns the answer."""

    tenant: str
    scope: str
    # Every deliverable row read, in comparison order.
    examined: tuple[DeliverableRecord, ...]
    # One resolution per DISTINCT artifact cited, keyed by artifact id — the
    # sweep's whole conversation with governance, kept so a report can say what
    # was asked as well as what was answered.
    resolutions: Mapping[str, VersionResolution]
    findings: tuple[StalenessFinding, ...]
    skipped: tuple[SkippedDeliverable, ...]
    drive: IntakeMintResult

    @property
    def resolved(self) -> tuple[ArtifactVersion, ...]:
        """The artifacts governance answered for, in id order."""
        return tuple(
            resolution
            for _, resolution in sorted(self.resolutions.items())
            if isinstance(resolution, ArtifactVersion)
        )

    @property
    def unresolved(self) -> tuple[ArtifactVersionError, ...]:
        """The artifacts that stalled this sweep, in id order — the visible half
        of "an unavailable governance never reads as not-stale"."""
        return tuple(
            resolution
            for _, resolution in sorted(self.resolutions.items())
            if isinstance(resolution, ArtifactVersionError)
        )

    @property
    def intake(self):
        return self.drive.intake

    @property
    def stall(self) -> EvaluationStalled | None:
        return self.drive.stall

    @property
    def mint(self) -> MintPassReport:
        return self.drive.mint

    @property
    def unavailable(self) -> MintOutcome | None:
        return self.drive.unavailable

    @property
    def minted(self) -> tuple[MintOutcome, ...]:
        """The findings this sweep turned into PM items. A finding already
        covered by an open item is absent from here and reports `deduplicated`
        on its delivery — §8's "does not double-file", visible."""
        return self.mint.minted

    @property
    def ok(self) -> bool:
        """The whole sweep completed and nothing needs a human: the drive is ok
        (`minting.IntakeMintResult.ok`) and every artifact resolved.

        An unresolved artifact is not a crash, but it means some deliverables
        were not judged this sweep — which is precisely the fact a caller must
        not be able to miss by reading a green flag."""
        return self.drive.ok and not self.unresolved


class SyntheticFindingSource:
    """An `EventSource` over one sweep's findings.

    The seam that lets the sweep reuse the intake loop verbatim (`sources.py`:
    "`EventSource` is the only seam the loop knows"), rather than reaching past
    it into `evaluate`. Everything the loop does for a real event — parse the
    envelope, hand it to the handler, ack it — then happens for a finding too,
    so a finding and an outbox event travel identical code.

    The body is serialized JSON, not the model, on purpose: the loop's contract
    is that a source hands over raw bodies which `events.parse_envelope`
    classifies, and round-tripping through it proves each synthesized envelope
    is a legal one. Positions are zero-padded ordinals — lexicographically
    monotone, as the `EventSource` contract requires — and mean nothing beyond
    this sweep (see the module docstring on why no cursor is persisted)."""

    def __init__(self, tenant: str, findings: Sequence[StalenessFinding]) -> None:
        self.source_key = f"{SOURCE_KEY_PREFIX}{tenant}"
        self._findings = tuple(findings)

    def read(self, *, after: str | None = None):
        for index, finding in enumerate(self._findings):
            position = f"{index:06d}"
            if after is not None and position <= after:
                continue
            yield RawEvent(
                position=position,
                body=finding.envelope.model_dump_json(),
                ref=finding.ref,
            )


class SyntheticEnvelopeError(RuntimeError):
    """A synthesized envelope did not survive its own round trip.

    Unreachable by construction: `StalenessFinding.envelope` builds a validated
    `EventEnvelope`, and `model_dump_json` of a valid envelope re-parses. It
    exists because the alternative is worse than a crash — the intake loop's
    malformed path REPORTS and ACKS PAST (`intake.py`), which is right for a
    producer's bad event and wrong here: a malformed synthetic event is a bug in
    this module, and silently sailing past it would drop a real finding while
    the sweep reported success."""


def _refuse_malformed(malformed: MalformedEnvelope) -> None:
    raise SyntheticEnvelopeError(
        f"synthesized staleness envelope at {malformed.ref or malformed.position} "
        f"did not re-parse ({malformed.reason.value}): {malformed.detail} — this "
        "is a bug in staleness.py, not a producer's malformed event"
    )


def compare_deliverable(
    record: DeliverableRecord,
    resolutions: Mapping[str, VersionResolution],
    *,
    scope: str,
    observed_at: datetime,
) -> StalenessFinding | SkippedDeliverable | None:
    """Compare one deliverable against the resolved current versions.

    Three answers, and the type says which: a `StalenessFinding` (at least one
    cited artifact has moved), a `SkippedDeliverable` (at least one cited
    artifact could not be resolved — compared whole or not at all, see the
    module docstring), or None (every cited version is still current, which is
    the system working and produces nothing at all).

    The unresolved check comes FIRST and covers the whole row: a deliverable
    with one moved artifact and one unresolvable artifact is skipped, not
    reported stale, because the finding's id — and therefore its dedup — would
    be derived from a current-version set that is still unknown."""
    comparisons: list[VersionComparison] = []
    unresolved: list[ArtifactVersionError] = []
    for version in record.source_versions:
        resolution = resolutions.get(version.artifact_id)
        if resolution is None or isinstance(resolution, ArtifactVersionError):
            unresolved.append(
                resolution
                if isinstance(resolution, ArtifactVersionError)
                # Unreachable while the caller resolves every cited artifact
                # (`sweep_staleness` does); stated rather than assumed, because
                # a missing resolution silently treated as "fine" is the one
                # failure that would make the sweep lie.
                else ArtifactVersionError(
                    artifact_id=version.artifact_id,
                    failure=VersionFailure.unavailable,
                    detail=(
                        f"no resolution was attempted for artifact "
                        f"{version.artifact_id!r}"
                    ),
                )
            )
            continue
        comparisons.append(
            VersionComparison(
                artifact_id=version.artifact_id,
                recorded_version_id=version.version_id,
                current_version_id=resolution.version_id,
                recorded_milestone=version.milestone,
                current_milestone=resolution.milestone,
            )
        )
    if unresolved:
        return SkippedDeliverable(deliverable=record, unresolved=tuple(unresolved))
    if not any(comparison.is_stale for comparison in comparisons):
        return None
    return StalenessFinding(
        deliverable=record,
        comparisons=tuple(comparisons),
        scope=scope,
        observed_at=observed_at,
    )


def sweep_staleness(
    tenant: str,
    *,
    scope: str,
    versions: ArtifactVersionProvider,
    provider: PolicyProvider,
    sink: WorkItemSink,
    deliverables: DeliverableInventory | None = None,
    ledger: MintingLedger | None = None,
    cache: PolicyCacheStore | None = None,
    clock: Callable[[], datetime] | None = None,
) -> SweepReport:
    """Sweep one tenant's deliverables for staleness and mint what its policies
    say (spec §8) — the composition an operator surface or a scheduled driver
    calls, and the one tests drive end to end.

    Four steps, in this order:

    1. read the tenant's deliverable rows and put them in natural-key order, so
       the sweep's findings — and therefore its synthetic stream — are the same
       sequence for the same rows whatever order a store returned them in;
    2. resolve each DISTINCT cited artifact exactly once
       (`artifact_versions.resolve_all`);
    3. compare per deliverable (`compare_deliverable`): a finding, a skip, or
       nothing;
    4. drive the findings through the STANDARD composition — a
       `MintingEvaluationHandler` over a `SyntheticFindingSource` — so tenant
       policies decide everything a finding produces.

    Nothing is minted for a tenant whose policies could not be resolved: the
    handler stalls exactly as it does for a real event (`engine.py`), the pass
    stops, and the report carries the stall. Nothing is lost either — the
    findings are recomputed from the ledger next sweep, and the ones that DID
    mint dedup on their deterministic ids.

    `clock` is the injectable time source for each finding's `observed_at` (the
    envelopes' `occurred_at`); it is deliberately not part of any event id, so a
    sweep at a different time mints nothing new."""
    deliverables = (
        deliverables if deliverables is not None else DeliverableProvenanceLedger()
    )
    now = clock() if clock is not None else utc_now()

    records = sorted(deliverables.list_for_tenant(tenant), key=lambda r: r.identity)
    resolutions = resolve_all(
        versions,
        (
            version.artifact_id
            for record in records
            for version in record.source_versions
        ),
    )

    findings: list[StalenessFinding] = []
    skipped: list[SkippedDeliverable] = []
    for record in records:
        compared = compare_deliverable(
            record, resolutions, scope=scope, observed_at=now
        )
        if isinstance(compared, SkippedDeliverable):
            log.warning(
                "staleness sweep skipped a deliverable for tenant %r: %s",
                tenant,
                compared.detail,
            )
            skipped.append(compared)
        elif compared is not None:
            findings.append(compared)

    handler = MintingEvaluationHandler(
        tenant, provider=provider, sink=sink, ledger=ledger, cache=cache
    )
    drive = drive_intake(
        handler,
        SyntheticFindingSource(tenant, findings),
        # The cursor is per-sweep and in-memory by construction, never a
        # caller's choice — see the module docstring on why a persisted one
        # would silently skip findings forever.
        cursor_store=InMemoryCursorStore(),
        on_malformed=_refuse_malformed,
    )
    return SweepReport(
        tenant=tenant,
        scope=scope,
        examined=tuple(records),
        resolutions=resolutions,
        findings=tuple(findings),
        skipped=tuple(skipped),
        drive=drive,
    )

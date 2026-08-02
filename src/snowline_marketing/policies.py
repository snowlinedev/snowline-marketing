"""The policy-set schema — what a tenant's marketing rules are allowed to say
(spec §6).

Policies are GOVERNANCE ARTIFACTS: one inline policy-set artifact per tenant
org scope, revised through `revise_artifact` like any governed doc, so
versioning, review and the decision trail come free. This module owns exactly
one thing — the shape of that artifact's BODY. Where the body comes from and
which version is current is `policy_source.py`'s job; what a parsed policy
MATCHES is `matching.py`'s, and what a match is owed is `engine.py`'s. Keeping
them apart is what lets the deterministic core be built and tested with no
gateway in sight.

Constraints this module encodes, and why:

- A malformed policy version quarantines the POLICY (spec §6: "never silently
  match-all or match-none"). That is why `parse_policy_set` returns a
  `MalformedPolicySet` — a distinct TYPE, not a `PolicySet` with an empty
  entry list. An empty `PolicySet` is a legitimate, evaluable state (a tenant
  whose artifact exists but declares no rules yet) and matches nothing; a
  `MalformedPolicySet` is an UNEVALUABLE state and the engine must refuse to
  evaluate against it rather than degrade. Those two must never be confusable
  by a caller that forgot to check, so they are not the same type.

- Quarantine is WHOLE-VERSION, not per-entry. One bad entry rejects the entire
  policy set. The alternative — keep the parseable entries, drop the rest — is
  precisely the ambiguity §6's rule exists to prevent: the engine would
  evaluate a policy version that no one authored and no one reviewed, mint (or
  fail to mint) work from it, and record that version id on the ledger as if
  it had been applied whole. Half a policy version is not a smaller version;
  it is a different one. An operator with a broken entry gets a loud
  quarantine naming the entry, fixes the artifact, and revises — which is the
  workflow governance already provides.

- Predicates are DATA (spec §3: the plugin never executes tenant-supplied
  code). A predicate is a list of fnmatch-style glob patterns, stored and
  validated as plain strings. Nothing here compiles a regex, and no policy
  field is ever `eval`'d, imported, or fed to a template engine.

- MATCHING SEMANTICS are defined here and implemented by the engine, because
  the shapes are meaningless without them:
    * patterns within one predicate field are a DISJUNCTION (any match wins);
      the fields are a CONJUNCTION (every non-empty field must match);
    * an EMPTY pattern list means UNCONSTRAINED — that field is not tested;
    * `"*"` means "has any value", NOT "may be absent": a scalar the envelope
      left `None` fails a non-empty pattern list, always. An absent initiative
      is not the empty string, and a policy that meant "regardless of
      initiative" says so by omitting the field;
    * relations match on relation KIND and signals on the signal string; the
      event's set matches if ANY member matches any pattern (spec §9's
      `marketing-impact` compatibility path is exactly this predicate);
    * the engine must use `fnmatch.fnmatchcase`, never `fnmatch.fnmatch`.
      `fnmatch` folds case through `os.path.normcase`, so the same policy
      would match differently on macOS and Linux — a deterministic policy
      machine (§6) cannot have a platform-dependent match.

- There is deliberately NO tenant predicate. The set's own `tenant` IS the
  isolation boundary (§3: never routes across isolated organization scopes);
  an envelope whose tenant differs from the set's is not a non-match, it is a
  cross-tenant delivery the engine must quarantine (§14). Letting a policy
  express a tenant would make that boundary a matter of configuration.

- The dedup-key template is validated against a CLOSED placeholder vocabulary,
  while title/body/owner templates are opaque strings. The asymmetry is
  deliberate: a bad title template renders an ugly string, but a dedup key
  that references a field the engine cannot fill either explodes per event or
  renders constant — and a constant dedup key silently collapses every future
  delivery of that policy into the first one. Silent lost work is exactly what
  parse-time validation is for. The default is spec §4's logical key.

- Every model is frozen and `extra="forbid"`, for the same reasons as
  `events.py`: a policy version is a record of what was reviewed and approved,
  nothing downstream may edit one in place, and a field we silently dropped is
  a rule the operator believes is in force and is not.

- Validation NEVER raises. `parse_policy_set` returns a `PolicySet` or a
  `MalformedPolicySet` with an operator-visible reason, mirroring
  `parse_envelope`. The policy cache persists whichever came back.
"""

from __future__ import annotations

import enum
import functools
import string
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    StringConstraints,
    ValidationError,
    model_validator,
)

from snowline_marketing import classify
from snowline_marketing.events import (
    CONDITIONAL_PAYLOAD_FIELDS,
    EventType,
    guaranteed_payload_fields,
)

# The policy-set body's own version, independent of the envelope's. Bumped only
# when the POLICY shape changes incompatibly; a body declaring any other
# version quarantines rather than being best-effort parsed, so a tenant who
# revised their artifact to a shape this deploy does not understand finds out
# from quarantine instead of from evaluation silently doing less.
POLICY_SCHEMA_VERSION = 1

# Same rule as `events.NonEmptyStr`: identifiers and patterns are refs, not
# prose. An all-whitespace glob would pass a bare `str` and then match nothing
# forever without ever being wrong out loud.
NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# A glob pattern, carried as DATA. There is no "invalid fnmatch syntax" to
# validate against — fnmatch has no error class; an unterminated `[` degrades
# to a literal — so the only shape check available is non-emptiness, and that
# is the whole check by design (spec §3: declarative values/globs, no code).
GlobPattern = NonEmptyStr

# Spec §4's logical key, spelled as a template. The engine renders it; the
# delivery ledger stores the result.
DEFAULT_DEDUP_KEY_TEMPLATE = "{tenant}:{policy_id}:{event_id}"

# What a dedup-key template may reference: the delivery's identity plus the
# envelope's predicate surface (`events.EventPayload`) and its subject ref.
# CLOSED on purpose — this is the contract the evaluation engine implements,
# and a placeholder outside it cannot be filled from an envelope. Note what is
# absent: `payload.details` is unschema'd free-form data (see `events.py`),
# and `work_kind`, which NO event type guarantees — a dedup key can never
# depend on a field no producer guarantees.
#
# ALWAYS: renderable for every event type — the delivery identity, the
# envelope's required fields, the subject ref, the policy's own consequence.
DEDUP_KEY_FIELDS_ALWAYS = frozenset(
    {
        "tenant",
        "policy_id",
        "event_id",
        "event_type",
        "entity_kind",
        "entity_id",
        "scope",
        "consequence",
    }
)
# CONDITIONAL: Optional on `events.EventPayload`, guaranteed only for the
# event types whose validation requires them (`events.guaranteed_payload_
# fields`). An entry may reference one ONLY when every event type it selects
# guarantees it — otherwise the template validates and then renders the
# constant "None" key that silently swallows every later delivery, the exact
# failure parse-time validation exists to catch.
DEDUP_KEY_FIELDS_CONDITIONAL = CONDITIONAL_PAYLOAD_FIELDS
DEDUP_KEY_FIELDS = DEDUP_KEY_FIELDS_ALWAYS | DEDUP_KEY_FIELDS_CONDITIONAL


class ConsequenceType(enum.StrEnum):
    """What a matched policy produces (spec §6, plus §12).

    Closed vocabulary: a consequence this plugin cannot carry out is a rule
    the operator believes is in force, so an unknown value quarantines the
    version rather than parsing into work nobody will do. `channel_publish`
    is §12's publisher-adapter path — listed in §12 rather than §6's original
    enumeration, and gated below."""

    messaging_refresh = "messaging_refresh"
    listing_regeneration = "listing_regeneration"
    screenshot_review = "screenshot_review"
    announcement_preparation = "announcement_preparation"
    launch_plan = "launch_plan"
    review_sweep = "review_sweep"
    metrics_snapshot = "metrics_snapshot"
    channel_publish = "channel_publish"


class PolicyMode(enum.StrEnum):
    """How far a match is allowed to go (spec §6).

    `active` mints; `approval_required` records the match and waits for an
    explicit operator verb (§12's approval surface); `dry_run` evaluates and
    reports, minting nothing (§11). All three still write a ledger row — the
    mode changes the consequence, never the audit."""

    active = "active"
    approval_required = "approval_required"
    dry_run = "dry_run"


class _Model(BaseModel):
    """Shared model config — frozen and `extra="forbid"` (module docstring)."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class PolicyPredicates(_Model):
    """The declarative predicate surface (spec §6), field-for-field against
    `events.EventPayload`.

    Every field is a tuple of glob patterns; every field defaults to empty,
    which means UNCONSTRAINED (see the module docstring for the full matching
    contract). Tuples, not lists: the model is frozen and a mutable default
    would let one evaluation mutate the patterns the next one reads."""

    # `payload.scope` — the project scope the subject lives on. Not the
    # tenant: that is the set's own field and is not predicable.
    scope: tuple[GlobPattern, ...] = ()
    initiative: tuple[GlobPattern, ...] = ()
    phase: tuple[GlobPattern, ...] = ()
    milestone: tuple[GlobPattern, ...] = ()
    work_kind: tuple[GlobPattern, ...] = ()
    # Matched against relation KINDS (`events.Relation.kind`), not targets:
    # §9's compatibility path asks whether a `marketing-impact` relation
    # EXISTS, and the relation's target is provenance, not a selector.
    relations: tuple[GlobPattern, ...] = ()
    signals: tuple[GlobPattern, ...] = ()


class PolicyDestination(_Model):
    """Where matched work lands (spec §6/§7).

    `scope` is required — minted work goes on the canonical roadmap at a named
    scope, and a destination the policy declines to name would leave minting
    guessing. `initiative`/`phase` are optional: plenty of marketing follow-up
    is loose work on a scope, not phase-placed.

    The destination scope is NOT validated against the set's tenant here. Scope
    hierarchy and ownership live in the platform, not in this body, so the only
    honest check is at mint time (spec §14 isolation) where the plugin can
    actually resolve the scope tree."""

    scope: NonEmptyStr
    initiative: NonEmptyStr | None = None
    phase: NonEmptyStr | None = None


class PolicyEntry(_Model):
    """One rule: which events, which predicates, and what it produces."""

    # Stable across revisions of the artifact — it is a component of the
    # delivery ledger's logical key (spec §4), so renaming one silently
    # un-dedups every delivery it ever made. Unique within the set (enforced
    # on `PolicySet`).
    policy_id: NonEmptyStr

    # §6's "event selectors". Values come from the intake vocabulary
    # (`events.EventType`); an unrecognized selector quarantines the version
    # rather than being dropped, because a selector we skip is a rule the
    # operator believes is armed. At least one: an entry selecting no event
    # type can never fire, which is a mistake, not a way to disable a policy
    # (that is `mode`).
    event_types: tuple[EventType, ...]

    predicates: PolicyPredicates = PolicyPredicates()

    consequence: ConsequenceType
    destination: PolicyDestination

    # Opaque template strings — validated for PRESENCE and shape, never for
    # syntax. The rendering vocabulary belongs to the minting item (spec §7),
    # and pinning it here would make every new provenance field a policy-schema
    # change. (Contrast `dedup_key_template`, whose failure mode is silent.)
    title_template: NonEmptyStr
    body_template: NonEmptyStr
    # §6's "ownership template" — who the minted item is assigned to, when the
    # policy wants to say. Optional: `human_owned` alone is enough for the
    # common case.
    owner_template: NonEmptyStr | None = None

    # Spec §6 opt-in flags. `human_owned` marks work the operator does
    # personally; `musher_dispatch` sets PM's dispatch opt-in (spec §7 — the
    # plugin sets a flag, it never calls musher). Both default OFF: autonomy is
    # opted into, never inherited.
    human_owned: bool = False
    musher_dispatch: bool = False

    # Open string lists (spec §6). Deliberately not enums: artifact refs are
    # governance ids, channels grow with §12's adapters, and deliverable
    # classes are tenant vocabulary. A closed set here would make every new
    # channel a code change in the plugin that must never hold
    # organization-specific marketing rules (spec §1).
    artifact_refs: tuple[NonEmptyStr, ...] = ()
    channels: tuple[NonEmptyStr, ...] = ()
    deliverable_classes: tuple[NonEmptyStr, ...] = ()

    # Defaults to spec §4's logical key. Validated against DEDUP_KEY_FIELDS —
    # see the module docstring on why this one template is not opaque.
    dedup_key_template: NonEmptyStr = DEFAULT_DEDUP_KEY_TEMPLATE

    mode: PolicyMode = PolicyMode.active

    @model_validator(mode="after")
    def _check_entry(self) -> PolicyEntry:
        if not self.event_types:
            raise ValueError(
                f"policy {self.policy_id!r}: event_types must name at least one "
                "event type (an entry that selects nothing can never fire; use "
                "mode='dry_run' to disarm a policy)"
            )
        if (
            self.consequence is ConsequenceType.channel_publish
            and self.mode is PolicyMode.active
        ):
            # Spec §3/§12: publishing is never implicit — an adapter push runs
            # only from an explicit operator command or an APPROVAL-GATED
            # consequence. An `active` publish policy would push governed
            # content to a live external channel the moment an event matched,
            # with no human in the loop. Rejected here rather than defaulted
            # silently, so the artifact says out loud that a human gates it.
            raise ValueError(
                f"policy {self.policy_id!r}: consequence 'channel_publish' may not "
                "run in mode 'active' — publishing is approval-gated (spec §12); "
                "use 'approval_required' or 'dry_run'"
            )
        _validate_dedup_template(
            self.policy_id, self.dedup_key_template, self.event_types
        )
        return self


@functools.lru_cache
def dedup_template_fields(template: str) -> frozenset[str]:
    """The placeholder fields `template` references — THE parse of a dedup-key
    template, shared by `_validate_dedup_template` and the engine's
    `render_dedup_key` so the parse logic exists exactly once and the two ends
    of the contract cannot drift apart.

    Raises ValueError on the deferred-crash forms (unbalanced braces, nested
    placeholders in a format spec, unknown conversions) — the validator wraps
    the message with the owning policy id. The engine only calls this on
    templates that already validated, so its hot path never raises — and,
    lru_cached, never re-parses a template it has rendered before (one parse
    per distinct template per process, against a vocabulary bounded by the
    tenants' policy sets)."""
    fields: set[str] = set()
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError as exc:
        # Unbalanced braces. `str.format` would raise this per event at mint
        # time instead — a policy that fails only on the events it matches.
        raise ValueError(
            f"dedup_key_template is not a valid format string ({exc})"
        ) from exc
    for _, field, format_spec, conversion in parsed:
        if field is None:
            continue
        if format_spec and "{" in format_spec:
            # A nested placeholder hides inside the format spec, where
            # Formatter().parse does not surface it as a field — str.format
            # would KeyError per event at mint time, a policy that fails only
            # on the events it matches.
            raise ValueError(
                f"dedup_key_template uses a nested placeholder in a format "
                f"spec ({format_spec!r}) — not renderable from an envelope"
            )
        if conversion is not None and conversion not in ("s", "r", "a"):
            # `str.format` accepts only !s/!r/!a and raises on anything else —
            # at mint time, per matched event, unless rejected here.
            raise ValueError(
                f"dedup_key_template uses unknown conversion {conversion!r} "
                "(only !s, !r, !a exist)"
            )
        fields.add(field)
    return frozenset(fields)


def _validate_dedup_template(
    policy_id: str, template: str, event_types: tuple[EventType, ...]
) -> None:
    """Reject a dedup-key template the engine could not render — or could only
    render into the constant/crashing keys the module docstring describes.
    Raises ValueError — called from inside model validation, so it surfaces
    through the same never-raises classification as everything else. The parse
    itself lives in `dedup_template_fields` (shared with the engine's render);
    this function owns the policy-level judgments about what it found."""
    try:
        fields = dedup_template_fields(template)
    except ValueError as exc:
        raise ValueError(f"policy {policy_id!r}: {exc}") from exc
    if not fields:
        raise ValueError(
            f"policy {policy_id!r}: dedup_key_template {template!r} references no "
            "placeholder — a constant dedup key collapses every delivery of this "
            "policy into the first one"
        )
    unknown = sorted(fields - DEDUP_KEY_FIELDS)
    if unknown:
        # Also catches `{0}`, `{a.b}` and `{a[0]}`: the parsed field name is
        # compared whole, so attribute/index access never resolves to a known
        # field and lands here with the same message.
        raise ValueError(
            f"policy {policy_id!r}: dedup_key_template references unknown "
            f"placeholder(s) {', '.join(repr(u) for u in unknown)} — known "
            f"placeholders are {', '.join(sorted(DEDUP_KEY_FIELDS))}"
        )
    # Final gate: DRY-RENDER with sentinel string values. The engine renders
    # every placeholder from string fields, so whatever the explicit arms
    # above did not name — a type-incompatible spec like `{event_id:d}`, a
    # datetime code on a string — must fail HERE at parse time, not per
    # matched event at mint time (a policy that fails only on the events it
    # matches is the failure mode this whole function exists to prevent).
    try:
        template.format(**{field: "sentinel" for field in DEDUP_KEY_FIELDS})
    except Exception as exc:
        raise ValueError(
            f"policy {policy_id!r}: dedup_key_template {template!r} does not "
            f"render against string values ({exc})"
        ) from exc
    # A conditional placeholder must be GUARANTEED by every event type this
    # entry selects — Optional-but-absent renders the constant "None" key.
    for field in sorted(fields & DEDUP_KEY_FIELDS_CONDITIONAL):
        lacking = sorted(
            et.value for et in event_types if field not in guaranteed_payload_fields(et)
        )
        if lacking:
            raise ValueError(
                f"policy {policy_id!r}: dedup_key_template references "
                f"{field!r}, which event type(s) {', '.join(lacking)} do not "
                "guarantee — an absent value would render a constant 'None' "
                "key and swallow every later delivery as a duplicate"
            )


class PolicySet(_Model):
    """One tenant's policy artifact body — the whole reviewable unit (spec §6).

    An EMPTY `policies` tuple is valid and evaluable: a tenant whose artifact
    exists but declares no rules yet matches nothing, audits every event as
    `ignored` (spec §14), and mints nothing. That is a state an operator can
    reach deliberately, and it is categorically different from a quarantined
    version — which is why the malformed case is a different type entirely."""

    # A Literal, not an int compared later: a v2 body fails at the same seam as
    # any other shape violation, so version skew reaches quarantine by one path.
    schema_version: Literal[1]

    # The tenant org scope this set governs — one artifact per tenant (spec
    # §6). This is the isolation boundary the engine checks envelopes against;
    # it is deliberately not something an entry can predicate on.
    tenant: NonEmptyStr

    policies: tuple[PolicyEntry, ...] = ()

    @model_validator(mode="after")
    def _policy_ids_are_unique(self) -> PolicySet:
        seen: set[str] = set()
        duplicates: list[str] = []
        for entry in self.policies:
            if entry.policy_id in seen:
                duplicates.append(entry.policy_id)
            seen.add(entry.policy_id)
        if duplicates:
            # `policy_id` is a component of the ledger's logical key (spec §4).
            # Two entries sharing one would have their deliveries dedup against
            # EACH OTHER: the second policy's work would be swallowed as a
            # duplicate of the first's, silently, forever.
            raise ValueError(
                "duplicate policy_id(s) "
                f"{', '.join(repr(d) for d in sorted(set(duplicates)))} — "
                "policy_id is part of the delivery ledger's dedup key, so two "
                "entries sharing one would dedup each other's work away"
            )
        return self

    @model_validator(mode="after")
    def _dedup_templates_cannot_collide(self) -> PolicySet:
        # The duplicate-policy_id failure above, resurrected by another route:
        # two entries whose IDENTICAL template omits {policy_id} render the
        # IDENTICAL key for any envelope both select (same fields, same
        # literals), so whenever their event_types overlap the same-key
        # collision is GUARANTEED, not merely possible — the second policy's
        # work would be swallowed as a duplicate of the first's, silently,
        # forever. Identical templates WITH {policy_id} are safe (distinct ids
        # render distinct keys), and the check deliberately stops at IDENTICAL
        # template strings: non-identical templates differ in literals for the
        # same envelope, so colliding takes equal field VALUES — a per-event
        # fact no parse can see, and the engine's runtime guard owns it.
        for index, first in enumerate(self.policies):
            for second in self.policies[index + 1 :]:
                if first.dedup_key_template != second.dedup_key_template:
                    continue
                if "policy_id" in dedup_template_fields(first.dedup_key_template):
                    continue
                if not set(first.event_types) & set(second.event_types):
                    continue
                raise ValueError(
                    f"policies {first.policy_id!r} and {second.policy_id!r} "
                    "select overlapping event types and share the "
                    f"dedup_key_template {first.dedup_key_template!r}, which "
                    "omits {policy_id} — every envelope both select would "
                    "render the identical key, so one policy's work would be "
                    "swallowed as a duplicate of the other's"
                )
        return self

    def entry(self, policy_id: str) -> PolicyEntry | None:
        """The entry with `policy_id`, or None. A ledger row records the
        policy id and the version id; this is how a reader gets from those two
        facts back to the rule that was applied."""
        for entry in self.policies:
            if entry.policy_id == policy_id:
                return entry
        return None


class MalformedPolicyReason(enum.StrEnum):
    """Why a policy body could not be understood — the operator-visible reason
    shown next to the raw body on the quarantine surface (spec §4/§11).

    Coarse on purpose, like `events.MalformedReason`: the actionable specifics
    (which entry, which field) live in `MalformedPolicySet.detail`. The reason
    an operator acts on is "the version is quarantined", which is carried by
    the RESULT TYPE, not by which of these values it holds."""

    # The artifact body was not JSON at all (a policy artifact revised to prose
    # is the realistic case — governance stores bodies as text).
    not_json = "not_json"
    # Valid JSON, but not a JSON object (a bare list of entries, say).
    not_an_object = "not_an_object"
    # A JSON object that does not satisfy the policy-set contract — including
    # duplicate policy ids, unknown event selectors and unknown consequences.
    invalid_policy_set = "invalid_policy_set"
    # A structurally valid set whose declared tenant is not the tenant it was
    # resolved FOR. Quarantined, not evaluated: caching or evaluating it would
    # attribute one tenant's rules to another's audit trail — the cross-tenant
    # misrepresentation §3/§14 forbid.
    tenant_mismatch = "tenant_mismatch"
    # The version id is already cached for a DIFFERENT tenant, so the cache
    # refused the write (`policy_cache.put`'s tenant-guarded upsert). The
    # version is unevaluable while the collision stands: a ledger row
    # recording this id would join to the OTHER tenant's policy text, and an
    # audit row that joins to the wrong rules is worse than a visible stall.
    version_collision = "version_collision"


# Import-time pin: `parse_policy_set` maps `classify.DecodeFailure` into this
# enum by VALUE. A decode failure added in classify.py without a member here
# would turn the never-raises malformed path into a ValueError at runtime —
# fail at import instead, where the suite cannot miss it.
if not {f.value for f in classify.DecodeFailure} <= {
    r.value for r in MalformedPolicyReason
}:
    raise AssertionError(
        "MalformedPolicyReason must cover every classify.DecodeFailure value"
    )


@dataclass(frozen=True)
class MalformedPolicySet:
    """A quarantined policy VERSION, kept whole.

    A RESULT, not an error (mirrors `events.MalformedEnvelope`). The engine
    that receives one must REFUSE TO EVALUATE the tenant for as long as it is
    the current version — never fall back to an older cached version, never
    treat it as "no policies". Both fallbacks are the silent match-all /
    match-none §6 forbids; the visible failure is the point.

    `raw` is the body verbatim, because the operator's fix is to compare it
    against the artifact in governance and revise — a normalized copy would
    diff against a fiction. `version_id` is the governance artifact version
    this body came from, when the caller knew it: that is what the quarantine
    row keys on and what an operator quotes in the revision."""

    reason: MalformedPolicyReason
    detail: str
    raw: Any
    # A human locator (fixture filename, artifact ref) and the governance
    # artifact version id, both carried through from the caller.
    ref: str | None = None
    version_id: str | None = None
    # Best-effort: a body can be malformed for some other reason and still name
    # its tenant, which is how the quarantine surface says WHOSE policy broke.
    tenant: str | None = None


# Same introspectable unhashability as `events.MalformedEnvelope`: the frozen
# dataclass would generate a field hash over `raw` (arbitrary JSON) that raises
# only at runtime while still advertising `Hashable`. Quarantine-side dedup
# keys on the version id, never on the object.
MalformedPolicySet.__hash__ = None  # type: ignore[assignment]

# What a caller gets back for one policy body: understood, or explained.
ParsedPolicySet = PolicySet | MalformedPolicySet


def parse_policy_set(
    raw: object,
    *,
    ref: str | None = None,
    version_id: str | None = None,
    expected_tenant: str | None = None,
) -> ParsedPolicySet:
    """Classify one policy artifact body as a valid policy set or a
    quarantined one.

    Accepts JSON text/bytes (the governance artifact body, and the fixture
    files on disk — deliberately undecoded, so "not JSON" classifies here
    instead of raising in the caller) or an already-decoded mapping. The
    decode/validate skeleton is `classify.py`, shared with `parse_envelope`;
    policy errors additionally quote offending scalars, because a policy body
    is hand-authored and the operator's question is "which of my values is
    wrong?".

    `expected_tenant` is the tenant the body was resolved FOR. When given, a
    structurally valid set declaring a DIFFERENT tenant quarantines as
    `tenant_mismatch` — a misregistered artifact must never let one tenant's
    rules cache, evaluate, or audit under another's name (§3/§14).

    Never raises. `ref` and `version_id` are locators the caller knows and
    this function only carries through, so a `MalformedPolicySet` is
    self-contained for the cache row that persists it."""
    body, decode_failure = classify.decode_json_object(raw)
    if decode_failure is not None:
        failure, detail = decode_failure
        return MalformedPolicySet(
            # By-value mapping: DecodeFailure's values are this enum's values.
            reason=MalformedPolicyReason(failure.value),
            detail=detail,
            raw=raw,
            ref=ref,
            version_id=version_id,
        )
    # Best-effort tenant BEFORE validation: the quarantine surface's first
    # question is whose policy broke, and that answer usually survives whatever
    # else is wrong with the body.
    tenant = classify.best_effort_str(body, "tenant")
    try:
        parsed = PolicySet.model_validate(dict(body))
    except ValidationError as exc:
        return MalformedPolicySet(
            reason=MalformedPolicyReason.invalid_policy_set,
            detail=classify.compact_errors(exc, quote_scalars=True),
            raw=raw,
            ref=ref,
            version_id=version_id,
            tenant=tenant,
        )
    if expected_tenant is not None and parsed.tenant != expected_tenant:
        return MalformedPolicySet(
            reason=MalformedPolicyReason.tenant_mismatch,
            detail=(
                f"body declares tenant {parsed.tenant!r} but was resolved for "
                f"tenant {expected_tenant!r} — a misregistered artifact; not "
                "cached as valid, not evaluated"
            ),
            raw=raw,
            ref=ref,
            version_id=version_id,
            tenant=parsed.tenant,
        )
    return parsed

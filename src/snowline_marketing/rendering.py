"""Turning a matched policy into the text of a work item (spec §7).

`policies.PolicyEntry` carries `title_template`, `body_template` and
`owner_template` as OPAQUE strings and says so explicitly: "the rendering
vocabulary belongs to the minting item (spec §7), and pinning it here would make
every new provenance field a policy-schema change". This module is that
vocabulary, and the deferred decision made.

**The vocabulary, and why it has two halves.**

- A CLOSED half — the delivery's identity, the envelope's predicate surface, the
  subject ref, and the matched entry's own declarations. Every name is listed in
  `TEMPLATE_FIELDS`, every value is a string, and the list is stable.
- An OPEN half — `{details.<key>}`, over `payload.details`, the free-form map
  `events.py` deliberately refuses to schematize ("per-type variation lives in
  the free-form `details` map, which no predicate reads... leaving room for
  facts a consequence template wants to quote"). This is that room. A policy
  quoting `{details.title}` is quoting the completed item's title, which is the
  single most useful thing a marketing follow-up can say, and no closed
  vocabulary this module could write would contain it.

**The asymmetry with the dedup-key template is deliberate and load-bearing.**
`policies._validate_dedup_template` validates dedup templates at PARSE time
against a closed vocabulary and quarantines the whole policy version if one
references a field an event might not carry. It can do that precisely BECAUSE
that vocabulary is closed and every name in it is guaranteed per event type.
Title/body templates are open over `details`, whose keys vary per event and per
producer, so there is no parse-time check that could be sound: a template
referencing `{details.release_notes}` is correct for the events that carry it
and unrenderable for the ones that do not, and only the event in hand can say
which this is. So the failure is detectable only at RENDER time, per delivery —
and the costs of the two failures are not comparable, which is why the postures
differ. A bad dedup key renders a CONSTANT and silently swallows every later
delivery of that policy forever; a bad title template makes one mint fail
loudly, with the row saying which placeholder was missing. Parse-time
quarantine for the silent one, per-delivery failure for the loud one.

**A render failure is a per-delivery MINT failure, never a crashed pass.**
`render_mint_request` returns a `RenderFailure` rather than raising: the minting
pass turns it into a terminal `failed` row carrying the operator's fix (which
template, which placeholders), the rest of the pass's consequences mint
normally, and §11's dead-letter replay picks it up once the artifact is revised.
One tenant's typo must never stop another policy's work.

**Rendering is exact-name lookup, not `str.format`.** The renderer walks
`string.Formatter().parse` and looks each field name up WHOLE in a flat mapping
of strings. That is what lets `{details.title}` be a name rather than an
attribute access — and, more importantly, it is why a template can never
traverse INTO a Python object: `str.format` would resolve `{entry.__class__}`
and `{envelope.payload}` by attribute lookup on whatever it was handed, which is
a tenant-authored string reaching into plugin internals (spec §3: the plugin
never executes tenant-supplied code, and a policy is data). Here an unknown name
— dotted, indexed or otherwise — resolves to nothing and fails cleanly.

**Every rendered body carries the provenance block** (`provenance_block`),
appended regardless of what the template said, because spec §7 requires it of
every minted item: originating event and subject, matched policy and evaluated
artifact version, source scope/initiative/phase/milestone, external refs,
affected artifacts/channels/deliverable classes, the dispatch intent, and the
delivery ledger key. Machine-greppable `label: value` lines, but written for the
person who opens the roadmap item — a minted item is read by people, and a JSON
blob in a work-item body is a thing people scroll past. And because the
rendered body ABOVE the block is tenant/producer-authored text, any occurrence
of the provenance heading inside it is neutralized before the genuine block is
appended (`NEUTRALIZED_PROVENANCE_HEADING`): the un-escaped heading appears
exactly once per minted body, on the block this plugin wrote, which is the
invariant readers and the §8 sweep get to trust.
"""

from __future__ import annotations

import json
import re
import string
from collections.abc import Callable
from dataclasses import dataclass

from snowline_marketing.engine import PendingConsequence
from snowline_marketing.events import EventEnvelope
from snowline_marketing.policies import PolicyEntry
from snowline_marketing.work_sink import MintRequest

# The prefix that opens the free-form half of the vocabulary onto
# `payload.details` (see the module docstring). A name is looked up WHOLE, so
# this is a naming convention rather than a traversal: "details.title" is the
# key, not `details` followed by `.title`.
DETAILS_PREFIX = "details."

# How a list-valued field renders. One separator for every such field, so a
# template author learns it once and a body reads the same whichever list it
# quotes.
_LIST_SEPARATOR = ", "

# The conversions `str.format` itself accepts. Anything else is a template bug
# and fails the delivery rather than being silently ignored.
_CONVERSIONS: dict[str, Callable[[str], str]] = {
    "s": str,
    "r": repr,
    "a": ascii,
}

# The largest number a format spec may carry (width, padding, precision). A
# spec's width is a MEMORY ALLOCATION — `format("x", ">999999999")` builds the
# whole padded string before any output cap could see it — and the spec is
# tenant-authored text, so an unbounded width is a tenant choosing an
# allocation inside the minting pass. 1000 comfortably exceeds any legitimate
# alignment and stays far under anything that could hurt.
_MAX_FORMAT_SPEC_NUMBER = 1000
_FORMAT_SPEC_NUMBERS = re.compile(r"\d+")

# Hard caps on each rendered template's OUTPUT, checked before the request is
# built: a runaway template (a template quoting a huge `details` value, or a
# producer that started sending one) must not post megabytes to PM. Exceeding
# a cap is a per-delivery RenderFailure naming the field and the cap — the
# same posture as every other template problem, because the fix is the same
# operator's (revise the template, or the producer's payload).
_OUTPUT_CAPS = {
    "title_template": 500,
    "body_template": 65536,
    "owner_template": 200,
}


def _text(value: object | None) -> str | None:
    """One value, as template text — or None for "this event does not carry it".

    Strings pass through; everything else goes through `json.dumps` so a
    `details` value renders the way the operator saw it on the wire (`true`, not
    `True`) and a nested structure renders deterministically rather than through
    Python's repr. `sort_keys` and `default=str` keep that promise for a mapping
    whose key order varies and for the exotic value a hand-built envelope can
    hold (`details` is typed `Mapping[str, Any]`)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, default=str)


def _joined(values: tuple[str, ...]) -> str | None:
    """A list field, joined — or None when it is empty.

    Empty renders as ABSENT, not as the empty string: "this policy declares no
    channels" and "this event named no signals" are the same fact, and a minted
    body reading "Channels: " with nothing after it is a broken item, not a
    terse one. A template that quotes a list the delivery has nothing for fails
    the mint and says so."""
    return _LIST_SEPARATOR.join(values) if values else None


# The CLOSED half of the render vocabulary: name -> how to read it off the
# frozen (entry, envelope) pair the consequence carries. Every value is a string
# or None, and None means "this delivery does not carry it" — which is a render
# failure for any template that referenced the name, never a rendered "None"
# (the mistake `engine.DedupKeyUnrenderable` exists to refuse on the dedup side,
# for far higher stakes).
TEMPLATE_FIELDS: dict[str, Callable[[PolicyEntry, EventEnvelope], str | None]] = {
    # The delivery's identity.
    "tenant": lambda entry, envelope: envelope.tenant,
    "event_id": lambda entry, envelope: envelope.event_id,
    "event_type": lambda entry, envelope: envelope.event_type.value,
    # ISO 8601 with the offset the producer sent (envelopes are validated
    # timezone-aware, `events.py`) — a timestamp a reader can paste anywhere.
    "occurred_at": lambda entry, envelope: envelope.occurred_at.isoformat(),
    # The subject ref.
    "entity_kind": lambda entry, envelope: envelope.subject.kind.value,
    "entity_id": lambda entry, envelope: envelope.subject.id,
    "entity_phase": lambda entry, envelope: envelope.subject.phase,
    # The predicate surface — the same fields policies match on, so a template
    # can quote what its own predicate selected.
    "scope": lambda entry, envelope: envelope.payload.scope,
    "initiative": lambda entry, envelope: envelope.payload.initiative,
    "phase": lambda entry, envelope: envelope.payload.phase,
    "milestone": lambda entry, envelope: envelope.payload.milestone,
    "work_kind": lambda entry, envelope: envelope.payload.work_kind,
    # The set-valued payload fields, joined.
    "signals": lambda entry, envelope: _joined(envelope.payload.signals),
    "relations": lambda entry, envelope: _joined(
        tuple(relation.kind for relation in envelope.payload.relations)
    ),
    "external_refs": lambda entry, envelope: _joined(
        tuple(f"{ref.kind}: {ref.url}" for ref in envelope.payload.external_refs)
    ),
    # The matched entry's own declarations. A policy quoting its own consequence
    # or destination in the title is how one template serves several entries.
    "policy_id": lambda entry, envelope: entry.policy_id,
    "consequence": lambda entry, envelope: entry.consequence.value,
    "mode": lambda entry, envelope: entry.mode.value,
    "destination_scope": lambda entry, envelope: entry.destination.scope,
    "destination_initiative": lambda entry, envelope: entry.destination.initiative,
    "destination_phase": lambda entry, envelope: entry.destination.phase,
    "artifact_refs": lambda entry, envelope: _joined(entry.artifact_refs),
    "channels": lambda entry, envelope: _joined(entry.channels),
    "deliverable_classes": lambda entry, envelope: _joined(entry.deliverable_classes),
}

# The two fields that describe the DELIVERY rather than the (entry, envelope)
# pair, filled from the consequence itself. Named here so the vocabulary is one
# list wherever a reader looks for it.
_CONSEQUENCE_FIELDS = ("policy_version_id", "dedup_key")

# Every name a title/body/owner template may use, apart from the open
# `{details.*}` half. Public because it is the answer to "what can I write in a
# template?", and because a test pins it against the renderer.
TEMPLATE_FIELD_NAMES = frozenset(TEMPLATE_FIELDS) | frozenset(_CONSEQUENCE_FIELDS)


def template_values(consequence: PendingConsequence) -> dict[str, str]:
    """The flat name -> text mapping a template renders against.

    Absent values are OMITTED rather than mapped to None, so "the name is not in
    the mapping" is the single condition the renderer tests: an unknown
    placeholder and a placeholder this event cannot fill are the same failure to
    the operator ("your template asked for something that is not there") and
    keeping them one case is what keeps the message honest for both."""
    values: dict[str, str] = {}
    for name, read in TEMPLATE_FIELDS.items():
        text = read(consequence.entry, consequence.envelope)
        if text is not None:
            values[name] = text
    values["policy_version_id"] = consequence.policy_version_id
    values["dedup_key"] = consequence.dedup_key
    for key, value in consequence.envelope.payload.details.items():
        text = _text(value)
        if text is not None:
            values[f"{DETAILS_PREFIX}{key}"] = text
    return values


@dataclass(frozen=True)
class RenderFailure:
    """A template that could not be rendered for THIS delivery.

    A RESULT, not an exception (the house posture: `events.MalformedEnvelope`,
    `policies.MalformedPolicySet`, `policy_source.PolicyResolutionError`). The
    minting pass writes it to the row as a terminal `failed` with `detail`, and
    the pass carries on — one policy's unrenderable template is not a reason to
    stop minting everything else the event owed.

    `missing` names the placeholders that could not be filled, sorted, because
    that is the operator's actual fix: either the template quotes a field this
    event type does not carry, or the producer stopped sending a `details` key
    the policy depends on."""

    policy_id: str
    event_id: str
    detail: str
    missing: tuple[str, ...] = ()


class _Unrenderable(Exception):
    """Internal: one template's failure, on its way to a `RenderFailure`."""

    def __init__(self, detail: str, missing: tuple[str, ...] = ()) -> None:
        super().__init__(detail)
        self.detail = detail
        self.missing = missing


def render_template(template: str, values: dict[str, str]) -> str:
    """Render one template by EXACT-NAME lookup (see the module docstring).

    Deterministic and total over its inputs: the same template and the same
    delivery always produce the same string, and every way it can fail raises
    `_Unrenderable` with a message naming the cause rather than propagating a
    `KeyError`/`ValueError` from deep inside `str.format`."""
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError as exc:
        # Unbalanced braces. Unlike a dedup template — dry-rendered at parse
        # time — a title/body template reaches here unvalidated by design, so
        # this is a real, reachable input.
        raise _Unrenderable(f"template is not a valid format string ({exc})") from exc
    out: list[str] = []
    missing: list[str] = []
    for literal, name, format_spec, conversion in parsed:
        out.append(literal)
        if name is None:
            continue
        if format_spec and "{" in format_spec:
            # A nested placeholder inside a format spec. Refused rather than
            # recursed into: the vocabulary is flat, and a spec that computes
            # itself from another field is a template language this plugin has
            # no reason to own.
            raise _Unrenderable(
                f"placeholder {name!r} uses a nested placeholder in its format "
                f"spec ({format_spec!r})"
            )
        if conversion is not None and conversion not in _CONVERSIONS:
            raise _Unrenderable(
                f"placeholder {name!r} uses unknown conversion {conversion!r} "
                "(only !s, !r, !a exist)"
            )
        if format_spec and any(
            int(number) > _MAX_FORMAT_SPEC_NUMBER
            for number in _FORMAT_SPEC_NUMBERS.findall(format_spec)
        ):
            # Refused BEFORE format() runs: the width/padding number in a spec
            # is an allocation (see _MAX_FORMAT_SPEC_NUMBER), so an oversized
            # one must fail as a template bug rather than be attempted.
            raise _Unrenderable(
                f"placeholder {name!r} has a format spec {format_spec!r} whose "
                f"width/precision exceeds {_MAX_FORMAT_SPEC_NUMBER} — a spec's "
                "width is a memory allocation, and a tenant-authored one is "
                "capped"
            )
        value = values.get(name)
        if value is None:
            missing.append(name)
            continue
        if conversion is not None:
            value = _CONVERSIONS[conversion](value)
        try:
            out.append(format(value, format_spec or ""))
        except (ValueError, MemoryError) as exc:
            # ValueError: a numeric/date format spec on a string value — every
            # value in the vocabulary is text, so this is a template bug, per
            # delivery. MemoryError: an allocation the spec guard above did not
            # foresee — the pass must fail ONE delivery, never die; caught here
            # while nothing is held so the recovery is trivial.
            raise _Unrenderable(
                f"placeholder {name!r} has a format spec {format_spec!r} that "
                f"could not be applied to text ({type(exc).__name__}: {exc})"
            ) from exc
    if missing:
        raise _Unrenderable(
            "template references "
            + ", ".join(repr(name) for name in sorted(set(missing)))
            + ", which this delivery does not carry",
            missing=tuple(sorted(set(missing))),
        )
    return "".join(out)


# The heading that opens every minted item's provenance block. A constant
# because it is what an operator (and, later, a §8 provenance sweep) greps for.
PROVENANCE_HEADING = "— Snowline marketing provenance —"

# What a TEMPLATE-DERIVED occurrence of that heading is rewritten to before
# the genuine block is appended. Template output is tenant/producer-authored
# text (`details` values above all), so a `details` value carrying a forged
# provenance block would otherwise land in the body verbatim and read exactly
# like the real one. Every occurrence inside the rendered body is replaced
# with this variant — visibly marked, and NOT containing the exact heading
# string (the em-dashes become tildes), so the ONLY un-escaped heading in a
# minted body is the appended genuine block. Readers, and the §8 provenance
# sweep, may therefore trust the LAST (equivalently: only) un-escaped block.
NEUTRALIZED_PROVENANCE_HEADING = "(escaped) ~ Snowline marketing provenance ~"


def _provenance_lines(consequence: PendingConsequence) -> list[str]:
    """The §7 provenance facts, in a fixed order.

    Absent optional facts are OMITTED rather than printed empty: a body listing
    "source milestone:" with nothing after it teaches a reader to distrust the
    block. The facts that are always there — the event, the policy, the version,
    the scope, the dispatch intent and the ledger key — are never omitted, so
    the block always answers "where did this come from and what would make it
    happen again?"."""
    entry = consequence.entry
    envelope = consequence.envelope
    payload = envelope.payload
    subject = envelope.subject
    lines = [
        f"originating event: {envelope.event_id} ({envelope.event_type.value}) "
        f"at {envelope.occurred_at.isoformat()}",
        f"subject entity: {subject.kind.value} {subject.id}"
        + (f" (phase {subject.phase})" if subject.phase else ""),
        f"matched policy: {entry.policy_id} "
        f"(consequence {entry.consequence.value}, mode {entry.mode.value})",
        f"evaluated policy artifact version: {consequence.policy_version_id}",
        f"source scope: {payload.scope}",
    ]
    for label, value in (
        ("source initiative", payload.initiative),
        ("source phase", payload.phase),
        ("source milestone", payload.milestone),
        ("source work kind", payload.work_kind),
    ):
        if value:
            lines.append(f"{label}: {value}")
    if payload.signals:
        lines.append(f"semantic signals: {_LIST_SEPARATOR.join(payload.signals)}")
    if payload.relations:
        kinds = _LIST_SEPARATOR.join(relation.kind for relation in payload.relations)
        lines.append(f"item relations: {kinds}")
    for ref in payload.external_refs:
        # One line per ref, not a joined list: these are the reconciled PR /
        # release URLs (spec §7) and a URL that has to be split out of a
        # comma-separated run is a URL nobody clicks.
        lines.append(f"external ref ({ref.kind}): {ref.url}")
    for label, values in (
        ("affected artifact refs", entry.artifact_refs),
        ("channels", entry.channels),
        ("deliverable classes", entry.deliverable_classes),
    ):
        if values:
            lines.append(f"{label}: {_LIST_SEPARATOR.join(values)}")
    destination = entry.destination.scope
    if entry.destination.initiative:
        destination += f" / {entry.destination.initiative}"
    if entry.destination.phase:
        destination += f" / {entry.destination.phase}"
    lines.append(f"destination: {destination}")
    lines.append(f"human owned: {'yes' if entry.human_owned else 'no'}")
    # Stated on EVERY item, both ways round. The dispatch opt-in has no PM field
    # to land in yet (snowline-pm #65, see `work_sink.py`), so if the payload
    # key is ignored this line is the only durable record that the policy asked
    # for autonomous dispatch — and an intent that can go silent is one that
    # eventually surprises someone. Printed as "no" too, so its absence never
    # has to be interpreted.
    lines.append(
        "musher dispatch requested: "
        + (
            "yes — PM's watcher routes it; this plugin never calls musher"
            if entry.musher_dispatch
            else "no"
        )
    )
    lines.append(f"delivery ledger key: {consequence.dedup_key}")
    return lines


def provenance_block(consequence: PendingConsequence) -> str:
    """Spec §7's provenance block, as text appended to every minted body."""
    lines = [PROVENANCE_HEADING]
    lines.extend(f"- {line}" for line in _provenance_lines(consequence))
    lines.append(
        "(generated by the Snowline marketing plugin from a policy match — "
        "edits here do not change the rule)"
    )
    return "\n".join(lines)


def render_mint_request(consequence: PendingConsequence) -> MintRequest | RenderFailure:
    """Render one matched consequence into the request a `WorkItemSink` takes.

    Deterministic: the same frozen consequence always produces the same request,
    which is what makes a re-delivery's mint the same item and a fixtures run
    reproducible.

    All three templates are attempted even when the first fails, so an operator
    fixing a policy sees every broken template at once rather than one per
    replay."""
    values = template_values(consequence)
    entry = consequence.entry
    rendered: dict[str, str] = {}
    problems: list[str] = []
    missing: set[str] = set()
    for field_name, template in (
        ("title_template", entry.title_template),
        ("body_template", entry.body_template),
        ("owner_template", entry.owner_template),
    ):
        if template is None:
            continue
        try:
            text = render_template(template, values)
        except _Unrenderable as exc:
            problems.append(f"{field_name}: {exc.detail}")
            missing.update(exc.missing)
            continue
        cap = _OUTPUT_CAPS[field_name]
        if len(text) > cap:
            # A runaway output is a template problem like any other: fail THIS
            # delivery, name the field and the cap, and let the rest of the
            # pass mint (see _OUTPUT_CAPS).
            problems.append(
                f"{field_name}: rendered to {len(text)} characters, over the "
                f"{cap}-character cap"
            )
            continue
        rendered[field_name] = text
    if problems:
        return RenderFailure(
            policy_id=entry.policy_id,
            event_id=consequence.envelope.event_id,
            detail=(
                f"policy {entry.policy_id!r} could not be rendered for event "
                f"{consequence.envelope.event_id!r} "
                f"({consequence.envelope.event_type.value}): " + "; ".join(problems)
            ),
            missing=tuple(sorted(missing)),
        )
    # Neutralize any forged heading BEFORE appending the genuine block (see
    # NEUTRALIZED_PROVENANCE_HEADING): the rendered body is template-derived
    # text, and the one invariant a reader gets is that the un-escaped heading
    # appears exactly once, on the block this plugin wrote.
    body_text = rendered["body_template"].replace(
        PROVENANCE_HEADING, NEUTRALIZED_PROVENANCE_HEADING
    )
    return MintRequest(
        tenant=consequence.tenant,
        scope=entry.destination.scope,
        initiative=entry.destination.initiative,
        phase=entry.destination.phase,
        title=rendered["title_template"],
        # The provenance block is appended REGARDLESS of the template (spec §7:
        # "every minted item body carries provenance"), separated by a blank
        # line so the policy's own words stay the first thing read.
        body=f"{body_text}\n\n{provenance_block(consequence)}",
        human_owned=entry.human_owned,
        musher_dispatch=entry.musher_dispatch,
        owner=rendered.get("owner_template"),
        dedup_key=consequence.dedup_key,
        policy_id=entry.policy_id,
        policy_version_id=consequence.policy_version_id,
        event_id=consequence.envelope.event_id,
    )

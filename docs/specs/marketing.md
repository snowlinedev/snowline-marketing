# Snowline Marketing Plugin — Implementation Spec

Status: draft. Implements the generalized marketing plugin product contract
(governance artifact `b964d217`, revision `dfda09fa` — adds channel
publishing). Design calls settled with the owner 2026-07-18 (governance
decision on `snowlinedev/snowline`): own repo, provenance by
watch-and-quarantine, policies as governance artifacts. Registered as
governance artifact `390f3b14` — revise BOTH on change.

## 1. Purpose

Turn canonical Snowline state changes — PM lifecycle events, governance
artifact revisions, recurring schedules — into scoped marketing
follow-through, staleness signals, and audit trails, without creating a
second backlog and without generating marketing content itself.

The plugin is a **deterministic policy machine**: events in, work items and
signals out. There is no LLM and no copywriting inside the plugin. Content
work happens in the minted PM items — done by the operator, or by
musher-dispatched runs when an item is opted into autonomous dispatch. The
intelligence lives at the edges: governance holds canonical messaging;
dispatched runs draft deliverables.

Tenant policies and marketing content are configuration and governance
artifacts. No organization-specific marketing rules in plugin code; Turtle's
Edge is the first configuration, Snowline itself is the intended second.

## 2. Service shape

- Own repo (`snowlinedev/snowline-marketing`), own scope, PM-dogfooded
  development — the plugin that consumes PM events is built through them.
- Python uv project modeled on musher: FastAPI service, loopback bind
  (default port 8805), always serves `/health`, registers with the platform
  (`SNOWLINE_PLATFORM_URL`) on boot.
- Off by default: `MARKETING_ENABLED=1` enables the intake and evaluation
  loops. Disabled, the service still serves health + read-only audit surfaces.
- Calls other plugins (PM, governance) through the Snowline gateway as an
  ordinary client; no direct table access anywhere (contract requirement).

## 3. Boundaries (what it never does)

- Never **generates** marketing content — content is canonical in
  governance; the plugin mints work about it and, where a channel adapter
  exists, publishes the approved content verbatim (§12). Publishing is
  never implicit: adapter pushes run only from an explicit operator command
  or an approval-gated policy consequence.
- Never marks work complete because generation ran.
- Never routes across isolated organization scopes.
- Never executes tenant-supplied code — policy predicates are declarative
  data, evaluated deterministically.
- Never infers the roadmap from GitHub naming conventions; PM semantics are
  the trigger source.

## 4. Data model (plugin-owned)

- **Delivery ledger** — one row per consumed event × matched policy:
  logical key `tenant + policy_id + event_id`, outcome
  (matched / claimed / created / awaiting_approval / dry_run / ignored /
  quarantined / failed; `deduplicated` is a delivery-level answer, never a
  row state), created item ref, evaluated policy artifact version id,
  timestamps. Row states beyond the original enumeration exist because
  minting is two durable steps: `claimed` is the compare-and-set marker
  written BEFORE the mint call (closing the double-mint crash window; a
  fresh claim is an in-flight mint, a stale one surfaces for
  reconciliation), `awaiting_approval` parks an approval-gated match
  (drained by §12's operator verb or by the policy's mode being revised to
  active), and `dry_run` terminally closes a match whose mode minted
  nothing on purpose. `failed` is the dead-letter; declared operator verbs
  (`replay`, `release_stale_claim`) are its only exits. Minting happens per
  event BEFORE the intake ack, so creation and ledger write are recoverably
  convergent; re-delivery returns the existing result.
- **Deliverable provenance ledger** — one row per deliverable instance:
  channel, deliverable class, source artifact version ids (carrying their
  milestone stamps), producing item ref, produced_at, external URL. Logical
  key `tenant + producing item ref + channel + deliverable class` — the
  producing ITEM, not the producing event, so an item reopened and completed
  again re-declares one deliverable rather than accumulating a row per
  completion. The write is an UPSERT, deliberately unlike the delivery
  ledger's: that row is a claim on work that may already have been done, while
  this one is a statement of fact about what was produced, so the latest
  declaration is the truth and a re-delivery converges onto it. Source
  artifact versions live in an association table — one row per artifact, one
  version per artifact per deliverable, with its milestone stamp — rather than
  a JSON column, because §8's sweep compares PER version id and those ids are
  therefore queryable facts. `produced_at` is the completion event's
  `occurred_at`, never a producer-declared time.
- **Quarantine** — two populations, one operator surface, separate storage.
  Provenance-missing (and provenance-malformed) COMPLETIONS of
  marketing-minted items key on `tenant + event id`, so at-least-once
  re-delivery converges to one open row and a row an operator already closed
  can never be silently reopened by the stream; each keeps the completion
  event whole and carries an operator-visible reason plus status
  open/resolved/dismissed. The operator verbs are guarded transitions:
  `resolve` attaches provenance after the fact — closing the row FIRST (the
  attached declaration is persisted on it) and writing the deliverable rows
  second, so a crash between them re-applies from the row's own stored
  declaration — `requeue` re-runs the watch's classification over the stored
  completion (recording and resolving when the declaration now parses,
  refreshing the open row's reason when it still does not), and `dismiss`
  records the judgment that there was no deliverable. Malformed/unmapped
  EVENTS are the second population and a separate table: an unparseable
  envelope may carry no tenant and no event id at all, so its identity is the
  source's `(source_key, position)` and its verb is requeue-the-raw-bytes. It
  lands with the §11 surfaces that read it; the intake loop's `on_malformed`
  seam is the durable handoff until then.
- **Policy cache** — resolved policy bodies keyed by governance artifact
  version id; the ledger records the exact version evaluated.
- **Cursor state** — per-source consumer cursors (PM outbox cursor,
  governance poll watermark).

## 5. Event intake

Primary source: PM's durable lifecycle event outbox (snowline-pm #64) —
at-least-once delivery, acknowledged by stable event id against a cursor.
Event types consumed at v1: item completed / reopened / abandoned /
re-scoped, initiative phase completed, milestone state changed / released,
recurring item fired, explicit semantic signals.

**Fixtures mode is a first-class dev/CI surface, not a shim**: the intake
loop runs identically against captured event fixtures (JSON envelopes on
disk) and the live outbox. The deterministic core — policy evaluation,
dedup, ledger — is built and tested fixtures-first, so the #64 outbox is an
integration point, not a build prerequisite.

Governance is **polled, not evented**, at v1: a scheduled sweep compares
artifact leaves/version ids against the deliverable provenance ledger and
the policy cache watermark. Staleness is not latency-sensitive; no second
event spine gets built before PM's exists. Revisit if governance grows a
plugin-consumable outbox.

## 6. Policy model

Policies are **governance artifacts** (inline, `doc_kind: reference`), one
policy-set artifact per tenant org scope, revised through
`revise_artifact` like any governed doc — versioning, review, and decision
trail come free. The plugin resolves the current version through the
gateway, caches it, and records the evaluated version id on every ledger
row (contract: deterministic evaluation, exact version recorded).

A policy entry (schema versioned inside the body):

- event selectors (event types) and predicates over scope, initiative,
  phase, milestone, work kind, relations, semantic signals — declarative
  values/globs, no code;
- consequence type: messaging refresh, listing regeneration, screenshot
  review, announcement preparation, launch plan, review sweep, metrics
  snapshot, channel publish (§12; never legal in mode `active` —
  publishing is approval-gated, and a policy declaring otherwise
  quarantines);
- destination scope / initiative / phase for minted work, title/body/
  ownership templates, `human_owned` and musher-dispatch opt-in flags;
- affected artifact refs, channels, deliverable classes;
- dedup-key template (default `tenant + policy_id + event_id`);
- mode: active / approval-required / dry-run.

Malformed policy versions quarantine the *policy*, never silently match-all
or match-none.

## 7. Follow-through creation

Minted through PM's surface (gateway), landing on the canonical roadmap.
Every minted item body carries provenance: originating event + entity,
matched policy + version, source scope/initiative/milestone, external refs
(reconciled PR / release URL) when present, affected artifacts/channels,
and the delivery ledger key. The provenance block is appended by the
plugin regardless of template, and template-derived text is neutralized
against the block's own grammar so provenance cannot be forged by a
producer or a template. Title/body/ownership templates render from a
defined vocabulary: the delivery identity and the envelope's predicate
surface as closed placeholders, plus open `{details.<key>}` access; a
template that cannot render for an event is a per-delivery failure
(dead-lettered), never a crashed pass. No standalone GitHub marketing
issues — GitHub involvement stays PM mirroring.

Items minted with the policy's dispatch opt-in flow to musher through PM's
watcher (snowline-pm #65/#66) — the plugin sets the flag, it does not call
musher directly.

## 8. Deliverable provenance and staleness

**Watch-and-quarantine** (settled 2026-07-18): completing a marketing-minted
item is watched via the same PM event stream; a completion carrying a
provenance payload (channel, deliverable class, source artifact versions,
URL) upserts the deliverable ledger; a completion without one lands in
quarantine — visible, auditable, resolvable by attaching provenance after
the fact. No hard completion gate, no friction on the PM verb itself.

The watch recognizes a marketing-minted completion by the DELIVERY LEDGER's
`created_item_ref` — the ref written at mint time (§7), and the authoritative
statement that this plugin created that item. Completions whose subject matches
no `created` row are ordinary roadmap work and pass through silently: the
plugin consumes the whole lifecycle stream (§5), so most completions it sees
belong to other people's work. The provenance payload rides
`payload.details.deliverable_provenance` — a versioned sub-document in the
free-form half of the envelope (§5), listing one or more deliverables (one
completion may have produced a listing update AND a screenshot set) — and is
read with the house never-raises classification: a malformed declaration is
quarantine-with-a-reason-naming-the-defect, never a crash and never silently
treated as absent. The quarantine row keeps the completion whole, so an
operator can resolve it by attaching provenance, requeue it through the same
classification after a reader-side fix, or dismiss it (§4's verbs). The watch
runs as a handler in the same per-event,
before-the-ack composition minting uses, so an acked completion is one whose
deliverable rows (or quarantine row) are durable; a watch store failure stalls
the pass and the completion re-delivers, because the watch may never DROP an
observation even though it may never BLOCK a completion.

The staleness sweep compares, per channel/deliverable class:

- source artifact current version vs the version recorded in deliverable
  provenance (the milestone stamp from Snowline#141 gives the release
  boundary — "listing reflects the v1-stamped feature list, but a
  v2-stamped version now exists");
- release milestone events vs deliverable produced_at;
- asset freshness for screenshot classes, delegated to the asset plugin
  (walkthrough-mcp) for capture — the marketing plugin only tracks
  staleness and mints review work.

Findings mint (deduplicated) staleness items through the same policy
machinery; a finding whose deliverable is already covered by an open minted
item does not double-file.

## 9. Feature signaling

Durable contract: an explicit marketing-impact semantic signal on the PM
item, set at triage (a small PM-side addition, analogous to `human_owned`),
carried on completion events. Until PM exposes it, v1 uses the contract's
compatibility path: an explicit item relation (`marketing-impact`) that the
plugin reads from the event's relation set. The compatibility path is
config-visible and replaceable; it never falls back to matching every
implementation item.

## 10. Recurring policies

Calendar-driven marketing work (monthly review/metrics cycles — the
currently hand-held August-2026 loop) rides PM's recurring-work machinery:
a schedule mints the PM item, the fired-event matches a recurring policy,
and the plugin does any enrichment. The plugin owns no scheduler.
(Depends on the PM recurring-work management tools, snowline-pm item
`bfd1c5fe`.)

## 11. Operator surfaces

- Dashboard contribution via the platform's declarative `ui` manifest block
  (stat/list/table kinds; `/ui-api/marketing/` data plane): delivery-ledger
  audit (received / ignored / matched / created / deduplicated / failed),
  quarantine with reasons, dead-letter, staleness overview per channel.
- Dry-run: evaluate a policy version against captured fixtures, report
  what would have been minted, mint nothing.
- Retry: transient failures with bounded backoff; permanent failures to
  dead-letter with replay.

## 12. Channel publishing (App Store first)

The plugin can **apply** approved canonical content to external channels
through per-channel **publisher adapters** — provider-agnostic (the
external-task-sink provider pattern), App Store Connect first, website and
others later. Publishing is application of governed content, never
generation: the render step maps the canonical artifact (store-listing
fields, keywords, promotional text, what's-new) plus a channel template to
the channel payload, deterministically.

- **App Store Connect adapter (v1):** pushes listing marketing metadata and
  screenshot sets (screenshots produced by the asset plugin, referenced by
  the deliverable ledger) via the ASC API. Tenant API key is secret config;
  no key, adapter dormant. Marketing metadata ONLY — never binaries,
  build submission, or release/version state; the app release pipeline
  owns those.
- **Dry-run diff:** a publish evaluated in dry-run fetches live channel
  state and reports an exact field-level diff, pushing nothing. This is the
  approval surface.
- **Approval-gated:** publish consequences default `approval-required`; an
  explicit operator verb executes them. Every push writes an audit row
  (payload hash, source artifact versions, diff summary, outcome).
- **Provenance closes automatically on this path:** a successful publish
  upserts the deliverable-provenance ledger directly (channel, class,
  source artifact versions with milestone stamps, timestamp) — quarantine
  (§8) remains for human/agent-produced deliverables only.
- **Failure posture:** bounded retry for transient ASC errors; partial
  pushes recorded per field-group; permanent failures dead-letter with
  replay.

## 13. Dependencies and sequencing

| Dependency | What it unblocks | Build posture |
|---|---|---|
| snowline-pm #64 (durable lifecycle events) | live intake | fixtures-first until it lands |
| snowline-pm `20d77c95` (explicit milestone release) | release-checklist + release staleness boundary | policy defined now, dormant until the event exists |
| Snowline#141 (milestone stamp on ArtifactVersion) | staleness release boundary | sweep works without stamps (version compare only), better with |
| snowline-pm #65/#66 (musher dispatch opt-in + provider) | autonomous content runs | opt-in flag is inert until they land |
| snowline-pm `bfd1c5fe` (recurring CRUD tools) | operator-managed recurring policies | recurring policies dormant until schedules manageable |
| App Store Connect API key (tenant secret) | listing publishing (§12) | adapter dormant until configured |

Build order: (1) service skeleton + registration + health; (2) fixtures
intake + policy engine + delivery ledger + dry-run (the deterministic
core); (3) minting through PM + dedup; (4) provenance watch + quarantine +
staleness sweep; (5) dashboard surfaces; (6) live outbox cutover when #64
lands; (7) App Store Connect publisher adapter (dry-run diff before first
real push).

## 14. Acceptance criteria

Carried from the contract, implementation-refined:

- Duplicate delivery of the same event creates exactly one result
  (ledger-proven, tested at the fixtures layer).
- Feature triggering keys on PM semantics (signal/relation), never GitHub
  titles.
- Routing is configuration-driven and isolation-safe; cross-tenant fixtures
  are rejected with quarantine, not silently dropped.
- Minted items appear on the canonical roadmap with full provenance.
- Staleness findings cite the exact source artifact versions and recorded
  deliverable provenance they compared.
- A provenance-less completion is visible in quarantine within one sweep.
- Unmatched events audit as `ignored` and create no work.
- Dead-lettered deliveries replay safely (dedup holds).
- TurtleTracks and a second tenant (Snowline itself) run on the same code
  with separate policy artifacts.
- A publish dry-run reports the exact field-level diff against live channel
  state and pushes nothing; a real publish is approval-gated, audited, and
  records deliverable provenance atomically or recoverably convergently.
- Publisher adapters push marketing metadata only — never binaries, build
  submission, or release state.

# snowline-marketing

**Marketing follow-through plugin for [Snowline](https://github.com/snowlinedev/Snowline).**

A deterministic policy machine that turns canonical Snowline state changes —
PM lifecycle events, governance artifact revisions, recurring schedules —
into scoped marketing work items, staleness signals, and audit trails.
Events in, work out. No LLM and no copywriting inside the plugin: content
work happens in the minted PM items, done by the operator or by
musher-dispatched runs.

- **Policies are governance artifacts** — versioned tenant configuration,
  revised through governance like any governed doc. No organization-specific
  rules in plugin code.
- **Provenance by watch-and-quarantine** — completing a marketing-minted
  item with a provenance payload updates the deliverable ledger; completing
  without one lands in an operator-visible quarantine.
- **Staleness, not regeneration** — the plugin compares source artifact
  versions (with their release-milestone stamps) against recorded
  deliverable provenance and mints review work; it never generates content.
- **Publishing applies, never authors** — per-channel publisher adapters
  (App Store Connect first) push approved canonical content to external
  channels: approval-gated, dry-run diffable, marketing metadata only.

## Status

Spec-first, pre-implementation. See
[`docs/specs/marketing.md`](docs/specs/marketing.md) for the governing
implementation spec, which implements the generalized marketing plugin
product contract (Snowline governance artifact `b964d217`).

## License

Apache-2.0

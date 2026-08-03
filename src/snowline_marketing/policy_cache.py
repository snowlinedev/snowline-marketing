"""The policy cache — resolved policy bodies, by governance version id
(spec §4).

Spec §6's loop is: resolve the current policy version through the gateway,
cache it, evaluate it, and record the exact version id on every ledger row.
This module is the middle step. It stores what a version CONTAINED (the body,
verbatim) and how that body CLASSIFIED (valid, or quarantined with a reason) —
keyed by the governance artifact version id, which is also the cache key, the
ledger's `evaluated policy artifact version id`, and the thing an operator
quotes when revising a broken artifact.

Two design points worth stating, because both look like omissions:

**The upsert is guarded on TENANT, not on content.** `cursors.DbCursorStore.ack`
guards against rewinds; the racing-writer story here is different. For
GOVERNANCE-issued version ids — immutable content addresses — two writers on
the same key necessarily write the same body and last-writer-wins converges,
so no content guard is needed. But version ids are the CALLER's to choose on
the fixtures/dry-run path (`InMemoryPolicyProvider`), and nothing stops two
tenants' dry-runs reusing a readable id like `pv-0001` — an unguarded upsert
would let the second silently rewrite the first tenant's row, stealing the
version out of its listing and re-pointing any ledger row that recorded it at
the other tenant's policy text. So the conflict update applies only when the
row's tenant matches; a cross-tenant collision is REFUSED (logged, row
unchanged) rather than absorbed. `fetched_at` still advances on every
same-tenant re-fetch — the later fetch is the right answer for it.

**No "current version" column, and no invalidation.** Which version is current
is governance's answer, re-resolved per sweep (spec §5: governance is polled,
not evented). If this table also held a current-pointer it would be a second,
staler answer to a question that already has an authority — and the failure
mode would be evaluating a superseded policy version while recording it on the
ledger as though it were in force. Rows here therefore accumulate: the ledger
references old version ids forever, and an audit row that cannot reach the
policy it was evaluated against is not an audit row.

**Read re-parses.** `CachedPolicySet.parse()` runs `parse_policy_set` over the
stored text rather than storing a serialized model. The stored `outcome` is an
audit fact — how this version classified when it was fetched, which is what
§11's dashboard lists and what an operator's timeline needs — while the engine
always evaluates against today's parser. When a schema fix ships, a re-fetch
re-classifies; nothing has to be migrated, and no stale parse silently governs.

`PolicyCacheStore` is the protocol `engine.resolve_policy_set` actually
depends on — `put`, the only method it calls. `InMemoryPolicyCache` is the
second implementation: held in the process rather than Postgres, so spec
§11's dry-run can classify a candidate policy body through the exact same
`parse_policy_set` call production makes without writing a row to
`policy_cache`. It shares `PolicyCache.put`'s classification via `_classify`
and mirrors its tenant-guarded upsert (see "The upsert is guarded on TENANT,
not on content" above), because that guard is precisely the one a dry-run
must not weaken — the whole point of testing a candidate against captured
fixtures is that the classification it reports is the one production would
give the same body.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from snowline_marketing.db import session_scope
from snowline_marketing.models import CachedPolicySet as CachedPolicySetRow
from snowline_marketing.policies import (
    MalformedPolicyReason,
    MalformedPolicySet,
    ParsedPolicySet,
    parse_policy_set,
)
from snowline_marketing.policy_source import ResolvedPolicySet


class ParseOutcome(enum.StrEnum):
    """How a cached body classified, as stored in `policy_cache.parse_outcome`.

    Two values because there are two states an operator acts on differently:
    `valid` is evaluable, `quarantined` means the engine must REFUSE to
    evaluate this tenant (spec §6 — never a silent match-all or match-none).
    Deliberately not the `MalformedPolicyReason` enum: the reason is the
    detail, the outcome is the decision."""

    valid = "valid"
    quarantined = "quarantined"


@dataclass(frozen=True)
class CachedPolicyVersion:
    """One cache row, read back.

    A plain record rather than the ORM object so callers hold something that
    outlives the session (and cannot lazily emit SQL from inside the engine's
    evaluation loop)."""

    version_id: str
    tenant: str
    body: str
    outcome: ParseOutcome
    fetched_at: datetime
    quarantine_reason: str | None = None
    quarantine_detail: str | None = None

    def parse(self) -> ParsedPolicySet:
        """Re-derive the policy model from the stored body (see the module
        docstring on why this is not stored pre-parsed). Carries the version id
        through, so a `MalformedPolicySet` produced here still names the
        governance version an operator has to revise.

        `expected_tenant` is the ROW's tenant — the same cross-check `put`
        applied at write time. Without it, a row stored as
        quarantined/tenant_mismatch would re-parse here as a fully valid
        `PolicySet` for the OTHER tenant, and the engine would evaluate across
        the §3/§14 boundary that the write-side check exists to hold."""
        return parse_policy_set(
            self.body, version_id=self.version_id, expected_tenant=self.tenant
        )


def _classify(
    resolved: ResolvedPolicySet,
) -> tuple[ParsedPolicySet, ParseOutcome, str | None, str | None]:
    """Parse and classify one resolved body — the (outcome, reason, detail)
    triple both `PolicyCache.put` and `InMemoryPolicyCache.put` persist.

    Shared for the reason `_namespaced_key` is shared in `ledger.py`: the
    classification is the one thing a dry-run's quarantine verdict must not
    disagree with production about, so it exists in exactly one place rather
    than two calls to `parse_policy_set` that could drift in what they pass
    it. Reason/detail are `None` on the valid path — enforced downstream by
    `ck_policy_cache_quarantine_reason` for the real store; a leftover reason
    from an earlier classification would make a healthy policy read as broken
    on the operator surface."""
    parsed = parse_policy_set(
        resolved.body,
        ref=resolved.artifact_id,
        version_id=resolved.version_id,
        expected_tenant=resolved.tenant,
    )
    if isinstance(parsed, MalformedPolicySet):
        return parsed, ParseOutcome.quarantined, parsed.reason.value, parsed.detail
    return parsed, ParseOutcome.valid, None, None


def _collision_refusal(
    version_id: str, holder_tenant: str | None, tenant: str, body: str
) -> MalformedPolicySet:
    """The refusal both stores hand back when a caller-chosen version id
    collides across tenants (module docstring: "The upsert is guarded on
    TENANT, not on content").

    Shared for the reason `_classify` is: the refusal's shape — reason
    `version_collision`, a detail naming the version id and BOTH tenants (the
    row's holder and the requester), the row left unchanged — is precisely the
    behavior a dry-run must not weaken, so it exists in exactly one place
    rather than two hand-copied constructions that could drift."""
    return MalformedPolicySet(
        reason=MalformedPolicyReason.version_collision,
        detail=(
            f"version id {version_id!r} is already cached for "
            f"tenant {holder_tenant!r}; refused for tenant {tenant!r} — "
            "the row is unchanged and the audit join is broken "
            "until the collision is resolved"
        ),
        raw=body,
        version_id=version_id,
        tenant=tenant,
    )


class PolicyCacheStore(Protocol):
    """What `engine.resolve_policy_set` needs from a policy cache — `put`,
    and nothing else it ever calls.

    `PolicyCache` (Postgres) and `InMemoryPolicyCache` (spec §11's dry-run)
    both satisfy this without inheriting from it — the engine takes the
    protocol so a dry-run can classify a candidate policy body without a row
    ever landing in `policy_cache`, and `engine.py` never has to import, or
    know about, the dry-run at all."""

    def put(self, resolved: ResolvedPolicySet) -> ParsedPolicySet: ...


class PolicyCache:
    """The `policy_cache` table (spec §4).

    One method to write, two to read. `put` is a single-statement Postgres
    upsert for the same reason the cursor store's ack is — the first fetch of a
    version and every later one take one code path — but unguarded (see the
    module docstring). `put` also OWNS classification: it parses the resolved
    body itself (tenant cross-checked) and returns the result, so no caller
    can persist a classification derived from a different body."""

    def put(self, resolved: ResolvedPolicySet) -> ParsedPolicySet:
        """Cache one resolved version, classifying it HERE.

        Takes the `ResolvedPolicySet` straight from the provider and runs
        `parse_policy_set` itself (via `_classify`) — with `expected_tenant`
        set from the resolution, so a body declaring a different tenant
        quarantines as `tenant_mismatch` instead of caching one tenant's
        rules under another's name. Parsing inside `put` (and returning the
        result for the caller to evaluate) is what makes the row's
        classification UNABLE to disagree with its body: there is no API
        through which a caller can hand in a parsed result derived from
        something else.

        When the tenant-guarded upsert REFUSES the write (the version id is
        already another tenant's row), the parsed set is NOT returned even if
        it was valid: the caller gets a `MalformedPolicySet` with reason
        `version_collision`, and the tenant stalls as quarantined rather than
        evaluating a version whose audit join points at someone else's
        policy text."""
        version_id = resolved.version_id
        tenant = resolved.tenant
        body = resolved.body
        parsed, outcome, reason, detail = _classify(resolved)
        log_ = logging.getLogger("snowline_marketing.policy_cache")
        statement = pg_insert(CachedPolicySetRow).values(
            version_id=version_id,
            tenant=tenant,
            body=body,
            parse_outcome=outcome.value,
            quarantine_reason=reason,
            quarantine_detail=detail,
        )
        with session_scope() as session:
            applied = session.execute(
                statement.on_conflict_do_update(
                    index_elements=[CachedPolicySetRow.version_id],
                    set_={
                        "tenant": statement.excluded.tenant,
                        "body": statement.excluded.body,
                        "parse_outcome": statement.excluded.parse_outcome,
                        "quarantine_reason": statement.excluded.quarantine_reason,
                        "quarantine_detail": statement.excluded.quarantine_detail,
                        # `fetched_at` deliberately keeps its INSERT default on
                        # the conflict path too — the column answers "when did
                        # we last see this version?", and a re-fetch that left
                        # it frozen at the first sighting would make the policy
                        # sweep look stalled.
                        "fetched_at": statement.excluded.fetched_at,
                    },
                    # The cross-tenant guard (see module docstring): a
                    # caller-chosen id colliding across tenants must not
                    # rewrite another tenant's row.
                    where=CachedPolicySetRow.tenant == statement.excluded.tenant,
                )
                # RETURNING, not `rowcount`: under psycopg an upsert reports
                # rowcount -1, so the `== 0` test this guard used to make was
                # never true and the refusal below was silently unreachable. A
                # suppressed conflict update returns NO row; an insert or an
                # applied update returns one.
                .returning(CachedPolicySetRow.version_id)
            )
            if applied.first() is None:
                # The conflict row belongs to ANOTHER tenant — refused, loudly,
                # and NOT returned as the parsed set. A valid parse handed back
                # from here would evaluate against a version the cache refused
                # to record, so every ledger row naming this version id would
                # join to the OTHER tenant's policy text — a broken audit join
                # is worse than a stalled tenant. Returned as a
                # `MalformedPolicySet` instead, which the engine's existing
                # quarantine path turns into a visible PolicyQuarantined stall
                # until the collision is resolved. Only reachable with
                # caller-chosen ids (dry-run/fixtures); governance ids are
                # unique by construction.
                holder = session.scalar(
                    select(CachedPolicySetRow.tenant).where(
                        CachedPolicySetRow.version_id == version_id
                    )
                )
                log_.warning(
                    "policy cache put refused: version id %r already cached "
                    "for a different tenant (requested for %r) — row unchanged",
                    version_id,
                    tenant,
                )
                return _collision_refusal(version_id, holder, tenant, body)
        return parsed

    def get(self, version_id: str) -> CachedPolicyVersion | None:
        """The cached version, or None when it was never fetched."""
        with session_scope() as session:
            row = session.get(CachedPolicySetRow, version_id)
            if row is None:
                return None
            return _to_record(row)

    def list_for_tenant(self, tenant: str) -> list[CachedPolicyVersion]:
        """Every version cached for one tenant, newest fetch first — the §11
        operator listing ("which policy versions has this tenant had, and which
        are quarantined"). Rides `ix_policy_cache_tenant`."""
        statement = (
            select(CachedPolicySetRow)
            .where(CachedPolicySetRow.tenant == tenant)
            .order_by(CachedPolicySetRow.fetched_at.desc())
        )
        with session_scope() as session:
            return [_to_record(row) for row in session.scalars(statement)]


def _to_record(row: CachedPolicySetRow) -> CachedPolicyVersion:
    return CachedPolicyVersion(
        version_id=row.version_id,
        tenant=row.tenant,
        body=row.body,
        # The column is a plain String (no native PG enum — see models.py); a
        # value outside the enum means someone wrote the table by hand, and
        # StrEnum's lookup raising here is the right, loud answer.
        outcome=ParseOutcome(row.parse_outcome),
        fetched_at=row.fetched_at,
        quarantine_reason=row.quarantine_reason,
        quarantine_detail=row.quarantine_detail,
    )


class InMemoryPolicyCache:
    """A policy cache held in the process — spec §11's dry-run cache.

    Not a mock: `put` runs the exact same `_classify` (therefore the exact
    same `parse_policy_set` call, tenant cross-check included) that
    `PolicyCache.put` does, so a candidate body's verdict — valid, or
    quarantined with a reason — is the one production would give it. What
    differs is only where the row lands: a dict, never `policy_cache`.

    Mirrors the real store's TENANT-GUARDED upsert (module docstring: "The
    upsert is guarded on TENANT, not on content"), because that guard exists
    precisely for the caller-chosen ids this store's only caller uses — two
    dry-runs (or a dry-run and a fixture-driven test) reusing a readable
    version id like `pv-0001` must not let the second silently steal the row
    out from under the first tenant's listing. A cross-tenant collision is
    therefore REFUSED here too (a `version_collision` `MalformedPolicySet`,
    row unchanged), never absorbed — same outcome shape as `PolicyCache.put`,
    minus the logged warning (there is no operator-facing log for a process
    that exits with the dry-run).

    Satisfies `PolicyCacheStore`, which is all `resolve_policy_set` requires;
    `get` mirrors `PolicyCache.get` for symmetry and for tests that want to
    assert on what a dry-run classified without re-deriving it."""

    def __init__(self) -> None:
        self._rows: dict[str, CachedPolicyVersion] = {}

    def put(self, resolved: ResolvedPolicySet) -> ParsedPolicySet:
        """See `PolicyCacheStore.put` / `PolicyCache.put`."""
        parsed, outcome, reason, detail = _classify(resolved)
        existing = self._rows.get(resolved.version_id)
        if existing is not None and existing.tenant != resolved.tenant:
            # The tenant-guarded refusal (see class docstring): the row
            # belongs to another tenant, so this put does not touch it.
            return _collision_refusal(
                resolved.version_id, existing.tenant, resolved.tenant, resolved.body
            )
        self._rows[resolved.version_id] = CachedPolicyVersion(
            version_id=resolved.version_id,
            tenant=resolved.tenant,
            body=resolved.body,
            outcome=outcome,
            fetched_at=datetime.now(timezone.utc),
            quarantine_reason=reason,
            quarantine_detail=detail,
        )
        return parsed

    def get(self, version_id: str) -> CachedPolicyVersion | None:
        """See `PolicyCache.get`."""
        return self._rows.get(version_id)

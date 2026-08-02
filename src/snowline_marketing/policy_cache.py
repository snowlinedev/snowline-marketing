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

**No guarded upsert.** `cursors.DbCursorStore.ack` guards its `ON CONFLICT`
with a rewind check, because a stale racing writer there would move a cursor
BACKWARD and re-deliver a span. Nothing analogous exists here: a governance
artifact version id is an immutable content address, so two writers racing on
the same key are necessarily writing the SAME body, and last-writer-wins
converges on the identical row. The only column that moves is `fetched_at`, and
the later of two concurrent fetches is the right answer for it. A guard would
be ceremony protecting against a state that cannot occur.

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
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from snowline_marketing.db import session_scope
from snowline_marketing.models import CachedPolicySet as CachedPolicySetRow
from snowline_marketing.policies import (
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
        governance version an operator has to revise."""
        return parse_policy_set(self.body, version_id=self.version_id)


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
        `parse_policy_set` itself — with `expected_tenant` set from the
        resolution, so a body declaring a different tenant quarantines as
        `tenant_mismatch` instead of caching one tenant's rules under
        another's name. Parsing inside `put` (and returning the result for
        the caller to evaluate) is what makes the row's classification
        UNABLE to disagree with its body: there is no API through which a
        caller can hand in a parsed result derived from something else."""
        version_id = resolved.version_id
        tenant = resolved.tenant
        body = resolved.body
        parsed = parse_policy_set(
            body,
            ref=resolved.artifact_id,
            version_id=version_id,
            expected_tenant=tenant,
        )
        if isinstance(parsed, MalformedPolicySet):
            outcome = ParseOutcome.quarantined
            reason: str | None = parsed.reason.value
            detail: str | None = parsed.detail
        else:
            outcome = ParseOutcome.valid
            # NULL on the valid path, enforced by
            # `ck_policy_cache_quarantine_reason`: a leftover reason from an
            # earlier classification would make a healthy policy read as broken
            # on the operator surface.
            reason = None
            detail = None
        statement = pg_insert(CachedPolicySetRow).values(
            version_id=version_id,
            tenant=tenant,
            body=body,
            parse_outcome=outcome.value,
            quarantine_reason=reason,
            quarantine_detail=detail,
        )
        with session_scope() as session:
            session.execute(
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
                )
            )
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

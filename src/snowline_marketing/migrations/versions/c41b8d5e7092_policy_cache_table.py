"""policy model: policy_cache table

Creates `policy_cache` (spec §4, "Policy cache"): one row per resolved
governance artifact VERSION, holding the policy body verbatim and how it
classified. Columns are spelled literally here rather than imported from
`models` — a migration must describe the schema as of ITS revision, and an app
model that drifts later would silently rewrite history.

Revision ID: c41b8d5e7092
Revises: 7a09f4f3eaed
Create Date: 2026-08-01

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c41b8d5e7092"
down_revision: str | None = "7a09f4f3eaed"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "policy_cache",
        # The governance artifact VERSION id — an immutable content address,
        # which is why this is the key and why the upsert needs no guard.
        sa.Column("version_id", sa.String(length=255), primary_key=True),
        # The tenant org scope the version governs. Denormalized out of the
        # body because a quarantined body may be unparseable, and "whose
        # policy broke?" must still be answerable.
        sa.Column("tenant", sa.Text(), nullable=False),
        # The artifact body VERBATIM. Text, not JSONB: a quarantined body may
        # not be JSON at all, and JSONB is lossy (key order, duplicates,
        # number normalization) against an artifact an operator wants to diff.
        sa.Column("body", sa.Text(), nullable=False),
        # 'valid' | 'quarantined' — the audit classification as of the fetch.
        # A plain String with an app-side enum, not a native PG ENUM type
        # (adding a value later would need its own ALTER TYPE migration).
        sa.Column("parse_outcome", sa.String(length=16), nullable=False),
        # The MalformedPolicyReason value and the detail naming the offending
        # entry/field; NULL on the valid path.
        sa.Column("quarantine_reason", sa.Text(), nullable=True),
        sa.Column("quarantine_detail", sa.Text(), nullable=True),
        # timestamptz, so "when did we last see this version?" reads the same
        # on any server/session timezone.
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        # A quarantined row with no reason is an operator staring at a broken
        # policy with nothing to fix, and a valid row carrying a stale reason
        # reads as broken when it is not. Both are invariants, so both are
        # enforced by the database rather than by every future writer
        # remembering.
        sa.CheckConstraint(
            "(parse_outcome = 'valid' AND quarantine_reason IS NULL "
            "AND quarantine_detail IS NULL) "
            "OR (parse_outcome = 'quarantined' AND quarantine_reason IS "
            "NOT NULL AND quarantine_detail IS NOT NULL)",
            name="ck_policy_cache_quarantine_reason",
        ),
    )
    # Not unique: a tenant accumulates one row per version it has ever had
    # (the ledger references old version ids forever). Serves the §11
    # per-tenant operator listing.
    op.create_index("ix_policy_cache_tenant", "policy_cache", ["tenant"])


def downgrade() -> None:
    op.drop_index("ix_policy_cache_tenant", table_name="policy_cache")
    op.drop_table("policy_cache")

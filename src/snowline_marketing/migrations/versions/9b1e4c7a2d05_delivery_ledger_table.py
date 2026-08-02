"""evaluation engine: delivery_ledger table

Creates `delivery_ledger` (spec §4, "Delivery ledger"): one row per consumed
event × matched policy, keyed by (tenant, rendered dedup key) — the uniqueness
that makes at-least-once re-delivery converge instead of duplicating work.
Columns are spelled literally here rather than imported from `models` — a
migration must describe the schema as of ITS revision, and an app model that
drifts later would silently rewrite history.

Revision ID: 9b1e4c7a2d05
Revises: c41b8d5e7092
Create Date: 2026-08-01

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9b1e4c7a2d05"
down_revision: str | None = "c41b8d5e7092"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "delivery_ledger",
        # The isolation boundary, and the first component of the primary key:
        # a tenant may author a dedup template that omits {tenant}, and
        # uniqueness on the rendered string alone would let one tenant's
        # delivery read back another's row.
        sa.Column("tenant", sa.String(length=255), primary_key=True),
        # The rendered dedup key — the policy's template filled from the
        # envelope, or one of the engine's reserved event-level shapes. Text:
        # the template is tenant-authored, so any width picked here would be a
        # policy limit invented in the schema layer.
        sa.Column("dedup_key", sa.Text(), primary_key=True),
        # NULL exactly on the event-level outcomes (ignored / quarantined),
        # where no rule was involved — enforced by the CHECK below.
        sa.Column("policy_id", sa.Text(), nullable=True),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        # Spec §4's outcome vocabulary. A plain String with an app-side enum
        # plus a value CHECK, not a native PG ENUM type; the CHECK earns its
        # migration cost because §11's dashboard filters on this column.
        sa.Column("outcome", sa.String(length=16), nullable=False),
        # The evaluated governance artifact VERSION (spec §6 contract
        # requirement). NULL only where no policy set applied at all.
        sa.Column("policy_version_id", sa.String(length=255), nullable=True),
        # Filled by the minting layer (spec §7); required once the outcome says
        # `created`.
        sa.Column("created_item_ref", sa.Text(), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        # timestamptz, so "when did this delivery first converge?" reads the
        # same on any server/session timezone. Never updated: the conflict path
        # is DO NOTHING, which is what makes a re-delivery a visible no-op.
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "outcome IN ('matched', 'ignored', 'created', 'deduplicated', "
            "'quarantined', 'failed')",
            name="ck_delivery_ledger_outcome",
        ),
        # Policy-level outcomes must name the rule that produced them.
        sa.CheckConstraint(
            "policy_id IS NOT NULL OR outcome IN ('ignored', 'quarantined')",
            name="ck_delivery_ledger_policy_id",
        ),
        # A matched/created/deduplicated/failed row that cannot say WHICH
        # policy version decided it is not an audit row.
        sa.CheckConstraint(
            "policy_version_id IS NOT NULL OR outcome IN ('ignored', 'quarantined')",
            name="ck_delivery_ledger_policy_version_id",
        ),
        # A `created` row with nothing to point at claims work exists that
        # nobody can find.
        sa.CheckConstraint(
            "outcome <> 'created' OR created_item_ref IS NOT NULL",
            name="ck_delivery_ledger_created_item_ref",
        ),
        # A rejection with no explanation is an operator staring at a refusal
        # with nothing to fix (same invariant `policy_cache` enforces).
        sa.CheckConstraint(
            "outcome <> 'quarantined' OR detail IS NOT NULL",
            name="ck_delivery_ledger_quarantine_detail",
        ),
    )
    # The §11 audit listing, "this tenant's deliveries, newest first": the
    # primary key already indexes `tenant` as its leading column, so this
    # exists for the ORDERING.
    op.create_index(
        "ix_delivery_ledger_tenant_created_at",
        "delivery_ledger",
        ["tenant", "created_at"],
    )
    # "What happened to event X?" — not answerable from the key, because a
    # custom dedup template need not contain the event id at all.
    op.create_index(
        "ix_delivery_ledger_tenant_event_id",
        "delivery_ledger",
        ["tenant", "event_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_delivery_ledger_tenant_event_id", table_name="delivery_ledger")
    op.drop_index("ix_delivery_ledger_tenant_created_at", table_name="delivery_ledger")
    op.drop_table("delivery_ledger")

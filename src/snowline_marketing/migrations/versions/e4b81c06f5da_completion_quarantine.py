"""provenance watch: completion quarantine

Creates `completion_quarantine` (spec §4's "Quarantine", provenance-missing
half; spec §8's watch-and-quarantine): one row per completion of a
marketing-minted item that recorded no deliverable — visible, auditable, and
resolvable by attaching provenance after the fact.

Keyed by (tenant, event_id): the event id is the plugin's dedup key everywhere
else, so at-least-once re-delivery of one completion converges to ONE row. The
item-keyed alternative was rejected because a row an operator had already
resolved would silently absorb a LATER provenance-less completion of the same
item; `ix_completion_quarantine_tenant_item_ref` answers the per-item question
instead.

The MALFORMED-EVENT half of §4's quarantine bullet is deliberately not here:
an unparseable envelope has no tenant and possibly no event id, its identity is
`(source_key, position)` (`intake.run_intake`'s `on_malformed` seam), and its
verb is requeue-the-raw-bytes rather than attach-provenance — a different key
and a different verb, so a different table, landing with the operator surfaces
that read it (§11).

Columns are spelled literally rather than imported from `models`, for the
reason every migration here does.

Revision ID: e4b81c06f5da
Revises: d7a2c93f4b16
Create Date: 2026-08-21

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e4b81c06f5da"
down_revision: str | None = "d7a2c93f4b16"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "completion_quarantine",
        sa.Column("tenant", sa.String(length=255), primary_key=True),
        sa.Column("event_id", sa.Text(), primary_key=True),
        # The marketing-minted item whose completion this was — the delivery
        # ledger's `created_item_ref`, and what the resolve verb keys the
        # deliverable rows it writes on.
        sa.Column("item_ref", sa.Text(), nullable=False),
        # Why it is here, and the operator-visible specifics. Both required: a
        # quarantine row with no reason is an operator staring at a refusal with
        # nothing to fix.
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
        # The completion event, whole: the resolve verb reads it, and Text (not
        # JSONB) because JSONB reorders keys and renormalizes numbers, so a
        # stored event could no longer be compared with what the producer sent.
        sa.Column("raw_event", sa.Text(), nullable=False),
        # open / resolved / dismissed. String + CHECK, not a native PG ENUM:
        # growing the vocabulary must not be an ALTER TYPE that cannot run
        # inside a transaction.
        sa.Column(
            "status", sa.String(length=16), nullable=False, server_default="open"
        ),
        # What the operator said when they closed it. Required exactly when the
        # row is closed (see the CHECK below).
        sa.Column("resolution_detail", sa.Text(), nullable=True),
        # When the completion happened, as distinct from when we recorded it:
        # "the item completed three weeks ago and nothing was ever recorded" is
        # the sentence this queue exists to make sayable.
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        # NULL until the row is resolved or dismissed — `created_at` never
        # moves, so this is the only column that can say how long a row waited.
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "reason IN ('provenance_malformed', 'provenance_missing')",
            name="ck_completion_quarantine_reason",
        ),
        sa.CheckConstraint(
            "status IN ('dismissed', 'open', 'resolved')",
            name="ck_completion_quarantine_status",
        ),
        # Both directions: an open row carrying a resolution is a row someone
        # closed without saying so, and a closed row with none is a decision
        # with no author.
        sa.CheckConstraint(
            "(status = 'open') = (resolution_detail IS NULL)",
            name="ck_completion_quarantine_resolution_detail",
        ),
    )
    # §11's queue: this tenant's open rows, oldest first.
    op.create_index(
        "ix_completion_quarantine_tenant_status",
        "completion_quarantine",
        ["tenant", "status", "created_at"],
    )
    # "What is unrecorded about this item?" — the question the item-keyed
    # alternative would have answered by construction.
    op.create_index(
        "ix_completion_quarantine_tenant_item_ref",
        "completion_quarantine",
        ["tenant", "item_ref"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_completion_quarantine_tenant_item_ref", table_name="completion_quarantine"
    )
    op.drop_index(
        "ix_completion_quarantine_tenant_status", table_name="completion_quarantine"
    )
    op.drop_table("completion_quarantine")

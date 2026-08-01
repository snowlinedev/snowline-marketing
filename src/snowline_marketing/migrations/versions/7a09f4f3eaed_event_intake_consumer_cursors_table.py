"""event intake: consumer_cursors table

Creates `consumer_cursors` (spec §4, "Cursor state"): one row per event
source, holding the last acknowledged position of the intake loop. Columns are
spelled literally here rather than imported from `models` — a migration must
describe the schema as of ITS revision, and an app model that drifts later
would silently rewrite history.

Revision ID: 7a09f4f3eaed
Revises: 5cddd792fe7d
Create Date: 2026-08-01

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "7a09f4f3eaed"
down_revision: str | None = "5cddd792fe7d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "consumer_cursors",
        # The source's stable name (`EventSource.source_key`) — one cursor per
        # source, enforced by the key rather than by convention.
        sa.Column("source_key", sa.String(length=255), primary_key=True),
        # Opaque, source-defined resume token (outbox event id, fixture file
        # name); Text so no source's position shape is baked into the schema.
        sa.Column("position", sa.Text(), nullable=False),
        # Audit only — a malformed envelope can be acked past with no id.
        sa.Column("last_event_id", sa.Text(), nullable=True),
        # timestamptz, so "when did this cursor last move?" reads the same on
        # any server/session timezone (a naive column would store local wall
        # time and be re-labelled UTC by readers).
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def downgrade() -> None:
    op.drop_table("consumer_cursors")

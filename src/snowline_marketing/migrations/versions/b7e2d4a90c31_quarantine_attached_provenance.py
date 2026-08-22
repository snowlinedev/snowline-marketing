"""completion quarantine: attached provenance on resolve

Adds `completion_quarantine.attached_provenance`: the declaration an operator
attaches to resolve a row, persisted verbatim BY the open->resolved transition
itself — before the deliverable rows are written (`watch.resolve_quarantined`
closes first) — so a crash between the close and the writes loses nothing and
re-invoking the verb re-applies the stored declaration idempotently.

Nullable, and guarded to RESOLVED rows only: the watch's item-keyed self-close
and the dismiss verb close rows without attaching anything, and an open or
dismissed row carrying a declaration would claim an attachment that closed
nothing.

Columns are spelled literally rather than imported from `models`, for the
reason every migration here does.

Revision ID: b7e2d4a90c31
Revises: f19c5a3ed204
Create Date: 2026-08-21

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b7e2d4a90c31"
down_revision: str | None = "f19c5a3ed204"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "completion_quarantine",
        sa.Column("attached_provenance", sa.Text(), nullable=True),
    )
    op.create_check_constraint(
        "ck_completion_quarantine_attached_provenance",
        "completion_quarantine",
        "attached_provenance IS NULL OR status = 'resolved'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_completion_quarantine_attached_provenance",
        "completion_quarantine",
        type_="check",
    )
    op.drop_column("completion_quarantine", "attached_provenance")

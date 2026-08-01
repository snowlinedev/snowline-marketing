"""genesis: baseline (empty) migration

Marketing's first migration establishes the alembic chain against
`snowline_marketing.models.Base` (currently empty — spec §13 stage 1 is
scaffold-only, no delivery/provenance ledger tables yet). Deliberately a
no-op: it exists so `alembic upgrade head` and later migrations have a
genesis revision to chain from, without inventing a schema ahead of the
policy-engine item that defines it (spec §4).

Revision ID: 5cddd792fe7d
Revises:
Create Date: 2026-08-01
"""

from collections.abc import Sequence

revision: str = "5cddd792fe7d"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

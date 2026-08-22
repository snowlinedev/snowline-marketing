"""provenance watch: minted-item join index on delivery_ledger

Adds `ix_delivery_ledger_tenant_created_item_ref`, the index behind the
provenance watch's one question per completion: "did this plugin mint the item
that just completed?" (spec §8). The answer is the delivery ledger's
`created_item_ref` — the authoritative join, written at mint time (§7) — and
without an index it is a sequential scan of the tenant's whole delivery history
on EVERY completion event, including the ordinary roadmap completions that are
none of the watch's business and are the common case.

PARTIAL (`WHERE created_item_ref IS NOT NULL`): only `created` rows carry a ref
(`ck_delivery_ledger_created_item_ref`), and they are the minority of an audit
table that also holds every ignored, deduplicated and quarantined delivery. The
index therefore covers exactly the rows the join can match.

Not unique: nothing in the schema promises one created row per item ref, and a
unique index here would turn a hypothetical PM-side ref collision into a failed
mint rather than a visible duplicate. The watch only needs "at least one".

Revision ID: f19c5a3ed204
Revises: e4b81c06f5da
Create Date: 2026-08-21

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f19c5a3ed204"
down_revision: str | None = "e4b81c06f5da"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_delivery_ledger_tenant_created_item_ref",
        "delivery_ledger",
        ["tenant", "created_item_ref"],
        postgresql_where=sa.text("created_item_ref IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_delivery_ledger_tenant_created_item_ref", table_name="delivery_ledger"
    )

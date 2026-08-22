"""provenance watch: deliverable provenance ledger

Creates §4's "Deliverable provenance ledger" as two tables:

- `deliverable_provenance` — one row per deliverable instance, keyed by
  (tenant, producing item ref, channel, deliverable class). The producing ITEM
  rather than the producing EVENT, so an item reopened and completed again
  re-declares the same deliverable instead of leaving the roadmap holding two
  rows for one listing.
- `deliverable_source_versions` — the source artifact versions that deliverable
  was produced from, one row each, with their optional milestone stamps. Rows
  rather than a JSON column because spec §8's staleness sweep compares PER
  version id: as rows that is an indexed lookup on the artifact id with typed
  columns the database constrains, and the fidelity argument that makes
  `policy_cache.body` Text does not apply (the whole declaration is kept
  verbatim on quarantined completions instead).

Columns are spelled literally rather than imported from `models` — a migration
must describe the schema as of ITS revision, and an app model that drifts later
would silently rewrite history.

Revision ID: d7a2c93f4b16
Revises: 2f6c40a91d84
Create Date: 2026-08-21

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d7a2c93f4b16"
down_revision: str | None = "2f6c40a91d84"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "deliverable_provenance",
        # The isolation boundary and the first component of the natural key.
        sa.Column("tenant", sa.String(length=255), primary_key=True),
        # The PM item whose completion produced this — the same ref the delivery
        # ledger's `created_item_ref` holds, which is the join that makes a
        # completion recognizable as marketing-minted at all. Text: PM owns the
        # shape of a ref.
        sa.Column("item_ref", sa.Text(), primary_key=True),
        # Open vocabulary (channels grow with §12's adapters, deliverable
        # classes are tenant vocabulary) — bounded only against a producer
        # writing a paragraph into a key column.
        sa.Column("channel", sa.String(length=128), primary_key=True),
        sa.Column("deliverable_class", sa.String(length=128), primary_key=True),
        # The completion that last declared this deliverable. Not part of the
        # key, and not unique: one completion declaring a listing update AND a
        # screenshot set writes two rows carrying it.
        sa.Column("event_id", sa.Text(), nullable=False),
        # Nullable on purpose: a screenshot set may have no public URL yet, and
        # refusing an otherwise-complete declaration over it would push an
        # honest completion into a queue meant for MISSING provenance.
        sa.Column("external_url", sa.Text(), nullable=True),
        # The completion event's `occurred_at`, never a producer-declared time.
        # timestamptz: §8 compares this against release milestone events.
        sa.Column("produced_at", sa.DateTime(timezone=True), nullable=False),
        # `created_at` never moves, so a re-delivery is visible as convergence
        # rather than as a fresh deliverable; `updated_at` is NULL until the
        # first re-declaration.
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    # §11's per-tenant listing: the primary key already indexes `tenant` as its
    # leading column, so this exists purely for the ORDERING.
    op.create_index(
        "ix_deliverable_provenance_tenant_created_at",
        "deliverable_provenance",
        ["tenant", "created_at"],
    )

    op.create_table(
        "deliverable_source_versions",
        # The parent's whole natural key, repeated — the cost of keying the
        # parent naturally rather than on a surrogate id, paid deliberately.
        sa.Column("tenant", sa.String(length=255), primary_key=True),
        sa.Column("item_ref", sa.Text(), primary_key=True),
        sa.Column("channel", sa.String(length=128), primary_key=True),
        sa.Column("deliverable_class", sa.String(length=128), primary_key=True),
        # In the key: one version per artifact per deliverable. A deliverable
        # claiming artifact A at both v1 and v2 cannot answer the sweep's only
        # question, so the refusal is structural rather than merely diligent.
        sa.Column("artifact_id", sa.String(length=255), primary_key=True),
        sa.Column("version_id", sa.String(length=255), nullable=False),
        # Snowline#141's release stamp, when the producer knew it. NULL is
        # expected: §13 says the sweep works without stamps, better with.
        sa.Column("milestone", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["tenant", "item_ref", "channel", "deliverable_class"],
            [
                "deliverable_provenance.tenant",
                "deliverable_provenance.item_ref",
                "deliverable_provenance.channel",
                "deliverable_provenance.deliverable_class",
            ],
            name="fk_deliverable_source_versions_deliverable",
            ondelete="CASCADE",
        ),
    )
    # The §8 sweep's access path, and the whole reason these are rows: "every
    # deliverable citing artifact X", per tenant.
    op.create_index(
        "ix_deliverable_source_versions_artifact",
        "deliverable_source_versions",
        ["tenant", "artifact_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_deliverable_source_versions_artifact",
        table_name="deliverable_source_versions",
    )
    op.drop_table("deliverable_source_versions")
    op.drop_index(
        "ix_deliverable_provenance_tenant_created_at",
        table_name="deliverable_provenance",
    )
    op.drop_table("deliverable_provenance")

"""minting: row-transition outcomes + updated_at

Grows `delivery_ledger` for the minting layer (spec §7), in three parts that
are one change:

- Three new `outcome` values. `claimed` is the durable marker a minting pass
  writes BEFORE calling PM, so a mint whose confirmation is lost leaves a row
  that is never silently re-minted; `awaiting_approval` is the mode-gated match
  that is owed and deliberately unminted (§12's approval surface); `dry_run` is
  the TERMINAL closure of a match whose policy mode forbids minting — without
  it a dry-run match sits at `matched` and re-owes a mint that must never
  happen, on every re-delivery, forever. See `ledger.DeliveryOutcome` for the
  full semantics of each.
- `outcome` widens from String(16) to String(32), because `awaiting_approval`
  is 17 characters. The width was chosen before the vocabulary needed it;
  abbreviating an audit value to fit a column would be the schema choosing the
  operator's words.
- `updated_at`, nullable. Rows now TRANSITION, and `created_at` deliberately
  never moves (it marks the delivery's first convergence), so without this
  column §11 could see a claim stuck mid-mint but not how long it had been
  stuck. NULL means "recorded, never transitioned".

The CHECK is dropped and recreated rather than altered — Postgres has no ALTER
CONSTRAINT for a CHECK expression. The downgrade recreates the old, narrower
CHECK, which will (correctly) fail if any row is sitting in one of the new
states: those rows are claims and gates the old vocabulary cannot express, and
silently rewriting them to `matched` would re-owe mints that were already made.

Revision ID: 2f6c40a91d84
Revises: 9b1e4c7a2d05
Create Date: 2026-08-15

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2f6c40a91d84"
down_revision: str | None = "9b1e4c7a2d05"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Spelled literally, not imported from `ledger` — a migration must describe the
# schema as of ITS revision, and an app enum that grows later would silently
# rewrite history.
_OUTCOMES_BEFORE = (
    "'matched', 'ignored', 'created', 'deduplicated', 'quarantined', 'failed'"
)
_OUTCOMES_AFTER = (
    "'matched', 'ignored', 'claimed', 'created', 'awaiting_approval', "
    "'dry_run', 'deduplicated', 'quarantined', 'failed'"
)


def upgrade() -> None:
    op.alter_column(
        "delivery_ledger",
        "outcome",
        existing_type=sa.String(length=16),
        type_=sa.String(length=32),
        existing_nullable=False,
    )
    op.drop_constraint("ck_delivery_ledger_outcome", "delivery_ledger", type_="check")
    op.create_check_constraint(
        "ck_delivery_ledger_outcome",
        "delivery_ledger",
        f"outcome IN ({_OUTCOMES_AFTER})",
    )
    op.add_column(
        "delivery_ledger",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("delivery_ledger", "updated_at")
    op.drop_constraint("ck_delivery_ledger_outcome", "delivery_ledger", type_="check")
    # Fails loudly if any row holds one of the new states — see the module
    # docstring on why rewriting them would be worse than refusing.
    op.create_check_constraint(
        "ck_delivery_ledger_outcome",
        "delivery_ledger",
        f"outcome IN ({_OUTCOMES_BEFORE})",
    )
    op.alter_column(
        "delivery_ledger",
        "outcome",
        existing_type=sa.String(length=32),
        type_=sa.String(length=16),
        existing_nullable=False,
    )

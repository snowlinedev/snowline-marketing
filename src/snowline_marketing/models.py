"""Marketing's persisted models.

No models are defined yet (spec §13 stage 1 is scaffold-only — no delivery
ledger, no deliverable provenance ledger, no quarantine, no policy cache, no
cursor state; spec §4 defines that shape for a later item). `Base` exists so
the alembic env and the app lifespan's boot-migrate have something real to
import, and the genesis migration has metadata to diff against.
"""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass

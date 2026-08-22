"""The deliverable provenance ledger — one row per deliverable instance
(spec §4/§8).

This is the store; `models.DeliverableProvenanceEntry` and
`models.DeliverableSourceVersion` are the schema and their docstrings carry the
key design (the natural key, the producing ITEM rather than the producing event,
and why the source artifact versions are rows rather than a JSON column). What
lives here is the WRITE SHAPE, and it is deliberately the OPPOSITE of the
delivery ledger's:

    INSERT ... ON CONFLICT DO UPDATE, then replace the version set.

**DO UPDATE, not DO NOTHING — and the contrast is the point.** `ledger.py`
refuses to update because its row is a CLAIM on work that may already have been
done, and overwriting it would lose the link to a real PM item. A deliverable
row is not a claim; it is a STATEMENT OF FACT about what a completed item
produced, and nothing downstream holds a handle to it that an update could
break. So when a completion is re-delivered — or an item is reopened, corrected
and completed again — the latest declaration is the truth and the row must
converge onto it. Spec §8 says "upserts the deliverable ledger" in exactly those
words, and this is that sentence in SQL. The convergence is what makes the watch
safe under at-least-once delivery: the same completion applied twice leaves one
row, identical both times.

**The version set is REPLACED, not merged.** A deliverable's source versions are
a SET-valued attribute of the row, so the write deletes the association rows and
re-inserts the declared ones inside the SAME transaction. Merging would be worse
in the only case that matters: a producer correcting a completion that named the
wrong artifact version would leave the wrong one behind, and §8's sweep would
then compare against a version this deliverable never reflected — a staleness
finding that cites evidence nobody wrote. (It is the same reasoning that makes
`ledger.py` REPLACE a row's `detail` rather than append to it.) A delete plus an
insert is two statements where every other write in this codebase is one; they
are one transaction, and the transaction is the unit that has to be atomic.

**Nothing here classifies.** The store takes primitives — channel, class, source
versions, produced_at — and never imports `provenance.py`, exactly as `ledger.py`
never imports `policies.py`. The watch converts a parsed declaration into these
arguments, which is what lets the store be tested against a real database with
no envelope in sight.

`DeliverableStore` is the protocol `watch.py` depends on. `InMemoryDeliverables`
is its second implementation, held in the process rather than Postgres, for the
reason `InMemoryDeliveryLedger` is: the fixtures-first flow (spec §5) and §11's
dry-run drive the same code with no database, so a convergence property proved
by the suite is proved for both paths rather than for half of them. The
contract tests run over both as one parametrized suite.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from sqlalchemy import delete, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from snowline_marketing.db import session_scope
from snowline_marketing.models import DeliverableProvenanceEntry as DeliverableRow
from snowline_marketing.models import DeliverableSourceVersion as SourceVersionRow


def _utc_now() -> datetime:
    """`InMemoryDeliverables`' default clock — the in-process stand-in for the
    timestamptz `func.now()` the real store's columns default to."""
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class SourceVersion:
    """One source artifact version a deliverable reflects, as stored.

    The store's own record rather than `provenance.SourceArtifactVersion`: the
    two carry the same three facts and are deliberately different types, because
    one is a wire declaration this plugin validates and the other is a row it
    owns. Keeping them apart is what stops the store from growing an opinion
    about the payload schema (and what lets §12's publish path, which upserts
    this ledger directly with no completion event in sight, use the store
    without inventing a payload to parse)."""

    artifact_id: str
    version_id: str
    milestone: str | None = None


@dataclass(frozen=True)
class DeliverableRecord:
    """One deliverable row, read back with its source versions.

    A plain record rather than the ORM objects, for the reason
    `ledger.LedgerRecord` is one: callers hold it outside the session that
    produced it, and an ORM instance would lazily emit SQL — here, one query per
    association row — from inside a watch pass.

    `source_versions` is ordered by artifact id, so two reads of an unchanged row
    compare equal and a test can assert on the tuple without sorting it first."""

    tenant: str
    item_ref: str
    channel: str
    deliverable_class: str
    source_versions: tuple[SourceVersion, ...]
    produced_at: datetime
    event_id: str
    created_at: datetime
    external_url: str | None = None
    updated_at: datetime | None = None

    @property
    def identity(self) -> tuple[str, str, str, str]:
        """This row's natural key — the tuple `get` takes and the one §8's sweep
        will name a finding by."""
        return (self.tenant, self.item_ref, self.channel, self.deliverable_class)


def _validated_versions(
    source_versions: Iterable[SourceVersion],
) -> tuple[SourceVersion, ...]:
    """The declared versions, checked and ordered — the guard both stores share.

    Shared for the reason `ledger._namespaced_key` is shared: the two stores may
    differ in how they write, never in what a write MEANS, and a rule enforced
    in only the Postgres path would be a rule the fixtures-first flow could
    quietly violate. Raises ValueError before either store is touched:

    - EMPTY is refused. A deliverable citing no source version is a row §8's
      sweep can never evaluate — recorded, unfalsifiable, and silently exempt
      from the staleness the ledger exists to surface.
    - A REPEATED artifact id is refused. It is the association row's key, so the
      second would overwrite the first, and a deliverable that reflects one
      artifact at two versions cannot answer the sweep's only question anyway.
      (`provenance.py` refuses the same payload at the wire; this is the same
      rule where the rows are actually written, because §12's publish path will
      call the store without going through a payload at all.)

    Ordered by artifact id so the stored set and every read-back agree, and so
    the in-memory store's tuple matches the real store's `ORDER BY`."""
    versions = tuple(source_versions)
    if not versions:
        raise ValueError(
            "a deliverable must name at least one source artifact version — one "
            "that names none is a row the staleness sweep can never evaluate "
            "(spec §8)"
        )
    counts = Counter(version.artifact_id for version in versions)
    duplicated = sorted(name for name, count in counts.items() if count > 1)
    if duplicated:
        raise ValueError(
            "a deliverable may cite each source artifact at exactly one version; "
            f"{', '.join(repr(name) for name in duplicated)} appears more than "
            "once"
        )
    return tuple(sorted(versions, key=lambda version: version.artifact_id))


class DeliverableStore(Protocol):
    """What `watch.py` needs from a deliverable provenance ledger.

    `DeliverableProvenanceLedger` (Postgres) and `InMemoryDeliverables` (the
    fixtures-first / dry-run store) both satisfy it without inheriting from it,
    so the watch can be driven end to end with no database and the conformance
    suite proves the two answer the same questions the same way."""

    def upsert(
        self,
        *,
        tenant: str,
        item_ref: str,
        channel: str,
        deliverable_class: str,
        source_versions: Iterable[SourceVersion],
        produced_at: datetime,
        event_id: str,
        external_url: str | None = None,
    ) -> DeliverableRecord: ...

    def get(
        self, tenant: str, item_ref: str, channel: str, deliverable_class: str
    ) -> DeliverableRecord | None: ...

    def list_for_item(self, tenant: str, item_ref: str) -> list[DeliverableRecord]: ...


class DeliverableProvenanceLedger:
    """The `deliverable_provenance` (+ `deliverable_source_versions`) tables
    (spec §4)."""

    def upsert(
        self,
        *,
        tenant: str,
        item_ref: str,
        channel: str,
        deliverable_class: str,
        source_versions: Iterable[SourceVersion],
        produced_at: datetime,
        event_id: str,
        external_url: str | None = None,
    ) -> DeliverableRecord:
        """Record what one completed item produced on one channel, converging on
        re-declaration (see the module docstring for why this upserts where the
        delivery ledger refuses to).

        One transaction: the row is upserted, its association rows are replaced,
        and the whole thing is read back — so the record handed to a caller is
        the row as it stands, never the arguments echoed back. `created_at` keeps
        its INSERT default on the conflict path (a re-declaration is convergence,
        not a fresh deliverable) while `updated_at` moves, which is what makes
        "we heard about this again" a fact the row states rather than one a
        reader infers.

        Raises ValueError — before the database is touched — for an empty or
        artifact-duplicated version set (`_validated_versions`). Raises like any
        other store call if the database is unreachable: the never-raises
        contract belongs to the CLASSIFIERS, and a watch whose write failed must
        stall its pass rather than ack an observation it never recorded."""
        versions = _validated_versions(source_versions)
        key = (tenant, item_ref, channel, deliverable_class)
        statement = pg_insert(DeliverableRow).values(
            tenant=tenant,
            item_ref=item_ref,
            channel=channel,
            deliverable_class=deliverable_class,
            event_id=event_id,
            external_url=external_url,
            produced_at=produced_at,
        )
        upsert = statement.on_conflict_do_update(
            index_elements=[
                DeliverableRow.tenant,
                DeliverableRow.item_ref,
                DeliverableRow.channel,
                DeliverableRow.deliverable_class,
            ],
            set_={
                "event_id": statement.excluded.event_id,
                "external_url": statement.excluded.external_url,
                "produced_at": statement.excluded.produced_at,
                # `created_at` deliberately keeps its INSERT default here: it
                # marks when this deliverable was FIRST recorded, which is what
                # makes a re-delivery visible as convergence.
                "updated_at": func.now(),
            },
        )
        with session_scope() as session:
            session.execute(upsert)
            # Replace, never merge (module docstring): a corrected declaration
            # must not leave the version it corrected behind for §8's sweep to
            # compare against.
            session.execute(
                delete(SourceVersionRow).where(
                    SourceVersionRow.tenant == tenant,
                    SourceVersionRow.item_ref == item_ref,
                    SourceVersionRow.channel == channel,
                    SourceVersionRow.deliverable_class == deliverable_class,
                )
            )
            session.execute(
                insert(SourceVersionRow),
                [
                    {
                        "tenant": tenant,
                        "item_ref": item_ref,
                        "channel": channel,
                        "deliverable_class": deliverable_class,
                        "artifact_id": version.artifact_id,
                        "version_id": version.version_id,
                        "milestone": version.milestone,
                    }
                    for version in versions
                ],
            )
            record = self._read(session, key)
            if record is None:  # pragma: no cover - the upsert just wrote it
                raise AssertionError(
                    f"deliverable {key!r} vanished between its upsert and its "
                    "read-back in one transaction"
                )
            return record

    def get(
        self, tenant: str, item_ref: str, channel: str, deliverable_class: str
    ) -> DeliverableRecord | None:
        """One deliverable by its natural key, or None. Rides the primary key."""
        with session_scope() as session:
            return self._read(session, (tenant, item_ref, channel, deliverable_class))

    def list_for_item(self, tenant: str, item_ref: str) -> list[DeliverableRecord]:
        """Everything one producing item recorded, in key order — "what did this
        completion actually produce?", which is the question an operator asks of
        a resolved quarantine row and the one §8's sweep enumerates per item.
        Rides the primary key's leading columns."""
        return self._read_many(
            DeliverableRow.tenant == tenant, DeliverableRow.item_ref == item_ref
        )

    def list_for_tenant(
        self, tenant: str, *, limit: int | None = None
    ) -> list[DeliverableRecord]:
        """One tenant's deliverables, newest first — §11's provenance listing
        (the input to "staleness overview per channel"). Rides
        `ix_deliverable_provenance_tenant_created_at`; the natural key breaks
        ties so the order is total and a paged listing cannot repeat or skip a
        row when several deliverables share a timestamp.

        DB-only, like the delivery ledger's listings: the dashboard reads
        Postgres, and an in-memory store has no dashboard to serve."""
        statement = (
            select(DeliverableRow)
            .where(DeliverableRow.tenant == tenant)
            .order_by(
                DeliverableRow.created_at.desc(),
                DeliverableRow.item_ref.desc(),
                DeliverableRow.channel.desc(),
                DeliverableRow.deliverable_class.desc(),
            )
        )
        if limit is not None:
            statement = statement.limit(max(0, limit))
        with session_scope() as session:
            rows = list(session.scalars(statement))
            return [self._with_versions(session, row) for row in rows]

    def _read_many(self, *criteria) -> list[DeliverableRecord]:
        statement = (
            select(DeliverableRow)
            .where(*criteria)
            .order_by(
                DeliverableRow.item_ref,
                DeliverableRow.channel,
                DeliverableRow.deliverable_class,
            )
        )
        with session_scope() as session:
            rows = list(session.scalars(statement))
            return [self._with_versions(session, row) for row in rows]

    def _read(
        self, session, key: tuple[str, str, str, str]
    ) -> DeliverableRecord | None:
        row = session.get(DeliverableRow, key)
        return None if row is None else self._with_versions(session, row)

    def _with_versions(self, session, row: DeliverableRow) -> DeliverableRecord:
        """One row plus its association rows, ordered by artifact id — the same
        order `_validated_versions` stores them in, so a read-back compares equal
        to what the in-memory store holds."""
        versions = session.scalars(
            select(SourceVersionRow)
            .where(
                SourceVersionRow.tenant == row.tenant,
                SourceVersionRow.item_ref == row.item_ref,
                SourceVersionRow.channel == row.channel,
                SourceVersionRow.deliverable_class == row.deliverable_class,
            )
            .order_by(SourceVersionRow.artifact_id)
        )
        return DeliverableRecord(
            tenant=row.tenant,
            item_ref=row.item_ref,
            channel=row.channel,
            deliverable_class=row.deliverable_class,
            source_versions=tuple(
                SourceVersion(
                    artifact_id=version.artifact_id,
                    version_id=version.version_id,
                    milestone=version.milestone,
                )
                for version in versions
            ),
            produced_at=row.produced_at,
            event_id=row.event_id,
            created_at=row.created_at,
            external_url=row.external_url,
            updated_at=row.updated_at,
        )


class InMemoryDeliverables:
    """A deliverable provenance ledger held in the process.

    Not a mock, for the reason `InMemoryDeliveryLedger` is not one: this is the
    store the fixtures-first flow (spec §5) and §11's dry-run drive, so the
    convergence the suite proves here is the convergence production has. It
    shares `_validated_versions` with the real store, keys on the same natural
    tuple, replaces the version set the same way, and keeps `created_at` frozen
    while `updated_at` moves — the four things a re-delivered completion's
    behaviour depends on.

    The LISTING surface stays partly DB-only (`list_for_tenant`): that exists for
    §11's dashboard, which reads Postgres. `list_for_item` is here because it is
    not a dashboard read — it is how a caller (and the acceptance criteria) asks
    what a completion produced.

    Not thread-safe, unlike the real store, whose uniqueness the DATABASE
    enforces: a fixtures run is one caller driving one capture in one thread.

    `clock` is the injectable time source for `created_at`/`updated_at` — the
    in-process analogue of the real store's server-side `func.now()`."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._rows: dict[tuple[str, str, str, str], DeliverableRecord] = {}
        self._clock = clock if clock is not None else _utc_now

    def upsert(
        self,
        *,
        tenant: str,
        item_ref: str,
        channel: str,
        deliverable_class: str,
        source_versions: Iterable[SourceVersion],
        produced_at: datetime,
        event_id: str,
        external_url: str | None = None,
    ) -> DeliverableRecord:
        """See `DeliverableStore.upsert` / `DeliverableProvenanceLedger.upsert`.
        Raises the same ValueError, on the same terms, before anything is
        stored."""
        versions = _validated_versions(source_versions)
        key = (tenant, item_ref, channel, deliverable_class)
        existing = self._rows.get(key)
        if existing is None:
            record = DeliverableRecord(
                tenant=tenant,
                item_ref=item_ref,
                channel=channel,
                deliverable_class=deliverable_class,
                source_versions=versions,
                produced_at=produced_at,
                event_id=event_id,
                created_at=self._clock(),
                external_url=external_url,
            )
        else:
            # The conflict path: everything the latest declaration says, with
            # `created_at` left where it was — convergence, not a new
            # deliverable.
            record = dataclasses.replace(
                existing,
                source_versions=versions,
                produced_at=produced_at,
                event_id=event_id,
                external_url=external_url,
                updated_at=self._clock(),
            )
        self._rows[key] = record
        return record

    def get(
        self, tenant: str, item_ref: str, channel: str, deliverable_class: str
    ) -> DeliverableRecord | None:
        """See `DeliverableProvenanceLedger.get`."""
        return self._rows.get((tenant, item_ref, channel, deliverable_class))

    def list_for_item(self, tenant: str, item_ref: str) -> list[DeliverableRecord]:
        """See `DeliverableProvenanceLedger.list_for_item` — same key order, so
        a test asserting on the sequence asserts the same thing on both."""
        return sorted(
            (
                record
                for record in self._rows.values()
                if record.tenant == tenant and record.item_ref == item_ref
            ),
            key=lambda record: record.identity,
        )

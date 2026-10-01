"""Shared ingestion primitives: task interface and the generic per-object ingest loop with audit bookkeeping."""
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Iterator

from audit.audit_repository import AuditRepository
from audit.logger import log_event
from config.settings import Settings
from master_db.repository import MasterRepository, RawRecord, SourceRef
from master_db.schema_manager import SchemaManager

OVERLAP = timedelta(minutes=5)            # re-read window for timestamp watermarks (late commits / clock skew)
OBJECTID_OVERLAP = timedelta(hours=1)     # same for ObjectId (second-resolution, multi-writer) watermarks

SUCCESS, FAILED, PARTIAL, SKIPPED = "SUCCESS", "FAILED", "PARTIAL", "SKIPPED"


@dataclass
class IngestContext:
    run_id: str
    settings: Settings
    master: MasterRepository
    schema: SchemaManager
    audit: AuditRepository
    logger: logging.Logger
    cutoff: datetime | None = None   # records created after this (the run start) are left for the next run


@dataclass
class TaskResult:
    status: str
    objects_total: int = 0
    objects_success: int = 0
    objects_failed: int = 0
    read: int = 0
    inserted: int = 0
    skipped: int = 0
    failed: int = 0
    error: str | None = None

    def add(self, o: "ObjectResult") -> None:
        self.objects_total += 1
        self.objects_success += o.status == SUCCESS
        self.objects_failed += o.status != SUCCESS
        self.read += o.read
        self.inserted += o.inserted
        self.skipped += o.skipped
        self.failed += o.failed

    def finalise(self) -> "TaskResult":
        if self.status == FAILED and self.error:      # fatal, keep
            return self
        if self.objects_failed == 0:
            self.status = SUCCESS
        elif self.objects_success == 0:
            self.status = FAILED
            self.error = self.error or f"all {self.objects_total} objects failed"
        else:
            self.status = PARTIAL
            self.error = self.error or f"{self.objects_failed} of {self.objects_total} objects failed"
        return self


@dataclass
class ObjectResult:
    status: str
    read: int = 0
    inserted: int = 0            # new + updated version rows
    skipped: int = 0
    failed: int = 0
    error: str | None = None
    new: int = 0
    updated: int = 0
    flagged_deleted: int = 0


class ObjectReader(ABC):
    """Yields RawRecords for one source object. Sets `watermark` as it reads, counts `failed` records."""
    failed: int = 0
    watermark: str | None = None

    @abstractmethod
    def records(self) -> Iterator[RawRecord]: ...

    def source_ids(self) -> set[str] | None:
        """Identities of EVERY record currently in the source (not just the changed ones), used to flag deletions.
        None = cannot be determined reliably (then no deletion is flagged for this object)."""
        return None


class SourceTask(ABC):
    name: str
    source_system: str

    def enabled(self) -> tuple[bool, str]:
        return True, ""

    @abstractmethod
    def check_connection(self) -> None:
        """Raise on failure. Must not print/log credentials."""

    @abstractmethod
    def run(self, ctx: IngestContext) -> TaskResult: ...


def ingest_object(ctx: IngestContext, task: str, ref: SourceRef, key: str, source_object: str,
                  target_schema: str, wanted_target: str, shared: bool, strategy: str, wm_column: str | None,
                  make_reader: Callable[[str | None], ObjectReader], with_endpoint: bool = False) -> ObjectResult:
    """Ingest one collection/table/endpoint in chunks, recording a batch row in audit.ingestion_batches.
    Every change is kept as history: new record -> NEW, changed record -> a new UPDATED version (the previous one is
    retained), record gone from the source -> flagged DELETED (never removed). Deletions are detected after a complete,
    failure-free read by comparing ALL source ids with the target, whatever the incremental strategy is."""
    t0 = time.monotonic()
    batch_id = None
    try:
        target = wanted_target if shared else ctx.audit.unique_target(target_schema, wanted_target, key)
        target, wm_from = ctx.audit.register_source(
            key, ref.source_system, ref.source_type, ref.source_database, ref.source_schema, source_object,
            target_schema, target, strategy, wm_column)
        ctx.schema.ensure_raw_table(target_schema, target, shared=shared, with_endpoint=with_endpoint)
        batch_id = ctx.audit.start_batch(ctx.run_id, task, ref.source_system, ref.source_type, ref.source_database,
                                         ref.source_schema, source_object, target_schema, target, strategy, wm_from)
        reader = make_reader(wm_from)
        read = new = updated = 0
        chunk: list[RawRecord] = []
        for rec in reader.records():
            chunk.append(rec)
            read += 1
            if len(chunk) >= ctx.settings.chunk_size:
                c = ctx.master.apply_records(target_schema, target, ref, ctx.run_id, chunk, with_endpoint)
                new, updated, chunk = new + c.new, updated + c.updated, []
        c = ctx.master.apply_records(target_schema, target, ref, ctx.run_id, chunk, with_endpoint)
        new, updated = new + c.new, updated + c.updated
        inserted = new + updated
        skipped = read - inserted
        flagged, note = 0, None
        if reader.failed == 0:
            try:
                ids = reader.source_ids()
                if ids is not None:
                    flagged = ctx.master.flag_deleted(target_schema, target, ref, ids, ctx.run_id)
            except Exception as e:  # noqa: BLE001 - the data is already ingested; report, do not fail the object
                note = f"deletion detection skipped: {type(e).__name__}: {e}"
        # Only advance the watermark when every record was captured.
        wm_to = reader.watermark if reader.failed == 0 else None
        if wm_to:
            ctx.audit.set_watermark(key, wm_to)
        err = None
        if reader.failed:
            err = f"{reader.failed} record(s) could not be serialised"
        if note:
            err = f"{err}; {note}" if err else note
        status = SUCCESS if not err else PARTIAL
        ctx.audit.finish_batch(batch_id, status, read, inserted, skipped, reader.failed, wm_to or wm_from, err,
                               new=new, updated=updated, flagged_deleted=flagged)
        if err:
            ctx.audit.log_error(ctx.run_id, task, ref.source_system, source_object, err, batch_id)
        log_event(ctx.logger, logging.INFO if not err else logging.WARNING, "object ingested",
                  pipeline_run_id=ctx.run_id, source=ref.source_system, task=task, object=source_object,
                  status=status, record_count=read, inserted=inserted, skipped=skipped, failed=reader.failed,
                  new=new, updated=updated, flagged_deleted=flagged,
                  strategy=strategy, duration_ms=int((time.monotonic() - t0) * 1000), error=err)
        return ObjectResult(status, read, inserted, skipped, reader.failed, err, new, updated, flagged)
    except Exception as e:  # noqa: BLE001 - deliberately isolate per-object failures, but record them
        ctx.audit.log_error(ctx.run_id, task, ref.source_system, source_object, e, batch_id)
        if batch_id is not None:
            ctx.audit.finish_batch(batch_id, FAILED, 0, 0, 0, 0, None, f"{type(e).__name__}: {e}")
        log_event(ctx.logger, logging.ERROR, "object ingestion failed", pipeline_run_id=ctx.run_id,
                  source=ref.source_system, task=task, object=source_object, status=FAILED,
                  duration_ms=int((time.monotonic() - t0) * 1000), error=f"{type(e).__name__}: {e}")
        return ObjectResult(FAILED, error=f"{type(e).__name__}: {e}")

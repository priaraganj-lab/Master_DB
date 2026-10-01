"""One-off maintenance: delete a target object's rows so the next load is a clean full copy (manual only).
Audited as a batch with status RESET (records_deleted); the object's watermark is cleared."""
import logging

from audit.logger import log_event
from ingestion.base import IngestContext
from master_db.repository import SourceRef


def reset_objects(ctx: IngestContext, keys: list[str]) -> None:
    for key in keys:
        row = ctx.audit._exec("""SELECT source_system, source_type, source_database, source_schema, source_object,
            target_schema, target_table FROM audit.source_registry WHERE source_key=%s""", (key,), fetch=True)
        if not row:
            raise ValueError(f"cannot reset unknown source object: {key}")
        system, stype, db, schema, obj, tschema, ttable = row[0]
        if stype == "mongodb":
            ref = SourceRef(system, stype, db, schema, None, obj.split(".", 1)[1])
        else:
            ref = SourceRef(system, stype, db, schema, obj[len(db) + len(schema) + 2:], None)
        batch = ctx.audit.start_batch(ctx.run_id, "maintenance_reset", system, stype, db, schema, obj, tschema,
                                      ttable, "reset", None)
        deleted = ctx.master.delete_object_rows(tschema, ttable, ref)
        ctx.audit.set_watermark(key, None)
        ctx.audit.finish_batch(batch, "RESET", 0, 0, 0, 0, None, None, deleted)
        log_event(ctx.logger, logging.WARNING, "target rows reset", pipeline_run_id=ctx.run_id, source=system,
                  task="maintenance_reset", object=obj, status="RESET", deleted=deleted)

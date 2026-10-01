"""Writes ingestion metadata to the audit schema. Uses its own autocommit connection so audit rows
survive rollbacks on the data connection."""
import socket
from datetime import datetime

from utils.hashing import pg_identifier
from utils.security import scrub
from utils.timeutil import now_ist

LOCK_KEY = 727_001_001  # advisory lock: one pipeline run at a time


def now() -> datetime:
    return now_ist()


class AuditRepository:
    def __init__(self, conn):
        assert conn.autocommit, "audit connection must be autocommit"
        self.conn = conn

    def _exec(self, query: str, params=(), fetch: bool = False):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if fetch else None

    # --- run lock / ids -------------------------------------------------------------
    def try_lock(self) -> bool:
        return self._exec("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,), fetch=True)[0][0]

    def next_run_id(self, ts: datetime) -> str:
        prefix = f"RUN_{ts:%Y%m%d_%H%M%S}_"
        n = self._exec("SELECT count(*) FROM audit.pipeline_runs WHERE pipeline_run_id LIKE %s",
                       (prefix + "%",), fetch=True)[0][0]
        return f"{prefix}{n + 1:03d}"

    # --- run / task -----------------------------------------------------------------
    def start_run(self, run_id: str, start: datetime) -> None:
        self._exec("INSERT INTO audit.pipeline_runs (pipeline_run_id, start_time, status, host) VALUES (%s,%s,'RUNNING',%s)",
                   (run_id, start, socket.gethostname()))

    def finish_run(self, run_id: str, end: datetime, status: str, counts: dict, error: str | None = None) -> None:
        self._exec("""UPDATE audit.pipeline_runs SET end_time=%s, duration_ms=(extract(epoch from (%s - start_time))*1000)::bigint,
            status=%s, tasks_total=%s, tasks_success=%s, tasks_failed=%s, tasks_skipped=%s,
            records_read=%s, records_inserted=%s, records_skipped=%s, records_failed=%s, error_message=%s
            WHERE pipeline_run_id=%s""",
                   (end, end, status, counts["tasks_total"], counts["tasks_success"], counts["tasks_failed"],
                    counts["tasks_skipped"], counts["read"], counts["inserted"], counts["skipped"], counts["failed"],
                    scrub(error) if error else None, run_id))

    def record_task(self, run_id: str, task: str, source: str, start: datetime, end: datetime, status: str,
                    c: dict, error: str | None) -> None:
        self._exec("""INSERT INTO audit.job_execution (pipeline_run_id, task, source, start_time, end_time, duration_ms, status,
            objects_total, objects_success, objects_failed, records_read, records_inserted, records_skipped,
            records_failed, error_message) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                   (run_id, task, source, start, end, int((end - start).total_seconds() * 1000), status,
                    c["objects_total"], c["objects_success"], c["objects_failed"], c["read"], c["inserted"],
                    c["skipped"], c["failed"], scrub(error) if error else None))

    # --- source registry / watermark ---------------------------------------------------
    def register_source(self, key: str, system: str, stype: str, database: str | None, schema: str | None,
                        obj: str, target_schema: str, default_target: str, strategy: str,
                        wm_column: str | None) -> tuple[str, str | None]:
        """Register/refresh a source object; returns (target_table, stored_watermark). An existing mapping is
        never renamed, so target tables stay stable across runs."""
        row = self._exec("""INSERT INTO audit.source_registry (source_key, source_system, source_type, source_database,
            source_schema, source_object, target_schema, target_table, incremental_strategy, watermark_column)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source_key) DO UPDATE SET last_seen_at=now(), incremental_strategy=EXCLUDED.incremental_strategy,
                watermark_column=EXCLUDED.watermark_column
            RETURNING target_table, watermark_value""",
                         (key, system, stype, database, schema, obj, target_schema, default_target, strategy,
                          wm_column), fetch=True)[0]
        return row[0], row[1]

    def unique_target(self, target_schema: str, wanted: str, key: str) -> str:
        """Resolve a target table name that no other source object uses."""
        existing = self._exec("SELECT target_table FROM audit.source_registry WHERE source_key=%s", (key,), fetch=True)
        if existing:
            return existing[0][0]
        taken = self._exec("SELECT 1 FROM audit.source_registry WHERE target_schema=%s AND target_table=%s",
                           (target_schema, wanted), fetch=True)
        return pg_identifier(wanted, key[-6:]) if taken else wanted

    def set_watermark(self, key: str, value: str) -> None:
        self._exec("UPDATE audit.source_registry SET watermark_value=%s, watermark_updated_at=now() WHERE source_key=%s",
                   (value, key))

    # --- batches / errors ----------------------------------------------------------------
    def start_batch(self, run_id: str, task: str, system: str, stype: str, database: str | None, schema: str | None,
                    obj: str, target_schema: str, target_table: str, strategy: str, wm_from: str | None) -> int:
        return self._exec("""INSERT INTO audit.ingestion_batches (pipeline_run_id, task, source_system, source_type,
            source_database, source_schema, source_object, target_schema, target_table, strategy, watermark_from,
            start_time, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'RUNNING') RETURNING batch_id""",
                          (run_id, task, system, stype, database, schema, obj, target_schema, target_table, strategy,
                           wm_from, now()), fetch=True)[0][0]

    def finish_batch(self, batch_id: int, status: str, read: int, inserted: int, skipped: int, failed: int,
                     wm_to: str | None, error: str | None = None, deleted: int = 0, new: int | None = None,
                     updated: int | None = None, flagged_deleted: int | None = None) -> None:
        """inserted = new + updated version rows; `deleted` = rows physically removed (only the manual reset);
        flagged_deleted = records flagged DELETED (rows kept)."""
        self._exec("""UPDATE audit.ingestion_batches SET end_time=now(), records_deleted=%s,
            duration_ms=(extract(epoch from (now() - start_time))*1000)::bigint, status=%s, records_read=%s,
            records_inserted=%s, records_skipped=%s, records_failed=%s, watermark_to=%s, error_message=%s,
            records_new=%s, records_updated=%s, records_flagged_deleted=%s
            WHERE batch_id=%s""",
                   (deleted, status, read, inserted, skipped, failed, wm_to, scrub(error) if error else None,
                    new, updated, flagged_deleted, batch_id))

    def log_error(self, run_id: str | None, task: str | None, source: str | None, obj: str | None,
                  err: BaseException | str, batch_id: int | None = None) -> None:
        etype = type(err).__name__ if isinstance(err, BaseException) else "Error"
        self._exec("""INSERT INTO audit.error_logs (pipeline_run_id, batch_id, task, source, source_object, error_type,
            error_message) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                   (run_id, batch_id, task, source, obj, etype, scrub(err)[:4000]))

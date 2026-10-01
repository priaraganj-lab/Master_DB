"""Generic PostgreSQL -> Master DB ingestion (read-only transactions against the source).
Each row is captured with to_jsonb(row) so numerics, dates, arrays and json columns keep source fidelity."""
import logging
import uuid
from datetime import datetime, timezone
from typing import Iterator

import psycopg2
from psycopg2 import sql

from audit.logger import log_event
from config.settings import Settings, to_libpq_url
from config.sources import WATERMARK_CANDIDATES, PgSource, PgTelemetryTable
from ingestion.base import (OVERLAP, FAILED, IngestContext, ObjectReader, ObjectResult, SourceTask, TaskResult,
                            ingest_object)
from ingestion.cutoff import load_tz_columns, pg_cutoff_clause, pick_pg_cutoff_column
from master_db.repository import RawRecord, SourceRef
from utils.hashing import occurrence_id, payload_hash_from_text, pg_identifier
from utils.security import scrub
from utils.timeutil import ist_iso

_TS_TYPES = ("timestamp with time zone", "timestamp without time zone")


class PgReader(ObjectReader):
    def __init__(self, conn, schema, table, id_cols, ts_col, wm_col, wm, chunk, cut: tuple | None = None):
        self.conn, self.schema, self.table = conn, schema, table
        self.id_cols, self.ts_col, self.wm_col, self.wm, self.chunk = id_cols, ts_col, wm_col, wm, chunk
        self.cut = cut                      # (timestamptz column, cutoff) or None
        self.failed = 0
        self.watermark = wm
        self._seen_ids: set[str] = set()    # keyless tables: identities handed out during the (full) read

    def records(self) -> Iterator[RawRecord]:
        rid = (sql.SQL("concat_ws('|', {})").format(sql.SQL(", ").join(
            sql.SQL("t.{}::text").format(sql.Identifier(c)) for c in self.id_cols))
            if self.id_cols else sql.SQL("NULL::text"))
        ts = sql.SQL("t.{}").format(sql.Identifier(self.ts_col)) if self.ts_col else sql.SQL("NULL")
        wm = sql.SQL("t.{}").format(sql.Identifier(self.wm_col)) if self.wm_col else sql.SQL("NULL")
        q = sql.SQL("SELECT to_jsonb(t)::text, {}, {}, {} FROM {} t").format(
            rid, ts, wm, sql.Identifier(self.schema, self.table))
        conds, params = [], []
        if self.wm_col and self.wm:
            conds.append(sql.SQL("t.{} >= %s").format(sql.Identifier(self.wm_col)))
            params.append(datetime.fromisoformat(self.wm) - OVERLAP)
        if self.cut:
            conds.append(pg_cutoff_clause(self.cut[0]))
            params.append(self.cut[1])
        if conds:
            q += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conds)
        if self.wm_col:
            q += sql.SQL(" ORDER BY t.{}").format(sql.Identifier(self.wm_col))
        max_wm = None
        with self.conn.cursor(name=f"crl_{uuid.uuid4().hex[:12]}") as cur:
            cur.itersize = self.chunk
            cur.execute(q, params)
            occurrences: dict = {}
            for text, record_id, src_ts, wm_val in cur:
                if isinstance(wm_val, datetime) and (max_wm is None or wm_val > max_wm):
                    max_wm = wm_val
                if isinstance(src_ts, datetime) and src_ts.tzinfo is None:
                    src_ts = src_ts.replace(tzinfo=timezone.utc)   # naive source values are treated as UTC (session is IST)
                h = payload_hash_from_text(text)
                if not self.id_cols:         # no key: identity = content hash + occurrence, so identical rows are all kept
                    record_id = occurrence_id(h, occurrences)
                    self._seen_ids.add(record_id)
                yield RawRecord(record_id, src_ts if isinstance(src_ts, datetime) else None, text, h)
        if max_wm is not None:
            if self.cut and max_wm.tzinfo:
                max_wm = min(max_wm, self.cut[1])   # rows created after the cutoff were skipped: keep them inside the overlap
            self.watermark = ist_iso(max_wm)      # aware -> IST; naive source columns keep their own (naive) value

    def source_ids(self) -> set[str]:
        """Identity of every row currently in the table (not limited by the cutoff or the watermark)."""
        if not self.id_cols:                  # keyless tables are always read in full, so what was read is the table
            return set(self._seen_ids)
        rid = sql.SQL("concat_ws('|', {})").format(sql.SQL(", ").join(
            sql.SQL("t.{}::text").format(sql.Identifier(c)) for c in self.id_cols))
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT {} FROM {} t").format(rid, sql.Identifier(self.schema, self.table)))
            ids = {r[0] for r in cur.fetchall()}
        self.conn.rollback()
        return ids


class PgIngestTask(SourceTask):
    def __init__(self, cfg: PgSource, settings: Settings):
        self.cfg, self.settings = cfg, settings
        self.name, self.source_system = cfg.task, cfg.source_system
        self._tz_cols: dict[str, dict] = {}         # database -> {(schema, table): timestamptz columns}

    def _connect(self, dbname: str):
        conn = psycopg2.connect(to_libpq_url(self.settings.source_uri(self.source_system), dbname),
                                connect_timeout=self.settings.connect_timeout, application_name="crl_pipeline")
        conn.set_session(readonly=True)
        return conn

    def _databases(self) -> list[str]:
        dbs = list(self.cfg.databases) if self.cfg.discover_application else []
        for t in self.cfg.telemetry_tables:
            if t.database not in dbs:
                dbs.append(t.database)
        return dbs

    def check_connection(self) -> None:
        for db in self._databases():
            conn = self._connect(db)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            finally:
                conn.close()

    def run(self, ctx: IngestContext) -> TaskResult:
        result = TaskResult(status="RUNNING")
        for db in self._databases():
            try:
                conn = self._connect(db)
            except Exception as e:  # noqa: BLE001
                self._db_failure(ctx, result, db, e)
                continue
            try:
                for schema, table in self._application_tables(conn, db):
                    result.add(self._ingest_table(ctx, conn, db, schema, table, None))
                for t in (x for x in self.cfg.telemetry_tables if x.database == db):
                    result.add(self._ingest_table(ctx, conn, db, t.schema, t.table, t))
            except Exception as e:  # noqa: BLE001 - discovery failure for this database
                self._db_failure(ctx, result, db, e)
            finally:
                conn.close()
        return result.finalise()

    def _db_failure(self, ctx, result: TaskResult, db: str, e: Exception) -> None:
        ctx.audit.log_error(ctx.run_id, self.name, self.source_system, db, e)
        log_event(ctx.logger, logging.ERROR, "database ingestion failed", pipeline_run_id=ctx.run_id,
                  source=self.source_system, task=self.name, object=db, status=FAILED,
                  error=scrub(f"{type(e).__name__}: {e}"))
        result.objects_total += 1
        result.objects_failed += 1
        result.error = scrub(f"{db}: {type(e).__name__}: {e}")

    def _application_tables(self, conn, db: str) -> list[tuple[str, str]]:
        if not (self.cfg.discover_application and db in self.cfg.databases):
            return []
        with conn.cursor() as cur:
            cur.execute("""SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                           WHERE c.relkind IN ('r','p') AND NOT c.relispartition AND n.nspname = ANY(%s)
                           ORDER BY 1,2""", (list(self.cfg.schemas),))
            return cur.fetchall()

    def _profile(self, conn, schema: str, table: str) -> tuple[list[str], str | None, str | None]:
        """(primary-key columns, watermark column, source-timestamp column) - only verified, reliable ones."""
        with conn.cursor() as cur:
            cur.execute("""SELECT a.attname FROM pg_index i JOIN pg_attribute a
                           ON a.attrelid=i.indrelid AND a.attnum = ANY(i.indkey)
                           WHERE i.indrelid = %s::regclass AND i.indisprimary
                           ORDER BY array_position(i.indkey::int2[], a.attnum)""",
                        (f'"{schema}"."{table}"',))
            pk = [r[0] for r in cur.fetchall()]
            cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                           WHERE table_schema=%s AND table_name=%s""", (schema, table))
            cols = {n: t for n, t in cur.fetchall()}
            wm = next((c for c in cols if c.lower() in WATERMARK_CANDIDATES and cols[c] in _TS_TYPES), None)
            if wm:  # reliable only if no NULLs (NULL rows would never match the filter)
                cur.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} IS NULL)").format(
                    sql.Identifier(schema, table), sql.Identifier(wm)))
                if cur.fetchone()[0]:
                    wm = None
            created = next((c for c in cols if c.lower() in ("created_at", "createdat") and cols[c] in _TS_TYPES), None)
        conn.rollback()
        return pk, wm, (wm or created)

    def _ingest_table(self, ctx, conn, db: str, schema: str, table: str, tel: PgTelemetryTable | None):
        ref = SourceRef(self.source_system, "postgresql", source_database=db, source_schema=schema, source_table=table)
        key = f"{self.source_system}|{db}|{schema}|{table}"
        try:
            pk, wm_col, ts_col = self._profile(conn, schema, table)
        except Exception as e:  # noqa: BLE001 - a wrong key would mis-identify every record: fail this table instead
            conn.rollback()
            ctx.audit.log_error(ctx.run_id, self.name, self.source_system, f"{db}.{schema}.{table}", e)
            log_event(ctx.logger, logging.ERROR, "object ingestion failed", pipeline_run_id=ctx.run_id,
                      source=self.source_system, task=self.name, object=f"{db}.{schema}.{table}", status=FAILED,
                      error=scrub(f"{type(e).__name__}: {e}"))
            return ObjectResult(FAILED, error=scrub(f"{type(e).__name__}: {e}"))
        if tel:
            pk, wm_col, ts_col = list(tel.id_columns) or pk, tel.watermark_column, tel.timestamp_column
            target_schema, target, shared = "telemetry", tel.target, True
        else:
            target_schema, shared = "application", False
            target = pg_identifier(self.cfg.short, db, schema if schema != "public" else "", table)
        if not pk:
            wm_col = None            # keyless: identity is the row content, so it must be read in full every run
        strategy = "timestamp" if wm_col else "full"
        cut = None
        if ctx.cutoff and pk:
            try:
                if db not in self._tz_cols:
                    self._tz_cols[db] = load_tz_columns(conn)
                col = pick_pg_cutoff_column(self._tz_cols[db].get((schema, table), set()), tel, wm_col)
                cut = (col, ctx.cutoff) if col else None
            except Exception:  # noqa: BLE001 - cannot determine a creation column -> no cutoff (read in full)
                conn.rollback()
        result = ingest_object(ctx, self.name, ref, key, f"{db}.{schema}.{table}", target_schema, target, shared,
                               strategy, wm_col,
                               lambda wm: PgReader(conn, schema, table, pk, ts_col, wm_col, wm, ctx.settings.chunk_size, cut))
        conn.rollback()  # end the read-only transaction (also clears any aborted state)
        return result

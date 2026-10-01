"""End-to-end reconciliation: for EVERY source->target mapping in audit.source_registry, compare the source with the
LIVE records of the target (latest version of each record, not flagged DELETED) and write the result to
audit.reconciliation (one row per mapping, refreshed on every pipeline run). History is reported next to it: total rows
including old versions, records flagged DELETED, superseded versions.

Source counts are taken as of the run's cutoff (its start time), the same scope ingestion used, so records written to
the live sources during the run cannot cause a FAIL; status_detail says how many were left for the next run.

Statuses:  PASS  = source row count equals the target's live record count (and the history is consistent)
           FAIL  = counts differ or the history is inconsistent (status_detail explains the likely cause)
           ERROR = the source or the target could not be counted (counts stay NULL - never a silent 0;
                   status_detail holds the error)
"""
import logging
from datetime import datetime

import psycopg2
import pymongo
from psycopg2 import sql
from psycopg2.extras import execute_values

from audit.logger import log_event
from config.settings import to_libpq_url
from config.sources import MONGO_SOURCES, PG_SOURCES
from ingestion.base import IngestContext
from ingestion.cutoff import (load_tz_columns, mongo_cutoff_filter, pg_cutoff_clause,
                              pick_pg_cutoff_column)
from ingestion.mongodb.engine import record_id
from ingestion.postgresql.engine import PgIngestTask
from master_db.repository import SourceRef, _OBJ_WHERE, _obj_params
from utils.hashing import occurrence_id, payload_hash_from_text
from utils.security import scrub
from utils.timeutil import ist_text as _ist_text

_PG_CFG = {c.source_system: c for c in PG_SOURCES}
_MONGO_CFG = {c.source_system: c for c in MONGO_SOURCES}


def _short(e: BaseException) -> str:
    return scrub(f"{type(e).__name__}: {e}").replace("\n", " ")[:500]


class _Sources:
    """Lazily opened, cached read-only source connections."""

    def __init__(self, settings):
        self.settings, self._mongo, self._pg = settings, {}, {}
        self._tz: dict = {}           # (system, db) -> {(schema, table): timestamptz columns}
        self.dead: dict = {}          # unreachable source (system[, db]) -> error, so we fail fast instead of
                                      # waiting for a connection timeout on every mapping

    def mongo(self, system: str):
        if system not in self._mongo:
            self._mongo[system] = pymongo.MongoClient(
                self.settings.source_uri(system), appname="crl_pipeline_reconcile",
                serverSelectionTimeoutMS=self.settings.connect_timeout * 1000)
        return self._mongo[system]

    def pg(self, system: str, db: str):
        key = (system, db)
        if key not in self._pg:
            conn = psycopg2.connect(to_libpq_url(self.settings.source_uri(system), db),
                                    connect_timeout=self.settings.connect_timeout, application_name="crl_pipeline_reconcile")
            conn.set_session(readonly=True)
            self._pg[key] = conn
        return self._pg[key]

    def tz_columns(self, system: str, db: str) -> dict:
        if (system, db) not in self._tz:
            self._tz[(system, db)] = load_tz_columns(self.pg(system, db))
        return self._tz[(system, db)]

    def reset_pg(self, system: str, db: str) -> None:
        self._tz.pop((system, db), None)
        conn = self._pg.pop((system, db), None)
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def close(self) -> None:
        for c in self._mongo.values():
            c.close()
        for c in self._pg.values():
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass


def _source_count(src: _Sources, system, stype, db, schema, obj, wm_col, cutoff):
    """(count as of the run cutoff, total count now, scope used) - scope is reused by _explain so both agree."""
    dead = src.dead.get(system if stype == 'mongodb' else (system, db))
    if dead:
        raise ConnectionError(dead)
    try:
        return _source_count_inner(src, system, stype, db, schema, obj, wm_col, cutoff)
    except (pymongo.errors.ConnectionFailure, psycopg2.OperationalError) as e:
        src.dead[system if stype == 'mongodb' else (system, db)] = f'source unreachable: {_short(e)}'
        raise


def _source_count_inner(src: _Sources, system, stype, db, schema, obj, wm_col, cutoff):
    if stype == "mongodb":
        name = obj.split(".", 1)[1]
        database = src.mongo(system)[db]
        if name not in database.list_collection_names():        # count_documents on a missing collection returns 0
            raise LookupError(f"source collection '{db}.{name}' not found")
        coll = database[name]
        cut = mongo_cutoff_filter(coll, cutoff) if cutoff else {}
        n = coll.count_documents(cut)
        return n, (coll.count_documents({}) if cut else n), {"mongo": cut}
    if stype == "postgresql":
        table = obj[len(db) + len(schema) + 2:]
        conn = src.pg(system, db)
        try:
            col = None
            if cutoff:
                tel = next((t for t in _PG_CFG[system].telemetry_tables
                            if (t.database, t.schema, t.table) == (db, schema, table)), None)
                col = pick_pg_cutoff_column(src.tz_columns(system, db).get((schema, table), set()), tel, wm_col)
                if col and not wm_col:      # ingestion reads keyless tables in full, without a cutoff
                    pk, _, _ = PgIngestTask(_PG_CFG[system], src.settings)._profile(conn, schema, table)
                    if not (list(tel.id_columns) if tel and tel.id_columns else pk):
                        col = None
            with conn.cursor() as cur:
                if col:
                    cur.execute(sql.SQL("SELECT count(*) FILTER (WHERE {}), count(*) FROM {} t").format(
                        pg_cutoff_clause(col), sql.Identifier(schema, table)), (cutoff,))
                    n, total = cur.fetchone()
                else:
                    cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, table)))
                    n = total = cur.fetchone()[0]
            return n, total, {"pg": (col, cutoff) if col else None}
        finally:
            conn.rollback()
    raise NotImplementedError(f"row counts are not supported for source type '{stype}'")


def _ref(system, stype, db, schema, obj) -> SourceRef:
    if stype == "mongodb":
        return SourceRef(system, stype, db, schema, None, obj.split(".", 1)[1])
    return SourceRef(system, stype, db, schema, obj[len(db) + len(schema) + 2:], None)


def _target_stats(master_conn, tschema, ttable, ref: SourceRef) -> dict:
    """Live record count + history figures of one source object in its raw table."""
    try:
        with master_conn.cursor() as cur:
            cur.execute(sql.SQL("""SELECT count(*) FILTER (WHERE t.is_current AND t.change_type <> 'DELETED'),
                    count(*), count(*) FILTER (WHERE t.change_type = 'DELETED'), count(*) FILTER (WHERE NOT t.is_current),
                    count(*) FILTER (WHERE t.record_version > 1 AND t.previous_master_record_id IS NULL)
                    FROM {} t WHERE """).format(sql.Identifier(tschema, ttable)) + sql.SQL(_OBJ_WHERE), _obj_params(ref))
            live, total, deleted, superseded, broken = cur.fetchone()
            return {"live": live, "total": total, "deleted": deleted, "superseded": superseded, "broken": broken}
    finally:
        master_conn.rollback()


def _explain(src: _Sources, master_conn, system, stype, db, schema, obj, tschema, ttable, ref, diff, scope) -> str:
    """Cheap, id-level explanation of a count difference (ids only, no payload hashing), over the same cutoff scope
    as the count. Compares the source ids with the LIVE record ids of the target."""
    try:
        if stype == "mongodb":
            coll = src.mongo(system)[db][obj.split(".", 1)[1]]
            source_ids = {record_id(d["_id"]) for d in coll.find(scope.get("mongo") or {}, {"_id": 1})}
            all_ids = ({record_id(d["_id"]) for d in coll.find({}, {"_id": 1})} if scope.get("mongo") else source_ids)
        else:
            table = obj[len(db) + len(schema) + 2:]
            conn = src.pg(system, db)
            task = PgIngestTask(_PG_CFG[system], src.settings)
            pk, _, _ = task._profile(conn, schema, table)
            tel = next((t for t in _PG_CFG[system].telemetry_tables
                        if (t.database, t.schema, t.table) == (db, schema, table)), None)
            if tel and tel.id_columns:
                pk = list(tel.id_columns)
            if not pk:      # keyless table: identity = content hash + occurrence (always read in full)
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT to_jsonb(t)::text FROM {} t").format(sql.Identifier(schema, table)))
                    occ: dict = {}
                    source_ids = {occurrence_id(payload_hash_from_text(r[0]), occ) for r in cur.fetchall()}
                conn.rollback()
                all_ids = source_ids
            else:
                rid = sql.SQL("concat_ws('|', {})").format(sql.SQL(", ").join(
                    sql.SQL("t.{}::text").format(sql.Identifier(c)) for c in pk))
                base = sql.SQL("SELECT {} FROM {} t").format(rid, sql.Identifier(schema, table))
                with conn.cursor() as cur:
                    cur.execute(base)
                    all_ids = {r[0] for r in cur.fetchall()}
                    if scope.get("pg"):
                        cur.execute(base + sql.SQL(" WHERE ") + pg_cutoff_clause(scope["pg"][0]), (scope["pg"][1],))
                        source_ids = {r[0] for r in cur.fetchall()}
                    else:
                        source_ids = all_ids
                conn.rollback()
        with master_conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT coalesce(t.source_record_id,'') FROM {} t WHERE ").format(sql.Identifier(tschema, ttable))
                        + sql.SQL(_OBJ_WHERE + " AND t.is_current AND t.change_type <> 'DELETED'"), _obj_params(ref))
            live_ids = {r[0] for r in cur.fetchall()}
        master_conn.rollback()
        missing = len(source_ids - live_ids)                     # in the source, not live in the target
        undeleted = len(live_ids - all_ids)                      # live in the target, gone from the source
        ahead = len((live_ids & all_ids) - source_ids)           # live in the target, created after the cutoff
        parts = []
        if missing:
            parts.append(f"{missing} source record(s) NOT live in target - investigate")
        if undeleted:
            parts.append(f"{undeleted} record(s) live in target but no longer in source (deletion not flagged) - investigate")
        if ahead:
            parts.append(f"{ahead} live record(s) in target newer than the run cutoff")
        return "; ".join(parts) or "counts differ; no id-level cause found - investigate"
    except Exception as e:  # noqa: BLE001
        return f"counts differ; explanation unavailable ({_short(e)})"


def reconcile_all(ctx: IngestContext, master_conn, ran_at: datetime) -> dict:
    """Refresh audit.reconciliation for every mapping. `master_conn` is a read connection to the master DB."""
    settings, audit = ctx.settings, ctx.audit
    ist_text = _ist_text(ran_at)
    src = _Sources(settings)
    rows, counts = [], {"PASS": 0, "FAIL": 0, "ERROR": 0}
    try:
        mappings = audit._exec("""SELECT source_key, source_system, source_type, source_database, source_schema,
            source_object, target_schema, target_table, watermark_column FROM audit.source_registry
            WHERE target_schema IN ('application', 'telemetry') ORDER BY source_system, source_object""", fetch=True)
        for key, system, stype, db, schema, obj, tschema, ttable, wm_col in mappings:
            ref = _ref(system, stype, db, schema, obj)
            s_cnt = t_cnt = None
            total = 0
            scope: dict = {}
            hist = {"total": None, "deleted": None, "superseded": None, "broken": 0}
            errors = []
            try:
                s_cnt, total, scope = _source_count(src, system, stype, db, schema, obj, wm_col, ctx.cutoff)
            except Exception as e:  # noqa: BLE001
                errors.append(f"source: {_short(e)}")
                if stype == "postgresql":
                    src.reset_pg(system, db)
            try:
                hist = _target_stats(master_conn, tschema, ttable, ref)
                t_cnt = hist["live"]
            except Exception as e:  # noqa: BLE001
                errors.append(f"target: {_short(e)}")
            if errors:
                status, diff, detail = "ERROR", None, " | ".join(errors)
            else:
                diff = s_cnt - t_cnt
                status, detail = "PASS", None
                if diff:
                    status = "FAIL"
                    text = _explain(src, master_conn, system, stype, db, schema, obj, tschema, ttable, ref, diff, scope)
                    detail = (f"target has {-diff} more live record(s) than source: " if diff < 0
                              else f"target has {diff} fewer live record(s) than source: ") + text
                    if diff > 0 and not (scope.get("mongo") or scope.get("pg")):
                        detail += (" (no creation timestamp to apply the run cutoff: records written during the run "
                                   "can show up here and are ingested next run)")
                if hist["broken"]:
                    status = "FAIL"
                    note = f"history integrity: {hist['broken']} version(s) above v1 without a link to the previous version"
                    detail = f"{detail}; {note}" if detail else note
                if total > s_cnt:    # explicit label: source rows newer than the run are outside this comparison
                    note = (f"{total - s_cnt} record(s) created after this run started ({ist_text}) are not counted; "
                            f"they are ingested by the next run")
                    detail = f"{detail}; {note}" if detail else note
            counts[status] += 1
            rows.append((f"{system}.{obj}", f"{tschema}.{ttable}", s_cnt, t_cnt, diff, status, detail, ist_text,
                         hist["total"], hist["deleted"], hist["superseded"]))
    finally:
        src.close()
    execute_values(audit.conn.cursor(), """INSERT INTO audit.reconciliation (source_table, target_table,
        source_row_count, target_row_count, difference, status, status_detail, time_pipeline_last_ran_ist,
        target_total_rows, deleted_records, superseded_versions) VALUES %s
        ON CONFLICT (source_table) DO UPDATE SET target_table=EXCLUDED.target_table,
        source_row_count=EXCLUDED.source_row_count, target_row_count=EXCLUDED.target_row_count,
        difference=EXCLUDED.difference, status=EXCLUDED.status, status_detail=EXCLUDED.status_detail,
        time_pipeline_last_ran_ist=EXCLUDED.time_pipeline_last_ran_ist, target_total_rows=EXCLUDED.target_total_rows,
        deleted_records=EXCLUDED.deleted_records, superseded_versions=EXCLUDED.superseded_versions""",
                   rows, page_size=500)
    log_event(ctx.logger, logging.INFO if not counts["ERROR"] else logging.ERROR, "reconciliation refreshed",
              pipeline_run_id=ctx.run_id, status="DONE", mappings=len(rows), **counts, last_ran_ist=ist_text)
    return {"mappings": len(rows), **counts, "last_ran_ist": ist_text}

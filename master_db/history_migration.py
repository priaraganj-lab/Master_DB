"""One-time, idempotent migration of the raw tables to the versioned-history layout (see repository.py).

Per target table, in ONE transaction (all or nothing; a table already migrated is skipped):
  1. add the history columns,
  2. backfill: versions per record ordered by ingestion time (oldest = NEW v1, later = UPDATED), the latest is current,
  3. recompute payload_hash for collections whose volatile fields are now excluded from the change hash,
  4. copy rows that the old purge archive (audit.retired_records) removed back in, flagged DELETED
     (audit.retired_records itself is kept),
  5. new unique indexes (one version number per record, one current version per record), drop the old
     unique (identity, payload_hash) index, create the current-state view,
  6. one audit batch per source object (strategy 'history_backfill') so the live count stays reconcilable.
Nothing is removed from any raw table. History that older code physically deleted before this migration (mirror mode)
cannot be recovered."""
import logging

from psycopg2 import sql
from psycopg2.extras import execute_values

from audit.logger import log_event
from config.sources import MONGO_SOURCES
from master_db.repository import _OBJ_WHERE, _obj_params, ref_from_registry
from master_db.schema_manager import (HISTORY_COLUMNS, current_view_ddl, history_index_ddl, identity_sql,
                                      old_unique_index)
from utils.hashing import payload_hash_from_text

_SHARED_SCHEMA = "telemetry"          # raw tables in this schema hold several source objects


def _has_column(cur, schema: str, table: str, column: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",
                (schema, table, column))
    return cur.fetchone() is not None


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s", (schema, table))
    return cur.fetchone() is not None


def _migrate_table(conn, run_id: str | None, schema: str, table: str, objects: list, dry_run: bool) -> dict:
    """objects: audit.source_registry rows (system, type, db, schema, object, key) mapped to this table."""
    shared = schema == _SHARED_SCHEMA
    tbl = sql.Identifier(schema, table)
    ident = identity_sql(shared)
    stats = {"table": f"{schema}.{table}", "rows": 0, "superseded": 0, "reinserted_retired": 0, "rehashed": 0}
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT count(*) FROM {}").format(tbl))
        stats["rows"] = cur.fetchone()[0]
        for name, ddl in HISTORY_COLUMNS:
            cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} " + ddl).format(tbl, sql.Identifier(name)))
        # 2. versions: per record, ordered by when each version was first ingested
        cur.execute(sql.SQL("""WITH ranked AS (
              SELECT master_record_id, ingestion_timestamp,
                     row_number() OVER w AS v,
                     lag(master_record_id) OVER w AS prev,
                     lead(master_record_id) OVER w AS nxt,
                     lead(ingestion_timestamp) OVER w AS next_ts
              FROM {tbl}
              WINDOW w AS (PARTITION BY """ + ident + """ ORDER BY ingestion_timestamp, master_record_id))
            UPDATE {tbl} t SET record_version = r.v,
                   change_type = CASE WHEN r.v = 1 THEN 'NEW' ELSE 'UPDATED' END,
                   is_current = (r.nxt IS NULL), valid_from = r.ingestion_timestamp, valid_to = r.next_ts,
                   previous_master_record_id = r.prev
            FROM ranked r WHERE t.master_record_id = r.master_record_id""").format(tbl=tbl))
        # 3. volatile fields are now excluded from the change hash: recompute it for those collections
        for system, stype, db, sch, obj, _key in objects:
            if stype != "mongodb":
                continue
            cfg = next((c for c in MONGO_SOURCES if c.source_system == system), None)
            coll = obj.split(".", 1)[1]
            ignore = cfg.ignore_for(db, coll) if cfg else frozenset()
            if not ignore:
                continue
            ref = ref_from_registry(system, stype, db, sch, obj)
            cur.execute(sql.SQL("SELECT t.master_record_id, t.payload::text FROM {} t WHERE ").format(tbl)
                        + sql.SQL(_OBJ_WHERE), _obj_params(ref))
            new_hashes = [(mid, payload_hash_from_text(text, ignore)) for mid, text in cur.fetchall()]
            execute_values(cur, sql.SQL("UPDATE {} t SET payload_hash = v.h FROM (VALUES %s) AS v(id, h) "
                                        "WHERE t.master_record_id = v.id").format(tbl).as_string(cur), new_hashes)
            stats["rehashed"] += len(new_hashes)
        # 4. rows the old purge archive removed: back in, flagged DELETED
        cur.execute(sql.SQL("""INSERT INTO {tbl} (source_system, source_type, source_database, source_schema, source_table,
                source_collection, source_record_id, source_timestamp, ingestion_id, ingestion_timestamp, payload_hash,
                payload, record_version, change_type, is_current, valid_from, valid_to, previous_master_record_id,
                deleted_at, deleted_ingestion_id)
            SELECT source_system, source_type, source_database, source_schema, source_table, source_collection,
                   source_record_id, source_timestamp, original_ingestion_id, original_ingestion_timestamp, payload_hash,
                   payload, v, CASE WHEN v = n THEN 'DELETED' WHEN v = 1 THEN 'NEW' ELSE 'UPDATED' END, v = n,
                   original_ingestion_timestamp, retired_at, NULL,
                   CASE WHEN v = n THEN retired_at END, CASE WHEN v = n THEN pipeline_run_id END
            FROM (SELECT r.*, row_number() OVER (PARTITION BY """ + ident + """ ORDER BY original_ingestion_timestamp, retired_id) AS v,
                         count(*) OVER (PARTITION BY """ + ident + """) AS n
                  FROM audit.retired_records r WHERE r.target_schema = %s AND r.target_table = %s) x""").format(tbl=tbl),
                    (schema, table))
        stats["reinserted_retired"] = cur.rowcount
        cur.execute(sql.SQL("UPDATE {} SET valid_from = ingestion_timestamp WHERE valid_from IS NULL").format(tbl))
        cur.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN valid_from SET NOT NULL").format(tbl))
        cur.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN valid_from SET DEFAULT clock_timestamp()").format(tbl))
        cur.execute(sql.SQL("SELECT count(*) FROM {} WHERE NOT is_current").format(tbl))
        stats["superseded"] = cur.fetchone()[0]
        # 5. indexes, view
        for stmt in history_index_ddl(schema, table, shared):
            cur.execute(stmt)
        cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(schema, old_unique_index(schema, table))))
        cur.execute(current_view_ddl(schema, table))
        # 6. audit batch per source object: new = distinct records, flagged_deleted = records deleted, updated = old versions
        if not dry_run:
            for system, stype, db, sch, obj, key in objects:
                ref = ref_from_registry(system, stype, db, sch, obj)
                cur.execute(sql.SQL("""SELECT count(*), count(*) FILTER (WHERE is_current),
                        count(*) FILTER (WHERE change_type = 'DELETED'), count(*) FILTER (WHERE NOT is_current)
                        FROM {} t WHERE """).format(tbl) + sql.SQL(_OBJ_WHERE), _obj_params(ref))
                total, records, deleted, old_versions = cur.fetchone()
                cur.execute(sql.SQL("SELECT count(*) FROM audit.retired_records r WHERE r.target_schema=%s AND r.target_table=%s "
                                    "AND r.source_system=%s AND r.source_database IS NOT DISTINCT FROM %s AND "
                                    "r.source_schema IS NOT DISTINCT FROM %s AND r.source_table IS NOT DISTINCT FROM %s AND "
                                    "r.source_collection IS NOT DISTINCT FROM %s"),
                            [schema, table] + _obj_params(ref))
                reinserted = cur.fetchone()[0]
                cur.execute("""INSERT INTO audit.ingestion_batches (pipeline_run_id, task, source_system, source_type,
                    source_database, source_schema, source_object, target_schema, target_table, strategy, start_time,
                    end_time, duration_ms, status, records_read, records_inserted, records_skipped, records_failed,
                    records_new, records_updated, records_flagged_deleted, records_deleted)
                    VALUES (%s,'history_migration',%s,%s,%s,%s,%s,%s,%s,'history_backfill',now(),now(),0,'SUCCESS',
                            %s,%s,0,0,%s,%s,%s,0)""",
                            (run_id, system, stype, db, sch, obj, schema, table, total, reinserted, records,
                             old_versions, deleted))
    return stats


def migrate_history(conn, run_id: str | None, logger=None, dry_run: bool = False) -> list[dict]:
    """Migrate every target table that is still in the old layout. `conn` = non-autocommit data connection.
    dry_run: do everything, report, and roll each table back (no audit rows are written)."""
    done = []
    with conn.cursor() as cur:
        cur.execute("""SELECT target_schema, target_table, source_system, source_type, source_database, source_schema,
                       source_object, source_key FROM audit.source_registry
                       WHERE target_schema IN ('application', 'telemetry') ORDER BY 1, 2, 7""")
        mapping: dict[tuple[str, str], list] = {}
        for tschema, ttable, system, stype, db, sch, obj, key in cur.fetchall():
            mapping.setdefault((tschema, ttable), []).append((system, stype, db, sch, obj, key))
    conn.rollback()
    for (schema, table), objects in mapping.items():
        with conn.cursor() as cur:
            todo = _table_exists(cur, schema, table) and not _has_column(cur, schema, table, "record_version")
        conn.rollback()
        if not todo:
            continue
        try:
            stats = _migrate_table(conn, run_id, schema, table, objects, dry_run)
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        done.append(stats)
        if logger:
            log_event(logger, logging.INFO, "history migration" + (" (dry run)" if dry_run else ""),
                      pipeline_run_id=run_id, status="DRY_RUN" if dry_run else "MIGRATED", **stats)
    return done

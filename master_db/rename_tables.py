"""One-off, idempotent migration: rename the application raw tables to the descriptive source prefixes.

    mongo1__  -> network_data__     mongo2__ -> elevate_atlas__     pg1__ -> supabase__     (pg2 -> network_telemetry)

Per table, in ONE transaction while holding the pipeline lock: table, its `<table>__current` view, its indexes
(uv_/uc_/ix_/legacy ux_), its primary-key constraint, and every audit reference (source_registry, ingestion_batches,
retired_records, reconciliation). Verifies that nothing was lost (row counts, index counts, views) and that no old
name remains. Data is never touched. Default is a dry run that rolls back; `--apply` commits.

    python -m master_db.rename_tables            # dry run
    python -m master_db.rename_tables --apply
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from psycopg2 import sql  # noqa: E402

from audit.audit_repository import LOCK_KEY  # noqa: E402
from config.settings import load_settings  # noqa: E402
from master_db.connection import connect_master  # noqa: E402
from master_db.schema_manager import current_view_name  # noqa: E402
from utils.hashing import pg_identifier, short_hash  # noqa: E402

OLD_SHORT = {"mongodb_network_data": "mongo1", "mongodb_elevate_atlas": "mongo2",
             "postgresql_supabase": "pg1", "postgresql_network_telemetry": "pg2"}
NEW_SHORT = {"mongodb_network_data": "network_data", "mongodb_elevate_atlas": "elevate_atlas",
             "postgresql_supabase": "supabase", "postgresql_network_telemetry": "network_telemetry"}
SCHEMA = "application"


def index_names(table: str) -> list:
    s = SCHEMA
    return [f"uv_{table[:40]}_{short_hash(s + table + 'v', 6)}", f"uc_{table[:40]}_{short_hash(s + table + 'c', 6)}",
            f"ix_{table[:40]}_{short_hash(s + table + 'i', 6)}", f"ux_{table[:40]}_{short_hash(s + table, 6)}"]


def build_mapping(cur) -> tuple:
    cur.execute("""SELECT source_system, source_type, source_database, source_schema, source_object, target_table
                   FROM audit.source_registry WHERE target_schema = %s ORDER BY target_table""", (SCHEMA,))
    mapping, already, unexpected = {}, 0, []
    for system, stype, db, schema, obj, target in cur.fetchall():
        if stype == "mongodb":
            parts = (db, obj.split(".", 1)[1])
        else:
            parts = (db, schema if schema != "public" else "", obj[len(db) + len(schema) + 2:])
        old, new = pg_identifier(OLD_SHORT[system], *parts), pg_identifier(NEW_SHORT[system], *parts)
        if target == new:
            already += 1
        elif target == old:
            mapping[old] = new
        else:
            unexpected.append((target, old, new))
    return mapping, already, unexpected


def _exists(cur, kind, schema, name) -> bool:
    cur.execute("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s AND c.relkind = ANY(%s)",
                (schema, name, kind))
    return cur.fetchone() is not None


def _index_count(cur, table) -> int:
    cur.execute("SELECT count(*) FROM pg_indexes WHERE schemaname=%s AND tablename=%s", (SCHEMA, table))
    return cur.fetchone()[0]


def run(apply: bool) -> dict:
    settings = load_settings()
    conn = connect_master(settings, autocommit=False, app_name="crl_rename_tables")
    cur = conn.cursor()
    cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
    if not cur.fetchone()[0]:
        raise SystemExit("another pipeline run holds the lock; aborting")
    conn.commit()
    try:
        mapping, already, unexpected = build_mapping(cur)
        if unexpected:
            raise RuntimeError(f"registry names do not follow the naming rule (not touched): {unexpected[:5]}")
        news = list(mapping.values())
        if len(set(news)) != len(news):
            raise RuntimeError("two tables would get the same new name")
        clash = [n for n in news if _exists(cur, ["r", "v"], SCHEMA, n)]
        if clash:
            raise RuntimeError(f"target names already exist: {clash[:5]}")
        before = {}
        for old in mapping:
            cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(SCHEMA, old)))
            before[old] = (cur.fetchone()[0], _index_count(cur, old), _exists(cur, ["v"], SCHEMA, current_view_name(old)))
        for old, new in mapping.items():
            cur.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(sql.Identifier(SCHEMA, old), sql.Identifier(new)))
            ov, nv = current_view_name(old), current_view_name(new)
            if before[old][2]:
                cur.execute(sql.SQL("ALTER VIEW {} RENAME TO {}").format(sql.Identifier(SCHEMA, ov), sql.Identifier(nv)))
            for oi, ni in zip(index_names(old), index_names(new)):
                cur.execute(sql.SQL("ALTER INDEX IF EXISTS {} RENAME TO {}").format(sql.Identifier(SCHEMA, oi), sql.Identifier(ni)))
            cur.execute("SELECT conname FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'p'",
                        (f'"{SCHEMA}"."{new}"',))
            row = cur.fetchone()
            if row:
                cur.execute(sql.SQL("ALTER TABLE {} RENAME CONSTRAINT {} TO {}").format(
                    sql.Identifier(SCHEMA, new), sql.Identifier(row[0]), sql.Identifier(new[:57] + "_pkey")))
        # audit references
        audit_updates = 0
        for tbl in ("source_registry", "ingestion_batches", "retired_records"):
            for old, new in mapping.items():
                cur.execute(sql.SQL("UPDATE {} SET target_table = %s WHERE target_schema = %s AND target_table = %s").format(
                    sql.Identifier("audit", tbl)), (new, SCHEMA, old))
                audit_updates += cur.rowcount
        for old, new in mapping.items():
            cur.execute("UPDATE audit.reconciliation SET target_table = %s WHERE target_table = %s", (f"{SCHEMA}.{new}", f"{SCHEMA}.{old}"))
            audit_updates += cur.rowcount
        # verification
        problems = []
        for old, new in mapping.items():
            cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(SCHEMA, new)))
            n_rows = cur.fetchone()[0]
            if n_rows != before[old][0]: problems.append(("row count changed", new))
            if _index_count(cur, new) != before[old][1]: problems.append(("index count changed", new))
            if before[old][2] and not _exists(cur, ["v"], SCHEMA, current_view_name(new)): problems.append(("view missing", new))
            if _exists(cur, ["r", "v"], SCHEMA, old): problems.append(("old name still exists", old))
            for ni in index_names(new)[:3]:
                if not _exists(cur, ["i"], SCHEMA, ni): problems.append(("expected index missing", ni))
        cur.execute("""SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind IN ('r','v')
                       AND c.relname ~ '^(mongo[12]|pg[12])__'""", (SCHEMA,))
        old_prefix_left = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM audit.source_registry WHERE target_schema = %s AND target_table ~ '^(mongo[12]|pg[12])__'", (SCHEMA,))
        reg_left = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM audit.reconciliation WHERE target_table ~ '^application[.](mongo[12]|pg[12])__'")
        rec_left = cur.fetchone()[0]
        result = dict(renamed=len(mapping), already_renamed=already, audit_rows_updated=audit_updates, longest_new_name=max(map(len, news), default=0),
                      names_at_63_chars=sum(1 for n in news if len(n) == 63),
                      old_prefix_objects_left=old_prefix_left, registry_old_left=reg_left, reconciliation_old_left=rec_left,
                      problems=problems, applied=False)
        if problems or old_prefix_left or reg_left or rec_left:
            conn.rollback()
            raise RuntimeError(f"verification failed - rolled back: {result}")
        if apply:
            conn.commit()
            result["applied"] = True
        else:
            conn.rollback()
        return result
    finally:
        try:
            conn.rollback()
            cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
        finally:
            conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="commit (default: dry run, rolled back)")
    print(run(ap.parse_args().apply))

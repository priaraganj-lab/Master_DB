"""One-off, idempotent migration: rename the technical source keys stored in crl_master_db.

    mongodb_1 -> mongodb_network_data     mongodb_2 -> mongodb_elevate_atlas
    postgresql_1 -> postgresql_supabase   postgresql_2 -> postgresql_network_telemetry

Everything happens in ONE transaction while holding the pipeline advisory lock (no run can overlap). It updates
`source_system` in every raw table (application + telemetry) and the key/label columns in the audit tables, then
verifies that no old key remains and that no table changed its row count. `--apply` commits; the default is a dry run
that rolls back. Payloads are never touched.

    python -m master_db.rename_source_labels            # dry run
    python -m master_db.rename_source_labels --apply
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from psycopg2 import sql  # noqa: E402

from audit.audit_repository import LOCK_KEY  # noqa: E402
from config.settings import load_settings  # noqa: E402
from master_db.connection import connect_master  # noqa: E402

MAPPING = {"mongodb_1": "mongodb_network_data", "mongodb_2": "mongodb_elevate_atlas",
           "postgresql_1": "postgresql_supabase", "postgresql_2": "postgresql_network_telemetry"}
OLD = list(MAPPING)

# (schema, table, column, kind) - kind 'eq' = value is the key; 'prefix_pipe' = "key|..."; 'prefix_dot' = "key.…"
AUDIT_TARGETS = [
    ("audit", "source_registry", "source_system", "eq"), ("audit", "source_registry", "source_key", "prefix_pipe"),
    ("audit", "ingestion_batches", "source_system", "eq"), ("audit", "job_execution", "source", "eq"),
    ("audit", "error_logs", "source", "eq"), ("audit", "retired_records", "source_system", "eq"),
    ("audit", "reconciliation", "source_table", "prefix_dot"),
]


def _update(cur, schema, table, col, kind):
    tbl, c = sql.Identifier(schema, table), sql.Identifier(col)
    n = 0
    for old, new in MAPPING.items():
        if kind == "eq":
            cur.execute(sql.SQL("UPDATE {} SET {} = %s WHERE {} = %s").format(tbl, c, c), (new, old))
        else:
            sep = "|" if kind == "prefix_pipe" else "."
            cur.execute(sql.SQL("UPDATE {} SET {} = %s || substr({}, %s) WHERE {} LIKE %s").format(tbl, c, c, c),
                        (new, len(old) + 1, old + sep + "%"))
        n += cur.rowcount
    return n


def _remaining(cur, schema, table, col, kind):
    tbl, c = sql.Identifier(schema, table), sql.Identifier(col)
    cond = " OR ".join(["{c} = %s" if kind == "eq" else "{c} LIKE %s"] * len(OLD))
    params = OLD if kind == "eq" else [o + ("|" if kind == "prefix_pipe" else ".") + "%" for o in OLD]
    cur.execute(sql.SQL("SELECT count(*) FROM {} WHERE " + cond.replace("{c}", "{}")).format(tbl, *([c] * len(OLD))), params)
    return cur.fetchone()[0]


def run(apply: bool) -> dict:
    settings = load_settings()
    conn = connect_master(settings, autocommit=False, app_name="crl_rename_labels")
    cur = conn.cursor()
    cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
    if not cur.fetchone()[0]:
        raise SystemExit("another pipeline run holds the lock; aborting")
    conn.commit()
    try:
        cur.execute("""SELECT t.table_schema, t.table_name FROM information_schema.tables t
            JOIN information_schema.columns c ON c.table_schema=t.table_schema AND c.table_name=t.table_name AND c.column_name='source_system'
            WHERE t.table_schema IN ('application','telemetry') AND t.table_type='BASE TABLE' ORDER BY 1,2""")
        raw = cur.fetchall()
        targets = [(s, t, "source_system", "eq") for s, t in raw] + AUDIT_TARGETS
        # row counts before (must be identical after)
        before = {}
        for s, t in {(s, t) for s, t, _, _ in targets}:
            cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(s, t)))
            before[(s, t)] = cur.fetchone()[0]
        updated = 0
        per_schema = {}
        for s, t, col, kind in targets:
            n = _update(cur, s, t, col, kind)
            updated += n
            per_schema[s] = per_schema.get(s, 0) + n
        # verification: nothing old left, nothing added or lost
        left = sum(_remaining(cur, s, t, col, kind) for s, t, col, kind in targets)
        changed = [(s, t) for (s, t), n in before.items()
                   if (cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(s, t))), cur.fetchone()[0])[1] != n]
        result = dict(tables_checked=len(before), rows_updated=updated, by_schema=per_schema, old_keys_left=left,
                      tables_with_changed_row_count=changed, applied=False)
        if left or changed:
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

#!/usr/bin/env python
"""Read-only end-to-end reconciliation: every source object -> target table -> LIVE record count (latest version, not
DELETED), per-record (id, hash) comparison against the source, history integrity (one current version per record,
contiguous versions, version links, no deleted-then-live gaps), duplicate check, audit reconciliation.
Writes validation_report.md. Never writes to any database."""
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2
import pymongo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config.settings import load_settings, to_libpq_url  # noqa: E402
from config.sources import MONGO_SOURCES, PG_SOURCES  # noqa: E402
from ingestion.cutoff import (load_tz_columns, mongo_cutoff_filter, pg_cutoff_clause,  # noqa: E402
                              pick_pg_cutoff_column)
from ingestion.mongodb.engine import MongoIngestTask, record_id, serialise  # noqa: E402
from ingestion.postgresql.engine import PgIngestTask  # noqa: E402
from utils.hashing import occurrence_id, payload_hash_from_text  # noqa: E402
from utils.timeutil import PG_TIMEZONE, ist_text, now_ist  # noqa: E402

S = load_settings()
master = psycopg2.connect(S.master_url(), options=f"-c timezone={PG_TIMEZONE}")
master.set_session(readonly=True)
mc = master.cursor()


def q(sql, params=()):
    mc.execute(sql, params)
    return mc.fetchall()


registry = {r[0]: r for r in q("""SELECT source_key, source_system, source_type, source_database, source_schema,
    source_object, target_schema, target_table FROM audit.source_registry""")}


def target_rows(reg):
    """(source_record_id, payload_hash, payload, is_current, change_type, record_version, master_record_id,
    previous_master_record_id) of EVERY version of the object."""
    _, system, stype, db, schema, obj, tschema, ttable = reg
    if stype == "mongodb":
        col, val = "source_collection", obj.split(".", 1)[1]
    else:
        col, val = "source_table", obj[len(db) + len(schema) + 2:]
    return q(f'SELECT source_record_id, payload_hash, payload::text, is_current, change_type, record_version, '
             f'master_record_id, previous_master_record_id FROM "{tschema}"."{ttable}" '
             f'WHERE source_system=%s AND source_database IS NOT DISTINCT FROM %s AND '
             f'source_schema IS NOT DISTINCT FROM %s AND {col}=%s', (system, db, schema, val))


# Same snapshot as the pipeline's own reconciliation: only records created at or before the latest run's start.
_run = q("SELECT start_time FROM audit.pipeline_runs WHERE status <> 'FATAL' ORDER BY start_time DESC LIMIT 1")
CUTOFF = _run[0][0] if _run else None
WM_COLS = dict(q("SELECT source_key, watermark_column FROM audit.source_registry"))

results = []   # dicts


def reconcile(key, src_pairs, src_count, ignore=()):
    """src_pairs: list[(id, hash)] of every source record in scope. ignore: volatile fields excluded from the hash."""
    reg = registry.get(key)
    if reg is None:
        results.append(dict(key=key, src=src_count, tgt=None, target=None, status="FAIL", missing=src_count,
                            note="UNEXPLAINED: source object was never registered/ingested", explained=False,
                            total=None, deleted=None, superseded=None))
        return
    trows = target_rows(reg)
    live = [r for r in trows if r[3] and r[4] != "DELETED"]
    tgt = len(live)
    tpairs = {(r[0] or "", r[1]) for r in live}
    spairs = {(i or "", h) for i, h in src_pairs}
    bad_payload = sum(1 for r in trows if payload_hash_from_text(r[2], ignore) != r[1])
    missing = len(spairs - tpairs)                    # source record absent from the live target (or at another version)
    extra = len(tpairs - spairs)                      # live target record not in the source with that content
    src_dupe_rows = src_count - len(spairs)
    # history integrity per record: contiguous versions from 1, exactly one current = the last, DELETED only last,
    # every version after v1 linked to the one before it
    by_id = defaultdict(list)
    for r in trows:
        by_id[r[0] or ""].append(r)
    broken = 0
    for vs in by_id.values():
        vs.sort(key=lambda r: r[5])
        ok = [r[5] for r in vs] == list(range(1, len(vs) + 1))
        ok = ok and sum(1 for r in vs if r[3]) == 1 and bool(vs[-1][3])
        ok = ok and all(r[4] != "DELETED" for r in vs[:-1])
        ok = ok and all(vs[k][7] == vs[k - 1][6] for k in range(1, len(vs)))
        broken += not ok
    diff = src_count - tgt
    notes = []
    if missing:
        notes.append(f"UNEXPLAINED: {missing} source record(s) not live in target (absent or different content)")
    if extra:
        notes.append(f"UNEXPLAINED: {extra} live target record(s) not in source (deletion not flagged or stale version)")
    if src_dupe_rows:
        notes.append(f"UNEXPLAINED: {src_dupe_rows} source row(s) share an identity")
    if bad_payload:
        notes.append(f"UNEXPLAINED: {bad_payload} payload(s) do not match stored hash")
    if broken:
        notes.append(f"UNEXPLAINED: {broken} record(s) with an inconsistent version history")
    if diff:
        notes.append(f"UNEXPLAINED: live count differs by {diff}")
    ok_all = not notes
    results.append(dict(key=key, src=src_count, tgt=tgt, target=f"{reg[6]}.{reg[7]}", status="PASS" if ok_all else "FAIL",
                        missing=missing, note="; ".join(notes) or "exact match", explained=ok_all,
                        total=len(trows), deleted=sum(1 for r in trows if r[4] == "DELETED"),
                        superseded=sum(1 for r in trows if not r[3])))


# ---------------------------------------------------------------- MongoDB sources
for cfg in MONGO_SOURCES:
    task = MongoIngestTask(cfg, S)
    client = task._client()
    for dbname in task._databases(client):
        db = client[dbname]
        for info in sorted(db.list_collections(filter={"type": "collection"}), key=lambda i: i["name"]):
            name = info["name"]
            if name.startswith("system."):
                continue
            ignore = cfg.ignore_for(dbname, name)
            pairs = []
            cut = mongo_cutoff_filter(db[name], CUTOFF) if CUTOFF else {}
            for doc in db[name].find(cut, batch_size=1000):
                text, h = serialise(doc, ignore)
                pairs.append((record_id(doc["_id"]), h))
            reconcile(f"{cfg.source_system}|{dbname}||{name}", pairs, db[name].count_documents(cut), ignore)
    client.close()

# ---------------------------------------------------------------- PostgreSQL sources
from psycopg2 import sql  # noqa: E402

for cfg in PG_SOURCES:
    task = PgIngestTask(cfg, S)
    for dbname in task._databases():
        conn = task._connect(dbname)
        tz_cols = load_tz_columns(conn)
        objs = [(s, t, None) for s, t in task._application_tables(conn, dbname)]
        objs += [(t.schema, t.table, t) for t in cfg.telemetry_tables if t.database == dbname]
        for schema, table, tel in objs:
            pk, _, _ = task._profile(conn, schema, table)
            if tel and tel.id_columns:
                pk = list(tel.id_columns)
            rid = (sql.SQL("concat_ws('|', {})").format(sql.SQL(", ").join(
                sql.SQL("t.{}::text").format(sql.Identifier(c)) for c in pk)) if pk else sql.SQL("NULL::text"))
            ckey = f"{cfg.source_system}|{dbname}|{schema}|{table}"
            col = pick_pg_cutoff_column(tz_cols.get((schema, table), set()), tel, WM_COLS.get(ckey)) if CUTOFF and pk else None
            where, params = (sql.SQL(" WHERE ") + pg_cutoff_clause(col), (CUTOFF,)) if col else (sql.SQL(""), ())
            cur = conn.cursor()
            cur.execute(sql.SQL("SELECT to_jsonb(t)::text, {} FROM {} t").format(rid, sql.Identifier(schema, table)) + where, params)
            pairs, occ = [], {}
            for text, r_id in cur.fetchall():
                h = payload_hash_from_text(text)
                pairs.append((r_id if pk else occurrence_id(h, occ), h))      # keyless: content hash + occurrence
            cur.execute(sql.SQL("SELECT count(*) FROM {} t").format(sql.Identifier(schema, table)) + where, params)
            cnt = cur.fetchone()[0]
            conn.rollback()
            reconcile(ckey, pairs, cnt)
        conn.close()

# ---------------------------------------------------------------- registry entries with no discovered source (API etc.)
seen = {r["key"] for r in results}
orphans = [k for k in registry if k not in seen]

# ---------------------------------------------------------------- duplicate / integrity checks on target tables
nulls_total, dup_total = 0, 0
tables = sorted({(r[6], r[7]) for r in registry.values()})
for tschema, ttable in tables:
    shared = tschema == "telemetry"
    ident = ("source_system, coalesce(source_database,''), coalesce(source_schema,''), coalesce(source_table,''), "
             "coalesce(source_collection,''), coalesce(source_record_id,'')") if shared else "coalesce(source_record_id,'')"
    d_ver = q(f'SELECT count(*) FROM (SELECT 1 FROM "{tschema}"."{ttable}" GROUP BY {ident}, record_version '
              f'HAVING count(*)>1) x')[0][0]                      # two rows with the same version number
    d_cur = q(f'SELECT count(*) FROM (SELECT 1 FROM "{tschema}"."{ttable}" WHERE is_current GROUP BY {ident} '
              f'HAVING count(*)>1) x')[0][0]                      # two current versions of one record
    n = q(f'SELECT count(*) FROM "{tschema}"."{ttable}" WHERE source_system IS NULL OR source_type IS NULL OR '
          f'ingestion_id IS NULL OR payload_hash IS NULL OR payload IS NULL OR valid_from IS NULL')[0][0]
    ing = q(f'SELECT count(*) FROM "{tschema}"."{ttable}" t WHERE NOT EXISTS '
            f'(SELECT 1 FROM audit.pipeline_runs p WHERE p.pipeline_run_id=t.ingestion_id)')[0][0]
    dup_total += d_ver + d_cur
    nulls_total += n + ing

# ---------------------------------------------------------------- audit reconciliation
audit = {}
audit["batches_total"], audit["batches_bad"] = q(
    "SELECT count(*), count(*) FILTER (WHERE status NOT IN ('SUCCESS','RESET')) FROM audit.ingestion_batches")[0]
audit["failed_records"] = q("SELECT coalesce(sum(records_failed),0) FROM audit.ingestion_batches")[0][0]
audit["error_logs"] = q("SELECT count(*) FROM audit.error_logs")[0][0]
audit["runs"] = q("SELECT pipeline_run_id, status, tasks_failed, records_inserted FROM audit.pipeline_runs ORDER BY start_time")
audit["run_vs_jobs"] = q("""SELECT count(*) FROM audit.pipeline_runs p WHERE p.status<>'FATAL' AND p.end_time IS NOT NULL AND
    p.records_inserted <> (SELECT coalesce(sum(records_inserted),0) FROM audit.job_execution j WHERE j.pipeline_run_id=p.pipeline_run_id)""")[0][0]
audit["jobs_vs_batches"] = q("""SELECT count(*) FROM audit.job_execution j WHERE j.records_inserted <>
    (SELECT coalesce(sum(records_inserted),0) FROM audit.ingestion_batches b WHERE b.pipeline_run_id=j.pipeline_run_id AND b.task=j.task)""")[0][0]
audit["unfinished"] = q("SELECT count(*) FROM audit.pipeline_runs WHERE end_time IS NULL")[0][0]
# (a) ALL rows in the target (every version) = rows at the history migration (its baseline batch) + everything inserted
#     by the batches after it, minus rows removed by a manual reset. Batches before the migration used the old
#     mirror/purge accounting and are not part of this check.
ins = defaultdict(int)
for sysn, db, sch, obj, n in q("""SELECT b.source_system, b.source_database, b.source_schema, b.source_object,
        base.records_read + coalesce(sum(b.records_inserted - coalesce(b.records_deleted,0)) FILTER (WHERE b.batch_id > base.batch_id), 0)
        FROM audit.ingestion_batches b JOIN audit.ingestion_batches base ON base.task = 'history_migration'
          AND base.source_system = b.source_system AND base.source_database IS NOT DISTINCT FROM b.source_database
          AND base.source_schema IS NOT DISTINCT FROM b.source_schema AND base.source_object = b.source_object
        GROUP BY b.source_system, b.source_database, b.source_schema, b.source_object, base.records_read, base.batch_id"""):
    ins[(sysn, db, sch, obj)] += int(n)
# (b) live records = sum(new) - sum(flagged deleted) over the batches that carry change counts (incl. the migration baseline)
live_by_batches = defaultdict(int)
for sysn, db, sch, obj, n in q("""SELECT source_system, source_database, source_schema, source_object,
        sum(records_new - coalesce(records_flagged_deleted,0)) FROM audit.ingestion_batches
        WHERE records_new IS NOT NULL GROUP BY 1,2,3,4"""):
    live_by_batches[(sysn, db, sch, obj)] += int(n)
audit_mismatch, live_mismatch = [], []
for r in results:
    reg = registry.get(r["key"])
    if reg and r["tgt"] is not None:
        a = ins.get((reg[1], reg[3], reg[4], reg[5]), 0)
        if a != r["total"]:
            audit_mismatch.append((r["key"], a, r["total"]))
        b = live_by_batches.get((reg[1], reg[3], reg[4], reg[5]), 0)
        if b != r["tgt"]:
            live_mismatch.append((r["key"], b, r["tgt"]))
latest_bad = q("""SELECT count(*) FROM (SELECT DISTINCT ON (source_system, source_database, source_schema, source_object) status
        FROM audit.ingestion_batches ORDER BY source_system, source_database, source_schema, source_object, batch_id DESC) l
        WHERE status NOT IN ('SUCCESS')""")[0][0]

# ---------------------------------------------------------------- report
results.sort(key=lambda r: (r["target"] or "", r["key"]))
total = len(results)
passed = sum(r["status"] == "PASS" for r in results)
src_sum = sum(r["src"] for r in results)
tgt_sum = sum(r["tgt"] or 0 for r in results)
unexpl = [r for r in results if not r["explained"]]
rej = q("SELECT count(*) FROM telemetry.network_rejected_events")[0][0]

lines = ["# CRL Master DB - end-to-end reconciliation", "", f"Generated: {ist_text(now_ist())}",
         f"Source counted as of: {ist_text(CUTOFF) if CUTOFF else 'now (no pipeline run found)'} "
         "(start of the latest pipeline run; later source records belong to the next run)",
         "Target Live Records = latest version of each record, not flagged DELETED (view `<table>__current`).", "",
         "| Source Table | Target Table | Source Row Count | Target Live Records | Difference | Status | "
         "Deleted (flagged) | Superseded versions | Total rows (all versions) | Note |",
         "| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |"]
for r in results:
    src_name = r["key"].split("|", 1)[0] + ": " + ".".join(x for x in r["key"].split("|")[1:] if x)
    d = "" if r["tgt"] is None else r["src"] - r["tgt"]
    lines.append(f"| `{src_name}` | `{r['target']}` | {r['src']} | {r['tgt']} | {d} | {r['status']} | {r['deleted']} | "
                 f"{r['superseded']} | {r['total']} | {r['note']} |")
lines += ["", "## Summary", "",
          f"- Total tables validated: {total}", f"- Passed (live records match the source): {passed}",
          f"- Failed: {total - passed}",
          f"- Total source records: {src_sum}", f"- Total live target records (for these objects): {tgt_sum}",
          f"- Total record difference (source - live target): {src_sum - tgt_sum}",
          f"- History kept: {sum(r['total'] or 0 for r in results)} rows in total, "
          f"{sum(r['deleted'] or 0 for r in results)} records flagged DELETED, "
          f"{sum(r['superseded'] or 0 for r in results)} superseded versions",
          f"- Rejected records (source telemetry.rejected_events rows): {rej}",
          f"- Records failed during ingestion (audit): {audit['failed_records']}",
          f"- Source records not live in target: {sum(r['missing'] for r in results)}",
          f"- Unexplained discrepancies: {len(unexpl)} table(s)", f"- Registry entries with no discovered source: {len(orphans)}",
          f"- Duplicate versions / duplicate current versions in target tables: {dup_total}",
          f"- Target rows with null metadata / unknown ingestion_id: {nulls_total}",
          "", "## Audit checks", "",
          f"- Batches: {audit['batches_total']} total, {audit['batches_bad']} not SUCCESS, latest-batch-per-object not SUCCESS: {latest_bad}",
          f"- audit.retired_records (legacy archive of source-purged rows, kept): {q('SELECT count(*) FROM audit.retired_records')[0][0]}",
          f"- error_logs rows: {audit['error_logs']}", f"- Unfinished runs: {audit['unfinished']}",
          f"- Runs whose inserted total != sum of job_execution: {audit['run_vs_jobs']}",
          f"- Jobs whose inserted total != sum of batches: {audit['jobs_vs_batches']}",
          f"- Objects where sum(batch inserted) != ALL target rows: {len(audit_mismatch)} {audit_mismatch[:5]}",
          f"- Objects where sum(new) - sum(flagged deleted) != live target records: {len(live_mismatch)} {live_mismatch[:5]}",
          "- Runs: " + "; ".join(f"{a}={b}(failed tasks {c})" for a, b, c, _ in audit["runs"])]
Path("validation_report.md").write_text("\n".join(lines), encoding="utf-8")
print("\n".join(lines[-28:]))
print("\nnon-PASS / unexplained rows:")
for r in results:
    if r["status"] != "PASS" or not r["explained"]:
        print(r["key"], r["src"], r["tgt"], r["missing"], r["note"])

"""Raw-record persistence into the Master DB: versioned history. A payload row is never modified or removed; a changed
record becomes a new version (UPDATED), a record gone from the source is flagged DELETED, a new one is NEW. Only the
bookkeeping columns (is_current, valid_to, change_type -> DELETED, deleted_*) of existing rows are ever updated."""
from dataclasses import dataclass

from psycopg2 import sql
from psycopg2.extras import execute_values


@dataclass
class RawRecord:
    source_record_id: str | None
    source_timestamp: object | None      # datetime or None
    payload_text: str                    # original payload as JSON text
    payload_hash: str
    endpoint: str | None = None


@dataclass
class ChangeCounts:
    new: int = 0
    updated: int = 0
    skipped: int = 0

    @property
    def inserted(self) -> int:
        return self.new + self.updated


class DeletionGuard(RuntimeError):
    """Deletion flagging refused for safety (the source looks unreadable); data already ingested is unaffected."""


@dataclass(frozen=True)
class SourceRef:
    """Where the records come from. Fields that do not apply to a source stay None."""
    source_system: str
    source_type: str                     # mongodb | postgresql | api
    source_database: str | None = None
    source_schema: str | None = None
    source_table: str | None = None
    source_collection: str | None = None


_BASE_COLS = ("source_system", "source_type", "source_database", "source_schema", "source_table",
              "source_collection", "source_record_id", "source_timestamp", "ingestion_id",
              "payload_hash", "payload")


_OBJ_WHERE = ("t.source_system=%s AND t.source_database IS NOT DISTINCT FROM %s AND "
               "t.source_schema IS NOT DISTINCT FROM %s AND t.source_table IS NOT DISTINCT FROM %s AND "
               "t.source_collection IS NOT DISTINCT FROM %s")


def _obj_params(ref: SourceRef) -> list:
    return [ref.source_system, ref.source_database, ref.source_schema, ref.source_table, ref.source_collection]


def ref_from_registry(system: str, stype: str, db: str | None, schema: str | None, obj: str) -> SourceRef:
    """SourceRef for an audit.source_registry row."""
    if stype == "mongodb":
        return SourceRef(system, stype, db, schema, None, obj.split(".", 1)[1])
    if stype == "api":
        return SourceRef(system, stype, db)             # source_database = base URL; no schema/table
    return SourceRef(system, stype, db, schema, obj[len(db) + len(schema) + 2:], None)


class MasterRepository:
    def __init__(self, conn):
        self.conn = conn

    def delete_object_rows(self, schema: str, table: str, ref: SourceRef) -> int:
        """DESTROYS this source object's rows (all versions). Only the manual `--reset ... --destroy-history` uses it."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql.SQL("DELETE FROM {} t WHERE ").format(sql.Identifier(schema, table))
                            + sql.SQL(_OBJ_WHERE), _obj_params(ref))
                deleted = cur.rowcount
            self.conn.commit()
            return deleted
        except Exception:
            self.conn.rollback()
            raise

    def _current_rows(self, cur, schema: str, table: str, ref: SourceRef, idents: list[str]) -> dict:
        """identity -> (master_record_id, record_version, payload_hash, change_type) of each record's latest version."""
        cur.execute(sql.SQL("SELECT coalesce(t.source_record_id,''), t.master_record_id, t.record_version, "
                            "t.payload_hash, t.change_type FROM {} t WHERE ").format(sql.Identifier(schema, table))
                    + sql.SQL(_OBJ_WHERE + " AND t.is_current AND coalesce(t.source_record_id,'') = ANY(%s)"),
                    _obj_params(ref) + [idents])
        return {r[0]: r[1:] for r in cur.fetchall()}

    def apply_records(self, schema: str, table: str, ref: SourceRef, ingestion_id: str,
                      records: list[RawRecord], with_endpoint: bool = False) -> ChangeCounts:
        """Apply one chunk in its own transaction. Each record is compared with the latest version of its identity:
        unseen -> NEW (v1); same hash -> skipped; different hash -> the previous version is kept (is_current=false) and
        a new version is inserted as UPDATED, linked to it; latest version DELETED (the record came back) -> new
        version, NEW, linked to it. A record that returns to an earlier payload (A->B->A) is a new version too."""
        res = ChangeCounts()
        if not records:
            return res
        chains: dict[str, list[RawRecord]] = {}
        for r in records:
            chains.setdefault(r.source_record_id or "", []).append(r)
        cols = list(_BASE_COLS) + ["record_version", "change_type", "is_current", "valid_from",
                                   "previous_master_record_id"] + (["endpoint"] if with_endpoint else [])
        template = "(" + ",".join(["%s"] * (len(_BASE_COLS) - 1) + ["%s::jsonb", "%s", "%s", "true", "%s", "%s"]
                                  + (["%s"] if with_endpoint else [])) + ")"
        insert = sql.SQL("INSERT INTO {} ({}) VALUES %s RETURNING coalesce(source_record_id,''), master_record_id").format(
            sql.Identifier(schema, table), sql.SQL(",").join(map(sql.Identifier, cols)))
        try:
            with self.conn.cursor() as cur:
                current = self._current_rows(cur, schema, table, ref, list(chains))
                while chains:       # one record per identity per round, so versions of one record chain in order
                    cur.execute("SELECT clock_timestamp()")
                    ts = cur.fetchone()[0]
                    rows, supersede, meta = [], [], {}
                    for ident in list(chains):
                        rec = chains[ident].pop(0)
                        if not chains[ident]:
                            del chains[ident]
                        st = current.get(ident)
                        if st and st[3] != "DELETED" and st[2] == rec.payload_hash:
                            res.skipped += 1
                            continue
                        if st:
                            supersede.append(st[0])
                            version, prev = st[1] + 1, st[0]
                            ctype = "NEW" if st[3] == "DELETED" else "UPDATED"
                        else:
                            version, prev, ctype = 1, None, "NEW"
                        row = [ref.source_system, ref.source_type, ref.source_database, ref.source_schema,
                               ref.source_table, ref.source_collection, rec.source_record_id, rec.source_timestamp,
                               ingestion_id, rec.payload_hash, rec.payload_text, version, ctype, ts, prev]
                        if with_endpoint:
                            row.append(rec.endpoint)
                        rows.append(row)
                        meta[ident] = (version, rec.payload_hash, ctype)
                    if supersede:
                        cur.execute(sql.SQL("UPDATE {} SET is_current=false, valid_to=%s WHERE master_record_id = ANY(%s)")
                                    .format(sql.Identifier(schema, table)), (ts, supersede))
                    if rows:
                        out = execute_values(cur, insert.as_string(cur), rows, template=template,
                                             page_size=len(rows), fetch=True)
                        for ident, master_id in out:
                            version, h, ctype = meta[ident]
                            current[ident] = (master_id, version, h, ctype)
                            if ctype == "UPDATED":
                                res.updated += 1
                            else:
                                res.new += 1
            self.conn.commit()
            return res
        except Exception:
            self.conn.rollback()
            raise

    def flag_deleted(self, schema: str, table: str, ref: SourceRef, source_ids: set[str], run_id: str) -> int:
        """Flag every live record of this object that is no longer in the source as DELETED (rows are kept). Safety:
        a source that returns no ids while the target has live records looks like an outage/partial read: refused."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT coalesce(t.source_record_id,'') FROM {} t WHERE ").format(
                    sql.Identifier(schema, table))
                    + sql.SQL(_OBJ_WHERE + " AND t.is_current AND t.change_type <> 'DELETED'"), _obj_params(ref))
                live = [r[0] for r in cur.fetchall()]
                gone = [i for i in live if i not in source_ids]
                if live and not source_ids:
                    raise DeletionGuard(f"refusing to flag {len(gone)} of {len(live)} records as DELETED: the source "
                                        f"returned no ids (looks like an outage or a partial read)")
                if not gone:
                    self.conn.rollback()
                    return 0
                cur.execute("SELECT clock_timestamp()")
                ts = cur.fetchone()[0]
                cur.execute(sql.SQL("UPDATE {} t SET change_type='DELETED', deleted_at=%s, deleted_ingestion_id=%s, "
                                    "valid_to=%s WHERE ").format(sql.Identifier(schema, table))
                            + sql.SQL(_OBJ_WHERE + " AND t.is_current AND t.change_type <> 'DELETED' "
                                      "AND coalesce(t.source_record_id,'') = ANY(%s)"),
                            [ts, run_id, ts] + _obj_params(ref) + [gone])
                flagged = cur.rowcount
            self.conn.commit()
            return flagged
        except Exception:
            self.conn.rollback()
            raise

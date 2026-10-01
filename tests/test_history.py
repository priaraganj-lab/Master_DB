"""History-tracking behaviour (NEW / UPDATED / DELETED, versions, current-state view) against a throwaway schema in the
master DB. The schema is created and dropped by the fixture; real tables are never touched. Skipped if the DB is
unreachable."""
import json
import uuid

import psycopg2
import pytest
from psycopg2 import sql

from config.settings import load_settings
from master_db.connection import connect_master
from master_db.repository import DeletionGuard, MasterRepository, RawRecord, SourceRef
from master_db.schema_manager import SchemaManager, current_view_name
from utils.hashing import occurrence_id, payload_hash_from_text

REF = SourceRef("test_src", "mongodb", source_database="db", source_collection="coll")
REF2 = SourceRef("test_src", "mongodb", source_database="db", source_collection="other")


def rec(rid, **doc):
    text = json.dumps({"_id": rid, **doc})
    return RawRecord(rid, None, text, payload_hash_from_text(text))


@pytest.fixture()
def env():
    try:
        conn = connect_master(load_settings())
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"master DB not reachable: {e}")
    schema = "zz_hist_test_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    conn.commit()
    sm = SchemaManager(conn)
    sm.ensure_raw_table(schema, "t", shared=False)
    sm.ensure_raw_table(schema, "shared_t", shared=True)
    repo = MasterRepository(conn)

    def rows(table="t", where="true"):
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT source_record_id, record_version, change_type, is_current, valid_to IS NOT NULL, "
                                "previous_master_record_id, master_record_id, deleted_at IS NOT NULL FROM {} WHERE " + where
                                + " ORDER BY source_record_id, record_version").format(sql.Identifier(schema, table)))
            out = cur.fetchall()
        conn.rollback()
        return out

    def view_ids(table="t"):
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT source_record_id FROM {} ORDER BY 1").format(
                sql.Identifier(schema, current_view_name(table))))
            out = [r[0] for r in cur.fetchall()]
        conn.rollback()
        return out

    yield repo, rows, view_ids, schema, conn
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    conn.commit()
    conn.close()


def test_new_unchanged_updated_and_revert(env):
    repo, rows, view_ids, schema, _ = env
    c = repo.apply_records(schema, "t", REF, "run1", [rec("a", v=1), rec("b", v=1)])
    assert (c.new, c.updated, c.skipped) == (2, 0, 0)
    assert [r[:5] for r in rows()] == [("a", 1, "NEW", True, False), ("b", 1, "NEW", True, False)]

    c = repo.apply_records(schema, "t", REF, "run2", [rec("a", v=1), rec("b", v=2)])      # a unchanged, b changed
    assert (c.new, c.updated, c.skipped) == (0, 1, 1)
    b = rows(where="source_record_id='b'")
    assert [(r[1], r[2], r[3], r[4]) for r in b] == [(1, "NEW", False, True), (2, "UPDATED", True, False)]
    assert b[1][5] == b[0][6]                                   # v2 links to v1 (previous version retained)
    assert view_ids() == ["a", "b"]

    repo.apply_records(schema, "t", REF, "run3", [rec("b", v=1)])                        # back to the first payload
    assert [(r[1], r[2], r[3]) for r in rows(where="source_record_id='b'")] == \
        [(1, "NEW", False), (2, "UPDATED", False), (3, "UPDATED", True)]


def test_versions_chain_inside_one_chunk(env):
    repo, rows, _, schema, _ = env
    c = repo.apply_records(schema, "t", REF, "run1", [rec("a", v=1), rec("a", v=2), rec("a", v=3), rec("a", v=3)])
    assert (c.new, c.updated, c.skipped) == (1, 2, 1)
    a = rows()
    assert [(r[1], r[2], r[3]) for r in a] == [(1, "NEW", False), (2, "UPDATED", False), (3, "UPDATED", True)]
    assert a[1][5] == a[0][6] and a[2][5] == a[1][6]


def test_deleted_are_flagged_not_removed_and_can_return(env):
    repo, rows, view_ids, schema, _ = env
    repo.apply_records(schema, "t", REF, "run1", [rec(i, v=1) for i in "abc"])
    assert repo.flag_deleted(schema, "t", REF, {"a"}, "run2") == 2
    assert repo.flag_deleted(schema, "t", REF, {"a"}, "run3") == 0                       # idempotent
    dead = rows(where="change_type='DELETED'")
    assert [(r[0], r[1], r[3], r[4], r[7]) for r in dead] == [("b", 1, True, True, True), ("c", 1, True, True, True)]
    assert len(rows()) == 3                                                              # nothing was removed
    assert view_ids() == ["a"]                                                           # current state = live only

    c = repo.apply_records(schema, "t", REF, "run4", [rec("b", v=1)])                    # b comes back
    assert (c.new, c.updated) == (1, 0)
    b = rows(where="source_record_id='b'")
    assert [(r[1], r[2], r[3]) for r in b] == [(1, "DELETED", False), (2, "NEW", True)]
    assert b[1][5] == b[0][6]
    assert view_ids() == ["a", "b"]


def test_deletion_guard_refuses_empty_source(env):
    repo, rows, _, schema, _ = env
    repo.apply_records(schema, "t", REF, "run1", [rec("a", v=1)])
    with pytest.raises(DeletionGuard):
        repo.flag_deleted(schema, "t", REF, set(), "run2")
    assert rows()[0][2] == "NEW"                                                         # untouched


def test_shared_table_keeps_source_objects_apart(env):
    repo, rows, _, schema, _ = env
    repo.apply_records(schema, "shared_t", REF, "run1", [rec("a", v=1)])
    repo.apply_records(schema, "shared_t", REF2, "run1", [rec("a", v=9)])               # same id, other collection
    assert len(rows("shared_t")) == 2 and all(r[1] == 1 for r in rows("shared_t"))      # two records, not two versions
    assert repo.flag_deleted(schema, "shared_t", REF, {"other-id"}, "run2") == 1        # only REF's 'a' is flagged
    assert [r[2] for r in rows("shared_t", "source_collection='coll'")] == ["DELETED"]
    assert [r[2] for r in rows("shared_t", "source_collection='other'")] == ["NEW"]
    repo.apply_records(schema, "shared_t", REF2, "run3", [rec("a", v=10)])              # versions only within REF2
    assert [r[1] for r in rows("shared_t", "source_collection='other'")] == [1, 2]


def test_only_one_current_version_is_possible(env):
    repo, _, _, schema, conn = env
    repo.apply_records(schema, "t", REF, "run1", [rec("a", v=1)])
    with conn.cursor() as cur, pytest.raises(psycopg2.errors.UniqueViolation):
        cur.execute(sql.SQL("INSERT INTO {} (source_system, source_type, source_record_id, ingestion_id, payload_hash, "
                            "payload, record_version) VALUES ('test_src','mongodb','a','x','h','{{}}',2)")
                    .format(sql.Identifier(schema, "t")))
    conn.rollback()


def test_keyless_rows_keep_every_duplicate():
    seen: dict = {}
    assert [occurrence_id("h", seen), occurrence_id("h", seen), occurrence_id("g", seen)] == ["h#1", "h#2", "g#1"]


def test_migration_backfills_old_layout_and_restores_retired(env):
    from master_db.history_migration import _migrate_table
    from master_db.schema_manager import old_unique_index
    _, _, _, schema, conn = env
    tbl = sql.Identifier(schema, "old_t")
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""CREATE TABLE {} (master_record_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source_system text NOT NULL, source_type text NOT NULL, source_database text, source_schema text,
            source_table text, source_collection text, source_record_id text, source_timestamp timestamptz,
            ingestion_id text NOT NULL, ingestion_timestamp timestamptz NOT NULL DEFAULT clock_timestamp(),
            payload_hash text NOT NULL, payload jsonb NOT NULL)""").format(tbl))
        cur.execute(sql.SQL("CREATE UNIQUE INDEX {} ON {} (coalesce(source_record_id,''), payload_hash)").format(
            sql.Identifier(old_unique_index(schema, "old_t")), tbl))
        for rid, h, ts in [("a", "h1", "2026-01-01"), ("a", "h2", "2026-01-02"), ("b", "h3", "2026-01-01")]:
            cur.execute(sql.SQL("INSERT INTO {} (source_system, source_type, source_database, source_collection, "
                                "source_record_id, ingestion_id, ingestion_timestamp, payload_hash, payload) "
                                "VALUES ('test_src','mongodb','db','coll',%s,'run0',%s,%s,'{{}}')").format(tbl), (rid, ts, h))
        cur.execute("""INSERT INTO audit.retired_records (pipeline_run_id, reason, target_schema, target_table, source_system,
            source_type, source_database, source_collection, source_record_id, payload_hash, payload, original_ingestion_id,
            original_ingestion_timestamp) VALUES ('runX','purged_at_source',%s,'old_t','test_src','mongodb','db','coll','c',
            'h4','{}','run0','2026-01-01')""", (schema,))
        stats = _migrate_table(conn, None, schema, "old_t", [("test_src", "mongodb", "db", None, "db.coll", "k")], True)
        cur.execute(sql.SQL("SELECT source_record_id, record_version, change_type, is_current, previous_master_record_id "
                            "IS NOT NULL, deleted_at IS NOT NULL FROM {} ORDER BY 1, 2").format(tbl))
        out = cur.fetchall()
        cur.execute(sql.SQL("SELECT source_record_id FROM {} ORDER BY 1").format(
            sql.Identifier(schema, current_view_name("old_t"))))
        live = [r[0] for r in cur.fetchall()]
    conn.rollback()                      # nothing of this test (incl. the audit.retired_records row) is kept
    assert out == [("a", 1, "NEW", False, False, False), ("a", 2, "UPDATED", True, True, False),
                   ("b", 1, "NEW", True, False, False), ("c", 1, "DELETED", True, False, True)]
    assert live == ["a", "b"]
    assert stats["rows"] == 3 and stats["reinserted_retired"] == 1 and stats["superseded"] == 1

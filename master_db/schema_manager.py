"""Idempotent schema initialisation. Only CREATE ... IF NOT EXISTS; nothing is dropped or truncated."""
from psycopg2 import sql

from utils.hashing import pg_identifier, short_hash

SCHEMAS = ("application", "telemetry", "audit")

# History-tracking columns of every raw table (appended after the payload columns).
HISTORY_COLUMNS = (
    ("record_version", "int NOT NULL DEFAULT 1"),                 # 1, 2, 3 ... per record
    ("change_type", "text NOT NULL DEFAULT 'NEW' CHECK (change_type IN ('NEW','UPDATED','DELETED'))"),
    ("is_current", "boolean NOT NULL DEFAULT true"),              # the latest version of the record
    ("valid_from", "timestamptz"),                                # when this version was first seen
    ("valid_to", "timestamptz"),                                  # when it was superseded or the record deleted
    ("previous_master_record_id", "bigint"),                      # link to the version this one replaced
    ("deleted_at", "timestamptz"),                                # run that detected the deletion (not the source's time)
    ("deleted_ingestion_id", "text"),
)

CURRENT_FILTER = "is_current AND change_type <> 'DELETED'"        # live (current-state) records


def current_view_name(table: str) -> str:
    """Name of the current-state view of a raw table: <table>__current (stable truncation if too long)."""
    return pg_identifier(table, "current")


def identity_sql(shared: bool) -> str:
    """SQL expression list that identifies ONE source record inside a raw table."""
    if shared:
        return ("source_system, coalesce(source_database,''), coalesce(source_schema,''), coalesce(source_table,''), "
                "coalesce(source_collection,''), coalesce(source_record_id,'')")
    return "coalesce(source_record_id,'')"


def old_unique_index(schema: str, table: str) -> str:
    return f"ux_{table[:40]}_{short_hash(schema + table, 6)}"       # pre-history unique (identity, payload_hash)


def history_index_ddl(schema: str, table: str, shared: bool) -> list:
    """One version number per record, and at most ONE current version per record."""
    tbl = sql.Identifier(schema, table)
    ident = identity_sql(shared)
    uv = sql.Identifier(f"uv_{table[:40]}_{short_hash(schema + table + 'v', 6)}")
    uc = sql.Identifier(f"uc_{table[:40]}_{short_hash(schema + table + 'c', 6)}")
    return [sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (" + ident + ", record_version)").format(uv, tbl),
            sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (" + ident + ") WHERE is_current").format(uc, tbl)]


def current_view_ddl(schema: str, table: str):
    """Current-state view: only live records (latest version, not deleted). History stays in the table itself."""
    return sql.SQL("CREATE OR REPLACE VIEW {} AS SELECT * FROM {} WHERE " + CURRENT_FILTER).format(
        sql.Identifier(schema, current_view_name(table)), sql.Identifier(schema, table))

AUDIT_DDL = [
    """CREATE TABLE IF NOT EXISTS audit.source_registry (
        source_key          text PRIMARY KEY,
        source_system       text NOT NULL,
        source_type         text NOT NULL,
        source_database     text,
        source_schema       text,
        source_object       text NOT NULL,
        target_schema       text NOT NULL,
        target_table        text NOT NULL,
        incremental_strategy text,
        watermark_column    text,
        watermark_value     text,
        watermark_updated_at timestamptz,
        first_seen_at       timestamptz NOT NULL DEFAULT now(),
        last_seen_at        timestamptz NOT NULL DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
        pipeline_run_id text PRIMARY KEY,
        start_time      timestamptz NOT NULL,
        end_time        timestamptz,
        duration_ms     bigint,
        status          text NOT NULL,
        tasks_total     int, tasks_success int, tasks_failed int, tasks_skipped int,
        records_read bigint, records_inserted bigint, records_skipped bigint, records_failed bigint,
        host            text,
        error_message   text
    )""",
    """CREATE TABLE IF NOT EXISTS audit.job_execution (
        job_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        pipeline_run_id text NOT NULL REFERENCES audit.pipeline_runs(pipeline_run_id),
        task            text NOT NULL,
        source          text NOT NULL,
        start_time      timestamptz NOT NULL,
        end_time        timestamptz,
        duration_ms     bigint,
        status          text NOT NULL,
        objects_total int, objects_success int, objects_failed int,
        records_read bigint, records_inserted bigint, records_skipped bigint, records_failed bigint,
        error_message   text
    )""",
    """CREATE TABLE IF NOT EXISTS audit.ingestion_batches (
        batch_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        pipeline_run_id text NOT NULL REFERENCES audit.pipeline_runs(pipeline_run_id),
        task            text NOT NULL,
        source_system   text NOT NULL,
        source_type     text NOT NULL,
        source_database text,
        source_schema   text,
        source_object   text NOT NULL,
        target_schema   text NOT NULL,
        target_table    text NOT NULL,
        strategy        text,
        watermark_from  text,
        watermark_to    text,
        start_time      timestamptz NOT NULL,
        end_time        timestamptz,
        duration_ms     bigint,
        status          text NOT NULL,
        records_read bigint DEFAULT 0, records_inserted bigint DEFAULT 0,
        records_skipped bigint DEFAULT 0, records_failed bigint DEFAULT 0,
        error_message   text
    )""",
    """CREATE TABLE IF NOT EXISTS audit.error_logs (
        error_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        pipeline_run_id text,
        batch_id        bigint,
        task            text,
        source          text,
        source_object   text,
        occurred_at     timestamptz NOT NULL DEFAULT now(),
        error_type      text,
        error_message   text
    )""",
    "ALTER TABLE audit.ingestion_batches ADD COLUMN IF NOT EXISTS records_deleted bigint DEFAULT 0",   # legacy: physical deletes
    # change counts per batch: records_new / records_flagged_deleted track the live-record count exactly
    "ALTER TABLE audit.ingestion_batches ADD COLUMN IF NOT EXISTS records_new bigint, "
    "ADD COLUMN IF NOT EXISTS records_updated bigint, ADD COLUMN IF NOT EXISTS records_flagged_deleted bigint",
    """CREATE TABLE IF NOT EXISTS audit.retired_records (
        retired_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        pipeline_run_id text NOT NULL,
        retired_at      timestamptz NOT NULL DEFAULT now(),
        reason          text NOT NULL,
        target_schema   text NOT NULL,
        target_table    text NOT NULL,
        source_system   text NOT NULL,
        source_type     text NOT NULL,
        source_database text,
        source_schema   text,
        source_table    text,
        source_collection text,
        source_record_id text,
        source_timestamp timestamptz,
        payload_hash    text NOT NULL,
        payload         jsonb NOT NULL,
        original_master_record_id bigint,
        original_ingestion_id text,
        original_ingestion_timestamp timestamptz
    )""",
    "CREATE INDEX IF NOT EXISTS ix_audit_retired_run ON audit.retired_records (pipeline_run_id)",
    # one-time migration: the old layout (source_key PK + bookkeeping columns) is derived data that every run rebuilds
    """DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='audit'
                   AND table_name='reconciliation' AND column_name IN ('source_key', 'time_pipeline_last_ran')) THEN
            DROP VIEW IF EXISTS audit.reconciliation_report;
            DROP TABLE audit.reconciliation;
        END IF;
    END $$""",
    """CREATE TABLE IF NOT EXISTS audit.reconciliation (
        source_table    text PRIMARY KEY,
        target_table    text NOT NULL,
        source_row_count bigint,
        target_row_count bigint,
        difference      bigint,
        status          text NOT NULL CHECK (status IN ('PASS','FAIL','ERROR')),
        status_detail   text,
        time_pipeline_last_ran_ist text NOT NULL
    )""",
    "ALTER TABLE audit.reconciliation ADD COLUMN IF NOT EXISTS target_total_rows bigint, "
    "ADD COLUMN IF NOT EXISTS deleted_records bigint, ADD COLUMN IF NOT EXISTS superseded_versions bigint",
    "COMMENT ON TABLE audit.reconciliation IS 'One row per source->target mapping, refreshed by run_pipeline.py on every "
    "run. Source is counted as of the run start (records created later wait for the next run); target_row_count = LIVE "
    "records (latest version, not DELETED). PASS = they match; FAIL = counts differ (see status_detail); ERROR = "
    "source/target not countable (counts NULL). difference = source_row_count - target_row_count. target_total_rows = "
    "all rows incl. history; deleted_records = records flagged DELETED; superseded_versions = older versions kept. "
    "time_pipeline_last_ran_ist = start time of the last pipeline run in IST (Asia/Kolkata), e.g. 2026-09-30 22:04:34 IST.'",
    """CREATE OR REPLACE VIEW audit.reconciliation_report AS
        SELECT source_table AS "Source Table", target_table AS "Target Table",
               source_row_count AS "Source Row Count", target_row_count AS "Target Row Count",
               difference AS "Difference", status AS "Status",
               time_pipeline_last_ran_ist AS "Time Pipeline Last Ran (IST)",
               status_detail AS "Status Detail",
               target_total_rows AS "Target Total Rows (all versions)", deleted_records AS "Deleted Records",
               superseded_versions AS "Superseded Versions"
        FROM audit.reconciliation ORDER BY source_table""",
    "CREATE INDEX IF NOT EXISTS ix_audit_batches_run ON audit.ingestion_batches (pipeline_run_id)",
    "CREATE INDEX IF NOT EXISTS ix_audit_jobs_run ON audit.job_execution (pipeline_run_id)",
    "CREATE INDEX IF NOT EXISTS ix_audit_errors_run ON audit.error_logs (pipeline_run_id)",
]

REQUIRED_AUDIT_TABLES = ("source_registry", "pipeline_runs", "job_execution", "ingestion_batches", "error_logs",
                         "retired_records", "reconciliation")


class SchemaManager:
    def __init__(self, conn):
        self.conn = conn
        self._known: set[tuple[str, str]] = set()

    def initialise(self) -> None:
        with self.conn.cursor() as cur:
            for s in SCHEMAS:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(s)))
            for ddl in AUDIT_DDL:
                cur.execute(ddl)
        self.conn.commit()

    def verify(self) -> dict:
        with self.conn.cursor() as cur:
            cur.execute("SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)", (list(SCHEMAS),))
            schemas = {r[0] for r in cur.fetchall()}
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='audit'")
            audit_tables = {r[0] for r in cur.fetchall()}
        missing_s = set(SCHEMAS) - schemas
        missing_t = set(REQUIRED_AUDIT_TABLES) - audit_tables
        if missing_s or missing_t:
            raise RuntimeError(f"master DB verification failed: schemas={sorted(missing_s)} audit tables={sorted(missing_t)}")
        return {"schemas": sorted(schemas), "audit_tables": sorted(audit_tables)}

    def ensure_raw_table(self, schema: str, table: str, shared: bool = False, with_endpoint: bool = False) -> None:
        """Create a raw table if missing. `shared` = several source objects land in it (telemetry)."""
        if (schema, table) in self._known:
            return
        tbl = sql.Identifier(schema, table)
        endpoint_col = sql.SQL(", endpoint text") if with_endpoint else sql.SQL("")
        ddl = sql.SQL("""CREATE TABLE IF NOT EXISTS {tbl} (
            master_record_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source_system        text NOT NULL,
            source_type          text NOT NULL,
            source_database      text,
            source_schema        text,
            source_table         text,
            source_collection    text,
            source_record_id     text,
            source_timestamp     timestamptz,
            ingestion_id         text NOT NULL,
            ingestion_timestamp  timestamptz NOT NULL DEFAULT clock_timestamp(),
            payload_hash         text NOT NULL,
            payload              jsonb NOT NULL{endpoint},
            record_version       int NOT NULL DEFAULT 1,
            change_type          text NOT NULL DEFAULT 'NEW' CHECK (change_type IN ('NEW','UPDATED','DELETED')),
            is_current           boolean NOT NULL DEFAULT true,
            valid_from           timestamptz NOT NULL DEFAULT clock_timestamp(),
            valid_to             timestamptz,
            previous_master_record_id bigint,
            deleted_at           timestamptz,
            deleted_ingestion_id text
        )""").format(tbl=tbl, endpoint=endpoint_col)
        iidx = sql.Identifier(f"ix_{table[:40]}_{short_hash(schema + table + 'i', 6)}")
        with self.conn.cursor() as cur:
            cur.execute(ddl)
            for stmt in history_index_ddl(schema, table, shared):
                cur.execute(stmt)
            cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (ingestion_id)").format(iidx, tbl))
            cur.execute(current_view_ddl(schema, table))
        self.conn.commit()
        self._known.add((schema, table))

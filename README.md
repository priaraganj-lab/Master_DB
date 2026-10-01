# CRL Master DB – local ingestion pipeline

Raw, source-preserving ingestion of the application databases and APIs into PostgreSQL `crl_master_db`
(schemas: `application`, `telemetry`, `audit`). No transformations, KPIs or Bronze/Silver/Gold logic.

```
crontab -> run_pipeline.sh -> run_pipeline.py -> Orchestrator (sequential, isolated tasks)
   mongodb_network (mongodb_network_data) | mongodb_elevate (mongodb_elevate_atlas) | postgresql_registry (postgresql_supabase)
   postgresql_network_telemetry (postgresql_network_telemetry) | api_chatbot | api_interface
                     -> crl_master_db.{application, telemetry, audit}
```

## Source names

The four sources are referred to by these names; the technical keys are what the code, `.env` and the audit tables use.

| Source name | Technical key (`.env` / `source_system`) |
| --- | --- |
| MongoDB #1 — Network Data | `mongodb_network_data` |
| MongoDB #2 — Elevate (Atlas) | `mongodb_elevate_atlas` |
| PostgreSQL #1 — Supabase | `postgresql_supabase` |
| PostgreSQL #2 — Network Telemetry | `postgresql_network_telemetry` |

The technical keys were renamed from `mongodb_1/2`, `postgresql_1/2` to the keys above (one-off migration `python -m master_db.rename_source_labels`, applied once; it is idempotent and defaults to a dry run). Application table names use the matching prefixes (`network_data__`, `elevate_atlas__`, `supabase__`), set by `short` in `config/sources.py`; they were renamed from `mongo1__`, `mongo2__`, `pg1__` by `python -m master_db.rename_tables` (applied once; idempotent, dry run by default).

## Layout
`run_pipeline.py` entry point · `config/` (settings from `.env`, source registry) · `ingestion/` (orchestrator, `base.py`,
`mongodb/`, `postgresql/`, `api/`, `cutoff.py`) · `master_db/` (connection, schema manager, repository, `history_migration.py`) · `audit/` (audit repository,
JSON logger) · `utils/` (hashing, secret scrubbing) · `tests/`.

## Source → target mapping (discovered live; nothing assumed)
| Task | Env | Ingested | Target |
|---|---|---|---|
| mongodb_network | `mongodb_network_data` | all 15 non-system DBs (Beckn BAP/BPP/adaptors, marketplace), 66 collections | `application.network_data__<db>__<collection>` |
| mongodb_elevate | `mongodb_elevate_atlas` | all DBs except `sample_mflix` (Atlas demo data): samiksha (business plan), project, elevate-diksha, elevate-notification, Mobident, scp_service | `application.elevate_atlas__<db>__<collection>` (82 tables) |
| mongodb_elevate (telemetry) | `mongodb_elevate_atlas` | every `telemetry` collection incl. `crl_telemetry.telemetry` | `telemetry.elevate_events` (shared) |
| postgresql_registry | `postgresql_supabase` (Supabase) | DBs `postgres`, `mentoring`, `scp`, `public` schema base tables (67; incl. credential/session tables by decision) | `application.supabase__<db>__<table>` |
| postgresql_network_telemetry | `postgresql_network_telemetry` | **only** `global_registry.telemetry.events` | `telemetry.network_events` |
| api_chatbot / api_interface | `API_*` | no endpoint exists yet → task reports `SKIPPED` | `telemetry.chatbot_events` / `interface_events` |

Not ingested: `sample_mflix`; Supabase-managed schemas (`auth`, `storage`, `realtime`, `vault`); materialized views (derived);
`global_registry` registry/bronze/silver/gold/meta schemas (by decision).

## Raw table shape
`master_record_id, source_system, source_type, source_database, source_schema, source_table, source_collection,
source_record_id, source_timestamp, ingestion_id (= pipeline_run_id), ingestion_timestamp, payload_hash, payload jsonb`
(+ `endpoint` on API tables). Fields that don't apply to a source stay NULL. Mongo payloads are stored as canonical
extended JSON (BSON types preserved; round-trip verified byte-exact against source). PostgreSQL rows are captured with
`to_jsonb(row)`.

History columns on every raw table (all tables, all pipelines): `record_version` (1, 2, 3 ... per record), `change_type`
(`NEW` / `UPDATED` / `DELETED`), `is_current` (latest version of the record), `valid_from`, `valid_to`,
`previous_master_record_id` (link to the version this one replaced), `deleted_at`, `deleted_ingestion_id`. See **History
tracking** below.

**Incremental strategy (chosen per object by probing, recorded in `audit.source_registry.incremental_strategy`):**
`updated_at` (Mongo `updatedAt` present & date-typed on every doc) · `timestamp` (PG `updated_at`-like column with no NULLs;
`telemetry.events.received_at`) · `object_id` (immutable Mongo telemetry) · `full` (no verified watermark → full scan,
hash-dedupe). Watermarks re-read a 5 min (timestamp) / 1 h (ObjectId) overlap window and advance only after a fully
successful object.

## Reconciliation (`audit.reconciliation`, refreshed by `run_pipeline.py` on every run)
One row per source->target mapping (all of `audit.source_registry`; audit tables are never included). After ingestion,
the source is compared with the target's **live records** (latest version of each record, not flagged DELETED) and the
row is upserted. Read it with `select * from audit.reconciliation_report;` (headings: Source Table, Target Table, Source
Row Count, Target Row Count = live records, Difference, Status, Time Pipeline Last Ran (IST), Status Detail, Target Total
Rows (all versions), Deleted Records, Superseded Versions). `Difference = source - live target`. Status: **PASS** they
match · **FAIL** they differ or the version history is inconsistent (`status_detail` gives the cause) · **ERROR** source or
target not countable: counts stay NULL and `status_detail` has the error (never a silent 0). `time_pipeline_last_ran_ist` is
the run start as text in IST (Asia/Kolkata). FAIL/ERROR rows do not change the exit code.

**Run cutoff.** The sources are live, so each run fixes a cutoff (its start time, `IngestContext.cutoff`): ingestion reads
and reconciliation counts only records created at or before it (`ingestion/cutoff.py`: Mongo by `_id` timestamp or
`createdAt`; Postgres by a `timestamptz` created/arrival column). Records created later are left for the next run, and
`status_detail` says how many ("n record(s) created after this run started … are not counted"). Incremental watermarks are
capped at the cutoff so nothing created after it can be skipped. Objects with no usable creation timestamp (e.g. tables with
only a naive `timestamp` column) are read/counted in full as before; for those a count difference caused by writes during a
run can still appear and is noted in `status_detail`.

History is not a reconciliation difference: records flagged DELETED and superseded versions are kept in the table but are
not "live", so they are reported in their own columns instead of being netted against the source count.

## Audit (schema `audit`)
`pipeline_runs` (run status, totals) · `job_execution` (task level) · `ingestion_batches` (per collection/table: start/end,
duration, read/inserted/skipped/failed, new/updated/flagged-deleted counts, watermark, status, error) · `error_logs` · `source_registry` (source→target mapping,
strategy, watermark). Skipped `ingestion_logs`/`data_quality` as redundant (JSON logs in `logs/`).

```sql
select pipeline_run_id,status,duration_ms,records_inserted from audit.pipeline_runs order by start_time desc limit 5;
select task,status,records_read,records_inserted,duration_ms,error_message from audit.job_execution where pipeline_run_id='RUN_...';
```

## Run
Exit codes: `0` success (skips OK) · `1` a task failed/partial · `2` fatal (target unreachable, crash) · `3` another run in progress.
```
python -m venv .venv && .venv/bin/pip install -r requirements.txt     # (Windows: .venv\Scripts\...)
.venv/bin/python run_pipeline.py
.venv/bin/python -m pytest tests
```

## Cron (WSL Ubuntu)
```
cd /mnt/c/Users/LENOVO/CRL_master_DB && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
chmod +x run_pipeline.sh
sudo service cron start            # WSL does not start cron automatically; WSL must be running
crontab -e   ->   0 2 * * * /mnt/c/Users/LENOVO/CRL_master_DB/run_pipeline.sh
```
`run_pipeline.sh` only selects the project venv and appends stdout to `logs/cron.log`; cron owns *when*, the pipeline owns the rest.
Overlapping triggers are safe (Postgres advisory lock → exit 3).

## Environment variables
Sources (required): `mongodb_network_data`, `mongodb_elevate_atlas`, `postgresql_supabase`, `postgresql_network_telemetry`. Target: `CRL_MASTER_DB_URL` (set in `.env`,
separate from the sources; if unset, falls back to `CRL_MASTER_DB_NAME` on the `postgresql_network_telemetry` server/credentials). Tuning: `INGEST_CHUNK_SIZE`,
`CONNECT_TIMEOUT_SECONDS`, `LOG_DIR`, `LOG_LEVEL`. API: see `.env.example`.

## History tracking (every pipeline, every table)
Nothing is ever lost because the source changed. Payload rows are never modified or removed; only bookkeeping columns change.
- **New record** → inserted as `NEW`, version 1.
- **Updated record** (content hash differs from the record's latest version) → the previous version is kept
  (`is_current=false`, `valid_to` set) and a new version is inserted as `UPDATED`, linked through `previous_master_record_id`.
  A record that returns to an earlier payload (A→B→A) is a new version too.
- **Deleted record** → the row is kept and its latest version is flagged `change_type='DELETED'` (`deleted_at`,
  `deleted_ingestion_id` = the run that detected it, i.e. run granularity, not the source's exact deletion time). If the
  record comes back, a new `NEW` version is added, linked to the deleted one.
- **Deletion detection** runs for every object after a complete, failure-free read, whatever the incremental strategy: ALL
  source ids (not just the changed ones, and not limited by the run cutoff) are compared with the live records. Refused (and
  logged, object `PARTIAL`) if the source returns no ids while the target has live records (outage / partial read).
  A record created and deleted entirely between two runs is never seen.
- **Identity:** Mongo `_id`; PostgreSQL primary key columns joined with `|`; keyless tables (and API records without an id)
  use `<content hash>#<occurrence>`, so identical rows are all kept (a changed keyless row appears as one DELETED + one NEW).
  Keyless tables are always read in full.
- **Volatile fields** (`config/sources.py: volatile_fields`): top-level fields the source rewrites hourly without a real
  change (`discovery_index`: `seenAt`, `sourceTransactionId`; `discovery_sync_state`: `inFlightTransactionId`,
  `lastTransactionId`, `lastCompletedAt`, `lastRequestedAt`; verified on live data) are left out of the change hash only. The
  payload is stored in full, so these values are those of the last real change.
- **Constraints:** unique `(identity, record_version)` and a partial unique index on `(identity) WHERE is_current` → exactly
  one current version per record.
- Each batch records `records_new`, `records_updated`, `records_flagged_deleted`; live records = Σnew − Σflagged-deleted
  (checked by `validate_pipeline.py`). `records_deleted` now only counts rows removed by a manual reset.

### Current state vs history
- Every raw table `<schema>.<table>` has a view **`<schema>.<table>__current`** (name truncated with a stable hash suffix if
  over 63 chars): only live records, i.e. `is_current AND change_type <> 'DELETED'`. **Consume current-state data from
  these views.** The table itself is the complete history.
- In the table: `is_current` marks the latest version of each record; `change_type='DELETED'` marks records gone from the
  source; `record_version`/`previous_master_record_id` walk the versions.
```sql
select * from application.network_data__crl_discovery_adaptor__assignments__current;           -- current state
select * from application.network_data__crl_discovery_adaptor__assignments where source_record_id = '...' order by record_version;  -- history of one record
select change_type, count(*) from telemetry.network_events group by 1;                    -- NEW / UPDATED / DELETED
```
- **Migration:** the first run after this change converts every raw table in place (`master_db/history_migration.py`, one
  transaction per table, idempotent): versions are rebuilt from ingestion order, rows archived in `audit.retired_records`
  (`audit.retired_records` is kept) are copied back flagged `DELETED`, and one `history_backfill` batch per object keeps the
  audit counts reconcilable. `python run_pipeline.py --history-dry-run` shows what it would do and rolls everything back.
  History that older code physically deleted before the migration (mirror mode) cannot be recovered.
- **Manual reset** (never schedule): `python run_pipeline.py --reset "<source_key>" --destroy-history` permanently deletes ALL
  versions of one object's target rows, clears its watermark and reloads it in full (audited as a `RESET` batch). Without
  `--destroy-history` it refuses to run.

## Known limitations
- Rows changed in place *without* touching `updated_at`/`updatedAt`, in objects using an incremental strategy, are missed
  until a full scan (a hash comparison happens only for the records that are read).
- Naive `timestamp without time zone` source values are stored as UTC in `source_timestamp` (metadata only; payload untouched).
- **Time zone:** every timestamp the pipeline produces is IST (UTC+05:30): JSON logs, run ids (`RUN_<IST date/time>`),
  `audit.*` / `ingestion_timestamp` columns (the master DB session runs with `timezone=Asia/Kolkata`; `timestamptz`
  stores the same instant, only the display is IST), watermarks, `validation_report.md`. Defined once in `utils/timeutil.py`.
  Not converted: source payloads (raw copies, hash-protected) and files written before this change (older logs/watermarks stay UTC;
  they still parse correctly).
- API connectors are implemented and unit-tested against a mock server only; untested against real endpoints (none exist yet).
- First full load took ~8.5 min (remote round-trips per collection); reruns ~2–3 min. Sequential by design.
- Credential/session/OTP/device-token tables are copied raw per decision: restrict access to `crl_master_db`.
- `mongodb_network_data` accepts unauthenticated connections from this host (source-side security observation).

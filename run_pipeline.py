#!/usr/bin/env python
"""Single entry point for the CRL Master DB ingestion pipeline (triggered by host cron).

Exit codes: 0 = all tasks succeeded (skipped tasks are not failures)
            1 = at least one task failed / partially failed
            2 = fatal: target unreachable, schema init failed or unexpected crash
            3 = another pipeline run is already in progress (nothing was run)
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from audit.audit_repository import AuditRepository, now  # noqa: E402
from audit.logger import log_event, setup_logging  # noqa: E402
from audit.reconciliation import reconcile_all  # noqa: E402
from config.settings import load_settings  # noqa: E402
from ingestion.base import IngestContext  # noqa: E402
from ingestion.orchestrator import Orchestrator, build_tasks  # noqa: E402
from master_db.connection import connect_master  # noqa: E402
from master_db.history_migration import migrate_history  # noqa: E402
from master_db.repository import MasterRepository  # noqa: E402
from master_db.schema_manager import SchemaManager  # noqa: E402
from utils.security import scrub  # noqa: E402


def history_dry_run(settings) -> int:
    conn = connect_master(settings)
    try:
        stats = migrate_history(conn, None, dry_run=True)
    finally:
        conn.close()
    for st in stats:
        print(st)
    print(f"history migration dry run: {len(stats)} table(s) would be migrated, nothing was written")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="CRL Master DB ingestion pipeline")
    ap.add_argument("--reset", action="append", default=[], metavar="SOURCE_KEY",
                    help="MANUAL one-off: DESTROY this source object's target rows - all versions, i.e. its whole "
                         "history - (audit.source_registry.source_key), then reload it in full. Needs --destroy-history. "
                         "Never schedule.")
    ap.add_argument("--destroy-history", action="store_true",
                    help="confirms that --reset may permanently delete the history of the objects it names")
    ap.add_argument("--history-dry-run", action="store_true",
                    help="migrate every raw table to the versioned-history layout inside a transaction that is rolled "
                         "back, print what would change, and exit (nothing is written)")
    args = ap.parse_args()
    if args.reset and not args.destroy_history:
        ap.error("--reset permanently deletes ALL versions (the whole history) of the object; "
                 "add --destroy-history to confirm")
    settings = load_settings()
    if args.history_dry_run:
        return history_dry_run(settings)
    boot_logger = logging.getLogger("crl_pipeline_boot")
    audit_conn = data_conn = None
    logger = boot_logger
    run_id = None
    try:
        # target connectivity (audit connection is autocommit and independent of the data connection)
        audit_conn = connect_master(settings, autocommit=True, app_name="crl_pipeline_audit")
        audit = AuditRepository(audit_conn)
        if not audit.try_lock():
            print("Another pipeline run holds the lock; exiting.", file=sys.stderr)
            return 3
        SchemaManager(audit_conn).initialise()          # idempotent: schemas + audit tables only
        start = now()
        run_id = audit.next_run_id(start)
        logger = setup_logging(run_id, settings.log_dir, settings.log_level)
        log_event(logger, logging.INFO, "pipeline started", pipeline_run_id=run_id, status="STARTED",
                  target_database=settings.master_dbname())
        audit.start_run(run_id, start)

        data_conn = connect_master(settings, autocommit=False, app_name="crl_pipeline_data")
        schema = SchemaManager(data_conn)
        info = schema.verify()
        log_event(logger, logging.INFO, "master db verified", pipeline_run_id=run_id, status="OK",
                  schemas=info["schemas"], audit_tables=info["audit_tables"])

        ctx = IngestContext(run_id, settings, MasterRepository(data_conn), schema, audit, logger, cutoff=start)
        migrate_history(data_conn, run_id, logger)      # no-op once every raw table has the history layout
        if args.reset:
            from ingestion.maintenance import reset_objects
            reset_objects(ctx, args.reset)
        orch = Orchestrator(ctx, build_tasks(settings))
        summary = orch.run()
        # end-to-end validation: refresh audit.reconciliation for every source->target mapping
        try:
            rec = reconcile_all(ctx, data_conn, start)
        except Exception as e:  # noqa: BLE001 - never hide a failed reconciliation
            rec = {"error": scrub(f"{type(e).__name__}: {e}")}
            audit.log_error(run_id, "reconciliation", None, None, e)
            summary.status = "PARTIAL_FAILURE" if summary.status == "SUCCESS" else summary.status
        audit.finish_run(run_id, now(), summary.status, summary.totals())
        print(summary.render())
        print("RECONCILIATION:", rec)
        log_event(logger, logging.INFO if summary.status == "SUCCESS" else logging.ERROR, "pipeline finished",
                  pipeline_run_id=run_id, status=summary.status, duration_ms=int(summary.duration_s * 1000),
                  **summary.totals())
        return orch.exit_code(summary)
    except Exception as e:  # noqa: BLE001 - fatal; make sure it is visible and audited when possible
        msg = scrub(f"{type(e).__name__}: {e}")
        print(f"FATAL: {msg}", file=sys.stderr)
        log_event(logger, logging.CRITICAL, "pipeline fatal error", pipeline_run_id=run_id, status="FATAL", error=msg)
        if audit_conn is not None and run_id:
            try:
                AuditRepository(audit_conn).log_error(run_id, None, None, None, e)
                AuditRepository(audit_conn).finish_run(
                    run_id, now(), "FATAL",
                    {"tasks_total": 0, "tasks_success": 0, "tasks_failed": 0, "tasks_skipped": 0,
                     "read": 0, "inserted": 0, "skipped": 0, "failed": 0}, msg)
            except Exception:  # noqa: BLE001
                pass
        return 2
    finally:
        for c in (data_conn, audit_conn):
            if c is not None:
                try:
                    c.close()
                except Exception:  # noqa: BLE001
                    pass


if __name__ == "__main__":
    sys.exit(main())

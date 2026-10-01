"""Local, sequential orchestrator: preflight -> run each task in isolation -> audit -> summary."""
import logging
import time
from dataclasses import dataclass, field

from audit.audit_repository import AuditRepository, now
from audit.logger import log_event
from config.settings import Settings
from ingestion.api import chatbot, interface
from ingestion.base import FAILED, PARTIAL, SKIPPED, SUCCESS, IngestContext, SourceTask, TaskResult
from ingestion.mongodb import elevate, network
from ingestion.postgresql import network_telemetry, registry
from master_db.repository import MasterRepository
from master_db.schema_manager import SchemaManager
from utils.security import scrub


@dataclass
class PipelineSummary:
    run_id: str
    status: str
    duration_s: float
    tasks: list[tuple[str, str, TaskResult, float]] = field(default_factory=list)

    def totals(self) -> dict:
        t = {"tasks_total": len(self.tasks), "tasks_success": 0, "tasks_failed": 0, "tasks_skipped": 0,
             "read": 0, "inserted": 0, "skipped": 0, "failed": 0}
        for _, _, r, _ in self.tasks:
            t["tasks_success"] += r.status == SUCCESS
            t["tasks_skipped"] += r.status == SKIPPED
            t["tasks_failed"] += r.status in (FAILED, PARTIAL)
            for k in ("read", "inserted", "skipped", "failed"):
                t[k] += getattr(r, k)
        return t

    def render(self) -> str:
        lines = [f"PIPELINE {self.run_id}: {self.status} ({self.duration_s:.1f}s)",
                 f"{'TASK':32}{'STATUS':10}{'READ':>9}{'NEW':>9}{'DUP':>9}{'FAIL':>6}{'SEC':>7}  NOTE"]
        for name, _, r, secs in self.tasks:
            lines.append(f"{name:32}{r.status:10}{r.read:>9}{r.inserted:>9}{r.skipped:>9}{r.failed:>6}{secs:>7.1f}  "
                         f"{(r.error or '')[:90]}")
        return "\n".join(lines)


def build_tasks(settings: Settings) -> list[SourceTask]:
    return [network.build_task(settings), elevate.build_task(settings),
            registry.build_task(settings), network_telemetry.build_task(settings),
            chatbot.build_task(settings), interface.build_task(settings)]


class Orchestrator:
    def __init__(self, ctx: IngestContext, tasks: list[SourceTask]):
        self.ctx, self.tasks = ctx, tasks

    def run(self) -> PipelineSummary:
        ctx, log = self.ctx, self.ctx.logger
        t_run = time.monotonic()
        summary = PipelineSummary(ctx.run_id, "RUNNING", 0.0)

        # 1. preflight: validate every source before ingesting anything
        preflight: dict[str, str | None] = {}
        for task in self.tasks:
            ok, reason = task.enabled()
            if not ok:
                preflight[task.name] = f"SKIPPED:{reason}"
                continue
            try:
                task.check_connection()
                preflight[task.name] = None
                log_event(log, logging.INFO, "source reachable", pipeline_run_id=ctx.run_id, source=task.source_system,
                          task=task.name, status="OK")
            except Exception as e:  # noqa: BLE001
                preflight[task.name] = f"FAILED:{scrub(f'{type(e).__name__}: {e}')}"
                log_event(log, logging.ERROR, "source unreachable", pipeline_run_id=ctx.run_id,
                          source=task.source_system, task=task.name, status=FAILED, error=preflight[task.name][7:])

        # 2. run tasks sequentially; one failure never stops the others
        for task in self.tasks:
            start, t0 = now(), time.monotonic()
            pf = preflight[task.name]
            if pf is not None:
                status, msg = pf.split(":", 1)
                result = TaskResult(status=status, error=msg)
                if status == FAILED:
                    ctx.audit.log_error(ctx.run_id, task.name, task.source_system, None, msg)
            else:
                try:
                    result = task.run(ctx)
                except Exception as e:  # noqa: BLE001 - never let a task crash the pipeline silently
                    result = TaskResult(status=FAILED, error=scrub(f"{type(e).__name__}: {e}"))
                    ctx.audit.log_error(ctx.run_id, task.name, task.source_system, None, e)
            secs = time.monotonic() - t0
            ctx.audit.record_task(ctx.run_id, task.name, task.source_system, start, now(), result.status,
                                  {"objects_total": result.objects_total, "objects_success": result.objects_success,
                                   "objects_failed": result.objects_failed, "read": result.read,
                                   "inserted": result.inserted, "skipped": result.skipped, "failed": result.failed},
                                  result.error)
            log_event(log, logging.INFO if result.status in (SUCCESS, SKIPPED) else logging.ERROR, "task finished",
                      pipeline_run_id=ctx.run_id, source=task.source_system, task=task.name, status=result.status,
                      record_count=result.read, inserted=result.inserted, duration_ms=int(secs * 1000),
                      error=result.error)
            summary.tasks.append((task.name, task.source_system, result, secs))

        # 3. overall status: never SUCCESS when a task failed
        statuses = [r.status for _, _, r, _ in summary.tasks]
        bad = [s for s in statuses if s in (FAILED, PARTIAL)]
        if not bad:
            summary.status = SUCCESS
        elif SUCCESS in statuses or PARTIAL in statuses:
            summary.status = "PARTIAL_FAILURE"
        else:
            summary.status = FAILED
        summary.duration_s = time.monotonic() - t_run
        return summary

    @staticmethod
    def exit_code(summary: PipelineSummary) -> int:
        return 0 if summary.status == SUCCESS else 1

"""Structured (JSON-lines) logging with credential scrubbing."""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from utils.security import scrub
from utils.timeutil import IST


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, IST).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}))
        return scrub(json.dumps(entry, default=str, ensure_ascii=True))


def setup_logging(run_id: str, log_dir: Path, level: str = "INFO") -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("crl_pipeline")
    logger.setLevel(level.upper())
    logger.handlers.clear()
    logger.propagate = False
    fmt = JsonFormatter()
    for h in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_dir / f"{run_id}.jsonl", encoding="utf-8")):
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


def log_event(logger: logging.Logger, level: int, message: str, **fields) -> None:
    """Fields: pipeline_run_id, source, task, status, record_count, duration_ms, error (never raw payloads)."""
    logger.log(level, message, extra={"fields": fields})

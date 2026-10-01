"""Single source of truth for time in this project: every timestamp the pipeline produces is Indian Standard Time."""
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30), "IST")   # no DST, so a fixed offset is exact (and needs no tzdata)
PG_TIMEZONE = "Asia/Kolkata"                            # same zone, as Postgres names it


def now_ist() -> datetime:
    return datetime.now(IST)


def to_ist(dt: datetime) -> datetime:
    """Aware datetime -> IST. Naive values are returned unchanged (their zone is unknown; callers decide)."""
    return dt.astimezone(IST) if dt.tzinfo else dt


def ist_text(dt: datetime) -> str:
    return to_ist(dt).strftime("%Y-%m-%d %H:%M:%S") + " IST"


def ist_iso(dt: datetime) -> str:
    return to_ist(dt).isoformat()

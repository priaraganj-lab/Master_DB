"""PostgreSQL #1 — Supabase (env: postgresql_supabase): registry/user, mentoring and SCP service databases
(`postgres`, `mentoring`, `scp`; `public` schema tables)."""
from config.settings import Settings
from config.sources import PG_SOURCES
from ingestion.postgresql.engine import PgIngestTask


def build_task(settings: Settings) -> PgIngestTask:
    return PgIngestTask(next(s for s in PG_SOURCES if s.source_system == "postgresql_supabase"), settings)

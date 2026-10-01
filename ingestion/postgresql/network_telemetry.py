"""PostgreSQL #2 — Network Telemetry (env: postgresql_network_telemetry): network telemetry = every base table of global_registry.telemetry
(events, ingestion_batches, services, rejected_events, schema_migrations) -> telemetry.network_*.
Partitions are covered by the parent `events`; the view is derived and skipped; the registry/bronze/silver/gold schemas of global_registry are out of scope."""
from config.settings import Settings
from config.sources import PG_SOURCES
from ingestion.postgresql.engine import PgIngestTask


def build_task(settings: Settings) -> PgIngestTask:
    return PgIngestTask(next(s for s in PG_SOURCES if s.source_system == "postgresql_network_telemetry"), settings)

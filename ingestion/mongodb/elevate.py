"""MongoDB #2 — Elevate (Atlas) (env: mongodb_elevate_atlas): Elevate business plan/samiksha, project, diksha, notification, Mobident, SCP
service data. `telemetry` collections are routed to telemetry.elevate_events; sample_mflix is excluded."""
from config.settings import Settings
from config.sources import MONGO_SOURCES
from ingestion.mongodb.engine import MongoIngestTask


def build_task(settings: Settings) -> MongoIngestTask:
    return MongoIngestTask(next(s for s in MONGO_SOURCES if s.source_system == "mongodb_elevate_atlas"), settings)

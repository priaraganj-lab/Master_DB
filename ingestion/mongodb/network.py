"""MongoDB #1 — Network Data (env: mongodb_network_data): network transactional data and catalogs (Beckn BAP/BPP/adaptor databases)."""
from config.settings import Settings
from config.sources import MONGO_SOURCES
from ingestion.mongodb.engine import MongoIngestTask


def build_task(settings: Settings) -> MongoIngestTask:
    return MongoIngestTask(next(s for s in MONGO_SOURCES if s.source_system == "mongodb_network_data"), settings)

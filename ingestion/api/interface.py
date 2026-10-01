"""Interface telemetry API -> telemetry.interface_events (env prefix API_INTERFACE_*)."""
from config.settings import Settings
from ingestion.api.client import ApiIngestTask


def build_task(settings: Settings) -> ApiIngestTask:
    return ApiIngestTask("api_interface", "API_INTERFACE", "interface_events", settings)

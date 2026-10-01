"""Application chatbot telemetry API -> telemetry.chatbot_events (env prefix API_CHATBOT_*)."""
from config.settings import Settings
from ingestion.api.client import ApiIngestTask


def build_task(settings: Settings) -> ApiIngestTask:
    return ApiIngestTask("api_chatbot", "API_CHATBOT", "chatbot_events", settings)

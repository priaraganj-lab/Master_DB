"""PostgreSQL #3 — AI chatbot (env: postgresql_chatbot, database `crl`): the six chatbot tables of its public schema
(conversations, messages, feedback, agentic_interactions, ai_telemetry, response_cache) -> telemetry.chatbot_*.
Optional: skipped while the env var is unset. The server port is only reachable through the SSH tunnel."""
from config.settings import Settings
from config.sources import CHATBOT_SOURCE
from ingestion.postgresql.engine import PgIngestTask


class ChatbotTask(PgIngestTask):
    def enabled(self) -> tuple[bool, str]:
        if not self.settings.uris.get(self.source_system):
            return False, "postgresql_chatbot env var unset"
        return True, ""


def build_task(settings: Settings) -> ChatbotTask:
    return ChatbotTask(CHATBOT_SOURCE, settings)

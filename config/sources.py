"""Source registry. Databases / collections / tables are DISCOVERED at run time; this file only
declares which sources exist, which env var holds each connection, and explicit include/exclude rules.
Everything here was verified against the live systems (see README, 'Discovery')."""
from dataclasses import dataclass, field

from utils.timeutil import PG_TIMEZONE


@dataclass(frozen=True)
class MongoSource:
    task: str
    source_system: str            # env var name, also stored as source_system
    short: str                    # prefix for target table names
    exclude_databases: frozenset = frozenset({"admin", "config", "local"})
    include_databases: frozenset | None = None
    # collections with this name are event streams -> telemetry schema
    telemetry_collection_names: frozenset = frozenset()
    telemetry_target: str | None = None
    # 'db.collection' -> top-level fields the source rewrites without a real change (verified on live data: they differ
    # on every record every hour). They are left out of the change hash only (the payload is stored in full), so a
    # rewrite alone does not create a new version.
    volatile_fields: dict = field(default_factory=dict)

    def ignore_for(self, database: str, collection: str) -> frozenset:
        return frozenset(self.volatile_fields.get(f"{database}.{collection}", ()))


@dataclass(frozen=True)
class PgTelemetryTable:
    database: str
    schema: str
    table: str
    target: str                   # table in the telemetry schema
    watermark_column: str | None = None   # arrival time; None -> full scan + hash dedupe
    timestamp_column: str | None = None   # event time -> source_timestamp
    id_columns: tuple = ()                # override of primary-key based source_record_id


@dataclass(frozen=True)
class PgSource:
    task: str
    source_system: str
    short: str
    databases: tuple = ()                 # databases to scan for application tables
    schemas: tuple = ("public",)          # only user schemas; Supabase-managed schemas are platform internals
    discover_application: bool = True
    telemetry_tables: tuple = ()
    session_timezone: str | None = None   # source session TimeZone: timestamptz values inside the stored payload render in it


MONGO_SOURCES = (
    MongoSource(task="mongodb_network", source_system="mongodb_network_data", short="network_data",
                volatile_fields={
                    "crl_discovery_adaptor.discovery_index": frozenset({"seenAt", "sourceTransactionId"}),
                    "crl_discovery_adaptor.discovery_sync_state": frozenset({
                        "inFlightTransactionId", "lastTransactionId", "lastCompletedAt", "lastRequestedAt"}),
                }),
    MongoSource(
        task="mongodb_elevate", source_system="mongodb_elevate_atlas", short="elevate_atlas",
        exclude_databases=frozenset({"admin", "config", "local", "sample_mflix"}),  # sample_mflix = Atlas demo data
        telemetry_collection_names=frozenset({"telemetry"}),
        telemetry_target="elevate_events",
    ),
)

PG_SOURCES = (
    PgSource(task="postgresql_registry", source_system="postgresql_supabase", short="supabase",
             databases=("postgres", "mentoring", "scp")),
    PgSource(
        task="postgresql_network_telemetry", source_system="postgresql_network_telemetry", short="network_telemetry",
        discover_application=False,
        telemetry_tables=(
            PgTelemetryTable("global_registry", "telemetry", "events", "network_events",
                             watermark_column="received_at", timestamp_column="event_time", id_columns=("event_id",)),
            PgTelemetryTable("global_registry", "telemetry", "ingestion_batches", "network_ingestion_batches",
                             watermark_column="received_at", timestamp_column="received_at"),
            PgTelemetryTable("global_registry", "telemetry", "services", "network_services",
                             timestamp_column="created_at"),   # mutable (last_seen_at): every change becomes a new version
            PgTelemetryTable("global_registry", "telemetry", "rejected_events", "network_rejected_events"),
            PgTelemetryTable("global_registry", "telemetry", "schema_migrations", "network_schema_migrations"),
        ),
    ),
)

CHATBOT_SOURCE = PgSource(
    task="postgresql_chatbot", source_system="postgresql_chatbot", short="chatbot", discover_application=False,
    session_timezone=PG_TIMEZONE,
    telemetry_tables=tuple(
        PgTelemetryTable("crl", "public", t, f"chatbot_{t}", watermark_column=wm, timestamp_column=ts, id_columns=(pk,))
        for t, pk, wm, ts in (
            ("conversations", "id", "updated_at", "created_at"),        # mutable (title/updated_at)
            ("messages", "id", None, "created_at"),                     # full scan: small, may be edited
            ("feedback", "id", None, "created_at"),                     # full scan: small, votes may change
            ("agentic_interactions", "message_id", "created_at", "created_at"),   # append-only log
            ("ai_telemetry", "response_id", "created_at", "query_submitted_at"),  # append-only log
            ("response_cache", "cache_key", "updated_at", "created_at"),          # mutable (confirmations, status)
        )))

PG_SOURCES = PG_SOURCES + (CHATBOT_SOURCE,)

# API telemetry -> (task name, env prefix, telemetry table). Disabled until <PREFIX>_BASE_URL is set.
API_SOURCES = (
    ("api_chatbot", "API_CHATBOT", "chatbot_events"),
    ("api_interface", "API_INTERFACE", "interface_events"),
)

WATERMARK_CANDIDATES = ("updated_at", "updatedat", "modified_at", "last_modified")

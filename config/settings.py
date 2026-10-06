"""Environment-driven configuration. No credentials are hardcoded anywhere."""
import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from dotenv import load_dotenv

from utils.security import register_secrets

ROOT = Path(__file__).resolve().parent.parent
SOURCE_ENV_KEYS = ("mongodb_network_data", "mongodb_elevate_atlas", "postgresql_supabase", "postgresql_network_telemetry",
                   "postgresql_chatbot")  # last one is optional: its task is skipped when unset


def to_libpq_url(uri: str, dbname: str | None = None) -> str:
    """postgresql+psycopg2://... -> postgresql://... (optionally swapping the database name)."""
    u = urlparse(uri.replace("postgresql+psycopg2://", "postgresql://", 1))
    if dbname:
        u = u._replace(path="/" + dbname)
    return urlunparse(u)


def _password(uri: str) -> str:
    try:
        return urlparse(uri).password or ""
    except ValueError:
        return ""


@dataclass
class Settings:
    uris: dict[str, str]
    master_db_name: str
    master_url_override: str | None
    chunk_size: int
    connect_timeout: int
    log_dir: Path
    log_level: str
    env: dict[str, str] = field(default_factory=dict)  # process env, lower-cased keys (for api_* lookups)

    def source_uri(self, key: str) -> str:
        if not self.uris.get(key):
            raise KeyError(f"environment variable '{key}' is not set")
        return self.uris[key]

    def master_url(self) -> str:
        """Target URL: explicit override, else the postgresql_network_telemetry server/credentials with the master DB name."""
        if self.master_url_override:
            return to_libpq_url(self.master_url_override)
        return to_libpq_url(self.source_uri("postgresql_network_telemetry"), self.master_db_name)

    def master_dbname(self) -> str:
        return urlparse(self.master_url()).path.lstrip("/")


def load_settings() -> Settings:
    load_dotenv(ROOT / ".env", override=False)  # absolute path: works from cron regardless of cwd
    env = {k.lower(): v for k, v in os.environ.items()}  # case-insensitive (Windows upper-cases names)
    uris = {k: env.get(k, "") for k in SOURCE_ENV_KEYS}
    override = env.get("crl_master_db_url") or None

    for v in [*uris.values(), override or ""]:
        register_secrets(_password(v))
    for k, v in env.items():
        if k.startswith("api_") and any(t in k for t in ("token", "key", "secret", "password")):
            register_secrets(v)

    log_dir = Path(env.get("log_dir", "logs"))
    s = Settings(
        uris=uris,
        master_db_name=env.get("crl_master_db_name", "crl_master_db"),
        master_url_override=override,
        chunk_size=int(env.get("ingest_chunk_size", "2000")),
        connect_timeout=int(env.get("connect_timeout_seconds", "20")),
        log_dir=log_dir if log_dir.is_absolute() else ROOT / log_dir,
        log_level=env.get("log_level", "INFO"),
        env=env,
    )
    # Safety: never write raw data into a source database.
    if uris["postgresql_network_telemetry"] and s.master_url_override is None:
        src_db = urlparse(to_libpq_url(uris["postgresql_network_telemetry"])).path.lstrip("/")
        if s.master_dbname() == src_db:
            raise ValueError("target master database must differ from the postgresql_network_telemetry source database")
    return s

"""Connections to the CRL Master DB (target)."""
import psycopg2

from config.settings import Settings
from utils.timeutil import PG_TIMEZONE


def connect_master(settings: Settings, autocommit: bool = False, app_name: str = "crl_pipeline"):
    conn = psycopg2.connect(
        settings.master_url(),
        connect_timeout=settings.connect_timeout,
        application_name=app_name,
        options=f"-c timezone={PG_TIMEZONE}",   # now(), clock_timestamp() and all timestamptz output render in IST
    )
    conn.autocommit = autocommit
    return conn

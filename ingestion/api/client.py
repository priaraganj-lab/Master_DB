"""Config-driven REST telemetry ingestion. No endpoints exist in the repo/.env yet, so a source stays
SKIPPED until <PREFIX>_BASE_URL is set (see .env.example for all options)."""
import json
import logging
from datetime import datetime, timezone
from typing import Iterator

import requests

from config.settings import Settings
from ingestion.base import (SKIPPED, FAILED, IngestContext, ObjectReader, SourceTask, TaskResult, ingest_object)
from master_db.repository import RawRecord, SourceRef
from utils.hashing import occurrence_id, payload_hash_from_text


def _dig(obj, dotted: str):
    for part in filter(None, dotted.split(".")):
        obj = obj[part]
    return obj


def _parse_ts(v):
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000 if v > 1e11 else v, timezone.utc)
    if isinstance(v, str):
        try:
            d = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


class ApiConfig:
    def __init__(self, env: dict, prefix: str):
        g = lambda k, d="": env.get(f"{prefix}_{k}".lower(), d)  # noqa: E731
        self.base_url = g("BASE_URL").rstrip("/")
        self.endpoint = "/" + g("ENDPOINT").lstrip("/") if g("ENDPOINT") else ""
        self.auth_type, self.token = g("AUTH_TYPE", "none").lower(), g("AUTH_TOKEN")
        self.auth_header = g("AUTH_HEADER", "X-API-Key")
        self.records_key, self.id_field, self.ts_field = g("RECORDS_KEY"), g("ID_FIELD"), g("TIMESTAMP_FIELD")
        self.since_param, self.page_param = g("SINCE_PARAM"), g("PAGE_PARAM")
        self.page_size_param, self.page_size = g("PAGE_SIZE_PARAM"), int(g("PAGE_SIZE", "500"))
        self.max_pages = int(g("MAX_PAGES", "1000"))

    @property
    def url(self) -> str:
        return self.base_url + self.endpoint

    def headers(self) -> dict:
        h = {"Accept": "application/json"}
        if self.auth_type == "bearer":
            h["Authorization"] = f"Bearer {self.token}"
        elif self.auth_type == "header":
            h[self.auth_header] = self.token
        return h


class ApiReader(ObjectReader):
    def __init__(self, cfg: ApiConfig, wm: str | None, timeout: int):
        self.cfg, self.wm, self.timeout = cfg, wm, timeout
        self.failed = 0
        self.watermark = wm
        self._seen_ids: set[str] = set()
        self._complete = False           # True only if every page was read (no max_pages truncation)

    def _pages(self) -> Iterator[list]:
        c = self.cfg
        page = 1
        for _ in range(c.max_pages):
            params = {}
            if c.since_param and self.wm:
                params[c.since_param] = self.wm
            if c.page_param:
                params[c.page_param] = page
                if c.page_size_param:
                    params[c.page_size_param] = c.page_size
            r = requests.get(c.url, headers=c.headers(), params=params, timeout=self.timeout)
            r.raise_for_status()
            body = r.json()
            items = _dig(body, c.records_key) if c.records_key else body
            if not isinstance(items, list):
                raise ValueError("API response does not contain a list at the configured records key")
            if items:
                yield items
            if not c.page_param or not items:
                self._complete = True
                return
            page += 1

    def records(self) -> Iterator[RawRecord]:
        c = self.cfg
        max_ts = None
        occurrences: dict = {}
        for items in self._pages():
            for item in items:
                try:
                    text = json.dumps(item, ensure_ascii=False)
                    h = payload_hash_from_text(text)
                except (TypeError, ValueError):
                    self.failed += 1
                    continue
                raw_ts = item.get(c.ts_field) if isinstance(item, dict) and c.ts_field else None
                rid = item.get(c.id_field) if isinstance(item, dict) and c.id_field else None
                if raw_ts is not None and (max_ts is None or str(raw_ts) > str(max_ts)):
                    max_ts = raw_ts
                rid = occurrence_id(h, occurrences) if rid is None else str(rid)    # no id: identity = content + occurrence
                self._seen_ids.add(rid)
                yield RawRecord(rid, _parse_ts(raw_ts), text, h, endpoint=c.endpoint)
        if max_ts is not None:
            self.watermark = str(max_ts)

    def source_ids(self) -> set[str] | None:
        """Only a complete FULL read lists every record; an incremental or truncated read cannot show deletions."""
        incremental = bool(self.cfg.since_param and self.cfg.ts_field)
        return set(self._seen_ids) if (not incremental and self._complete) else None


class ApiIngestTask(SourceTask):
    def __init__(self, name: str, prefix: str, target: str, settings: Settings):
        self.name, self.source_system, self.target, self.settings = name, name, target, settings
        self.cfg = ApiConfig(settings.env, prefix)

    def enabled(self) -> tuple[bool, str]:
        if not self.cfg.base_url or not self.cfg.endpoint:
            return False, "API endpoint not configured (BASE_URL / ENDPOINT env vars unset)"
        return True, ""

    def check_connection(self) -> None:
        r = requests.get(self.cfg.base_url, headers=self.cfg.headers(), timeout=self.settings.connect_timeout)
        if r.status_code >= 500:
            raise ConnectionError(f"API base URL answered HTTP {r.status_code}")

    def run(self, ctx: IngestContext) -> TaskResult:
        ref = SourceRef(self.source_system, "api", source_database=self.cfg.base_url)
        key = f"{self.source_system}|{self.cfg.base_url}||{self.cfg.endpoint}"
        strategy = "timestamp" if self.cfg.since_param and self.cfg.ts_field else "full"
        res = TaskResult(status="RUNNING")
        res.add(ingest_object(ctx, self.name, ref, key, self.cfg.endpoint, "telemetry", self.target, True, strategy,
                              self.cfg.ts_field or None,
                              lambda wm: ApiReader(self.cfg, wm, self.settings.connect_timeout), with_endpoint=True))
        return res.finalise()

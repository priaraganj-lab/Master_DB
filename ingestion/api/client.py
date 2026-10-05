"""Config-driven REST telemetry ingestion. No endpoints exist in the repo/.env yet, so a source stays
SKIPPED until <PREFIX>_BASE_URL is set (see .env.example for all options)."""
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests

from config.settings import Settings
from ingestion.base import (SKIPPED, FAILED, IngestContext, ObjectReader, SourceTask, TaskResult, ingest_object)
from master_db.repository import RawRecord, SourceRef
from utils.hashing import occurrence_id, payload_hash_from_text
from utils.security import scrub

_log = logging.getLogger("crl_pipeline.api")
_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


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


def _newer(a, b) -> bool:
    """a is a later timestamp than b: compared as instants when both parse, else as strings (as before)."""
    pa, pb = _parse_ts(a), _parse_ts(b)
    return pa > pb if pa and pb else str(a) > str(b)


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
        self.retries = int(g("RETRIES", "4"))                       # attempts after the first, for 429/5xx/network errors
        self.backoff_s = float(g("BACKOFF_SECONDS", "1.0"))
        self.overlap = timedelta(minutes=float(g("OVERLAP_MINUTES", "10")))   # re-read window below the ISO watermark
        self.required = [f.strip() for f in g("REQUIRED_FIELDS").split(",") if f.strip()]   # records lacking any are rejected

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


def http_get(url: str, headers: dict, params: dict, timeout: int, retries: int = 4, backoff_s: float = 1.0):
    """GET with bounded retry on 429 / 5xx / network errors (exponential backoff + jitter, honours Retry-After).
    Auth and other 4xx errors are raised immediately: retrying cannot fix them."""
    for attempt in range(retries + 1):
        retry_after = 0.0
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt >= retries:
                raise
            reason = type(e).__name__
        else:
            if r.status_code not in _RETRY_STATUS or attempt >= retries:
                r.raise_for_status()
                return r
            reason = f"HTTP {r.status_code}"
            try:
                retry_after = float(r.headers.get("Retry-After", 0))
            except ValueError:
                pass
        delay = min(max(backoff_s * (2 ** attempt) * (1 + random.random() * 0.25), retry_after), 60)
        _log.warning(scrub(f"API request retry {attempt + 1}/{retries} after {reason} in {delay:.1f}s"))
        time.sleep(delay)
    raise RuntimeError("unreachable")  # pragma: no cover


def _since(wm: str, overlap: timedelta) -> str:
    """Watermark sent to the API, moved back by the overlap so late-committed records are re-read (hash dedupe makes
    that harmless). Non-ISO watermarks (epoch numbers etc.) are sent unchanged."""
    if isinstance(wm, str):
        try:
            d = datetime.fromisoformat(wm.replace("Z", "+00:00"))
        except ValueError:
            return wm
        d = d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        return (d - overlap).isoformat()
    return wm


class ApiReader(ObjectReader):
    def __init__(self, cfg: ApiConfig, wm: str | None, timeout: int):
        self.cfg, self.wm, self.timeout = cfg, wm, timeout
        self.failed = 0
        self.rejected: list[str] = []    # ids (or 'None') of records that violated REQUIRED_FIELDS
        self.watermark = wm
        self._seen_ids: set[str] = set()
        self._complete = False           # True only if every page was read (no max_pages truncation)

    def _pages(self) -> Iterator[list]:
        c = self.cfg
        page = 1
        for _ in range(c.max_pages):
            params = {}
            if c.since_param and self.wm:
                params[c.since_param] = _since(self.wm, c.overlap)
            if c.page_param:
                params[c.page_param] = page
                if c.page_size_param:
                    params[c.page_size_param] = c.page_size
            r = http_get(c.url, c.headers(), params, self.timeout, c.retries, c.backoff_s)
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
                if c.required and (not isinstance(item, dict) or any(item.get(f) in (None, "") for f in c.required)):
                    self.failed += 1          # contract violation: counted, audited by the task, watermark held back
                    self.rejected.append(str(item.get(c.id_field) if isinstance(item, dict) else None))
                    continue
                raw_ts = item.get(c.ts_field) if isinstance(item, dict) and c.ts_field else None
                rid = item.get(c.id_field) if isinstance(item, dict) and c.id_field else None
                if raw_ts is not None and (max_ts is None or _newer(raw_ts, max_ts)):
                    max_ts = raw_ts
                rid = occurrence_id(h, occurrences) if rid is None else str(rid)    # no id: identity = content + occurrence
                self._seen_ids.add(rid)
                yield RawRecord(rid, _parse_ts(raw_ts), text, h, endpoint=c.endpoint)
        if max_ts is not None:
            self.watermark = str(max_ts)

    @property
    def failure_note(self) -> str | None:
        if not self.rejected:
            return None
        return (f"{self.failed} record(s) rejected or unserialisable (missing required fields "
                f"{self.cfg.required}); ids: {', '.join(self.rejected[:10])}")

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
        """Calls the real endpoint with the real credentials (one record when a page size is configurable), so an
        expired/revoked key (401/403) or a wrong path (404) fails preflight instead of the ingest."""
        c = self.cfg
        params = {c.page_param: 1, c.page_size_param: 1} if c.page_param and c.page_size_param else {}
        r = requests.get(c.url, headers=c.headers(), params=params, timeout=self.settings.connect_timeout)
        if r.status_code in (401, 403):
            raise PermissionError(f"API rejected the credentials (HTTP {r.status_code}); check {c.auth_type} token")
        if r.status_code >= 400:
            raise ConnectionError(f"API endpoint answered HTTP {r.status_code}")

    def run(self, ctx: IngestContext) -> TaskResult:
        ref = SourceRef(self.source_system, "api", source_database=self.cfg.base_url)
        key = f"{self.source_system}|{self.cfg.base_url}||{self.cfg.endpoint}"
        strategy = "timestamp" if self.cfg.since_param and self.cfg.ts_field else "full"
        res = TaskResult(status="RUNNING")
        res.add(ingest_object(ctx, self.name, ref, key, self.cfg.endpoint, "telemetry", self.target, True, strategy,
                              self.cfg.ts_field or None,
                              lambda wm: ApiReader(self.cfg, wm, self.settings.connect_timeout), with_endpoint=True))
        return res.finalise()

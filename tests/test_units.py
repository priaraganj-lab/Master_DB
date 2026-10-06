"""Unit tests: hashing, scrubbing, naming, Mongo serialisation, API reader (against a local mock server)."""
import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from bson import Decimal128, Int64, ObjectId

from ingestion.api.client import ApiConfig, ApiReader
from ingestion.mongodb.engine import record_id, serialise
from utils.hashing import payload_hash_from_text, pg_identifier
from utils.security import register_secrets, scrub


def test_hash_ignores_key_order_but_detects_change():
    a = payload_hash_from_text('{"a":1,"b":{"x":1,"y":2}}')
    assert a == payload_hash_from_text('{"b":{"y":2,"x":1},"a":1}')
    assert a != payload_hash_from_text('{"a":2,"b":{"x":1,"y":2}}')


def test_pg_identifier_limits_and_stability():
    n = pg_identifier("elevate_atlas", "Samiksha", "businessPlan-Audit Events")
    assert n == "elevate_atlas__samiksha__businessplan_audit_events"
    long = pg_identifier("x" * 80)
    assert len(long) <= 63 and long == pg_identifier("x" * 80) and long != pg_identifier("x" * 79 + "y")


def test_scrub_removes_uri_credentials_and_registered_secrets():
    register_secrets("s3cr3t-token")
    out = scrub("failed mongodb+srv://user:p%40ss@host/db and token s3cr3t-token")
    assert "p%40ss" not in out and "s3cr3t-token" not in out and "user:***@host" in out


def test_mongo_serialise_preserves_bson_types_and_nul():
    oid = ObjectId()
    doc = {"_id": oid, "n": Int64(5), "d": Decimal128("1.10"), "when": datetime(2026, 1, 1, tzinfo=timezone.utc),
           "s": "a\x00b"}
    text, h = serialise(doc)
    p = json.loads(text)
    assert p["_id"] == {"$oid": str(oid)} and p["n"] == {"$numberLong": "5"} and p["d"] == {"$numberDecimal": "1.10"}
    assert p["s"] == "a\\u0000b" and len(h) == 64 and record_id(oid) == str(oid)


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        page = int(self.path.split("page=")[1].split("&")[0]) if "page=" in self.path else 1
        data = {"data": {"items": [{"id": f"e{page}", "ts": f"2026-01-0{page}T00:00:00Z", "k": page}] if page < 3 else []}}
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def test_api_reader_paginates_and_tracks_watermark():
    srv = HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    env = {"api_t_base_url": f"http://127.0.0.1:{srv.server_port}", "api_t_endpoint": "/ev",
           "api_t_records_key": "data.items", "api_t_id_field": "id", "api_t_timestamp_field": "ts",
           "api_t_page_param": "page"}
    r = ApiReader(ApiConfig(env, "API_T"), None, 5)
    recs = list(r.records())
    srv.shutdown()
    assert [x.source_record_id for x in recs] == ["e1", "e2"]
    assert recs[0].endpoint == "/ev" and recs[0].source_timestamp.year == 2026
    assert r.watermark == "2026-01-02T00:00:00Z"


def test_all_pipeline_timestamps_are_ist():
    import json
    import logging
    from audit.audit_repository import now
    from audit.logger import JsonFormatter
    from utils.timeutil import ist_iso, ist_text

    n = now()
    assert n.utcoffset().total_seconds() == 5.5 * 3600
    utc = datetime(2026, 10, 1, 4, 37, 38, tzinfo=timezone.utc)
    assert ist_text(utc) == "2026-10-01 10:07:38 IST"
    assert ist_iso(utc) == "2026-10-01T10:07:38+05:30"
    rec = logging.LogRecord("x", logging.INFO, __file__, 1, "m", None, None)
    assert json.loads(JsonFormatter().format(rec))["timestamp"].endswith("+05:30")


class _Coll:
    def __init__(self, last):
        self.last = last

    def find_one(self, sort=None):
        return self.last


def test_mongo_cutoff_filter_shapes():
    from ingestion.cutoff import mongo_cutoff_filter
    cut = datetime(2026, 10, 1, 5, 0, 0, tzinfo=timezone.utc)
    assert mongo_cutoff_filter(_Coll(None), cut) == {}                                   # empty collection
    assert mongo_cutoff_filter(_Coll({"_id": "a"}), cut) == {}                           # no creation marker -> no cutoff
    f = mongo_cutoff_filter(_Coll({"_id": ObjectId.from_datetime(cut)}), cut)            # ObjectId: second-resolution bound
    assert f["$or"][0]["_id"]["$lt"] == ObjectId.from_datetime(datetime(2026, 10, 1, 5, 0, 1, tzinfo=timezone.utc))
    assert f["$or"][1] == {"_id": {"$not": {"$type": "objectId"}}}                       # other id types are kept
    f = mongo_cutoff_filter(_Coll({"_id": "u", "createdAt": cut}), cut)                  # uuid ids -> createdAt
    assert f["$or"][0] == {"createdAt": {"$lte": cut}}


def test_pg_cutoff_column_choice():
    from config.sources import PgTelemetryTable
    from ingestion.cutoff import pick_pg_cutoff_column
    assert pick_pg_cutoff_column({"created_at", "updated_at"}, None, "updated_at") == "created_at"
    assert pick_pg_cutoff_column({"updated_at"}, None, "updated_at") is None             # no creation column
    assert pick_pg_cutoff_column({"created_at"}, None, "updated_at") is None             # naive watermark -> no cutoff
    tel = PgTelemetryTable("d", "s", "t", "x", watermark_column="received_at", timestamp_column="event_time")
    assert pick_pg_cutoff_column({"received_at", "event_time"}, tel, "received_at") == "received_at"


def test_mongo_cutoff_ignores_synthetic_future_objectids():
    from ingestion.cutoff import mongo_cutoff_filter
    cut = datetime(2026, 10, 1, 5, 0, 0, tzinfo=timezone.utc)
    synthetic = ObjectId("750000000000000000000003")                                      # timestamp in 2032
    assert mongo_cutoff_filter(_Coll({"_id": synthetic}), cut) == {}                      # no creation marker -> no cutoff
    f = mongo_cutoff_filter(_Coll({"_id": synthetic, "createdAt": cut}), cut)             # falls back to createdAt
    assert f["$or"][0] == {"createdAt": {"$lte": cut}}
    recent = ObjectId.from_datetime(datetime(2026, 10, 1, 5, 0, 30, tzinfo=timezone.utc))  # a normal id just after the run
    assert "$or" in mongo_cutoff_filter(_Coll({"_id": recent}), cut)


class _Flaky(BaseHTTPRequestHandler):
    hits = 0
    seen_since = []

    def do_GET(self):  # noqa: N802
        type(self).hits += 1
        if "since=" in self.path:
            type(self).seen_since.append(self.path.split("since=")[1].split("&")[0])
        if self.path.startswith("/denied"):
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if type(self).hits < 3:
            self.send_response(429)
            self.send_header("Retry-After", "0")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = json.dumps([{"id": "a", "ts": "2026-01-01T00:10:00Z"}, {"ts": "2026-01-01T00:20:00Z"}]).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def _serve(handler):
    srv = HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def test_api_retries_rejects_missing_required_fields_and_holds_watermark():
    srv = _serve(_Flaky)
    env = {"api_t_base_url": f"http://127.0.0.1:{srv.server_port}", "api_t_endpoint": "/ev", "api_t_id_field": "id",
           "api_t_timestamp_field": "ts", "api_t_backoff_seconds": "0", "api_t_required_fields": "id,ts",
           "api_t_since_param": "since"}
    r = ApiReader(ApiConfig(env, "API_T"), "2026-01-01T00:30:00Z", 5)
    recs = list(r.records())
    srv.shutdown()
    assert _Flaky.hits == 3                                    # two 429s retried, third succeeded
    assert [x.source_record_id for x in recs] == ["a"]
    assert r.failed == 1 and "missing required fields" in r.failure_note
    from urllib.parse import unquote
    assert unquote(_Flaky.seen_since[0]).startswith("2026-01-01T00:20:00")   # watermark minus the 10 min overlap


def test_api_auth_error_is_not_retried():
    import pytest
    import requests
    from ingestion.api.client import http_get
    srv = _serve(_Flaky)
    _Flaky.hits = 0
    with pytest.raises(requests.HTTPError):
        http_get(f"http://127.0.0.1:{srv.server_port}/denied", {}, {}, 5, retries=3, backoff_s=0)
    srv.shutdown()
    assert _Flaky.hits == 1


def test_api_preflight_reports_rejected_credentials():
    import pytest
    from config.settings import Settings
    from ingestion.api.client import ApiIngestTask
    srv = _serve(_Flaky)
    env = {"api_t_base_url": f"http://127.0.0.1:{srv.server_port}", "api_t_endpoint": "/denied"}
    s = Settings({}, "m", None, 10, 5, None, "INFO", env)
    with pytest.raises(PermissionError):
        ApiIngestTask("api_t", "API_T", "t", s).check_connection()
    srv.shutdown()

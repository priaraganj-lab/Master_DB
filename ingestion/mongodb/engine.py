"""Generic MongoDB -> Master DB ingestion (read-only against the source)."""
import json
import logging
from datetime import datetime, timezone
from typing import Iterator

import pymongo
from bson import ObjectId, json_util

from audit.logger import log_event
from config.settings import Settings
from config.sources import MongoSource
from ingestion.base import (OBJECTID_OVERLAP, OVERLAP, FAILED, IngestContext, ObjectReader, SourceTask, TaskResult,
                            ingest_object)
from ingestion.cutoff import mongo_cutoff_filter
from master_db.repository import RawRecord, SourceRef
from utils.hashing import payload_hash_from_text, pg_identifier
from utils.security import scrub
from utils.timeutil import ist_iso


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _strip_nul(o):
    """jsonb cannot hold U+0000; keep the character visible as the literal text \\u0000."""
    if isinstance(o, str):
        return o.replace("\x00", "\\u0000")
    if isinstance(o, list):
        return [_strip_nul(x) for x in o]
    if isinstance(o, dict):
        return {_strip_nul(k): _strip_nul(v) for k, v in o.items()}
    return o


def serialise(doc: dict, ignore=()) -> tuple[str, str]:
    """(payload_text, payload_hash). Canonical extended JSON keeps BSON types (ObjectId, Date, Int64, Decimal128...).
    `ignore`: volatile top-level fields left out of the hash (change detection) but kept in the stored payload."""
    text = json_util.dumps(doc, json_options=json_util.CANONICAL_JSON_OPTIONS)
    if "\\u0000" in text:
        text = json.dumps(_strip_nul(json.loads(text)), ensure_ascii=False)
    return text, payload_hash_from_text(text, ignore)


def record_id(_id) -> str:
    if isinstance(_id, (ObjectId, str, int)):
        return str(_id)
    return json_util.dumps(_id, json_options=json_util.CANONICAL_JSON_OPTIONS, sort_keys=True)


def probe_strategy(coll, is_telemetry: bool) -> tuple[str, str | None]:
    """Choose an incremental mechanism that was verified to be reliable for THIS collection."""
    if coll.estimated_document_count() == 0:
        return "full", None
    last = coll.find_one(sort=[("_id", -1)])
    if is_telemetry and isinstance(last["_id"], ObjectId):
        return "object_id", "_id"                       # immutable event stream, ObjectId timestamp
    if isinstance(last.get("updatedAt"), datetime) and \
            coll.count_documents({"updatedAt": {"$not": {"$type": "date"}}}) == 0:
        return "updated_at", "updatedAt"                # every document carries a date-typed updatedAt
    return "full", None                                  # no verified watermark: full scan + hash dedupe


class MongoReader(ObjectReader):
    def __init__(self, coll, strategy: str, wm: str | None, cutoff: datetime | None = None, cut: dict | None = None,
                 ignore=()):
        self.coll, self.strategy, self.wm = coll, strategy, wm
        self.ignore = ignore
        self.cutoff, self.cut = cutoff, cut or {}       # cut: 'created at or before cutoff' filter ({} = none)
        self.failed = 0
        self.watermark = wm

    def _query(self):
        if self.strategy == "object_id":
            q = {}
            if self.wm:
                low = _utc(ObjectId(self.wm).generation_time) - OBJECTID_OVERLAP
                q = {"_id": {"$gte": ObjectId.from_datetime(low)}}
            sort = [("_id", 1)]
        elif self.strategy == "updated_at":
            q = {}
            if self.wm:
                q = {"updatedAt": {"$gte": datetime.fromisoformat(self.wm) - OVERLAP}}
            sort = [("updatedAt", 1), ("_id", 1)]
        else:
            q, sort = {}, None
        if self.cut:
            q = {"$and": [q, self.cut]} if q else self.cut
        return q, sort

    def records(self) -> Iterator[RawRecord]:
        q, sort = self._query()
        max_wm = None
        for doc in self.coll.find(q, sort=sort, batch_size=1000, allow_disk_use=True):
            try:
                text, h = serialise(doc, self.ignore)
            except Exception:  # noqa: BLE001
                self.failed += 1
                continue
            ts = doc.get("updatedAt") if isinstance(doc.get("updatedAt"), datetime) else doc.get("createdAt")
            if isinstance(ts, datetime):
                ts = _utc(ts)
            elif isinstance(doc["_id"], ObjectId):
                ts = _utc(doc["_id"].generation_time)      # ObjectId timestamp
            else:
                ts = None
            if self.strategy == "object_id":
                max_wm = doc["_id"] if max_wm is None or doc["_id"] > max_wm else max_wm
            elif self.strategy == "updated_at" and isinstance(doc.get("updatedAt"), datetime):
                u = _utc(doc["updatedAt"])
                max_wm = u if max_wm is None or u > max_wm else max_wm
            yield RawRecord(record_id(doc["_id"]), ts, text, h)
        if max_wm is not None:
            if self.strategy == "updated_at" and self.cut and self.cutoff:
                # records created after the cutoff were skipped; never let the watermark pass the cutoff, or an
                # old record updated late in a long run would push it beyond them (outside the overlap window)
                max_wm = min(max_wm, _utc(self.cutoff))
            self.watermark = str(max_wm) if self.strategy == "object_id" else ist_iso(max_wm)


    def source_ids(self) -> set[str]:
        """Every _id currently in the collection (ids only; not limited by the run cutoff or the watermark)."""
        return {record_id(d["_id"]) for d in self.coll.find({}, {"_id": 1}, batch_size=5000)}


class MongoIngestTask(SourceTask):
    def __init__(self, cfg: MongoSource, settings: Settings):
        self.cfg, self.settings = cfg, settings
        self.name, self.source_system = cfg.task, cfg.source_system

    def _client(self):
        return pymongo.MongoClient(self.settings.source_uri(self.cfg.source_system), appname="crl_pipeline",
                                   serverSelectionTimeoutMS=self.settings.connect_timeout * 1000,
                                   connectTimeoutMS=self.settings.connect_timeout * 1000)

    def check_connection(self) -> None:
        c = self._client()
        try:
            c.admin.command("ping")
        finally:
            c.close()

    def _databases(self, client) -> list[str]:
        dbs = [d for d in client.list_database_names() if d not in self.cfg.exclude_databases]
        if self.cfg.include_databases is not None:
            dbs = [d for d in dbs if d in self.cfg.include_databases]
        return sorted(dbs)

    def run(self, ctx: IngestContext) -> TaskResult:
        result = TaskResult(status="RUNNING")
        client = self._client()
        try:
            for dbname in self._databases(client):
                db = client[dbname]
                for info in sorted(db.list_collections(filter={"type": "collection"}), key=lambda i: i["name"]):
                    name = info["name"]
                    if name.startswith("system."):
                        continue
                    result.add(self._ingest_collection(ctx, db, dbname, name))
        except Exception as e:  # noqa: BLE001 - source-level failure (discovery); recorded by the orchestrator
            result.status, result.error = FAILED, scrub(f"{type(e).__name__}: {e}")
            ctx.audit.log_error(ctx.run_id, self.name, self.source_system, None, e)
            log_event(ctx.logger, logging.ERROR, "source discovery failed", pipeline_run_id=ctx.run_id,
                      source=self.source_system, task=self.name, status=FAILED, error=result.error)
        finally:
            client.close()
        return result.finalise()

    def _ingest_collection(self, ctx, db, dbname: str, name: str):
        coll = db[name]
        is_tel = name in self.cfg.telemetry_collection_names
        try:
            strategy, wm_col = probe_strategy(coll, is_tel)
        except Exception:  # noqa: BLE001
            strategy, wm_col = "full", None
        cut = mongo_cutoff_filter(coll, ctx.cutoff) if ctx.cutoff else {}
        ref = SourceRef(self.source_system, "mongodb", source_database=dbname, source_collection=name)
        key = f"{self.source_system}|{dbname}||{name}"
        if is_tel:
            schema, target, shared = "telemetry", self.cfg.telemetry_target, True
        else:
            schema, shared = "application", False
            target = pg_identifier(self.cfg.short, dbname, name)
        return ingest_object(ctx, self.name, ref, key, f"{dbname}.{name}", schema, target, shared, strategy, wm_col,
                             lambda wm: MongoReader(coll, strategy, wm, ctx.cutoff, cut,
                                                    self.cfg.ignore_for(dbname, name)))

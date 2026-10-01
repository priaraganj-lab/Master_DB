"""Per-run snapshot cutoff. The sources are live: records keep arriving while a run is in progress, so a source count
taken after ingestion is always ahead of the target. The run therefore fixes a cutoff (its start time); ingestion reads
and reconciliation counts ONLY records created at or before it. Records created later are picked up by the next run.

The same helpers build the filter for both sides, so ingestion and reconciliation can never disagree about scope.
An object without a usable creation marker gets no cutoff ({} / None) and is read/counted in full as before."""
from datetime import datetime, timedelta

from bson import ObjectId
from psycopg2 import sql

_MAX_ID_SKEW = timedelta(hours=1)    # an ObjectId further ahead of the cutoff than this is not a creation time


def mongo_cutoff_filter(coll, cutoff: datetime) -> dict:
    """Mongo filter for 'created at or before cutoff'; {} if the collection has no creation marker."""
    try:
        last = coll.find_one(sort=[("_id", -1)])
    except Exception:  # noqa: BLE001 - cannot probe -> no cutoff (read everything, as before)
        return {}
    if last is None:
        return {}
    # An ObjectId only carries a creation time if it was generated normally: the largest id in the collection must not
    # lie (far) in the future. Hand-made/synthetic ids (e.g. 7500000000...) would look "newer than the run" forever and
    # their documents would never be ingested, so such collections fall back to createdAt, or to no cutoff.
    if isinstance(last["_id"], ObjectId) and last["_id"].generation_time <= cutoff + _MAX_ID_SKEW:
        # ObjectId time has 1 s resolution: everything stamped up to and including the cutoff's second.
        # Range operators only match same-typed values, so non-ObjectId _ids are kept explicitly.
        return {"$or": [{"_id": {"$lt": ObjectId.from_datetime(cutoff + timedelta(seconds=1))}},
                        {"_id": {"$not": {"$type": "objectId"}}}]}
    if isinstance(last.get("createdAt"), datetime):
        return {"$or": [{"createdAt": {"$lte": cutoff}}, {"createdAt": {"$not": {"$type": "date"}}}]}
    return {}


def load_tz_columns(conn) -> dict[tuple[str, str], set[str]]:
    """(schema, table) -> its timestamptz columns, for a whole database in one query."""
    with conn.cursor() as cur:
        cur.execute("SELECT table_schema, table_name, column_name FROM information_schema.columns "
                    "WHERE data_type = 'timestamp with time zone'")
        rows = cur.fetchall()
    conn.rollback()
    out: dict[tuple[str, str], set[str]] = {}
    for s, t, c in rows:
        out.setdefault((s, t), set()).add(c)
    return out


def pick_pg_cutoff_column(tz_cols: set[str], tel, wm_col: str | None) -> str | None:
    """Creation/arrival column for the cutoff. Only timestamptz columns qualify (a naive column has no defined zone),
    and if the table's watermark column is naive no cutoff is used: the watermark is capped at the cutoff, which needs
    comparable (aware) values."""
    if wm_col and wm_col not in tz_cols:
        return None
    if tel:
        return next((c for c in (tel.watermark_column, tel.timestamp_column) if c and c in tz_cols), None)
    return next((c for c in sorted(tz_cols) if c.lower() in ("created_at", "createdat")), None)


def pg_cutoff_clause(column: str) -> sql.Composable:
    """WHERE fragment (single %s = cutoff); NULL markers are kept because they cannot be proven newer."""
    return sql.SQL("(t.{c} <= %s OR t.{c} IS NULL)").format(c=sql.Identifier(column))

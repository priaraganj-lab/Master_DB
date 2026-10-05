"""One-off, idempotent copy of the AI chatbot tables (public.* in the chatbot `crl` DB) into crl_master_db.telemetry.chatbot_*.

Source: CHATBOT_SRC_URL (e.g. postgresql://user:pass@localhost:15432/crl via the SSH tunnel). Target: CRL_MASTER_DB_URL.
Each table is truncated and reloaded inside one transaction, so re-running is safe. pgvector is not installed on the
target, so response_cache.answer_emb (vector(1024)) is stored as its text literal.
"""
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values

# parents before children (messages -> conversations)
TABLES = ["conversations", "messages", "feedback", "agentic_interactions", "ai_telemetry", "response_cache"]
PKS = {"conversations": "id", "messages": "id", "feedback": "id", "agentic_interactions": "message_id",
       "ai_telemetry": "response_id", "response_cache": "cache_key"}
TARGET_SCHEMA = "telemetry"
PREFIX = "chatbot_"


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
    src_url, dst_url = os.environ.get("CHATBOT_SRC_URL"), os.environ.get("CRL_MASTER_DB_URL")
    if not src_url or not dst_url:
        print("CHATBOT_SRC_URL and CRL_MASTER_DB_URL must be set", file=sys.stderr)
        return 2
    src, dst = psycopg2.connect(src_url, connect_timeout=20), psycopg2.connect(dst_url, connect_timeout=20)
    try:
        with src.cursor() as sc, dst.cursor() as dc:
            for t in TABLES:
                sc.execute("""select a.attname, case when t.typname='vector' then 'text' else format_type(a.atttypid,a.atttypmod) end,
                                     t.typname='vector'
                              from pg_attribute a join pg_type t on t.oid=a.atttypid
                              where a.attrelid=%s::regclass and a.attnum>0 and not a.attisdropped order by a.attnum""",
                           (f"public.{t}",))
                cols = sc.fetchall()
                target = f"{TARGET_SCHEMA}.{PREFIX}{t}"
                ddl = ", ".join(f'"{n}" {ty}' for n, ty, _ in cols)
                dc.execute(f'create table if not exists {target} ({ddl}, primary key ("{PKS[t]}"))')
                dc.execute(f"truncate {target}")
                sel = ", ".join(f'"{n}"::text' if vec else f'"{n}"' for n, _, vec in cols)
                sc.execute(f"select {sel} from public.{t}")
                rows = [tuple(Json(v) if isinstance(v, (dict, list)) else v for v in r) for r in sc.fetchall()]
                if rows:
                    execute_values(dc, f"insert into {target} values %s", rows, page_size=500)
                dc.execute(f"select count(*) from {target}")
                print(f"{t:22s} -> {target}: source={len(rows)} target={dc.fetchone()[0]}")
        dst.commit()
    except Exception:
        dst.rollback()
        raise
    finally:
        src.close()
        dst.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

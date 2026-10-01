"""Deterministic payload hashing and naming helpers."""
import hashlib
import json
import re


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_hash_from_text(json_text: str, ignore=()) -> str:
    """SHA-256 of the canonical (key-sorted) form of a JSON document held as text. `ignore` = top-level keys left out
    of the hash (volatile fields that are rewritten without a real change, so they must not create a new version)."""
    doc = json.loads(json_text)
    if ignore and isinstance(doc, dict):
        doc = {k: v for k, v in doc.items() if k not in ignore}
    return hashlib.sha256(canonical_json(doc).encode("utf-8")).hexdigest()


def occurrence_id(payload_hash: str, seen: dict) -> str:
    """Identity for a record that has no key: its content hash plus an occurrence counter, so identical rows are all
    kept (hash#1, hash#2 ...). `seen` is the per-read counter dict."""
    seen[payload_hash] = seen.get(payload_hash, 0) + 1
    return f"{payload_hash}#{seen[payload_hash]}"


def short_hash(value: str, n: int = 8) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:n]


def pg_identifier(*parts: str, max_len: int = 63) -> str:
    """Lower-case [a-z0-9_] identifier; over-long names are truncated with a stable hash suffix."""
    name = "__".join(re.sub(r"[^a-z0-9_]", "_", p.lower()) for p in parts if p)
    if len(name) <= max_len:
        return name
    return f"{name[: max_len - 9]}_{short_hash(name)}"

# backend/app/services/mongo_schema.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple
from collections import Counter, defaultdict
from datetime import datetime
from pymongo import MongoClient


def get_client(uri: str) -> MongoClient:
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def list_databases(uri: str) -> List[str]:
    client = get_client(uri)
    try:
        return sorted(client.list_database_names())
    finally:
        client.close()


def list_collections(uri: str, db: str) -> List[str]:
    client = get_client(uri)
    try:
        return sorted(client[db].list_collection_names())
    finally:
        client.close()


def preview_documents(uri: str, db: str, collection: str, limit: int = 10) -> List[Dict[str, Any]]:
    client = get_client(uri)
    try:
        cur = client[db][collection].find({}, limit=limit)
        out: List[Dict[str, Any]] = []
        for d in cur:
            if "_id" in d:
                d["_id"] = str(d["_id"])
            out.append(d)
        return out
    finally:
        client.close()


def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "string"
    if isinstance(v, datetime):
        return "datetime"
    if isinstance(v, dict):
        return "object"
    if isinstance(v, list):
        return "array"
    return type(v).__name__.lower()


def _flatten(doc: Any, prefix: str = "", max_depth: int = 6) -> List[Tuple[str, Any]]:
    """
    Returns list of (field_path, value_sample).
    Arrays use [] marker: items[].price
    """
    out: List[Tuple[str, Any]] = []
    if max_depth <= 0:
        return out

    if isinstance(doc, dict):
        for k, v in doc.items():
            p = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                out.extend(_flatten(v, p, max_depth - 1))
            else:
                out.append((p, v))

    elif isinstance(doc, list):
        p = f"{prefix}[]" if prefix else "[]"
        for v in doc[:5]:  # sample a few elements
            if isinstance(v, (dict, list)):
                out.extend(_flatten(v, p, max_depth - 1))
            else:
                out.append((p, v))

    else:
        out.append((prefix, doc))

    return out


def infer_schema(uri: str, db: str, collection: str, sample_size: int = 400) -> Dict[str, Any]:
    """
    Schema inference that supports:
    - nested objects
    - arrays (using [] marker)
    - presence % (COUNTED ONCE PER DOCUMENT per path)
    - type distribution
    - sample values
    """
    client = get_client(uri)
    try:
        coll = client[db][collection]
        cur = coll.find({}, limit=sample_size)

        total = 0
        presence = Counter()
        types = defaultdict(Counter)
        samples = defaultdict(list)

        for d in cur:
            total += 1
            d.pop("_id", None)

            seen_paths = set()
            for path, val in _flatten(d):
                if not path:
                    continue

                # ✅ presence should be counted ONCE per document for each path
                if path not in seen_paths:
                    presence[path] += 1
                    seen_paths.add(path)

                # types can be counted per occurrence/sample (fine for heuristics)
                types[path][_type_name(val)] += 1

                if len(samples[path]) < 3:
                    sv = val
                    if isinstance(val, (dict, list)):
                        sv = str(val)[:140]
                    samples[path].append(sv)

        fields = []
        for path in sorted(presence.keys()):
            fields.append(
                {
                    "path": path,
                    "presence_pct": round((presence[path] / total) * 100, 1) if total else 0.0,
                    "types": dict(types[path]),
                    "samples": samples[path],
                }
            )

        return {"db": db, "collection": collection, "sample_size": total, "fields": fields}
    finally:
        client.close()


def build_mongo_schema_prompt(schema: Dict[str, Any]) -> str:
    """
    LLM-friendly prompt. Uses flattened field paths.
    """
    lines: List[str] = []
    lines.append(f'Collection: {schema.get("db")}.{schema.get("collection")}')
    lines.append(f'Sampled documents: {schema.get("sample_size")}')
    lines.append("Fields (flattened paths):")
    for f in schema.get("fields", []):
        types_s = ", ".join([f"{k}({v})" for k, v in (f.get("types") or {}).items()])
        samples_s = "; ".join([str(s) for s in (f.get("samples") or [])])
        lines.append(
            f'- {f.get("path")}: types={types_s}, presence={f.get("presence_pct")}%, samples=[{samples_s}]'
        )
    return "\n".join(lines)


def get_date_candidates(schema: Dict[str, Any]) -> List[str]:
    """
    Picks any field paths that look like datetime.
    """
    cands: List[str] = []
    for f in schema.get("fields", []):
        t = f.get("types") or {}
        if "datetime" in t:
            cands.append(f["path"])
    return cands

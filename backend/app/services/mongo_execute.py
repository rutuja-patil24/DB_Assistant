# backend/app/services/mongo_execute.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional
from time import perf_counter
from datetime import datetime

from bson import ObjectId
from pymongo import MongoClient


def get_client(uri: str) -> MongoClient:
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def _restore_dates(obj: Any) -> Any:
    """Convert ISO datetime strings back to datetime objects for Mongo queries."""
    if isinstance(obj, dict):
        return {k: _restore_dates(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_restore_dates(v) for v in obj]
    if isinstance(obj, str):
        try:
            if "T" in obj and len(obj) >= 19:
                return datetime.fromisoformat(obj)
        except (ValueError, TypeError):
            pass
    return obj


def _jsonify(v: Any) -> Any:
    """Recursively make Mongo values JSON-safe."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonify(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_jsonify(x) for x in v]
    return v


def execute_find(
    mongo_uri: str,
    db_name: str,
    collection: str,
    filter_doc: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, Any]] = None,
    sort: Optional[Dict[str, int]] = None,
    limit: int = 50,
    max_time_ms: int = 8000,
) -> Tuple[List[Dict[str, Any]], int]:
    client = get_client(mongo_uri)
    try:
        t0 = perf_counter()
        coll = client[db_name][collection]

        filter_doc = _restore_dates(filter_doc or {})
        q = coll.find(filter_doc, projection or None)
        if sort:
            q = q.sort(list(sort.items()))
        q = q.limit(int(limit)).max_time_ms(max_time_ms)

        docs = [_jsonify(d) for d in q]
        ms = int((perf_counter() - t0) * 1000)
        return docs, ms
    finally:
        client.close()


def execute_aggregate(
    mongo_uri: str,
    db_name: str,
    collection: str,
    pipeline: List[Dict[str, Any]],
    limit: int = 50,
    max_time_ms: int = 8000,
    allow_disk_use: bool = True,
) -> Tuple[List[Dict[str, Any]], int]:
    client = get_client(mongo_uri)
    try:
        t0 = perf_counter()
        coll = client[db_name][collection]

        # ✅ FIX: restore dates FIRST, then append $limit guard — do NOT reassign pipe twice
        pipe = _restore_dates(list(pipeline or []))

        # Always enforce a limit at the end (safety)
        if not any(isinstance(s, dict) and "$limit" in s for s in pipe):
            pipe.append({"$limit": int(limit)})

        cur = coll.aggregate(pipe, allowDiskUse=allow_disk_use, maxTimeMS=max_time_ms)

        docs = [_jsonify(d) for d in cur]
        ms = int((perf_counter() - t0) * 1000)
        return docs, ms
    finally:
        client.close()


def run_query(
    mongo_uri: str,
    db_name: str,
    collection: str,
    spec: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Dispatch to find or aggregate based on spec.query_type.
    Returns: (docs, execution_time_ms)
    """
    qt = (spec.get("query_type") or "").lower().strip()
    limit = int(spec.get("limit") or 50)

    if qt == "find":
        return execute_find(
            mongo_uri=mongo_uri,
            db_name=db_name,
            collection=collection,
            filter_doc=spec.get("filter") or {},
            projection=spec.get("projection"),
            sort=spec.get("sort"),
            limit=limit,
        )

    if qt == "aggregate":
        pipeline = spec.get("pipeline") or []
        if not isinstance(pipeline, list):
            raise ValueError("spec.pipeline must be a list for aggregate queries.")
        return execute_aggregate(
            mongo_uri=mongo_uri,
            db_name=db_name,
            collection=collection,
            pipeline=pipeline,
            limit=limit,
        )

    raise ValueError(f"Unknown query_type: {spec.get('query_type')}")
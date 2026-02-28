# backend/app/services/mongo_query_validator.py
from __future__ import annotations

from typing import Any, Dict, Set
from datetime import datetime, timedelta

ALLOWED_STAGES = {"$match", "$project", "$group", "$sort", "$limit", "$unwind", "$addFields"}
BLOCKED_KEYS = {"$where", "$function", "$accumulator", "$facet", "$lookup", "$merge", "$out"}

MAX_LIMIT = 200
MAX_PIPELINE_STAGES = 10


def _contains_blocked(obj: Any) -> bool:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in BLOCKED_KEYS:
                return True
            if _contains_blocked(v):
                return True
    elif isinstance(obj, list):
        return any(_contains_blocked(x) for x in obj)
    return False


def enforce_limit(spec: Dict[str, Any], limit: int) -> Dict[str, Any]:
    lim = min(max(1, int(limit)), MAX_LIMIT)
    spec["limit"] = lim

    if spec.get("query_type") == "aggregate":
        pipeline = spec.get("pipeline", [])
        pipeline = [st for st in pipeline if not (isinstance(st, dict) and "$limit" in st)]
        pipeline.append({"$limit": lim})
        spec["pipeline"] = pipeline

    return spec


def enforce_date_filter(
    spec: Dict[str, Any],
    date_field: str,
    default_days: int,
    user_question: str,
) -> Dict[str, Any]:
    """
    Injects a date range filter only when the user explicitly mentions a time
    window AND a date field was detected in the schema.
    Returns spec unchanged in all other cases.
    """
    uq = (user_question or "").lower()

    time_keywords = [
        "last", "past", "recent", "yesterday", "today",
        "this week", "this month", "this year",
        "days", "weeks", "months", "since", "between",
        "before", "after", "latest",
    ]
    wants_time_filter = any(p in uq for p in time_keywords)

    if not wants_time_filter or not date_field:
        return spec

    start = datetime.utcnow() - timedelta(days=int(default_days))
    date_match = {date_field: {"$gte": start}}

    qtype = spec.get("query_type", "")

    if qtype == "find":
        existing = spec.get("filter") or {}
        if date_field not in existing:
            spec["filter"] = {**existing, **date_match}

    elif qtype == "aggregate":
        pipeline = spec.get("pipeline") or []
        already_filtered = any(
            isinstance(st, dict)
            and "$match" in st
            and date_field in (st.get("$match") or {})
            for st in pipeline
        )
        if not already_filtered:
            spec["pipeline"] = [{"$match": date_match}] + pipeline

    return spec


def validate_spec(spec: Dict[str, Any]) -> None:
    if not isinstance(spec, dict):
        raise ValueError("Mongo query spec must be an object")

    qtype = spec.get("query_type")
    if qtype not in ("find", "aggregate"):
        raise ValueError("query_type must be 'find' or 'aggregate'")

    if _contains_blocked(spec):
        raise ValueError("Query uses blocked operators/stages (unsafe or too expensive)")

    if qtype == "aggregate":
        pipeline = spec.get("pipeline")
        if not isinstance(pipeline, list) or not pipeline:
            raise ValueError("aggregate pipeline must be a non-empty list")

        if len(pipeline) > MAX_PIPELINE_STAGES:
            raise ValueError(f"Pipeline too long (max {MAX_PIPELINE_STAGES} stages)")

        for st in pipeline:
            if not isinstance(st, dict) or len(st) != 1:
                raise ValueError("Each pipeline stage must be an object with exactly one key")
            stage_name = next(iter(st.keys()))
            if stage_name not in ALLOWED_STAGES:
                raise ValueError(f"Stage not allowed: {stage_name}")


# -----------------------------
# Field Validation
# -----------------------------

def _extract_field_refs(obj: Any) -> Set[str]:
    """
    Recursively collect all field names referenced in a Mongo spec.

    Captures both:
      - "$field" references (e.g. "$region" in $group / $project / $sort)
      - Plain dict keys that are field names in filter / $match contexts
        (e.g. {"region": "West"} -> "region")

    Skips:
      - Mongo operator keys starting with "$" (e.g. "$gte", "$and", "$sum")
      - System variables like "$$ROOT", "$$NOW"
      - "_id" (always allowed)
    """
    refs: Set[str] = set()

    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and not k.startswith("$") and k != "_id":
                refs.add(k)
            refs |= _extract_field_refs(v)

    elif isinstance(obj, list):
        for x in obj:
            refs |= _extract_field_refs(x)

    elif isinstance(obj, str):
        s = obj.strip()
        if s.startswith("$$"):
            return refs
        if s.startswith("$") and len(s) > 1:
            field = s[1:]
            if field != "_id":
                refs.add(field)

    return refs


def validate_fields_against_schema(spec: Dict[str, Any], allowed: Set[str]) -> None:
    """
    Validates field references in the query against the inferred schema.

    Safety rules:
    - Always allows "_id".
    - If allowed contains no real fields (schema inference returned nothing
      useful — e.g. empty collection or sample only had _id), skip validation
      entirely rather than blocking every query.
    - Only raises for fields clearly absent from a non-trivial schema.
    """
    # Separate out _id so we can check if the schema has any real fields
    real_fields = {f for f in allowed if f != "_id"}

    # If schema returned no real fields, we cannot make meaningful judgements
    # — skip validation to avoid false positives on legitimate queries.
    if not real_fields:
        return

    # Build full allowed set including _id
    allowed2 = real_fields | {"_id"}

    refs = _extract_field_refs(spec)

    for f in refs:
        f_norm = f.replace("[]", "").lstrip(".")
        if f_norm not in allowed2:
            raise ValueError(
                f"Query references non-existent field: '{f_norm}'. "
                f"Allowed fields (sample): {sorted(allowed2)[:50]}"
            )
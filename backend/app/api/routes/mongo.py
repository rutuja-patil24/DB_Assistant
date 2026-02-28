# backend/app/api/routes/mongo.py

from __future__ import annotations

import logging
import re
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import pymongo

from bson import ObjectId
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, field_validator

from app.agents.mongo_query_agent import MongoQueryAgent
from app.agents.orchestrator import Orchestrator
from app.state.agent_state import AgentState

# Shared orchestrator instance
_orchestrator = Orchestrator()
from app.services.mongo_execute import run_query
from app.services.mongo_query_validator import (
    enforce_date_filter,
    enforce_limit,
    validate_fields_against_schema,
    validate_spec,
)
from app.services.mongo_schema import (
    build_mongo_schema_prompt,
    get_date_candidates,
    infer_schema,
    list_collections,
    list_databases,
    preview_documents,
)

logger = logging.getLogger("db_assistant.mongo")

router = APIRouter(prefix="/mongo", tags=["mongo"])

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------
_SAFE_COLLECTION_RE = re.compile(r"^[a-zA-Z0-9_.\-]{1,128}$")
_BLOCKED_OPERATORS = {
    "$where", "$function", "$accumulator",
    "$merge", "$out",   # $lookup is now allowed for cross-collection joins
}

# $lookup is permitted but restricted to same-DB local collections only.
# Validated in _check_lookup_safe() before execution.
def _check_lookup_safe(pipeline: list, allowed_collections: set) -> None:
    """Ensure any $lookup stage only references known collections in the same DB."""
    for stage in pipeline:
        if not isinstance(stage, dict):
            continue
        lookup = stage.get("$lookup")
        if not lookup:
            continue
        from_coll = lookup.get("from", "")
        if from_coll and allowed_collections and from_coll not in allowed_collections:
            raise HTTPException(
                422,
                detail=f"$lookup references unknown collection '{from_coll}'. "
                       f"Allowed: {sorted(allowed_collections)}"
            )


def _check_blocked(obj: Any, path: str = "root") -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in _BLOCKED_OPERATORS:
                raise HTTPException(
                    422, detail=f"Operator '{k}' is not permitted (at {path}.{k})."
                )
            _check_blocked(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            _check_blocked(item, f"{path}[{i}]")


# ---------------------------------------------------------------------------
# JSON serialisation helper
# ---------------------------------------------------------------------------
def _json_safe(v: Any) -> Any:
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _json_safe(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_json_safe(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return v


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------
class MongoConnRequest(BaseModel):
    name: str
    mongo_uri: str


class MongoDirectQueryRequest(BaseModel):
    """
    Direct query endpoint — no LLM, no Postgres dependency.
    Use this for raw filter-based queries.
    """
    mongo_uri: str = Field(..., description="MongoDB connection URI")
    db_name: str = Field(..., description="Database name")
    collection: str = Field(..., description="Collection name")
    filter: Dict[str, Any] = Field(default_factory=dict)
    projection: Optional[Dict[str, Any]] = None
    sort: Optional[Dict[str, int]] = None
    limit: int = Field(50, ge=1, le=200)

    @field_validator("collection")
    @classmethod
    def safe_collection(cls, v: str) -> str:
        if not _SAFE_COLLECTION_RE.match(v):
            raise ValueError(f"Invalid collection name '{v}'.")
        return v


class MongoNLQRequest(BaseModel):
    """
    Natural language query endpoint — uses Gemini to generate Mongo spec.
    """
    mongo_uri: str = Field(..., description="MongoDB connection URI e.g. mongodb://localhost:27017")
    db_name: str = Field(..., description="Database name e.g. local")
    collection: str = Field(..., description="Collection name e.g. sales_data")
    question: str = Field(..., description="Natural language question")
    limit: int = Field(50, ge=1, le=200)
    default_days: int = Field(90, description="Default lookback days for date filtering")

    @field_validator("collection")
    @classmethod
    def safe_collection(cls, v: str) -> str:
        if not _SAFE_COLLECTION_RE.match(v):
            raise ValueError(f"Invalid collection name '{v}'.")
        return v


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

class MongoPingRequest(BaseModel):
    mongo_uri: str

@router.post("/ping-uri", tags=["mongo"])
def ping_mongo_uri(req: MongoPingRequest):
    """Test a MongoDB connection URI — returns ok + list of databases."""
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    try:
        client = MongoClient(req.mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        dbs = client.list_database_names()
        client.close()
        return {"status": "ok", "databases": dbs}
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        raise HTTPException(400, detail=f"Cannot connect to MongoDB: {e}")
    except Exception as e:
        raise HTTPException(400, detail=str(e))


@router.get("/collections", tags=["mongo"])
def get_all_collections(mongo_uri: str, db_name: str):
    """List all collections in a database."""
    try:
        return {"db": db_name, "collections": list_collections(mongo_uri, db_name)}
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))


@router.get("/databases", tags=["mongo"])
def get_databases(mongo_uri: str):
    """List all databases for a connection."""
    try:
        return {"databases": list_databases(mongo_uri)}
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))


@router.get("/preview", tags=["mongo"])
def get_preview(
    mongo_uri: str,
    db_name: str,
    collection: str,
    limit: int = 10,
):
    """Preview documents in a collection."""
    try:
        return {"data": preview_documents(mongo_uri, db_name, collection, limit)}
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))


@router.get("/schema", tags=["mongo"])
def get_schema(
    mongo_uri: str,
    db_name: str,
    collection: str,
    sample_size: int = 400,
):
    """Infer schema from a collection."""
    try:
        s = infer_schema(mongo_uri, db_name, collection, sample_size)
        prompt = build_mongo_schema_prompt(s)
        date_candidates = get_date_candidates(s)
        return {
            "schema": s,
            "schema_prompt": prompt,
            "date_candidates": date_candidates,
        }
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))


@router.post("/query", tags=["mongo"])
async def mongo_query(req: MongoDirectQueryRequest):
    """
    Direct MongoDB query — no LLM needed.

    Swagger test body:
    {
      "mongo_uri": "mongodb://localhost:27017",
      "db_name": "local",
      "collection": "sales_data",
      "filter": {},
      "limit": 10
    }
    """
    _check_blocked(req.filter, "filter")
    if req.projection:
        _check_blocked(req.projection, "projection")

    import motor.motor_asyncio
    client = motor.motor_asyncio.AsyncIOMotorClient(
        req.mongo_uri, serverSelectionTimeoutMS=5000
    )
    try:
        cursor = client[req.db_name][req.collection].find(
            req.filter,
            req.projection or None,
        )
        if req.sort:
            cursor = cursor.sort(list(req.sort.items()))
        cursor = cursor.limit(req.limit)

        raw: List[Dict] = await cursor.to_list(length=req.limit)

    except Exception as exc:
        logger.error(
            "Query failed collection=%s\n%s", req.collection, traceback.format_exc()
        )
        raise HTTPException(500, detail=f"MongoDB query error: {exc}")
    finally:
        client.close()

    safe_data = [_json_safe(d) for d in raw]
    cols = list(safe_data[0].keys()) if safe_data else []

    # Run post-processing for EDA profile + insights
    from app.state.agent_state import AgentState
    post = AgentState(
        user_question = f"Direct query on {req.collection}",
        results       = safe_data,
        columns       = cols,
    )
    post = _orchestrator.run_post_processing(post)

    return {
        "source":      "mongo",
        "db":          req.db_name,
        "collection":  req.collection,
        "filter":      req.filter,
        "count":       len(raw),
        "limit_applied": req.limit,
        "data":        safe_data,
        "summary":     post.summary,
        "viz":         post.viz,
        "profile":     post.profile,
        "eda_insights": post.eda_insights,
    }


@router.post("/nl-query", tags=["mongo"])
def mongo_nl_query(req: MongoNLQRequest):
    """
    Natural language → MongoDB query using Gemini LLM.

    Swagger test body:
    {
      "mongo_uri": "mongodb://localhost:27017",
      "db_name": "local",
      "collection": "sales_data",
      "question": "Show me all orders from Alice",
      "limit": 50
    }

    Requires GEMINI_API_KEY environment variable to be set.
    """
    uri = req.mongo_uri

    # 1) Schema inference
    try:
        schema = infer_schema(uri, req.db_name, req.collection, sample_size=400)
    except Exception as exc:
        raise HTTPException(500, detail=f"Schema inference failed: {exc}")

    schema_prompt = build_mongo_schema_prompt(schema)

    # 2) Determine date field dynamically
    date_candidates = get_date_candidates(schema)
    date_field = date_candidates[0] if date_candidates else ""

    # 3) LLM -> spec
    try:
        agent = MongoQueryAgent()
        spec = agent.run(
            schema_prompt=schema_prompt,
            question=req.question,
            date_field=date_field if date_field else None,
            default_days=req.default_days,
            limit=req.limit,
        )
    except Exception as exc:
        raise HTTPException(500, detail=f"LLM query generation failed: {exc}")

    # 4) Validate spec safety
    try:
        validate_spec(spec)
    except ValueError as exc:
        raise HTTPException(422, detail=f"Unsafe query spec: {exc}")

    # 5) Validate fields against inferred schema
    allowed_fields = set()
    for f in (schema.get("fields") or []):
        if isinstance(f, dict):
            p = f.get("path") or f.get("field")
            if isinstance(p, str) and p.strip():
                p = p.strip()
                allowed_fields.add(p)
                allowed_fields.add(p.replace("[]", ""))
    allowed_fields.add("_id")

    # Strip meta-keys that Gemini sometimes puts into the filter/sort dict
    # instead of treating them as query parameters. These are never document fields.
    _META_KEYS = {"limit", "skip", "sort", "projection", "hint",
                  "count", "offset", "page", "page_size", "max", "min"}

    def _strip_meta(obj: Any) -> Any:
        """Recursively remove meta-keys from filter dicts."""
        if isinstance(obj, dict):
            return {k: _strip_meta(v) for k, v in obj.items() if k not in _META_KEYS}
        if isinstance(obj, list):
            return [_strip_meta(i) for i in obj]
        return obj

    # Clean filter and match stages
    if isinstance(spec.get("filter"), dict):
        spec["filter"] = _strip_meta(spec["filter"])
    if isinstance(spec.get("pipeline"), list):
        for stage in spec["pipeline"]:
            if isinstance(stage, dict) and "$match" in stage:
                stage["$match"] = _strip_meta(stage["$match"])

    # If Gemini put limit at top level of spec as a field, move it to the right place
    if "limit" in spec and not isinstance(spec.get("limit"), int):
        spec.pop("limit", None)

    try:
        validate_fields_against_schema(spec, allowed_fields)
    except ValueError as exc:
        # Log but don't block — field validation is a safeguard, not a hard gate.
        # Gemini sometimes uses valid $expr or computed fields not in the schema sample.
        logger.warning("Field validation warning (non-fatal): %s", exc)

    # 6) Enforce limit + date policy
    spec = enforce_limit(spec, req.limit)
    spec = enforce_date_filter(
        spec,
        date_field=date_field,
        default_days=req.default_days,
        user_question=req.question,
    )

    # 7) Execute
    try:
        data, execution_time_ms = run_query(uri, req.db_name, req.collection, spec)
    except Exception as exc:
        logger.error("Mongo execute failed:\n%s", traceback.format_exc())
        raise HTTPException(500, detail=f"Query execution failed: {exc}")

    # 8) Serialise (handles ObjectId + datetime)
    safe_data = [_json_safe(d) for d in data]
    safe_spec = _json_safe(spec)

    # 9) Run InsightAgent + VisualizationAgent via Orchestrator
    post_state = AgentState(
        source        = "mongodb",
        user_question = req.question,
        results       = safe_data,
        columns       = list(safe_data[0].keys()) if safe_data else [],
    )
    post_state = _orchestrator.run_post_processing(post_state)

    return {
        "source": "mongo",
        "db_name": req.db_name,
        "collection": req.collection,
        "date_field_used": date_field or None,
        "question": req.question,
        "spec": safe_spec,
        "count": len(safe_data),
        "data": safe_data,
        "execution_time_ms": execution_time_ms,
        "summary": post_state.summary or f"Returned {len(safe_data)} rows.",
        "viz": post_state.viz,
        "profile": post_state.profile,
    }

# ---------------------------------------------------------------------------
# Multi-collection NL JOIN query  ($lookup / pipeline approach)
# ---------------------------------------------------------------------------
class MongoJoinNLRequest(BaseModel):
    mongo_uri:       str
    db_name:         str
    collections:     List[str]   = Field(..., description="2+ collection names to join")
    question:        str
    limit:           int         = Field(50, ge=1, le=500)
    default_days:    int         = Field(90, ge=1, le=3650)


@router.post("/nl-query-join", tags=["mongo"])
def mongo_nl_query_join(req: MongoJoinNLRequest):
    """
    Natural-language JOIN query across multiple MongoDB collections using $lookup.
    Gemini receives all collection schemas and writes an aggregation pipeline
    with $lookup stages to join them.
    """
    if len(req.collections) < 2:
        raise HTTPException(400, detail="Provide at least 2 collections to join.")

    uri = req.mongo_uri

    # 1) Infer schema for ALL collections
    all_schemas = {}
    for coll in req.collections:
        try:
            schema = infer_schema(uri, req.db_name, coll, sample_size=200)
            all_schemas[coll] = schema
        except Exception as exc:
            raise HTTPException(500, detail=f"Schema inference failed for '{coll}': {exc}")

    # 2) Build combined schema prompt
    schema_lines = []
    for coll, schema in all_schemas.items():
        schema_lines.append(f"Collection: {coll}")
        fields = schema.get("fields") or []
        for f in fields[:30]:  # cap at 30 fields per collection
            path = (f.get("path") or f.get("field") or "")
            ftype = f.get("type", "mixed")
            schema_lines.append(f"  - {path} ({ftype})")
        schema_lines.append("")

    # Detect potential join keys (fields shared across collections)
    field_sets = {}
    for coll, schema in all_schemas.items():
        field_sets[coll] = {
            (f.get("path") or f.get("field") or "").replace("[]", "")
            for f in (schema.get("fields") or [])
        }

    join_hints = []
    coll_list = list(all_schemas.keys())
    for i in range(len(coll_list)):
        for j in range(i + 1, len(coll_list)):
            c1, c2 = coll_list[i], coll_list[j]
            common = field_sets[c1] & field_sets[c2] - {"", "_id"}
            if common:
                join_hints.append(
                    f"  - '{c1}' and '{c2}' share fields: {', '.join(sorted(common)[:4])}"
                )

    hint_block = ""
    if join_hints:
        hint_block = "Potential $lookup join keys:\n" + "\n".join(join_hints) + "\n\n"

    schema_prompt = "\n".join(schema_lines)

    coll_list_str = ", ".join(req.collections)

    # Build actual field samples so Gemini knows the real join keys and enum values
    field_sample_lines = []
    client_tmp = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    db_tmp = client_tmp[req.db_name]
    coll_field_samples = {}
    # Collect distinct values for key categorical fields
    enum_value_lines = []
    ENUM_FIELDS = ["region", "category", "subcategory", "status", "tier",
                   "country", "payment_method", "warehouse", "brand"]
    for coll in req.collections:
        try:
            sample_doc = db_tmp[coll].find_one({}, {"_id": 0})
            if sample_doc:
                coll_field_samples[coll] = list(sample_doc.keys())
                field_sample_lines.append(
                    "Collection '" + coll + "' fields: " + ", ".join(list(sample_doc.keys())[:20])
                )
                # Get distinct values for categorical fields that exist in this collection
                for ef in ENUM_FIELDS:
                    if ef in sample_doc:
                        try:
                            vals = db_tmp[coll].distinct(ef)
                            if vals and len(vals) <= 20:
                                enum_value_lines.append(
                                    coll + "." + ef + " values: " + ", ".join(str(v) for v in sorted(vals)[:15])
                                )
                        except Exception:
                            pass
        except Exception:
            pass
    client_tmp.close()

    # Auto-detect join keys by finding matching field values across collections
    join_key_hints = []
    coll_names = list(coll_field_samples.keys())
    for i in range(len(coll_names)):
        for j in range(i + 1, len(coll_names)):
            c1, c2 = coll_names[i], coll_names[j]
            common_fields = set(coll_field_samples.get(c1, [])) & set(coll_field_samples.get(c2, []))
            common_fields -= {"_id", "is_active", "notes", "tags"}
            if common_fields:
                join_key_hints.append(
                    "'" + c1 + "' and '" + c2 + "' share fields: " + ", ".join(sorted(common_fields)[:6])
                )

    field_sample_block = "\n".join(field_sample_lines) + "\n\n"
    enum_block = ("Actual field values (use EXACTLY these for filtering):\n" + "\n".join(enum_value_lines) + "\n\n") if enum_value_lines else ""
    join_hint_block = ("Join key candidates:\n" + "\n".join(join_key_hints) + "\n\n") if join_key_hints else ""

    question_with_ctx = (
        req.question + "\n\n"
        + field_sample_block
        + enum_block
        + join_hint_block
        + "Available collections: " + coll_list_str + "\n\n"
        + "Rules:\n"
        + "- YOU decide which collection is the best primary (FROM) based on the question\n"
        + "- Use $lookup with the ACTUAL shared field names shown above (NOT assumed id fields)\n"
        + "- $lookup format: {from: 'other_collection', localField: 'field', foreignField: 'field', as: 'alias'}\n"
        + "- Always use $unwind after $lookup if you need fields from the joined collection\n"
        + "- Use case-insensitive regex for string filters: {field: {$regex: 'value', $options: 'i'}}\n"
        + "- When user asks about a product TYPE (e.g. Laptops, Chairs, Monitors), use regex on the product name AND category/subcategory fields\n"
        + "- Add $limit: " + str(req.limit) + " at the end of the pipeline\n"
        + "- First line of your response MUST be: PRIMARY: <collection_name>\n"
        + "- Then output the JSON pipeline array starting with ["
    )

    # 3) Call Gemini — ask for primary collection + aggregation pipeline
    from app.services.nl_to_sql import _call_gemini_text
    import json

    PIPELINE_PROMPT = """You are a MongoDB aggregation pipeline generator.
Output format — TWO parts:
1. First line: PRIMARY: <collection_name>  (the collection to run the pipeline on)
2. Second line onwards: a valid JSON array of pipeline stages starting with [

Rules:
- Choose the PRIMARY collection that is most central to answering the question
- Use $lookup to join other collections onto the primary
- Never use $where, $function, $accumulator, $merge, $out
- No markdown, no extra explanation
"""
    try:
        raw = _call_gemini_text(PIPELINE_PROMPT, schema_prompt + "\n\nQuestion:\n" + question_with_ctx)
    except Exception as exc:
        raise HTTPException(500, detail=f"Gemini call failed: {exc}")

    # Extract PRIMARY collection from first line
    raw = raw.strip()
    primary_coll = req.collections[0]  # fallback
    lines = raw.split("\n")
    for line in lines[:3]:
        if line.strip().upper().startswith("PRIMARY:"):
            candidate = line.split(":", 1)[1].strip().strip("'\"` ")
            if candidate in req.collections:
                primary_coll = candidate
            break
    # Remove the PRIMARY: line before parsing JSON
    raw = "\n".join(l for l in lines if not l.strip().upper().startswith("PRIMARY:")).strip()
    logger.info("Mongo JOIN — primary collection auto-selected: %s", primary_coll)

    # 4) Parse the pipeline
    raw = raw.strip()
    raw = re.sub(r"```json|```", "", raw, flags=re.IGNORECASE).strip()
    # Find the first [ ... ] array
    start = raw.find("[")
    if start == -1:
        raise HTTPException(500, detail=f"Gemini did not return a pipeline array. Raw: {raw[:300]}")
    # Find matching closing bracket
    depth = 0
    end = start
    for idx in range(start, len(raw)):
        if raw[idx] == "[": depth += 1
        elif raw[idx] == "]":
            depth -= 1
            if depth == 0:
                end = idx + 1
                break
    try:
        pipeline = json.loads(raw[start:end])
    except Exception as exc:
        raise HTTPException(500, detail=f"Failed to parse pipeline JSON: {exc}. Raw: {raw[:300]}")

    if not isinstance(pipeline, list):
        raise HTTPException(500, detail="Gemini returned a non-array pipeline.")

    # 5) Safety check — block dangerous operators, validate $lookup targets
    _check_blocked(pipeline)
    allowed_colls = set(req.collections)
    _check_lookup_safe(pipeline, allowed_colls)

    # 6) Enforce limit
    has_limit = any("$limit" in s for s in pipeline if isinstance(s, dict))
    if not has_limit:
        pipeline.append({"$limit": req.limit})

    # 7) Execute — try primary first, then all collections if 0 results
    t0 = time.perf_counter()
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[req.db_name]

    # First peek at actual values in the data to catch case issues
    debug_info = {}
    try:
        for coll in req.collections:
            sample = list(db[coll].find({}, {"_id": 0}).limit(1))
            if sample:
                debug_info[coll] = sample[0]
    except Exception:
        pass

    raw_docs = []
    winning_coll = primary_coll
    try_order = [primary_coll] + [c for c in req.collections if c != primary_coll]

    for try_coll in try_order:
        try:
            docs = list(db[try_coll].aggregate(pipeline))
            if docs:
                raw_docs = docs
                winning_coll = try_coll
                break
        except Exception as exc:
            logger.warning("Pipeline failed on %s: %s", try_coll, exc)
            continue

    # If still 0 results, try relaxing $match string filters to case-insensitive regex
    if not raw_docs:
        def _relax_match(stage):
            """Convert exact string matches to case-insensitive regex."""
            if not isinstance(stage, dict):
                return stage
            result = {}
            for k, v in stage.items():
                if isinstance(v, str) and not k.startswith("$"):
                    result[k] = {"$regex": v, "$options": "i"}
                elif isinstance(v, dict):
                    result[k] = _relax_match(v)
                else:
                    result[k] = v
            return result

        relaxed_pipeline = []
        for stage in pipeline:
            if isinstance(stage, dict) and "$match" in stage:
                relaxed_pipeline.append({"$match": _relax_match(stage["$match"])})
            else:
                relaxed_pipeline.append(stage)

        for try_coll in try_order:
            try:
                docs = list(db[try_coll].aggregate(relaxed_pipeline))
                if docs:
                    raw_docs = docs
                    winning_coll = try_coll
                    pipeline = relaxed_pipeline  # use relaxed version in response
                    break
            except Exception:
                continue

    client.close()
    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    # Flatten nested $lookup arrays into readable columns
    def _flatten_doc(doc):
        flat = {}
        for k, v in doc.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                if len(v) == 1:
                    # Single-element lookup — flatten fields with prefix
                    for sub_k, sub_v in v[0].items():
                        if sub_k != "_id":
                            flat[k + "_" + sub_k] = sub_v
                else:
                    # Multi-element — keep count + key fields only
                    flat[k + "_count"] = len(v)
                    # Try to extract a useful summary field
                    for summary_field in ["name", "customer_name", "product", "email"]:
                        vals = [str(item.get(summary_field, "")) for item in v if item.get(summary_field)]
                        if vals:
                            flat[k + "_names"] = ", ".join(vals[:5])
                            break
            elif isinstance(v, list) and v and not isinstance(v[0], dict):
                flat[k] = ", ".join(str(i) for i in v[:10])
            else:
                flat[k] = v
        return flat

    flattened = [_flatten_doc(d) for d in raw_docs]
    # Drop columns that are entirely None/empty
    if flattened:
        all_keys = list(flattened[0].keys())
        non_empty_keys = [
            k for k in all_keys
            if any(d.get(k) not in (None, "", "None") for d in flattened)
        ]
        flattened = [{k: d.get(k) for k in non_empty_keys} for d in flattened]
    safe_data = [_json_safe(d) for d in flattened]

    return {
        "source":             "mongo_join",
        "db_name":            req.db_name,
        "primary_collection": winning_coll,
        "collections":        req.collections,
        "question":           req.question,
        "pipeline":           _json_safe(pipeline),
        "debug_sample":       _json_safe(debug_info),
        "count":              len(safe_data),
        "data":               safe_data,
        "execution_time_ms":  elapsed_ms,
    }
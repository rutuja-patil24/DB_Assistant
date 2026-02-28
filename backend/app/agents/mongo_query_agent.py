# backend/app/agents/mongo_query_agent.py
from __future__ import annotations

from typing import Any, Dict, Optional
import json

from app.services.nl_to_sql import generate_json  # MUST return raw model text (string)


MONGO_QUERY_SYSTEM = """You are a MongoDB query planner for READ-ONLY analytics.

Return ONLY valid JSON. No markdown. No explanation. No SQL.

Output MUST match ONE of these formats:

1) FIND query:
{
  "query_type": "find",
  "filter": { ... },
  "projection": { ... },   // optional
  "sort": { ... },         // optional
  "limit": 50
}

2) AGGREGATE query:
{
  "query_type": "aggregate",
  "pipeline": [ ... ],
  "limit": 50
}

Hard Rules:
- READ-ONLY only.
- Keep output small: always include "limit".
- Allowed aggregate stages: $match, $project, $group, $sort, $limit, $unwind, $addFields
- FORBIDDEN: $where, $function, $accumulator, $facet, $lookup, $merge, $out
- Use ONLY fields that appear in the provided SCHEMA. Do not invent fields.
- If a requested field does not exist, choose the closest available field OR return a safe empty query:
  - find: {"filter": {"__no_such_field__": "__no_such_value__"}}
  - aggregate: start with {"$match": {"__no_such_field__": "__no_such_value__"}}
Date Filtering Policy:
- Only apply date filtering if:
  (a) the user explicitly asks for a time window (last N days, recent, between dates), AND
  (b) DATE_FIELD is provided (not null).
- If DATE_FIELD is null or user didn't ask for time filtering, do not add a date $match.
"""


def _extract_first_json_object(text: str) -> Dict[str, Any]:
    """
    Extract the first balanced JSON object from text.
    This is safer than a greedy regex and handles extra leading/trailing text.
    """
    s = (text or "").strip()
    if not s:
        raise ValueError("Empty model output (expected JSON).")

    # Fast path
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # Balanced-brace scan for first {...}
    start = s.find("{")
    if start == -1:
        raise ValueError(f"No '{{' found in model output. Raw: {s[:300]}")

    depth = 0
    in_str = False
    escape = False

    for i in range(start, len(s)):
        ch = s[i]

        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        else:
            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = s[start : i + 1]
                    try:
                        obj = json.loads(candidate)
                    except Exception as e:
                        raise ValueError(f"Invalid JSON extracted: {e}. Raw: {candidate[:300]}")
                    if not isinstance(obj, dict):
                        raise ValueError("Extracted JSON was not an object.")
                    return obj

    raise ValueError(f"Unbalanced JSON braces. Raw: {s[:300]}")


def _normalize_spec(spec: Dict[str, Any], limit: int) -> Dict[str, Any]:
    qt = (spec.get("query_type") or "").strip().lower()
    if qt not in {"find", "aggregate"}:
        raise ValueError(f"Invalid query_type returned: {spec.get('query_type')}")

    # enforce limit
    if "limit" not in spec:
        spec["limit"] = limit
    try:
        spec["limit"] = int(spec["limit"])
    except Exception:
        spec["limit"] = limit

    # required shape
    if qt == "aggregate":
        if not isinstance(spec.get("pipeline"), list):
            raise ValueError("Aggregate query must include a list 'pipeline'.")
    else:
        if not isinstance(spec.get("filter"), dict):
            spec["filter"] = {}

    return spec


def build_prompt(
    schema_prompt: str,
    question: str,
    date_field: Optional[str],
    default_days: int,
    limit: int,
) -> str:
    return f"""{MONGO_QUERY_SYSTEM}

SCHEMA (field paths + types + examples):
{schema_prompt}

DATE_FIELD: {date_field if date_field else "null"}
DEFAULT_DAYS: {default_days}
LIMIT: {limit}

USER_QUESTION: {question}
"""


def build_repair_prompt(bad_output: str) -> str:
    return f"""Your previous output was NOT valid JSON.

Fix it and return ONLY valid JSON (a single JSON object). No markdown. No explanation.

BAD_OUTPUT:
{bad_output}
"""


class MongoQueryAgent:
    """
    Plans a Mongo query spec using inferred schema prompt ONLY.
    Execution + safety validation must happen in mongo_query_validator.py before running.
    """

    def run(
        self,
        schema_prompt: str,
        question: str,
        date_field: Optional[str] = None,
        default_days: int = 90,
        limit: int = 50,
    ) -> Dict[str, Any]:
        prompt = build_prompt(schema_prompt, question, date_field, default_days, limit)

        raw = generate_json(prompt, question)  # must return raw text from Gemini
        try:
            spec = _extract_first_json_object(raw)
            return _normalize_spec(spec, limit)
        except Exception:
            # Retry once with a repair prompt (very effective in practice)
            repaired_raw = generate_json(build_repair_prompt(raw), "Return valid JSON only.")
            spec = _extract_first_json_object(repaired_raw)
            return _normalize_spec(spec, limit)

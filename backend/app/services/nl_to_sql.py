# backend/app/services/nl_to_sql.py

from __future__ import annotations

import os
import re
import json
import time
import logging
from google import genai
from google.genai import errors as genai_errors

logger = logging.getLogger("db_assistant.nl_to_sql")

SYSTEM_PROMPT_SQL = """You are a PostgreSQL SQL generator.
Rules:
- Use ONLY the provided table and columns
- Do NOT hallucinate tables or columns
- Do NOT use DELETE, UPDATE, DROP, INSERT, ALTER, CREATE
- Return ONLY SQL. No explanation. No markdown.
- The SQL MUST start with SELECT
"""

SYSTEM_PROMPT_JSON = """You are a JSON generator.
Rules:
- Return ONLY valid JSON. No SQL. No markdown. No extra text.
- The output MUST be parseable by json.loads().
"""

# Retry config for 429 rate limit errors
_MAX_RETRIES  = 3
_RETRY_DELAYS = [5, 15, 30]  # seconds between retries


def assert_safe_select(sql: str) -> None:
    s = sql.strip().lower()
    if not s.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")
    banned = ["delete", "update", "drop", "alter", "truncate",
              "insert", "create", "grant", "revoke"]
    if any(re.search(rf"\b{b}\b", s) for b in banned):
        raise ValueError("Unsafe SQL detected.")


def _extract_sql(text: str) -> str:
    """Extract first SELECT ... statement from model output."""
    text = (text or "").strip()
    text = re.sub(r"```sql", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```", "", text)
    text = text.strip()

    match = re.search(r"\bSELECT\b", text, re.IGNORECASE)
    if not match:
        raise ValueError(f"No SELECT statement found in model output. Raw: {text[:300]}")

    sql = text[match.start():].strip().rstrip(";").strip()
    return sql


def _extract_first_json_object(text: str) -> str:
    s = (text or "").strip()
    s = re.sub(r"```json|```", "", s, flags=re.IGNORECASE).strip()

    if not s:
        raise ValueError("Empty model output (expected JSON).")

    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return s
    except Exception:
        pass

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
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = s[start: i + 1]
                try:
                    obj = json.loads(candidate)
                except Exception as e:
                    raise ValueError(f"Invalid JSON extracted: {e}. Raw: {candidate[:300]}")
                if not isinstance(obj, dict):
                    raise ValueError("Extracted JSON was not an object.")
                return candidate

    raise ValueError(f"Unbalanced JSON braces in model output. Raw: {s[:300]}")


def _is_rate_limit_error(e: Exception) -> bool:
    """Check if error is a 429 rate limit / resource exhausted error."""
    msg = str(e).lower()
    return "429" in msg or "resource_exhausted" in msg or "resource exhausted" in msg


def _call_gemini_text(system_prompt: str, user_prompt: str) -> str:
    """
    Shared Gemini call with automatic retry on 429 rate limit errors.
    Retries up to 3 times with increasing delays: 5s, 15s, 30s.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    client = genai.Client(api_key=api_key)
    last_error = None

    for attempt in range(_MAX_RETRIES):
        try:
            resp = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=[system_prompt, user_prompt],
            )
            return (resp.text or "").strip()

        except genai_errors.ClientError as e:
            if _is_rate_limit_error(e) and attempt < _MAX_RETRIES - 1:
                delay = _RETRY_DELAYS[attempt]
                logger.warning(
                    "Gemini 429 rate limit hit (attempt %d/%d). "
                    "Retrying in %ds...", attempt + 1, _MAX_RETRIES, delay
                )
                time.sleep(delay)
                last_error = e
                continue
            raise RuntimeError(f"Gemini API error: {e}")

        except Exception as e:
            if _is_rate_limit_error(e) and attempt < _MAX_RETRIES - 1:
                delay = _RETRY_DELAYS[attempt]
                logger.warning(
                    "Gemini rate limit hit (attempt %d/%d). "
                    "Retrying in %ds...", attempt + 1, _MAX_RETRIES, delay
                )
                time.sleep(delay)
                last_error = e
                continue
            raise RuntimeError(f"Gemini call failed: {e}")

    raise RuntimeError(
        f"Gemini rate limit: all {_MAX_RETRIES} retries exhausted. "
        f"Please wait a minute and try again. Last error: {last_error}"
    )


def generate_sql(schema_prompt: str, user_question: str) -> str:
    prompt = f"""{schema_prompt}

User Question:
{user_question}

Return ONLY SQL:
"""
    raw_text = _call_gemini_text(SYSTEM_PROMPT_SQL, prompt)
    sql = _extract_sql(raw_text)
    assert_safe_select(sql)
    return sql


def generate_json(schema_prompt: str, user_question: str) -> str:
    """
    Gemini call for Mongo query generation.
    Returns raw LLM text so MongoQueryAgent can parse it itself.
    """
    prompt = f"""{schema_prompt}

USER_QUESTION:
{user_question}

Return ONLY valid JSON:
"""
    raw_text = _call_gemini_text(SYSTEM_PROMPT_JSON, prompt)
    return raw_text
# backend/app/services/nl_to_sql.py
import os
import re
from google import genai
from google.genai import errors as genai_errors

SYSTEM_PROMPT = """You are a PostgreSQL SQL generator.
Rules:
- Use ONLY the provided table and columns
- Do NOT hallucinate tables or columns
- Do NOT use DELETE, UPDATE, DROP, INSERT, ALTER, CREATE
- Return ONLY SQL. No explanation. No markdown.
- The SQL MUST start with SELECT
"""

def assert_safe_select(sql: str) -> None:
    s = sql.strip().lower()
    if not s.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")
    banned = ["delete", "update", "drop", "alter", "truncate", "insert", "create"]
    if any(b in s for b in banned):
        raise ValueError("Unsafe SQL detected.")

def _extract_sql(text: str) -> str:
    """
    Extract first SELECT ... ; statement from model output
    """
    text = text.strip()

    # Remove markdown
    text = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE)

    # Find SELECT statement
    match = re.search(r"(select[\s\S]+?;?$)", text, re.IGNORECASE)
    if not match:
        raise ValueError("No SELECT statement found in model output")

    return match.group(1).strip()

def generate_sql(schema_prompt: str, user_question: str) -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    client = genai.Client(api_key=api_key)

    prompt = f"""{schema_prompt}

User Question:
{user_question}

Return ONLY SQL:
"""

    try:
        resp = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[SYSTEM_PROMPT, prompt],
        )

        raw_text = resp.text or ""
        sql = _extract_sql(raw_text)

        assert_safe_select(sql)
        return sql

    except genai_errors.ClientError as e:
        raise RuntimeError(f"Gemini API error: {e}")

    except Exception as e:
        raise RuntimeError(f"SQL generation failed: {e}")

import re

FORBIDDEN = {
    "insert", "update", "delete", "drop", "alter", "truncate",
    "create", "grant", "revoke", "vacuum", "analyze",
    "copy", "call", "execute", "do",
}

def normalize_sql(sql: str) -> str:
    # remove comments
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    sql = re.sub(r"--.*?$", " ", sql, flags=re.M)
    return " ".join(sql.strip().split())

def ensure_safe_select(sql: str) -> str:
    if not sql or not sql.strip():
        raise ValueError("SQL cannot be empty")

    s = normalize_sql(sql)
    low = s.lower()

    # allow only SELECT / WITH
    if not (low.startswith("select") or low.startswith("with")):
        raise ValueError("Only SELECT queries are allowed")

    # block multiple statements
    if ";" in s[:-1]:
        raise ValueError("Multiple SQL statements are not allowed")

    # block forbidden keywords
    tokens = set(re.findall(r"[a-zA-Z_]+", low))
    bad = sorted(tokens.intersection(FORBIDDEN))
    if bad:
        raise ValueError(f"Forbidden keyword(s): {', '.join(bad)}")

    # auto-limit results
    if " limit " not in low:
        s = s.rstrip(";") + " LIMIT 100;"

    return s
# app/core/sql_guard.py

def assert_safe_select(sql: str) -> str:
    """
    Raises ValueError if SQL is unsafe.
    Returns sanitized SQL (optional) if you want to use it downstream.
    """
    return ensure_safe_select(sql)

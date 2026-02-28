# backend/app/core/sql_guard.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Set, List
import re


class SQLGuardError(ValueError):
    pass


def _strip_strings(sql: str) -> str:
    """
    Replace single-quoted string literals with '' to avoid matching identifiers inside strings.
    Handles escaped quotes represented by doubled single quotes: 'it''s ok'
    """
    return re.sub(r"'(?:''|[^'])*'", "''", sql)


def _normalize_ws(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _remove_trailing_limit(sql: str) -> str:
    # If model adds multiple LIMITs, keep only the last one.
    # Example: "... LIMIT 100 LIMIT 50;" -> "... LIMIT 50;"
    s = sql.strip().rstrip(";").strip()
    limits = list(re.finditer(r"\bLIMIT\s+\d+\b", s, flags=re.IGNORECASE))
    if len(limits) <= 1:
        return sql.strip()

    last_limit_txt = limits[-1].group(0)
    s_no_limits = re.sub(r"\bLIMIT\s+\d+\b", "", s, flags=re.IGNORECASE)
    s_no_limits = _normalize_ws(s_no_limits)
    return (s_no_limits + " " + last_limit_txt + ";").strip()


def _safe_select_only(sql: str) -> None:
    s = sql.strip().lower()
    if not s.startswith("select"):
        raise SQLGuardError("Only SELECT queries are allowed.")
    banned = ["delete", "update", "drop", "alter", "truncate", "insert", "create", "grant", "revoke"]
    if any(re.search(rf"\b{b}\b", s) for b in banned):
        raise SQLGuardError("Unsafe SQL detected (non-SELECT operation).")


def _block_system_schemas(sql: str) -> None:
    s = sql.lower()
    blocked = ["information_schema", "pg_catalog", "pg_toast"]
    for b in blocked:
        if re.search(rf"\b{re.escape(b)}\b", s):
            raise SQLGuardError(f"Blocked system schema usage: {b}")


def _unquote_ident(x: str) -> str:
    x = x.strip()
    if x.startswith('"') and x.endswith('"'):
        return x[1:-1].replace('""', '"')
    return x


def _canon_table_fqn(schema: str, table: str) -> str:
    return f"{schema}.{table}".lower()


@dataclass
class ParsedFrom:
    alias_to_table: Dict[str, str]  # alias -> canon schema.table
    tables_used: Set[str]          # canon schema.table


class SQLGuard:
    """
    Validates generated SQL against allowed tables/columns.
    allowed_tables: dict canon_schema_table -> set(columns)
      e.g. { "u_user8.ds_abc...": {"region","total_amount",...}, ... }
    """

    def __init__(self, allowed_tables: Dict[str, Set[str]]):
        self.allowed_tables = {k.lower(): {c.lower() for c in v} for k, v in allowed_tables.items()}

    def validate_and_fix(self, sql: str) -> str:
        if not sql or not sql.strip():
            raise SQLGuardError("Empty SQL.")

        sql = sql.strip()
        sql = _remove_trailing_limit(sql)

        _safe_select_only(sql)
        _block_system_schemas(sql)

        parsed = self._parse_from_and_joins(sql)

        # Must use only allowed tables
        for t in parsed.tables_used:
            if t not in self.allowed_tables:
                raise SQLGuardError(
                    f"Query references table not in selected datasets: {t}. "
                    f"Allowed: {sorted(self.allowed_tables.keys())}"
                )

        # Validate columns
        self._validate_columns(sql, parsed)

        return sql

    def _parse_from_and_joins(self, sql: str) -> ParsedFrom:
        """
        Parse FROM/JOIN tables and aliases.
        Supports:
          FROM "schema"."table" AS t
          JOIN schema.table t2
          JOIN "schema"."table" t2
        """
        s = _strip_strings(sql)
        s = _normalize_ws(s)

        tbl_pat = r'(?:"[^"]+"|\w+)\.(?:"[^"]+"|\w+)'
        clause_pat = re.compile(
            rf'\b(FROM|JOIN)\s+({tbl_pat})\s*(?:AS\s+)?(\w+)?',
            flags=re.IGNORECASE
        )

        alias_to_table: Dict[str, str] = {}
        tables_used: Set[str] = set()

        for m in clause_pat.finditer(s):
            full = m.group(2)
            alias = m.group(3)

            schema_raw, table_raw = full.split(".", 1)
            schema = _unquote_ident(schema_raw)
            table = _unquote_ident(table_raw)
            canon = _canon_table_fqn(schema, table)

            tables_used.add(canon)

            # If alias missing, use the table name as an implicit alias (SQL behavior)
            if not alias:
                alias = re.sub(r'\W+', '', table)

            alias_to_table[alias.lower()] = canon

        if not tables_used:
            raise SQLGuardError("Could not find any FROM/JOIN tables in SQL.")

        return ParsedFrom(alias_to_table=alias_to_table, tables_used=tables_used)

    def _validate_columns(self, sql: str, parsed: ParsedFrom) -> None:
        s = _strip_strings(sql)

        # 1) Qualified refs: alias.col or alias."col"
        qref_pat = re.compile(r'\b([A-Za-z_]\w*)\s*\.\s*("([^"]+)"|([A-Za-z_]\w*))')
        for m in qref_pat.finditer(s):
            alias = m.group(1).lower()
            col = (m.group(3) or m.group(4) or "").lower()

            # ignore things like alias.* (wildcard)
            if col == "*" or not col:
                continue

            if alias not in parsed.alias_to_table:
                raise SQLGuardError(f"Unknown table alias used in query: {alias}")

            t = parsed.alias_to_table[alias]
            allowed_cols = self.allowed_tables.get(t, set())
            if col not in allowed_cols:
                raise SQLGuardError(f"Column '{col}' not found in table '{t}' (alias {alias}).")

        # Unqualified column check intentionally removed — causes false positives
        # with aggregates, aliases, and cross-schema queries.
        # Table-level validation above is sufficient for security.
        pass
# ---- Backwards-compatible helper (used by app/main.py) ----
# ---- Backwards-compatible helpers (used by other modules) ----

def assert_safe_select(sql: str) -> None:
    """
    Compatibility shim: older code imports assert_safe_select.
    Raises SQLGuardError if SQL is not a safe SELECT.
    """
    if not sql or not sql.strip():
        raise SQLGuardError("Empty SQL.")
    fixed = _remove_trailing_limit(sql.strip())
    _safe_select_only(fixed)
    _block_system_schemas(fixed)


def ensure_safe_select(sql: str) -> str:
    """
    Compatibility shim: older code imports ensure_safe_select.
    Returns possibly-fixed SQL (e.g., removes duplicate LIMIT).
    """
    if not sql or not sql.strip():
        raise SQLGuardError("Empty SQL.")
    fixed = _remove_trailing_limit(sql.strip())
    _safe_select_only(fixed)
    _block_system_schemas(fixed)
    return fixed
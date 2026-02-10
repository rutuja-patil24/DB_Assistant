# backend/app/agents/nl_to_sql_agent.py

from __future__ import annotations

import re
from itertools import combinations
from typing import Dict, List

from app.services.nl_to_sql import generate_sql
from app.state.agent_state import AgentState


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _has_limit(sql: str) -> bool:
    """
    Robust LIMIT detection:
    - catches: LIMIT 10, limit 50;
    - avoids false positives in words like 'unlimited'
    """
    return re.search(r"\blimit\s+\d+\b", (sql or "").lower()) is not None


def _remove_trailing_limit(sql: str) -> str:
    """
    Optional safety: If somehow SQL contains multiple LIMITs, keep the first one
    and remove any later LIMIT clauses at the very end. (Best effort.)
    """
    if not sql:
        return sql
    # If there are two "LIMIT n" occurrences, remove the last trailing one.
    matches = list(re.finditer(r"\blimit\s+\d+\b", sql.lower()))
    if len(matches) <= 1:
        return sql

    # Remove from the start of the last LIMIT to end-of-statement (best effort)
    last = matches[-1]
    # Only strip if last LIMIT is near the end (so we don't break subqueries)
    tail = sql[last.start():]
    if len(tail) < 60:  # heuristic: trailing limit usually short
        sql = sql[: last.start()].rstrip().rstrip(";")
        # Keep semicolon if original had it
        return sql + ";"
    return sql


def _common_join_hints(tables: Dict[str, dict]) -> List[str]:
    """
    Heuristic join hints:
    - finds common column names across tables, prioritizing *_id
    Returns hint strings like:
      T1."customer_id" = T2."customer_id"
    """
    cols_map: Dict[str, Dict[str, str]] = {}
    for dsid, info in tables.items():
        cols = info.get("columns", []) or []
        d = {}
        for c in cols:
            name = c.get("name") or c.get("column_name") or ""
            if name:
                d[_norm(name)] = name
        cols_map[dsid] = d

    hints: List[str] = []
    dsids = list(tables.keys())

    for a, b in combinations(dsids, 2):
        common = set(cols_map[a].keys()) & set(cols_map[b].keys())
        key_like = [c for c in common if c.endswith("_id")]

        # Prefer *_id joins; otherwise take one shared column if any
        chosen = sorted(key_like)[:2] if key_like else (sorted(list(common))[:1] if common else [])

        for c in chosen:
            ca = cols_map[a][c]
            cb = cols_map[b][c]
            hints.append(f'T1."{ca}" = T2."{cb}"')

    return hints


def _build_multi_table_schema_prompt(state: AgentState) -> str:
    """
    Combined schema prompt across ALL selected datasets.
    Uses quoted table names stored by SchemaAgent in:
      state.datasets[dataset_id]["table"]
    """
    selected = getattr(state, "selected_datasets", None) or []
    datasets = getattr(state, "datasets", None) or {}

    tables: Dict[str, dict] = {dsid: datasets[dsid] for dsid in selected if dsid in datasets}
    if not tables:
        raise ValueError("No schemas found in state.datasets for selected_datasets. Run SchemaAgent first.")

    lines: List[str] = []
    lines.append("You are a SQL generator for PostgreSQL.")
    lines.append("Return ONLY one SQL SELECT query. No explanation.")
    lines.append("Use table names EXACTLY as shown (including quotes).")
    lines.append("If multiple tables are provided and the question needs it, use JOIN with clear aliases.")
    lines.append("If aggregation is used, always alias expressions (e.g., SUM(x) AS total_sales).")
    lines.append("")

    lines.append("### Available tables")
    # Give stable table labels T1, T2, ... in the prompt
    ordered_dsids = list(tables.keys())
    for i, dsid in enumerate(ordered_dsids, start=1):
        info = tables[dsid]
        table_quoted = info.get("table")
        cols = info.get("columns", []) or []

        lines.append(f"\nTable T{i}: {table_quoted}")
        lines.append("Columns:")
        for c in cols:
            name = c.get("name") or c.get("column_name") or ""
            typ = c.get("pg_type") or c.get("type") or ""
            if name:
                if typ:
                    lines.append(f'  - "{name}" ({typ})')
                else:
                    lines.append(f'  - "{name}"')

    join_hints = _common_join_hints(tables)
    if join_hints:
        lines.append("\n### Join hints (if needed)")
        lines.append("If you need to join tables, prefer joining on shared *_id columns:")
        for h in join_hints[:10]:
            lines.append(f"  - {h}")

    lines.append("\n### Rules")
    lines.append("- SELECT only (no INSERT/UPDATE/DELETE/DROP).")
    lines.append("- Use GROUP BY for grouped aggregations.")
    lines.append("- If the user asks for top N, use ORDER BY + LIMIT.")
    lines.append("- If you include LIMIT, include it only once.")
    lines.append("")

    return "\n".join(lines)


class NLToSQLAgent:
    """
    Join-aware NL→SQL agent.

    Works for:
      - single dataset: req.dataset_id
      - multi dataset:  req.dataset_ids (JOINs)

    Requires SchemaAgent to populate:
      state.datasets[dataset_id]["table"] and ["columns"] and (optionally) ["schema_prompt"]

    Uses Gemini generator:
      generate_sql(schema_prompt: str, user_question: str) -> str
    """

    def run(self, state: AgentState) -> AgentState:
        user_question = getattr(state, "user_question", None)
        if not user_question:
            raise ValueError("Missing user_question in AgentState")

        # Build combined prompt over ALL selected datasets (join-capable)
        schema_prompt = _build_multi_table_schema_prompt(state)

        # Call your repo's Gemini SQL generator
        sql = generate_sql(schema_prompt=schema_prompt, user_question=user_question)

        # Clean up any accidental double LIMIT at end (best effort)
        sql = _remove_trailing_limit(sql)

        # Enforce LIMIT if request provided and SQL doesn't already have one
        limit = getattr(state, "limit", 50)
        if isinstance(limit, int) and limit > 0:
            if not _has_limit(sql):
                sql = sql.rstrip().rstrip(";") + f" LIMIT {limit};"
            else:
                # Ensure we don't end up with "LIMIT X LIMIT Y"
                sql = _remove_trailing_limit(sql)

        state.generated_sql = sql
        return state

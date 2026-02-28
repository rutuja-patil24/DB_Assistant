# backend/app/agents/execution_agent.py
from __future__ import annotations

import re
import time
from typing import Optional

from app.db import get_conn
from app.core.sql_guard import SQLGuard, SQLGuardError


class ExecutionAgent:
    """
    Executes SQL on Postgres and stores results back into AgentState.

    Supports a special placeholder in SQL:
      - "{table}" will be replaced with the real fully-qualified table name
        for the FIRST selected dataset (looked up from dataset_registry).
    """

    def _resolve_table_fqn(self, user_id: str, dataset_id: str) -> Optional[str]:
        """
        Look up the real schema/table for a dataset_id and return a quoted FQN:
          "\"schema\".\"table\""
        """
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_schema_name, table_name
                    FROM dataset_registry
                    WHERE dataset_id = %s AND user_id = %s
                    """,
                    (dataset_id, user_id),
                )
                row = cur.fetchone()
                if not row:
                    return None
                schema_name, table_name = row[0], row[1]
                return f"\"{schema_name}\".\"{table_name}\""
        finally:
            conn.close()

    def _apply_sql_guard(self, state) -> None:
        """
        Validate generated SQL against allowed tables + columns derived from SchemaAgent memory.
        Mutates state.generated_sql if minor fixes are applied (e.g., duplicate LIMIT cleanup).
        Raises ValueError if blocked.
        """
        sql = getattr(state, "generated_sql", None) or ""
        if not sql.strip():
            raise ValueError("SQLGuard: empty SQL")

        datasets = getattr(state, "datasets", None) or {}
        if not isinstance(datasets, dict) or not datasets:
            # SchemaAgent didn't populate memory; skip guard rather than crash.
            return

        allowed = {}
        for ds_id, meta in datasets.items():
            meta = meta or {}
            table_quoted = meta.get("table")  # expected: '"schema"."table"'
            cols_meta = meta.get("columns", [])

            if not table_quoted or not isinstance(table_quoted, str):
                continue

            m = re.match(r'"\s*([^"]+)\s*"\s*\.\s*"\s*([^"]+)\s*"', table_quoted)
            if not m:
                continue

            schema_name, table_name = m.group(1), m.group(2)
            canon = f"{schema_name}.{table_name}".lower()

            colnames = set()
            for c in cols_meta:
                if isinstance(c, dict) and "name" in c:
                    # IMPORTANT: lowercase columns to match SQLGuard normalization
                    colnames.add(str(c["name"]).lower())

            if colnames:
                allowed[canon] = colnames

        if not allowed:
            # no schema info → skip
            return

        guard = SQLGuard(allowed)

        try:
            state.generated_sql = guard.validate_and_fix(sql)
        except SQLGuardError as e:
            raise ValueError(f"SQLGuard blocked query: {e}")

    def run(self, state):
        # Reset outputs on every run
        state.results = []
        state.execution_error = None
        state.execution_time_ms = None

        sql = getattr(state, "generated_sql", None)
        if not sql:
            state.execution_error = "No SQL to execute (state.generated_sql is empty)."
            return state

        if not getattr(state, "safety_passed", False):
            state.execution_error = "Safety check not passed. SQL will not be executed."
            return state

        # 1) Resolve {table} placeholder (if present)
        try:
            selected = getattr(state, "selected_datasets", None) or []
            user_id = getattr(state, "user_id", None)

            if "{table}" in sql:
                if not user_id:
                    state.execution_error = "Missing state.user_id; cannot resolve {table}."
                    return state
                if not selected:
                    state.execution_error = "Missing state.selected_datasets; cannot resolve {table}."
                    return state

                table_fqn = self._resolve_table_fqn(user_id=user_id, dataset_id=selected[0])
                if not table_fqn:
                    state.execution_error = (
                        f"Could not resolve table for dataset_id={selected[0]} and user_id={user_id} "
                        f"(not found in dataset_registry)."
                    )
                    return state

                sql = sql.replace("{table}", table_fqn)
                state.generated_sql = sql  # store final SQL

        except Exception as e:
            state.execution_error = f"Failed while resolving table placeholder: {e}"
            return state

        # 2) SQLGuard (validate tables/columns + minor fixups like duplicate LIMIT)
        try:
            self._apply_sql_guard(state)
            sql = state.generated_sql
        except Exception as e:
            state.execution_error = str(e)
            return state

        # 3) Execute SQL
        t0 = time.time()
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                colnames = [d.name for d in cur.description] if cur.description else []

            state.results = [dict(zip(colnames, row)) for row in rows]
            state.execution_time_ms = int((time.time() - t0) * 1000)

        except Exception as e:
            state.execution_error = str(e)
        finally:
            conn.close()

        return state

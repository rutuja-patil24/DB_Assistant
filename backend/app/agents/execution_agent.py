# app/agents/execution_agent.py

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from app.db import get_conn


class ExecutionAgent:
    """
    Executes SQL on Postgres and stores results back into AgentState.

    Supports a special placeholder in SQL:
      - "{table}" will be replaced with the real fully-qualified table name
        for the FIRST selected dataset (looked up from dataset_registry).

    Expected AgentState fields (best-effort; code uses getattr for safety):
      - user_id: str
      - selected_datasets: list[str]
      - generated_sql: str | None
      - safety_passed: bool
      - results: list[dict] (output)
      - execution_error: str | None (output)
      - execution_time_ms: int | None (output)

    Notes:
      - This agent assumes SafetyAgent already enforced SELECT-only.
      - If the query returns no rows, results will be [] with no error.
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

        # If user used {table} placeholder, replace it using dataset_registry
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
                state.generated_sql = sql  # keep the final SQL on state for debugging

        except Exception as e:
            state.execution_error = f"Failed while resolving table placeholder: {e}"
            return state

        # Execute SQL
        t0 = time.time()
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)

                # For SELECT, fetch all
                rows = cur.fetchall()
                colnames = [d.name for d in cur.description] if cur.description else []

            # Convert to list[dict] for API response
            state.results = [dict(zip(colnames, row)) for row in rows]
            state.execution_time_ms = int((time.time() - t0) * 1000)

        except Exception as e:
            state.execution_error = str(e)
        finally:
            conn.close()

        return state

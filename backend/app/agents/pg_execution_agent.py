# backend/app/agents/pg_execution_agent.py
from __future__ import annotations

import logging
import re
import time

import psycopg2
import psycopg2.extras
from fastapi import HTTPException

from app.state.agent_state import AgentState

logger = logging.getLogger("db_assistant.pg_execution_agent")


def _get_conn(pg_uri: str):
    try:
        return psycopg2.connect(
            pg_uri, connect_timeout=8,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    except Exception as exc:
        raise HTTPException(503, detail=f"Cannot connect to PostgreSQL: {exc}")


class PgExecutionAgent:
    """
    Agent 4 (PostgreSQL) — SQL Execution.

    Reads from:  state.pg_uri, state.generated_sql, state.safety_passed
    Writes to:   state.results, state.columns, state.tables_used,
                 state.execution_time_ms, state.execution_error
    """

    def run(self, state: AgentState) -> AgentState:
        state.results = []
        state.columns = []
        state.execution_error = None
        state.execution_time_ms = None

        if not state.safety_passed:
            state.execution_error = "ExecutionAgent: SQL did not pass safety check — not executing."
            return state

        sql = state.generated_sql
        if not sql:
            state.execution_error = "ExecutionAgent: No SQL to execute."
            return state

        if not state.pg_uri:
            state.execution_error = "ExecutionAgent: pg_uri is missing."
            return state

        conn = _get_conn(state.pg_uri)
        try:
            t0 = time.time()
            with conn.cursor() as cur:
                cur.execute(sql)
                raw = cur.fetchall()
                cols = [d.name for d in cur.description] if cur.description else []

            state.results = [dict(r) for r in raw]
            state.columns = cols
            state.execution_time_ms = int((time.time() - t0) * 1000)

            # Detect which tables were actually used
            state.tables_used = [
                fqn for fqn in state.tables_schema
                if fqn.split(".")[-1].lower() in sql.lower()
            ]

            logger.info(
                "PgExecutionAgent: %d rows in %dms, tables: %s",
                len(raw), state.execution_time_ms, state.tables_used
            )

        except Exception as e:
            logger.error("PgExecutionAgent SQL failed:\n%s\n%s", sql, str(e))
            state.execution_error = f"Query execution failed: {e}\nSQL was: {sql[:400]}"
        finally:
            conn.close()

        return state
# backend/app/agents/pg_safety_agent.py
from __future__ import annotations

import logging
import re
from app.state.agent_state import AgentState

logger = logging.getLogger("db_assistant.pg_safety_agent")

BANNED_KEYWORDS = [
    "delete", "update", "drop", "alter", "truncate",
    "insert", "create", "grant", "revoke", "execute",
]


class PgSafetyAgent:
    """
    Agent 3 (PostgreSQL) — SQL Safety Validation.

    Reads from:  state.generated_sql
    Writes to:   state.safety_passed
                 state.execution_error  (if blocked)
    """

    def run(self, state: AgentState) -> AgentState:
        state.safety_passed = False
        sql = (state.generated_sql or "").strip().lower()

        if not sql:
            state.execution_error = "SafetyAgent: No SQL to validate."
            return state

        if not sql.startswith("select"):
            state.execution_error = f"SafetyAgent: Query must start with SELECT. Got: {sql[:60]}"
            return state

        for keyword in BANNED_KEYWORDS:
            if re.search(rf"\b{keyword}\b", sql):
                state.execution_error = f"SafetyAgent: Blocked keyword '{keyword}' found in SQL."
                return state

        # Block system schema access
        for blocked_schema in ["information_schema", "pg_catalog", "pg_toast"]:
            if blocked_schema in sql:
                state.execution_error = f"SafetyAgent: Access to '{blocked_schema}' is not allowed."
                return state

        state.safety_passed = True
        logger.info("PgSafetyAgent: SQL passed safety check")
        return state
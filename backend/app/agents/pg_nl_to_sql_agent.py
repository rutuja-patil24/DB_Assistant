# backend/app/agents/pg_nl_to_sql_agent.py
from __future__ import annotations

import logging
from app.services.nl_to_sql import generate_sql
from app.state.agent_state import AgentState

logger = logging.getLogger("db_assistant.pg_nl_to_sql_agent")


def _build_schema_prompt(state: AgentState) -> str:
    prompt = "You have access to the following PostgreSQL tables:\n\n"
    for fqn, cols in state.tables_schema.items():
        prompt += f"Table: {fqn}\nColumns:\n"
        for c in cols:
            prompt += f"  - {c['name']} ({c['pg_type']})\n"
        prompt += "\n"
    return prompt


def _build_context_block(state: AgentState) -> str:
    blocks = []

    # JOIN hints
    if state.join_hints:
        blocks.append("JOIN keys (use these when joining tables):\n" +
                      "\n".join(state.join_hints))

    # Enum values — CRITICAL for correct WHERE filters
    if state.enum_values:
        lines = [f"  - {key}: {', '.join(vals)}"
                 for key, vals in state.enum_values.items()]
        blocks.append(
            "CRITICAL — actual data values "
            "(use ONLY these exact strings in WHERE filters, never invent others):\n" +
            "\n".join(lines)
        )

    blocks.append(
        "Rules:\n"
        "- Choose ONLY the tables needed to answer the question\n"
        "- Write JOIN queries when data spans multiple tables\n"
        "- Always use fully qualified table names (schema.table)\n"
        "- Use ONLY the exact values listed above for categorical WHERE filters\n"
        f"- Add LIMIT {state.limit} at the end\n"
        "- Return ONLY valid SQL, no explanation, no markdown"
    )

    return "\n\n".join(blocks)


class PgNLToSQLAgent:
    """
    Agent 2 (PostgreSQL) — Natural Language to SQL.

    Reads from:  state.tables_schema, state.enum_values,
                 state.join_hints, state.user_question, state.limit
    Writes to:   state.generated_sql
    """

    def run(self, state: AgentState) -> AgentState:
        if not state.user_question:
            state.execution_error = "PgNLToSQLAgent: user_question is missing."
            return state

        if not state.tables_schema:
            state.execution_error = "PgNLToSQLAgent: tables_schema is empty. Run PgSchemaAgent first."
            return state

        schema_prompt = _build_schema_prompt(state)
        context_block = _build_context_block(state)
        full_question = f"{state.user_question}\n\n{context_block}"

        try:
            sql = generate_sql(schema_prompt, full_question)
            sql = sql.strip().rstrip(";")

            # Ensure LIMIT is present
            if "limit" not in sql.lower():
                sql += f" LIMIT {state.limit}"

            state.generated_sql = sql
            logger.info("PgNLToSQLAgent: SQL generated (%d chars)", len(sql))

        except Exception as e:
            state.execution_error = f"SQL generation failed: {e}"

        return state
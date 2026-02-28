# backend/app/agents/pg_schema_agent.py
from __future__ import annotations

import logging
from typing import Dict, List

import psycopg2
import psycopg2.extras
from fastapi import HTTPException

from app.state.agent_state import AgentState

logger = logging.getLogger("db_assistant.pg_schema_agent")

# Columns whose distinct values we fetch and inject into the prompt
# so Gemini never guesses wrong filter values
ENUM_COLS = {
    "status", "tier", "region", "category", "subcategory",
    "payment_method", "method", "type", "country", "brand", "db_type",
    "payment_status", "order_status", "currency",
}


def _get_conn(pg_uri: str):
    try:
        return psycopg2.connect(
            pg_uri, connect_timeout=8,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    except Exception as exc:
        raise HTTPException(503, detail=f"Cannot connect to PostgreSQL: {exc}")


# Internal app tables that should never be exposed to Gemini
_INTERNAL_TABLES = {
    "users", "user_connections", "user_api_keys", "query_audit_log",
    "user_uploads", "dataset_registry", "dataset_columns",
}

def _fetch_all_tables(conn) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.table_schema, t.table_name
            FROM information_schema.tables t
            WHERE t.table_type='BASE TABLE'
              AND t.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
            ORDER BY t.table_schema, t.table_name
        """)
        return [
            dict(r) for r in cur.fetchall()
            if dict(r)["table_name"] not in _INTERNAL_TABLES
        ]


def _fetch_columns(conn, schema: str, table: str) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
        """, (schema, table))
        return [{"name": dict(r)["column_name"], "pg_type": dict(r)["data_type"]}
                for r in cur.fetchall()]


def _fetch_enum_values(conn, fqn: str, col_name: str) -> List[str]:
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT DISTINCT "{col_name}" FROM {fqn} '
                f'WHERE "{col_name}" IS NOT NULL LIMIT 20'
            )
            return [str(r[col_name]) for r in cur.fetchall() if r[col_name] is not None]
    except Exception:
        return []


class PgSchemaAgent:
    """
    Agent 1 (PostgreSQL) — Schema Discovery.

    Reads from:  state.pg_uri
    Writes to:   state.tables_schema     — {fqn: [{name, pg_type}]}
                 state.enum_values        — {"fqn.col": ["val1","val2"]}
                 state.join_hints         — ["t1 and t2 share: col1, col2"]
    """

    def run(self, state: AgentState) -> AgentState:
        if not state.pg_uri:
            state.execution_error = "PgSchemaAgent: pg_uri is missing in state."
            return state

        conn = _get_conn(state.pg_uri)
        try:
            # 1. Discover all tables
            all_tables = _fetch_all_tables(conn)
            if not all_tables:
                state.execution_error = "No tables found in this database."
                return state

            # 2. Fetch columns for each table
            tables_schema: Dict[str, List[Dict]] = {}
            for t in all_tables:
                fqn = f"{t['table_schema']}.{t['table_name']}"
                cols = _fetch_columns(conn, t["table_schema"], t["table_name"])
                if cols:
                    tables_schema[fqn] = cols
            state.tables_schema = tables_schema

            # 3. Fetch actual enum values for categorical columns
            enum_values: Dict[str, List[str]] = {}
            for fqn, cols in tables_schema.items():
                for c in cols:
                    if c["name"] in ENUM_COLS:
                        vals = _fetch_enum_values(conn, fqn, c["name"])
                        if vals:
                            enum_values[f"{fqn}.{c['name']}"] = vals
            state.enum_values = enum_values

            # 4. Detect JOIN hints from shared column names
            col_sets = {fqn: {c["name"] for c in cols}
                        for fqn, cols in tables_schema.items()}
            table_list = list(col_sets.keys())
            join_hints = []
            for i in range(len(table_list)):
                for j in range(i + 1, len(table_list)):
                    t1, t2 = table_list[i], table_list[j]
                    common = col_sets[t1] & col_sets[t2] - {""}
                    if common:
                        join_hints.append(
                            f"  - {t1} and {t2} share: {', '.join(sorted(common)[:5])}"
                        )
            state.join_hints = join_hints

            logger.info(
                "PgSchemaAgent: %d tables, %d enum columns, %d join hints",
                len(tables_schema), len(enum_values), len(join_hints)
            )

        finally:
            conn.close()

        return state
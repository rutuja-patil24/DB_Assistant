# backend/app/api/routes/pg_query.py
# Fully agentic — all NL queries go through the Orchestrator pipeline
from __future__ import annotations

import io
import logging
import re
import time
import traceback
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field

from app.agents.orchestrator import Orchestrator
from app.state.agent_state import AgentState
from app.api.routes.auth import get_current_user, get_connection_uri

logger = logging.getLogger("db_assistant.pg")
router = APIRouter(prefix="/pg", tags=["postgresql"])

# Single shared orchestrator instance
_orchestrator = Orchestrator()


# ─────────────────────────────────────────────────────────────
# Internal helpers (connection, upload, etc.)
# ─────────────────────────────────────────────────────────────
def _get_conn(pg_uri: str):
    try:
        return psycopg2.connect(
            pg_uri, connect_timeout=8,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    except Exception as exc:
        raise HTTPException(503, detail=f"Cannot connect to PostgreSQL: {exc}")


def _safe_col(name: str) -> str:
    name = re.sub(r"[^a-z0-9_]+", "_", name.strip().lower())
    name = re.sub(r"_+", "_", name).strip("_") or "col"
    return f"c_{name}" if name[0].isdigit() else name


def _infer_pg_type(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):           return "boolean"
    if pd.api.types.is_integer_dtype(s):        return "bigint"
    if pd.api.types.is_float_dtype(s):          return "double precision"
    if pd.api.types.is_datetime64_any_dtype(s): return "timestamp"
    return "text"


def _get_table_columns(conn, schema: str, table: str) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
        """, (schema, table))
        return [{"name": dict(r)["column_name"], "pg_type": dict(r)["data_type"]}
                for r in cur.fetchall()]


def _fetch_all_tables(pg_uri: str) -> dict:
    conn = _get_conn(pg_uri)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.table_schema, t.table_name,
                       COALESCE(s.n_live_tup, 0) AS approx_rows
                FROM information_schema.tables t
                LEFT JOIN pg_stat_user_tables s
                    ON s.schemaname=t.table_schema AND s.relname=t.table_name
                WHERE t.table_type='BASE TABLE'
                  AND t.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
                ORDER BY t.table_schema, t.table_name
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    all_tables = []
    schemas: Dict[str, List] = {}
    for r in rows:
        d = dict(r)
        fqn = f"{d['table_schema']}.{d['table_name']}"
        entry = {"schema": d["table_schema"], "table": d["table_name"],
                 "fqn": fqn, "approx_rows": d["approx_rows"]}
        schemas.setdefault(d["table_schema"], []).append(entry)
        all_tables.append(entry)
    return {"total": len(all_tables), "schemas": schemas, "tables": all_tables}


def _state_to_response(state: AgentState) -> Dict:
    """Convert AgentState to API response dict."""
    return {
        "source":            "postgresql_auto",
        "tables_used":       state.tables_used,
        "question":          state.user_question,
        "sql":               state.generated_sql,
        "count":             len(state.results),
        "columns":           state.columns,
        "data":              state.results,
        "execution_time_ms": state.execution_time_ms,
        "summary":           state.summary,
        "viz":               state.viz,
        "profile":           state.profile,
    }


# ─────────────────────────────────────────────────────────────
# Request models
# ─────────────────────────────────────────────────────────────
class PgPingRequest(BaseModel):
    pg_uri: str

class PgListTablesRequest(BaseModel):
    pg_uri: str
    schema: str = "public"

class PgAllTablesRequest(BaseModel):
    pg_uri: str

class PgAllTablesByIdRequest(BaseModel):
    connection_id: int

class PgMultiTableSchemaRequest(BaseModel):
    pg_uri: str
    tables: List[str]

class PgPreviewRequest(BaseModel):
    pg_uri: str
    table:  str
    limit:  int = Field(10, ge=1, le=100)

class PgNLQueryAutoRequest(BaseModel):
    pg_uri:    str
    question:  str
    limit:     int = Field(50, ge=1, le=500)

class PgDirectQueryRequest(BaseModel):
    pg_uri: str
    sql:    str


# ─────────────────────────────────────────────────────────────
# Utility endpoints (unchanged)
# ─────────────────────────────────────────────────────────────
@router.post("/ping")
def pg_ping(req: PgPingRequest):
    conn = _get_conn(req.pg_uri)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            row = dict(cur.fetchone())
        return {"status": "connected", "version": row.get("version", "")}
    finally:
        conn.close()


@router.post("/tables")
def list_tables(req: PgListTablesRequest):
    conn = _get_conn(req.pg_uri)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema=%s AND table_type='BASE TABLE'
                ORDER BY table_name
            """, (req.schema,))
            tables = [dict(r)["table_name"] for r in cur.fetchall()]
        return {"schema": req.schema, "tables": tables, "count": len(tables)}
    finally:
        conn.close()


@router.post("/all-tables")
def list_all_tables(req: PgAllTablesRequest):
    return _fetch_all_tables(req.pg_uri)


@router.post("/all-tables-by-id")
def list_all_tables_by_id(req: PgAllTablesByIdRequest,
                           user=Depends(get_current_user)):
    pg_uri = get_connection_uri(req.connection_id, user["user_id"])
    result = _fetch_all_tables(pg_uri)
    result["pg_uri"] = pg_uri
    return result


@router.post("/multi-schema")
def get_multi_table_schema(req: PgMultiTableSchemaRequest):
    conn = _get_conn(req.pg_uri)
    result = {}
    try:
        for fqn in req.tables:
            parts = fqn.replace('"', '').split(".")
            schema = parts[0] if len(parts) == 2 else "public"
            table  = parts[-1]
            result[fqn] = _get_table_columns(conn, schema, table)
        return {"schemas": result}
    finally:
        conn.close()


@router.post("/schema")
def get_table_schema(req: PgPreviewRequest):
    conn = _get_conn(req.pg_uri)
    try:
        parts = req.table.replace('"', '').split(".")
        schema = parts[0] if len(parts) == 2 else "public"
        table  = parts[-1]
        cols   = _get_table_columns(conn, schema, table)
        prompt = "\n".join([f"Table: {req.table}", "Columns:"] +
                           [f"  - {c['name']} ({c['pg_type']})" for c in cols])
        return {"table": req.table, "columns": cols, "schema_prompt": prompt}
    finally:
        conn.close()


@router.post("/preview")
def preview_table(req: PgPreviewRequest):
    conn = _get_conn(req.pg_uri)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM {req.table} LIMIT %s', (req.limit,))
            raw  = cur.fetchall()
            cols = [d.name for d in cur.description] if cur.description else []
        return {"table": req.table, "count": len(raw), "columns": cols,
                "data": [dict(r) for r in raw]}
    except Exception as e:
        raise HTTPException(500, detail=f"Preview failed: {e}")
    finally:
        conn.close()


@router.post("/upload")
async def upload_csv_to_pg(
    pg_uri:      str        = Form(...),
    schema_name: str        = Form("public"),
    table_name:  str        = Form(...),
    file:        UploadFile = File(...),
):
    ext = (file.filename or "").split(".")[-1].lower()
    if ext not in ("csv", "xlsx", "xls"):
        raise HTTPException(400, detail="Only CSV/XLSX/XLS.")
    raw = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(raw)) if ext == "csv" else pd.read_excel(io.BytesIO(raw))
    except Exception as e:
        raise HTTPException(400, detail=f"Parse failed: {e}")
    if df.empty:
        raise HTTPException(400, detail="File has no rows.")

    seen: set = set()
    new_cols: List[str] = []
    for c in df.columns:
        sc = _safe_col(str(c))
        if sc in seen:
            i = 2
            while f"{sc}_{i}" in seen: i += 1
            sc = f"{sc}_{i}"
        seen.add(sc)
        new_cols.append(sc)
    df.columns = new_cols

    col_defs  = [f'"{c}" {_infer_pg_type(df[c])}' for c in df.columns]
    cols_meta = [{"name": c, "pg_type": _infer_pg_type(df[c])} for c in df.columns]
    tbl_fqn   = f'"{schema_name}"."{table_name}"'

    conn = _get_conn(pg_uri)
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
            cur.execute(f'DROP TABLE IF EXISTS {tbl_fqn};')
            cur.execute(f'CREATE TABLE {tbl_fqn} ({", ".join(col_defs)});')
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            cur.copy_expert(
                f'COPY {tbl_fqn} ({",".join(f"{chr(34)}{c}{chr(34)}" for c in df.columns)}) '
                f'FROM STDIN WITH CSV HEADER', buf
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, detail=f"Upload failed: {e}")
    finally:
        conn.close()
    return {"status": "ok", "table": tbl_fqn, "row_count": len(df), "columns": cols_meta}


# ─────────────────────────────────────────────────────────────
# ✅ AGENTIC: NL Query — goes through full Orchestrator pipeline
# ─────────────────────────────────────────────────────────────
@router.post("/nl-query-auto")
def pg_nl_query_auto(req: PgNLQueryAutoRequest):
    """
    Fully agentic NL query:
    PgSchemaAgent → PgNLToSQLAgent → PgSafetyAgent → PgExecutionAgent
    → InsightAgent → VisualizationAgent
    """
    # Build initial state
    state = AgentState(
        source        = "postgresql",
        pg_uri        = req.pg_uri,
        user_question = req.question,
        limit         = req.limit,
    )

    # Run the full pipeline through the Orchestrator
    state = _orchestrator.run_pg_query(state)

    # Surface any error
    if state.execution_error:
        raise HTTPException(500, detail=state.execution_error)

    return _state_to_response(state)


# ─────────────────────────────────────────────────────────────
# ✅ AGENTIC: Multi-question — splits then runs pipeline per question
# ─────────────────────────────────────────────────────────────
@router.post("/nl-query-multi")
def pg_nl_query_multi(req: PgNLQueryAutoRequest):
    """
    Multi-question mode — splits compound questions and runs a
    separate Orchestrator pipeline for each one.
    """
    import re as _re

    SPLITTERS = [
        r"\band also\b", r"\balso show\b", r"\bas well as\b",
        r"\bfurthermore\b", r"\badditionally\b", r"\bplus\b",
        r"(?<=\?)\s+",  # split after question marks
    ]
    raw_q = req.question.strip()
    questions = [raw_q]
    for pat in SPLITTERS:
        parts = _re.split(pat, raw_q, flags=_re.IGNORECASE)
        if len(parts) > 1:
            questions = [p.strip().strip("?").strip() for p in parts if p.strip()]
            break
    # Also split on "and show|list|give|top|total|count|average|find"
    if len(questions) == 1 and " and " in raw_q.lower():
        parts = _re.split(
            r"\s+and\s+(?=show|list|give|what|which|how|top|total|count|average|find)",
            raw_q, flags=_re.IGNORECASE
        )
        if len(parts) > 1:
            questions = [p.strip() for p in parts if p.strip()]
    if len(questions) == 1 and "?" in raw_q:
        parts = [p.strip() for p in raw_q.split("?") if p.strip()]
        if len(parts) > 1:
            questions = parts

    results = []
    t0_total = time.time()

    for q in questions[:5]:
        state = AgentState(
            source        = "postgresql",
            pg_uri        = req.pg_uri,
            user_question = q,
            limit         = req.limit,
        )
        state = _orchestrator.run_pg_query(state)

        if state.execution_error:
            results.append({"question": q, "error": state.execution_error,
                             "count": 0, "data": []})
        else:
            results.append({
                "question":          q,
                "sql":               state.generated_sql,
                "tables_used":       state.tables_used,
                "count":             len(state.results),
                "columns":           state.columns,
                "data":              state.results,
                "execution_time_ms": state.execution_time_ms,
                "summary":           state.summary,
                "viz":               state.viz,
            })

    return {
        "source":        "postgresql_multi",
        "original":      req.question,
        "questions":     questions,
        "results":       results,
        "total_queries": len(results),
        "total_ms":      int((time.time() - t0_total) * 1000),
    }


# ─────────────────────────────────────────────────────────────
# Direct SQL (unchanged — bypasses agents intentionally)
# ─────────────────────────────────────────────────────────────
@router.post("/direct-query")
def pg_direct_query(req: PgDirectQueryRequest):
    sql = req.sql.strip()
    if not sql.lower().startswith("select"):
        raise HTTPException(422, detail="Only SELECT statements allowed.")
    conn = _get_conn(req.pg_uri)
    try:
        t0 = time.time()
        with conn.cursor() as cur:
            cur.execute(sql)
            raw  = cur.fetchall()
            cols = [d.name for d in cur.description] if cur.description else []
        ms = int((time.time() - t0) * 1000)
        results = [dict(r) for r in raw]

        # Run post-processing pipeline for EDA profile + insights
        from app.state.agent_state import AgentState
        post = AgentState(
            user_question = sql,
            results       = results,
            columns       = cols,
        )
        post = _orchestrator.run_post_processing(post)

        return {
            "sql":               sql,
            "count":             len(results),
            "columns":           cols,
            "data":              results,
            "execution_time_ms": ms,
            "summary":           post.summary,
            "viz":               post.viz,
            "profile":           post.profile,
            "eda_insights":      post.eda_insights,
        }
    except Exception as e:
        raise HTTPException(500, detail=f"Query failed: {e}")
    finally:
        conn.close()
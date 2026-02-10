from fastapi import FastAPI, HTTPException
from app.core.db import get_connection
from app.api.routes.customers import router as customers_router
from app.core.sql_guard import ensure_safe_select
from app.api.models import QueryRequest
from app.routes.datasets import router as datasets_router
from app.api.routes.query import router as query_router




app = FastAPI(title="Database Assistant", version="0.1.0")
app.include_router(customers_router)
app.include_router(datasets_router)
app.include_router(query_router)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db/ping")
def db_ping():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 AS ok;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        return {"db": "connected", "result": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/orders")
def get_orders():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT order_id, customer_id, amount, order_date FROM orders ORDER BY order_id;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"count": len(rows), "data": rows}

@app.get("/orders_with_customers")
def orders_with_customers():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            o.order_id,
            o.amount,
            o.order_date,
            c.customer_id,
            c.name,
            c.region
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        ORDER BY o.order_id;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"count": len(rows), "data": rows}

from pydantic import BaseModel

class SQLQueryRequest(BaseModel):
    sql: str



@app.get("/schema")
def get_schema():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows




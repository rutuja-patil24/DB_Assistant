from fastapi import APIRouter, Header
from app.db import get_conn

router = APIRouter(prefix="/history", tags=["history"])


@router.get("")
def get_history(limit: int = 20, x_user_id: str = Header(..., alias="X-User-Id")):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT question, sql, selected_datasets, row_count, execution_time_ms, created_at
                FROM query_history
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (x_user_id, limit),
            )
            rows = cur.fetchall()

        return {
            "count": len(rows),
            "data": [
                {
                    "question": r[0],
                    "sql": r[1],
                    "selected_datasets": r[2],
                    "row_count": r[3],
                    "execution_time_ms": r[4],
                    "created_at": str(r[5]),
                }
                for r in rows
            ],
        }
    finally:
        conn.close()

from __future__ import annotations
from typing import List, Optional
from app.db import get_conn


def log_query(
    user_id: str,
    workspace_id: str,
    question: str,
    sql: str,
    selected_datasets: List[str],
    row_count: int,
    execution_time_ms: Optional[int],
) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO query_history
                (user_id, workspace_id, question, sql, selected_datasets, row_count, execution_time_ms)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (user_id, workspace_id, question, sql, selected_datasets, row_count, execution_time_ms),
            )
        conn.commit()
    finally:
        conn.close()

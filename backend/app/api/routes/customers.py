from fastapi import APIRouter, HTTPException
from app.core.db import get_connection

router = APIRouter(prefix="/customers", tags=["customers"])


@router.get("")
def list_customers(limit: int = 50):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT customer_id, name, region
            FROM customers
            ORDER BY customer_id
            LIMIT %s;
            """,
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # psycopg2 RealDictCursor returns dict rows already
        return {"count": len(rows), "data": rows}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

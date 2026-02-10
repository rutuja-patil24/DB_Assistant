import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_connection():
    """
    Creates a new DB connection. (Simple version)
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
        connect_timeout=5,
    )

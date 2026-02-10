import os
import psycopg2


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5433")),
        dbname=os.getenv("DB_NAME", "da_db"),
        user=os.getenv("DB_USER", "da_user"),
        password=os.getenv("DB_PASSWORD", "da_pass"),
    )

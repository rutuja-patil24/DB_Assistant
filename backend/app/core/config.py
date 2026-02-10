import os

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5433"))   # <-- use 5433 now
DB_NAME = os.getenv("DB_NAME", "da_db")
DB_USER = os.getenv("DB_USER", "da_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "da_pass")

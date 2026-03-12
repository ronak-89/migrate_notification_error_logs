"""
PostgreSQL connection for notification_logs migration.
Standalone: uses DB_* env vars only.
"""
import os
from contextlib import contextmanager

import psycopg2

DB_CONNECTION_TIMEOUT = int(os.getenv("DB_CONNECTION_TIMEOUT", "30"))


@contextmanager
def get_db_connection():
    """Context manager: yield a PostgreSQL connection, close on exit."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", "5432"),
        connect_timeout=DB_CONNECTION_TIMEOUT,
    )
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass

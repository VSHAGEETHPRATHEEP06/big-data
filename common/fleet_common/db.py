"""Small PostgreSQL helpers (psycopg2) with start-up retries."""
from __future__ import annotations

import time
from contextlib import contextmanager

import psycopg2

from .settings import Settings, get_settings


def connect(settings: Settings | None = None, retries: int = 60, delay: float = 2.0, logger=None):
    """Open a connection, retrying while PostgreSQL is still starting."""
    settings = settings or get_settings()
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(settings.pg_dsn, connect_timeout=5, application_name="fleetpulse")
            return conn
        except psycopg2.OperationalError as exc:
            last_error = exc
            if logger:
                logger.warning("postgres_not_ready", extra={"attempt": attempt, "error": str(exc).strip()})
            time.sleep(delay)
    raise RuntimeError(f"PostgreSQL not reachable after {retries} attempts") from last_error


@contextmanager
def transaction(conn):
    """Yield a cursor; commit on success, roll back on any error."""
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

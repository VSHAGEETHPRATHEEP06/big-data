"""Thread-safe PostgreSQL access for the API (psycopg2 connection pool)."""
from __future__ import annotations

import threading
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool


class Database:
    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 6) -> None:
        self.dsn = dsn
        self.minconn = minconn
        self.maxconn = maxconn
        self._pool: ThreadedConnectionPool | None = None
        self._lock = threading.Lock()

    def _get_pool(self) -> ThreadedConnectionPool:
        # created lazily so the API can start before PostgreSQL is ready
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    self._pool = ThreadedConnectionPool(
                        self.minconn, self.maxconn, self.dsn,
                        application_name="fleet-api", connect_timeout=5,
                    )
        return self._pool

    def close(self) -> None:
        if self._pool is not None:
            self._pool.closeall()

    @contextmanager
    def cursor(self):
        pool = self._get_pool()
        conn = pool.getconn()
        broken = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET statement_timeout = 5000")
                yield cur
            conn.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            broken = True
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn, close=broken)

    def query(self, sql: str, params=None) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def one(self, sql: str, params=None) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params=None) -> int:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

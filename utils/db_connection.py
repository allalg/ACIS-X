"""
Database connection factory for ACIS-X.

Standard path: Postgres via DATABASE_URL / ACIS_DATABASE_URL (Supabase).
Fallback: SQLite via ACIS_DB_PATH when no DATABASE_URL is set (unit tests / offline).

Placeholders use ``?`` in application SQL; they are rewritten to ``%s`` for Postgres.
Prefer ``ON CONFLICT`` over ``INSERT OR IGNORE/REPLACE`` so the same SQL works on both.
"""

from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator, Optional

_PLACEHOLDER_RE = re.compile(r"\?")


def get_database_url() -> Optional[str]:
    return os.getenv("ACIS_DATABASE_URL") or os.getenv("DATABASE_URL") or None


def get_db_path() -> str:
    return os.getenv("ACIS_DB_PATH", "acis.db")


def db_dialect() -> str:
    url = get_database_url()
    if url and url.startswith(("postgres://", "postgresql://")):
        return "postgres"
    return "sqlite"


def is_postgres() -> bool:
    return db_dialect() == "postgres"


def adapt_sql(sql: str, dialect: Optional[str] = None) -> str:
    """Rewrite application SQL for the active dialect."""
    dialect = dialect or db_dialect()
    if dialect != "postgres":
        return sql
    return _PLACEHOLDER_RE.sub("%s", sql)


def _normalize_postgres_url(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


class CompatRow(dict):
    """Dict row that also supports integer index access like sqlite3.Row."""

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class _PgCursor:
    """sqlite3-cursor-like wrapper over a psycopg cursor."""

    def __init__(self, cursor: Any):
        self._cursor = cursor

    def execute(self, sql: str, params: Any = None):
        self._cursor.execute(adapt_sql(sql, "postgres"), params or ())
        return self

    def executemany(self, sql: str, seq_of_params):
        self._cursor.executemany(adapt_sql(sql, "postgres"), seq_of_params)
        return self

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            return CompatRow(row)
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        return [CompatRow(row) if isinstance(row, dict) else row for row in rows]

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description

    def close(self):
        self._cursor.close()


class PgConnection:
    """Minimal sqlite3.Connection-compatible wrapper for psycopg connections."""

    def __init__(self, conn: Any):
        self._conn = conn
        self.row_factory = None

    def execute(self, sql: str, params: Any = None):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def cursor(self):
        return _PgCursor(self._conn.cursor())

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    @property
    def in_transaction(self) -> bool:
        try:
            from psycopg import pq

            return self._conn.info.transaction_status != pq.TransactionStatus.IDLE
        except Exception:
            return False


def connect(db_path: Optional[str] = None):
    """Open a DB connection (Postgres if DATABASE_URL set, else SQLite)."""
    url = get_database_url()
    if url and url.startswith(("postgres://", "postgresql://")):
        import psycopg
        from psycopg.rows import dict_row

        conn = psycopg.connect(_normalize_postgres_url(url), row_factory=dict_row)
        return PgConnection(conn)

    path = db_path or get_db_path()
    if path.startswith("file:"):
        uri = path
    else:
        abs_path = os.path.abspath(path).replace("\\", "/")
        if not abs_path.startswith("/"):
            abs_path = "/" + abs_path
        uri = f"file:{abs_path}?nolock=1"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0, isolation_level="DEFERRED")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def connection(db_path: Optional[str] = None) -> Iterator[Any]:
    conn = connect(db_path)
    try:
        yield conn
        try:
            conn.commit()
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def row_to_dict(row: Any) -> Optional[dict]:
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return row

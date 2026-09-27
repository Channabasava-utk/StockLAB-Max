"""Database abstraction and schema migrations for StockLab MAX.

Supports SQLite for zero-config development and PostgreSQL for deployment.
The application uses DB-API style connections and parameterized SQL only.
"""
from __future__ import annotations
import os, sqlite3
from pathlib import Path
from dataclasses import dataclass

SCHEMA_VERSION = 4

@dataclass(frozen=True)
class DBInfo:
    backend: str
    configured: bool
    schema_version: int


def database_url() -> str:
    return os.getenv("STOCKLAB_DATABASE_URL", "").strip()


def is_postgres() -> bool:
    return database_url().startswith(("postgres://", "postgresql://"))


class _PGCompat:
    """Tiny DB-API compatibility wrapper: the app uses ? placeholders; psycopg uses %s."""
    def __init__(self, conn): self._conn=conn
    def execute(self, sql, params=()):
        sql=sql.replace("?", "%s")
        return self._conn.execute(sql, params)
    def commit(self): return self._conn.commit()
    def rollback(self): return self._conn.rollback()
    def close(self): return self._conn.close()


def connect(sqlite_path: str):
    url = database_url()
    if url.startswith(("postgres://", "postgresql://")):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("PostgreSQL is configured but psycopg is not installed. Install psycopg[binary].") from exc
        return _PGCompat(psycopg.connect(url))
    conn = sqlite3.connect(sqlite_path)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _exec(conn, sql: str):
    conn.execute(sql)


def migrate(sqlite_path: str):
    conn = connect(sqlite_path)
    try:
        statements = [
            """CREATE TABLE IF NOT EXISTS schema_meta(
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS experiments(
                id TEXT PRIMARY KEY, ts TEXT, kind TEXT, symbol TEXT, period TEXT,
                params TEXT, metrics TEXT, dataset_fingerprint TEXT, code_version TEXT,
                universe TEXT, tags TEXT, notes TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS theses(
                id TEXT PRIMARY KEY, ts TEXT, symbol TEXT, title TEXT,
                thesis TEXT, risks TEXT, catalysts TEXT, status TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS paper_orders(
                id TEXT PRIMARY KEY, ts TEXT, symbol TEXT, side TEXT,
                qty DOUBLE PRECISION, price DOUBLE PRECISION, note TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS data_snapshots(
                id TEXT PRIMARY KEY, symbol TEXT, provider TEXT, dataset_type TEXT,
                retrieved_at TEXT, effective_start TEXT, effective_end TEXT, available_at TEXT,
                fingerprint TEXT, row_count INTEGER, schema_version TEXT,
                point_in_time_ready INTEGER, metadata TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS fundamental_snapshots(
                id TEXT PRIMARY KEY, symbol TEXT, provider TEXT, retrieved_at TEXT,
                available_at TEXT, fingerprint TEXT, payload TEXT, point_in_time_ready INTEGER
            )""",
            """CREATE TABLE IF NOT EXISTS research_runs(
                id TEXT PRIMARY KEY, created_at TEXT, user_scope TEXT, hypothesis TEXT,
                symbol TEXT, period TEXT, dataset_fingerprint TEXT, code_fingerprint TEXT,
                status TEXT, result TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS users(
                id TEXT PRIMARY KEY, created_at TEXT, email TEXT UNIQUE,
                password_hash TEXT, role TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS sessions(
                token_hash TEXT PRIMARY KEY, user_id TEXT, created_at TEXT, expires_at TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS job_history(
                id TEXT PRIMARY KEY, created_at TEXT, finished_at TEXT, status TEXT,
                job_type TEXT, payload TEXT, result TEXT, error TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS audit_log(
                id TEXT PRIMARY KEY, ts TEXT, actor TEXT, event_type TEXT, details TEXT
            )""",
        ]
        for s in statements: _exec(conn, s)

        # Lightweight, idempotent migrations for databases created by v6.1/v6.2.
        if is_postgres():
            existing = {r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='experiments'"
            ).fetchall()}
        else:
            existing = {r[1] for r in conn.execute("PRAGMA table_info(experiments)").fetchall()}
        for name, typ in {
            "dataset_fingerprint":"TEXT", "code_version":"TEXT", "universe":"TEXT",
            "tags":"TEXT", "notes":"TEXT"
        }.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE experiments ADD COLUMN {name} {typ}")

        if is_postgres():
            job_existing = {r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='job_history'"
            ).fetchall()}
        else:
            job_existing = {r[1] for r in conn.execute("PRAGMA table_info(job_history)").fetchall()}
        for name, typ in {
            "finished_at":"TEXT", "payload":"TEXT", "result":"TEXT", "error":"TEXT"
        }.items():
            if name not in job_existing:
                conn.execute(f"ALTER TABLE job_history ADD COLUMN {name} {typ}")

        conn.execute(
            "INSERT INTO schema_meta(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value"
            if is_postgres() else
            "INSERT INTO schema_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("schema_version", str(SCHEMA_VERSION))
        )
        conn.commit()
    finally:
        conn.close()


def info(sqlite_path: str) -> DBInfo:
    backend = "postgresql" if is_postgres() else "sqlite"
    configured = bool(database_url())
    return DBInfo(backend, configured, SCHEMA_VERSION)

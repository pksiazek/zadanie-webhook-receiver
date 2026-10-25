import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

MIN_SQLITE_VERSION: tuple[int, int, int] = (3, 37, 0)

_WAL_ATTEMPTS = 5

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS webhook_events (
    id               INTEGER PRIMARY KEY,
    idempotency_key  TEXT    NOT NULL UNIQUE,
    source           TEXT    NOT NULL,
    event_type       TEXT    NOT NULL,
    payload_json     TEXT,
    status           TEXT    NOT NULL CHECK (status IN
                       ('pending','processing','retrying','completed','failed')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    run_at           INTEGER NOT NULL,
    last_error       TEXT,
    result_json      TEXT,
    traceparent      TEXT,
    created_at       INTEGER NOT NULL,
    finished_at      INTEGER
) STRICT;

CREATE INDEX IF NOT EXISTS ix_events_ready ON webhook_events (run_at, status)
    WHERE status IN ('pending', 'retrying');
CREATE INDEX IF NOT EXISTS ix_events_leased ON webhook_events (run_at)
    WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS ix_events_finished
    ON webhook_events (status, finished_at)
    WHERE finished_at IS NOT NULL;
"""


def check_sqlite_version(version: str = sqlite3.sqlite_version) -> None:
    parsed = tuple(int(part) for part in version.split("."))
    if parsed < MIN_SQLITE_VERSION:
        required = ".".join(str(p) for p in MIN_SQLITE_VERSION)
        raise RuntimeError(f"SQLite >= {required} required, found {version}")


def connect(path: Path | str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, autocommit=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=FULL")
    for _ in range(_WAL_ATTEMPTS):
        if conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal":
            break
    return conn


@contextmanager
def immediate(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def init_schema(conn: sqlite3.Connection) -> None:
    with immediate(conn):
        for statement in SCHEMA_SQL.split(";"):
            if statement.strip():
                conn.execute(statement)


class ConnectionPool:
    """Hands out one SQLite connection per thread."""

    def __init__(self, path: Path | str) -> None:
        self._path = path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._all: list[sqlite3.Connection] = []

    def get(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = connect(self._path)
            self._local.conn = conn
            with self._lock:
                self._all.append(conn)
        return conn

    def close_all(self) -> None:
        with self._lock:
            for conn in self._all:
                conn.close()
            self._all.clear()
        self._local = threading.local()

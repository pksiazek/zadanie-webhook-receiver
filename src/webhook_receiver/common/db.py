import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

MIN_SQLITE_VERSION: tuple[int, int, int] = (3, 37, 0)

_WAL_ATTEMPTS = 10

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


def _enable_wal(
    conn: sqlite3.Connection,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Switch to WAL, retrying on contention.

    busy_timeout does not cover the journal-mode switch, so concurrent first
    starts on a fresh file can see "database is locked"; retry those too.
    """
    last_exc: sqlite3.OperationalError | None = None
    for attempt in range(1, _WAL_ATTEMPTS + 1):
        try:
            if conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal":
                return
            last_exc = None
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            last_exc = exc
        if attempt < _WAL_ATTEMPTS:
            sleep(min(0.05 * attempt, 0.5))
    raise RuntimeError(
        f"could not enable WAL journal mode after {_WAL_ATTEMPTS} attempts"
    ) from last_exc


def connect(
    path: Path | str, *, check_same_thread: bool = True
) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        path, autocommit=True, check_same_thread=check_same_thread
    )
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA synchronous=FULL")
        _enable_wal(conn)
    except BaseException:
        conn.close()
        raise
    return conn


@contextmanager
def immediate(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass  # keep the original exception
        raise


@contextmanager
def read_txn(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Deferred transaction: a consistent read snapshot, no write lock."""
    conn.execute("BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass  # keep the original exception
        raise


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
            # Safe: the connection is handed out only to the thread that
            # created it (threading.local); the single cross-thread access
            # is close_all() at shutdown.
            conn = connect(self._path, check_same_thread=False)
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

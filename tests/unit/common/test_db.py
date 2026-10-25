import sqlite3
import threading

import pytest

from webhook_receiver.common.db import (
    ConnectionPool,
    _enable_wal,
    check_sqlite_version,
    connect,
    immediate,
    init_schema,
)
from webhook_receiver.common.models import Event, Status


@pytest.fixture
def open_db():
    """Open connections via connect() and close them after the test."""
    opened = []

    def opener(path, **kwargs):
        conn = connect(path, **kwargs)
        opened.append(conn)
        return conn

    yield opener
    for conn in opened:
        conn.close()


def _insert(conn, key="k", run_at=1):
    conn.execute(
        "INSERT INTO webhook_events (idempotency_key, source, event_type,"
        " status, run_at, created_at) VALUES (?, 's', 't', 'pending', ?, 1)",
        (key, run_at),
    )


def test_check_sqlite_version_rejects_old():
    with pytest.raises(RuntimeError, match="3.37.0"):
        check_sqlite_version("3.36.9")
    check_sqlite_version("3.46.1")


def test_connect_sets_pragmas(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
    assert conn.row_factory is sqlite3.Row
    assert not conn.in_transaction
    assert conn.autocommit is True


def test_connect_creates_parent_dir(tmp_path, open_db):
    open_db(tmp_path / "nested" / "dir" / "a.db")
    assert (tmp_path / "nested" / "dir" / "a.db").exists()


def test_immediate_commits(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    init_schema(conn)
    with immediate(conn) as c:
        _insert(c)
    assert not conn.in_transaction
    assert (
        conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0] == 1
    )


def test_immediate_rolls_back_on_error(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    init_schema(conn)
    with pytest.raises(ValueError, match="boom"):
        with immediate(conn) as c:
            _insert(c)
            raise ValueError("boom")
    assert not conn.in_transaction
    assert (
        conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0] == 0
    )


def test_schema_is_strict(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    init_schema(conn)
    with pytest.raises((sqlite3.IntegrityError, sqlite3.DataError)):
        _insert(conn, run_at="soon")


def test_schema_checks_status(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    init_schema(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO webhook_events (idempotency_key, source,"
            " event_type, status, run_at, created_at)"
            " VALUES ('k', 's', 't', 'bogus', 1, 1)"
        )


def test_init_schema_creates_indexes_and_is_idempotent(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    init_schema(conn)
    init_schema(conn)
    names = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        )
    }
    assert {"ix_events_ready", "ix_events_leased", "ix_events_finished"} <= (
        names
    )
    sql = {
        r["name"]: " ".join(r["sql"].split())
        for r in conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'index'"
            " AND name LIKE 'ix_events_%'"
        )
    }
    assert sql["ix_events_ready"].endswith(
        "WHERE status IN ('pending', 'retrying')"
    )
    assert sql["ix_events_leased"].endswith("WHERE status = 'processing'")
    assert sql["ix_events_finished"].endswith("WHERE finished_at IS NOT NULL")


def test_init_schema_concurrent(tmp_path, open_db):
    path = tmp_path / "a.db"
    barrier = threading.Barrier(4)
    errors = []

    def work():
        try:
            conn = connect(path)
            barrier.wait()
            init_schema(conn)
            conn.close()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    conn = open_db(path)
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE name = 'webhook_events'"
    ).fetchone()


def test_pool_returns_connection_per_thread(tmp_path):
    pool = ConnectionPool(tmp_path / "a.db")
    main = pool.get()
    assert pool.get() is main
    other = []
    t = threading.Thread(target=lambda: other.append(pool.get()))
    t.start()
    t.join()
    assert other[0] is not main
    pool.close_all()
    with pytest.raises(sqlite3.ProgrammingError):
        main.execute("SELECT 1")


def test_event_payload_purged():
    kwargs = dict(
        id=1, idempotency_key="k", source="s", event_type="t",
        status=Status.COMPLETED, attempts=1, run_at=1, last_error=None,
        result=None, traceparent=None, created_at=1,
    )  # fmt: skip
    assert Event(payload=None, finished_at=2, **kwargs).payload_purged
    assert not Event(payload={}, finished_at=2, **kwargs).payload_purged
    assert not Event(payload=None, finished_at=None, **kwargs).payload_purged


class _WalConn:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0

    def execute(self, sql):
        self.calls += 1
        return self

    def fetchone(self):
        return (self.results.pop(0),)


def test_enable_wal_retries_until_wal():
    conn = _WalConn(["delete", "delete", "wal"])
    sleeps = []
    _enable_wal(conn, sleeps.append)
    assert conn.calls == 3
    assert len(sleeps) == 2


def test_enable_wal_raises_after_last_attempt():
    conn = _WalConn(["delete"] * 5)
    sleeps = []
    with pytest.raises(RuntimeError, match="could not enable WAL"):
        _enable_wal(conn, sleeps.append)
    assert conn.calls == 5


def test_plain_connect_is_thread_bound(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    errors = []

    def use():
        try:
            conn.execute("SELECT 1")
        except sqlite3.ProgrammingError as exc:
            errors.append(exc)

    t = threading.Thread(target=use)
    t.start()
    t.join()
    assert len(errors) == 1


class _FailingCommitConn:
    def __init__(self):
        self.in_transaction = False
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)
        if sql == "BEGIN IMMEDIATE":
            self.in_transaction = True
        elif sql == "COMMIT":
            raise sqlite3.OperationalError("disk I/O error")
        elif sql == "ROLLBACK":
            self.in_transaction = False


def test_immediate_rolls_back_when_commit_fails():
    conn = _FailingCommitConn()
    with pytest.raises(sqlite3.OperationalError, match="disk I/O"):
        with immediate(conn):
            pass
    assert not conn.in_transaction
    assert conn.statements[-1] == "ROLLBACK"


def test_immediate_takes_write_lock_up_front(tmp_path, open_db):
    first = open_db(tmp_path / "a.db")
    init_schema(first)
    second = open_db(tmp_path / "a.db")
    second.execute("PRAGMA busy_timeout=50")
    with immediate(first):
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            second.execute("BEGIN IMMEDIATE")

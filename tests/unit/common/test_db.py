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
    read_txn,
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


def _join_all(threads):
    for t in threads:
        t.join(timeout=10)
    assert not any(t.is_alive() for t in threads)


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
    barrier = threading.Barrier(4, timeout=10)
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
    _join_all(threads)
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
    _join_all([t])
    assert other[0] is not main
    pool.close_all()
    with pytest.raises(sqlite3.ProgrammingError):
        main.execute("SELECT 1")


def test_pool_closes_connections_of_dead_threads(tmp_path):
    pool = ConnectionPool(tmp_path / "a.db")
    dead = []
    t = threading.Thread(target=lambda: dead.append(pool.get()))
    t.start()
    _join_all([t])
    main = pool.get()  # a new connection prunes the dead thread's one
    with pytest.raises(sqlite3.ProgrammingError):
        dead[0].execute("SELECT 1")
    assert list(pool._all) == [main]
    pool.close_all()


def test_pool_keeps_connections_of_live_threads(tmp_path):
    pool = ConnectionPool(tmp_path / "a.db")
    got, release = threading.Event(), threading.Event()
    live = []

    def hold():
        live.append(pool.get())
        got.set()
        release.wait(timeout=10)

    t = threading.Thread(target=hold)
    t.start()
    assert got.wait(timeout=10)
    try:
        main = pool.get()
        assert live[0].execute("SELECT 1").fetchone()[0] == 1
        assert list(pool._all) == [live[0], main]
    finally:
        release.set()
        _join_all([t])
    pool.close_all()


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
        self.results = list(results)  # str results or exceptions to raise
        self.calls = 0

    def execute(self, sql):
        self.calls += 1
        if isinstance(self.results[0], Exception):
            raise self.results.pop(0)
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
    conn = _WalConn(["delete"] * 10)
    sleeps = []
    with pytest.raises(RuntimeError, match="could not enable WAL"):
        _enable_wal(conn, sleeps.append)
    assert conn.calls == 10


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
    _join_all([t])
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


def test_enable_wal_retries_on_lock_errors():
    locked = sqlite3.OperationalError("database is locked")
    conn = _WalConn([locked, sqlite3.OperationalError("busy"), "wal"])
    sleeps = []
    _enable_wal(conn, sleeps.append)
    assert conn.calls == 3
    assert len(sleeps) == 2


def test_enable_wal_propagates_other_errors():
    conn = _WalConn([sqlite3.OperationalError("disk I/O error"), "wal"])
    with pytest.raises(sqlite3.OperationalError, match="disk I/O"):
        _enable_wal(conn, lambda _: None)
    assert conn.calls == 1


def test_enable_wal_chains_last_lock_error():
    conn = _WalConn([sqlite3.OperationalError("database is locked")] * 10)
    with pytest.raises(RuntimeError, match="could not enable WAL") as info:
        _enable_wal(conn, lambda _: None)
    assert isinstance(info.value.__cause__, sqlite3.OperationalError)


def test_concurrent_connect_on_fresh_files(tmp_path):
    errors = []

    def work(worker):
        for i in range(30):
            try:
                connect(tmp_path / f"f{i}.db").close()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    _join_all(threads)
    assert errors == []


def test_read_txn_commits_and_closes_transaction(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    init_schema(conn)
    with read_txn(conn) as c:
        assert c is conn
        assert conn.in_transaction
        c.execute("SELECT COUNT(*) FROM webhook_events").fetchone()
    assert not conn.in_transaction


def test_read_txn_is_deferred(tmp_path, open_db):
    first = open_db(tmp_path / "a.db")
    init_schema(first)
    second = open_db(tmp_path / "a.db")
    with read_txn(first):
        first.execute("SELECT COUNT(*) FROM webhook_events").fetchone()
        # A read snapshot must not block writers.
        with immediate(second):
            _insert(second)


def test_read_txn_propagates_error_and_rolls_back(tmp_path, open_db):
    conn = open_db(tmp_path / "a.db")
    init_schema(conn)
    with pytest.raises(ValueError, match="boom"):
        with read_txn(conn):
            raise ValueError("boom")
    assert not conn.in_transaction


class _ReadConn:
    """Fake connection that mimics SQLite's 'no transaction' errors."""

    def __init__(self, rollback_error=None):
        self.in_transaction = False
        self.statements = []
        self.rollback_error = rollback_error

    def execute(self, sql):
        self.statements.append(sql)
        if sql == "BEGIN":
            self.in_transaction = True
            return
        if not self.in_transaction:
            raise sqlite3.OperationalError(
                "cannot commit - no transaction is active"
            )
        if sql == "ROLLBACK" and self.rollback_error is not None:
            raise self.rollback_error
        self.in_transaction = False


def test_read_txn_does_not_mask_error_after_auto_rollback():
    conn = _ReadConn()
    original = sqlite3.DatabaseError("database disk image is malformed")
    with pytest.raises(sqlite3.DatabaseError) as info:
        with read_txn(conn):
            conn.in_transaction = False  # SQLite already rolled back
            raise original
    assert info.value is original
    assert conn.statements == ["BEGIN"]


def test_read_txn_swallows_rollback_error():
    conn = _ReadConn(rollback_error=sqlite3.OperationalError("io"))
    with pytest.raises(ValueError, match="boom"):
        with read_txn(conn):
            raise ValueError("boom")
    assert conn.statements == ["BEGIN", "ROLLBACK"]

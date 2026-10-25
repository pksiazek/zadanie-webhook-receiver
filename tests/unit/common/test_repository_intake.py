import sqlite3
import threading

import pytest

from tests.fakes import FakeClock
from webhook_receiver.common.db import ConnectionPool, init_schema
from webhook_receiver.common.models import Status
from webhook_receiver.common.repository import EventRepository


def test_insert_new_event(repo, clock):
    ev, created = repo.insert_or_get(
        "evt-001", "shop", "order.created", {"order_id": "ORD-2024-1234"}, None
    )
    assert created and ev.status is Status.PENDING
    assert ev.attempts == 0
    assert ev.run_at == ev.created_at == clock.now_ms()
    assert ev.payload == {"order_id": "ORD-2024-1234"}
    assert ev.finished_at is None and ev.last_error is None
    assert ev.result is None


def test_payload_stored_compact(repo, tmp_path):
    repo.insert_or_get("k", "s", "t", {"a": 1, "b": [1, 2]}, None)
    conn = sqlite3.connect(tmp_path / "t.db")
    try:
        (raw,) = conn.execute(
            "SELECT payload_json FROM webhook_events"
        ).fetchone()
    finally:
        conn.close()
    assert raw == '{"a":1,"b":[1,2]}'


def test_duplicate_returns_existing_regardless_of_payload(repo):
    first, _ = repo.insert_or_get("evt-1", "shop", "a", {"x": 1}, None)
    ev, created = repo.insert_or_get("evt-1", "other", "b", {"x": 2}, None)
    assert not created
    assert ev.id == first.id and ev.source == "shop"
    assert ev.event_type == "a" and ev.payload == {"x": 1}


def test_get_returns_stored_event(repo):
    created, _ = repo.insert_or_get("evt-1", "shop", "a", {"x": 1}, None)
    assert repo.get("evt-1") == created


def test_get_unknown_returns_none(repo):
    assert repo.get("nope") is None


def test_traceparent_persisted(repo):
    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    ev, _ = repo.insert_or_get("k", "s", "t", {}, tp)
    assert ev.traceparent == tp
    assert repo.get("k").traceparent == tp


def test_traceparent_none_persisted(repo):
    ev, _ = repo.insert_or_get("k", "s", "t", {}, None)
    assert ev.traceparent is None


def test_concurrent_insert_same_key(tmp_path):
    path = tmp_path / "t.db"
    setup = ConnectionPool(path)
    init_schema(setup.get())
    n = 8
    barrier = threading.Barrier(n, timeout=10)
    results: list[tuple[int, bool, str]] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def work(i: int) -> None:
        pool = ConnectionPool(path)
        try:
            repo = EventRepository(pool, FakeClock())
            barrier.wait()
            ev, created = repo.insert_or_get(
                "same", f"src-{i}", "t", {"i": i}, None
            )
            with lock:
                results.append((ev.id, created, ev.source))
        except BaseException as exc:
            with lock:
                errors.append(exc)
        finally:
            pool.close_all()

    threads = [threading.Thread(target=work, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    try:
        assert not any(t.is_alive() for t in threads)
        assert errors == []
        assert len(results) == n
        assert sum(1 for _, created, _ in results if created) == 1
        assert len({event_id for event_id, _, _ in results}) == 1
        (count,) = (
            setup.get()
            .execute("SELECT COUNT(*) FROM webhook_events")
            .fetchone()
        )
        assert count == 1
        (stored_source,) = (
            setup.get().execute("SELECT source FROM webhook_events").fetchone()
        )
        assert {source for _, _, source in results} == {stored_source}
    finally:
        setup.close_all()


@pytest.mark.parametrize(
    "status",
    [Status.PROCESSING, Status.RETRYING, Status.COMPLETED, Status.FAILED],
)
def test_row_mapping_for_all_statuses(repo, tmp_path, status):
    repo.insert_or_get("k", "s", "t", {"a": 1}, None)
    conn = sqlite3.connect(tmp_path / "t.db")
    try:
        conn.execute(
            "UPDATE webhook_events SET status = ?, result_json = ?,"
            " last_error = ?, finished_at = ?, attempts = ?",
            (status.value, '{"ok":true}', "boom", 1_700_000_005_000, 3),
        )
        conn.commit()
    finally:
        conn.close()
    ev = repo.get("k")
    assert ev.status is status
    assert ev.result == {"ok": True}
    assert ev.last_error == "boom"
    assert ev.finished_at == 1_700_000_005_000
    assert ev.attempts == 3
    assert ev.payload == {"a": 1}

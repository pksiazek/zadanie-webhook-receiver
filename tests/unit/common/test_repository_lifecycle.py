import threading

import pytest

from webhook_receiver.common.db import ConnectionPool, init_schema
from webhook_receiver.common.models import RecoveryResult, Status
from webhook_receiver.common.repository import (
    CLAIM_SQL,
    RECOVER_FAIL_SQL,
    RECOVER_RETRY_SQL,
    EventRepository,
)

LEASE = 30_000


def _add(repo, key, payload=None):
    ev, _ = repo.insert_or_get(key, "shop", "t", payload or {}, None)
    return ev


def test_claim_selects_by_run_at_and_increments_attempts(repo, clock):
    _add(repo, "old")
    (old,) = repo.claim(1, LEASE)
    # Older event, but retried later than the newer one is due.
    assert repo.record_failure(old.id, 1, "boom", clock.now_ms() + 1_000)
    clock.advance(10)
    _add(repo, "new")
    clock.advance(2_000)
    (first,) = repo.claim(1, LEASE)
    assert first.idempotency_key == "new"
    assert first.status is Status.PROCESSING
    assert first.attempts == 1
    assert first.run_at == clock.now_ms() + LEASE
    (second,) = repo.claim(1, LEASE)
    assert second.idempotency_key == "old" and second.attempts == 2


def test_claim_rejects_non_positive_arguments(repo):
    for limit, lease in ((0, LEASE), (-1, LEASE), (1, 0), (1, -5)):
        with pytest.raises(ValueError):
            repo.claim(limit, lease)


def test_claim_skips_not_yet_due_retrying(repo, clock):
    _add(repo, "a")
    (ev,) = repo.claim(1, LEASE)
    now = clock.now_ms()
    assert repo.record_failure(ev.id, 1, "boom", now + 5_000)
    assert repo.claim(10, LEASE) == []
    clock.advance(5_000)
    (again,) = repo.claim(10, LEASE)
    assert again.id == ev.id and again.attempts == 2


def test_two_claims_never_share_rows(tmp_path, clock):
    path = tmp_path / "q.db"
    setup = ConnectionPool(path)
    init_schema(setup.get())
    seed = EventRepository(setup, clock)
    for i in range(100):
        seed.insert_or_get(f"k{i}", "s", "t", {}, None)
    pools = [ConnectionPool(path), ConnectionPool(path)]
    barrier = threading.Barrier(2, timeout=10)
    results: list[list[int]] = [[], []]
    errors: list[BaseException] = []

    def run(i):
        try:
            r = EventRepository(pools[i], clock)
            barrier.wait()
            results[i] = [e.id for e in r.claim(50, LEASE)]
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
        assert not t.is_alive()
    try:
        assert not errors
        a, b = set(results[0]), set(results[1])
        assert not a & b
        assert len(a) + len(b) == 100
    finally:
        for p in (setup, *pools):
            p.close_all()


def test_complete_with_current_attempt(repo, clock):
    _add(repo, "a")
    (ev,) = repo.claim(1, LEASE)
    clock.advance(100)
    assert repo.complete(ev.id, 1, {"ok": True}) is True
    done = repo.get("a")
    assert done.status is Status.COMPLETED
    assert done.result == {"ok": True}
    assert done.finished_at == clock.now_ms()
    assert done.last_error is None


def test_complete_rejected_for_stale_attempt(repo, clock):
    _add(repo, "a")
    (first,) = repo.claim(1, LEASE)
    clock.advance(31_000)
    assert repo.recover_expired(5) == RecoveryResult(retried=1, failed=0)
    (second,) = repo.claim(1, LEASE)
    assert second.attempts == 2
    assert repo.complete(first.id, 1, {"late": True}) is False
    assert repo.get("a").status is Status.PROCESSING
    assert repo.complete(first.id, 2, {"ok": True}) is True


def test_record_failure_rejected_for_stale_attempt(repo, clock):
    _add(repo, "a")
    (first,) = repo.claim(1, LEASE)
    clock.advance(LEASE)
    repo.recover_expired(5)
    (second,) = repo.claim(1, LEASE)
    assert second.attempts == 2
    before = repo.get("a")
    assert repo.record_failure(first.id, 1, "late", None) is False
    assert repo.get("a") == before


def test_record_failure_schedules_retry(repo, clock):
    _add(repo, "a")
    (ev,) = repo.claim(1, LEASE)
    retry_at = clock.now_ms() + 2_000
    assert repo.record_failure(ev.id, 1, "boom", retry_at) is True
    got = repo.get("a")
    assert got.status is Status.RETRYING
    assert got.run_at == retry_at
    assert got.last_error == "boom"
    assert got.finished_at is None


def test_record_failure_without_retry_marks_failed(repo, clock):
    _add(repo, "a")
    (ev,) = repo.claim(1, LEASE)
    clock.advance(10)
    assert repo.record_failure(ev.id, 1, "boom", None) is True
    got = repo.get("a")
    assert got.status is Status.FAILED
    assert got.finished_at == clock.now_ms()
    assert got.last_error == "boom"


def test_recover_expired_moves_to_retrying_with_lease_expired(repo, clock):
    _add(repo, "a")
    repo.claim(1, LEASE)
    assert repo.recover_expired(5) == RecoveryResult(0, 0)  # lease still valid
    clock.advance(LEASE)
    assert repo.recover_expired(5) == RecoveryResult(retried=1, failed=0)
    got = repo.get("a")
    assert got.status is Status.RETRYING
    assert got.run_at == clock.now_ms()
    assert got.last_error == "lease expired"
    assert got.finished_at is None


def test_recover_expired_at_max_attempts_fails(repo, clock):
    _add(repo, "a")
    repo.claim(1, LEASE)
    clock.advance(LEASE)
    assert repo.recover_expired(1) == RecoveryResult(retried=0, failed=1)
    got = repo.get("a")
    assert got.status is Status.FAILED
    assert got.finished_at == clock.now_ms()
    assert got.last_error == "lease expired"


def test_recover_expired_mixed_batch(repo, clock):
    _add(repo, "low")
    (low,) = repo.claim(1, LEASE)
    repo.record_failure(low.id, 1, "x", clock.now_ms())
    (low,) = repo.claim(1, LEASE)  # attempts == 2
    _add(repo, "high")
    (high,) = repo.claim(1, LEASE)  # attempts == 1
    assert (low.attempts, high.attempts) == (2, 1)
    clock.advance(LEASE)
    assert repo.recover_expired(2) == RecoveryResult(retried=1, failed=1)
    assert repo.get("high").status is Status.RETRYING
    assert repo.get("low").status is Status.FAILED


def test_recover_expired_lease_boundary(repo, clock):
    _add(repo, "a")
    repo.claim(1, LEASE)
    clock.advance(LEASE - 1)
    assert repo.recover_expired(5) == RecoveryResult(0, 0)
    assert repo.get("a").status is Status.PROCESSING
    clock.advance(1)
    assert repo.recover_expired(5) == RecoveryResult(1, 0)


def test_queue_queries_use_partial_indexes(repo):
    conn = repo._pool.get()
    params = {"now": 0, "lease": 1, "limit": 1, "max": 5}
    cases = [
        (CLAIM_SQL, "ix_events_ready"),
        (RECOVER_RETRY_SQL, "ix_events_leased"),
        (RECOVER_FAIL_SQL, "ix_events_leased"),
    ]
    for sql, index in cases:
        plan = conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
        details = [row["detail"] for row in plan]
        assert not any(d.startswith("SCAN webhook_events") for d in details)
        assert any(index in d for d in details), details

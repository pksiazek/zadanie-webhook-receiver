import pytest

from webhook_receiver.common.models import (
    PurgeResult,
    QueueStats,
    RetentionPolicy,
    Status,
)
from webhook_receiver.common.repository import (
    OLDEST_LEASED_SQL,
    OLDEST_READY_SQL,
    PURGE_COMPLETED_SQL,
    PURGE_FAILED_SQL,
    PURGE_PAYLOAD_SQL,
    STATS_PROCESSING_SQL,
    STATS_READY_SQL,
)

LEASE = 30_000
DAY = 86_400_000
POLICY = RetentionPolicy(
    payload_ms=7 * DAY,
    completed_ms=30 * DAY,
    failed_ms=30 * DAY,
    batch_size=1000,
)


def _add(repo, key, payload=None):
    ev, _ = repo.insert_or_get(key, "shop", "t", payload or {"k": key}, None)
    return ev


def _completed(repo, key):
    _add(repo, key)
    (ev,) = repo.claim(1, LEASE)
    assert repo.complete(ev.id, ev.attempts, {"ok": key})
    return ev


def _failed(repo, key):
    _add(repo, key)
    (ev,) = repo.claim(1, LEASE)
    assert repo.record_failure(ev.id, ev.attempts, "boom", None)
    return ev


def test_queue_stats_empty(repo):
    stats = repo.queue_stats()
    assert stats == QueueStats(0, 0, 0, 0)
    assert stats.backlog == 0


def test_queue_stats_counts_by_status(repo, clock):
    _completed(repo, "done")
    _failed(repo, "dead")
    _add(repo, "r")
    (r,) = repo.claim(1, LEASE)
    assert repo.record_failure(r.id, 1, "x", clock.now_ms() + 1_000)
    _add(repo, "busy1")
    _add(repo, "busy2")
    assert len(repo.claim(2, LEASE)) == 2
    _add(repo, "p1")
    _add(repo, "p2")
    _add(repo, "p3")
    stats = repo.queue_stats()
    assert (stats.pending, stats.retrying, stats.processing) == (3, 1, 2)
    assert stats.backlog == 4


def test_oldest_due_age_counts_from_run_at(repo, clock):
    _add(repo, "a")
    clock.advance(500)
    _add(repo, "b")
    clock.advance(1_000)
    assert repo.queue_stats().oldest_due_age_ms == 1_500


def test_oldest_due_age_ignores_future_retries(repo, clock):
    _add(repo, "a")
    (ev,) = repo.claim(1, LEASE)
    assert repo.record_failure(ev.id, 1, "x", clock.now_ms() + 5_000)
    clock.advance(4_999)
    stats = repo.queue_stats()
    assert stats.retrying == 1
    assert stats.oldest_due_age_ms == 0
    clock.advance(1_001)  # retry due since 1 000 ms
    assert repo.queue_stats().oldest_due_age_ms == 1_000


def test_oldest_due_age_ignores_valid_leases(repo, clock):
    _add(repo, "a")
    repo.claim(1, LEASE)
    clock.advance(LEASE - 1)
    stats = repo.queue_stats()
    assert stats.processing == 1
    assert stats.oldest_due_age_ms == 0


def test_oldest_due_age_includes_expired_leases(repo, clock):
    _add(repo, "leased")
    repo.claim(1, LEASE)  # run_at = t0 + LEASE
    clock.advance(LEASE + 2_000)  # lease expired 2 s ago
    _add(repo, "fresh")  # due now, age 0
    clock.advance(1_000)
    stats = repo.queue_stats()
    assert (stats.pending, stats.processing) == (1, 1)
    assert stats.oldest_due_age_ms == 3_000


def test_oldest_due_age_takes_older_ready_over_expired_lease(repo, clock):
    _add(repo, "leased")
    repo.claim(1, LEASE)  # run_at = t0 + LEASE
    _add(repo, "ready")  # run_at = t0
    clock.advance(LEASE + 5)  # lease expired 5 ms ago
    assert repo.queue_stats().oldest_due_age_ms == LEASE + 5


def test_purge_clears_payload_after_7_days(repo, clock):
    _completed(repo, "a")
    clock.advance(7 * DAY)
    assert repo.purge(POLICY) == PurgeResult(0, 0, 0)
    assert repo.get("a").payload == {"k": "a"}
    clock.advance(1)
    assert repo.purge(POLICY) == PurgeResult(1, 0, 0)
    got = repo.get("a")
    assert got is not None
    assert got.status is Status.COMPLETED
    assert got.payload is None
    assert got.result is None
    assert got.payload_purged is True
    # Idempotent: already-cleared rows are not counted again.
    assert repo.purge(POLICY) == PurgeResult(0, 0, 0)


def test_purge_deletes_completed_after_30_days(repo, clock):
    _completed(repo, "a")
    clock.advance(30 * DAY)
    assert repo.purge(POLICY) == PurgeResult(1, 0, 0)
    assert repo.get("a") is not None
    clock.advance(1)
    assert repo.purge(POLICY) == PurgeResult(0, 1, 0)
    assert repo.get("a") is None


def test_purge_deletes_completed_in_single_run(repo, clock):
    _completed(repo, "a")
    clock.advance(30 * DAY + 1)
    # Payload step runs first, then the row goes in the same purge.
    assert repo.purge(POLICY) == PurgeResult(1, 1, 0)
    assert repo.get("a") is None


def test_purge_keeps_failed_payload_until_deleted(repo, clock):
    _failed(repo, "a")
    clock.advance(8 * DAY)
    assert repo.purge(POLICY) == PurgeResult(0, 0, 0)
    got = repo.get("a")
    assert got.payload == {"k": "a"}
    assert got.payload_purged is False
    clock.advance(22 * DAY)
    assert repo.purge(POLICY) == PurgeResult(0, 0, 0)
    assert repo.get("a") is not None
    clock.advance(1)
    assert repo.purge(POLICY) == PurgeResult(0, 0, 1)
    assert repo.get("a") is None


def test_purge_uses_separate_thresholds(repo, clock):
    policy = RetentionPolicy(
        payload_ms=10, completed_ms=100, failed_ms=50, batch_size=10
    )
    _completed(repo, "c")
    _failed(repo, "f")
    clock.advance(51)
    assert repo.purge(policy) == PurgeResult(1, 0, 1)
    assert repo.get("c") is not None
    assert repo.get("f") is None


def test_purge_rejects_non_positive_batch_size(repo):
    for size in (0, -1):
        policy = RetentionPolicy(DAY, DAY, DAY, batch_size=size)
        with pytest.raises(ValueError):
            repo.purge(policy)


def test_purge_never_touches_unfinished(repo, clock):
    _add(repo, "retrying")
    (r,) = repo.claim(1, LEASE)
    assert repo.record_failure(r.id, 1, "x", clock.now_ms() + 400 * DAY)
    _add(repo, "processing")
    repo.claim(1, LEASE)
    _add(repo, "pending")
    # Far beyond every threshold (the lease expired long ago too).
    clock.advance(365 * DAY)
    keys = ("pending", "retrying", "processing")
    before = {k: repo.get(k) for k in keys}
    assert {k: e.status for k, e in before.items()} == {
        "pending": Status.PENDING,
        "retrying": Status.RETRYING,
        "processing": Status.PROCESSING,
    }
    assert repo.purge(POLICY) == PurgeResult(0, 0, 0)
    after = {k: repo.get(k) for k in keys}
    assert after == before
    assert all(e.payload is not None for e in after.values())


def test_purge_works_in_batches(repo, clock):
    for i in range(5):
        _completed(repo, f"c{i}")
        _failed(repo, f"f{i}")
    policy = RetentionPolicy(
        payload_ms=DAY,
        completed_ms=30 * DAY,
        failed_ms=30 * DAY,
        batch_size=2,
    )
    statements: list[str] = []
    conn = repo._pool.get()
    conn.set_trace_callback(statements.append)
    try:
        clock.advance(DAY + 1)
        assert repo.purge(policy) == PurgeResult(5, 0, 0)
        # 3 changing batches (2 + 2 + 1) plus the final empty one, each
        # committed in its own transaction.
        payload_txns = [
            "BATCH" if "payload_json = NULL" in s else s
            for s in statements
            if s in ("BEGIN IMMEDIATE", "COMMIT") or "payload_json = NULL" in s
        ]
        assert payload_txns[:12] == ["BEGIN IMMEDIATE", "BATCH", "COMMIT"] * 4
        assert "BATCH" not in payload_txns[12:]
        clock.advance(30 * DAY)
        assert repo.purge(policy) == PurgeResult(0, 5, 5)
    finally:
        conn.set_trace_callback(None)
    for i in range(5):
        assert repo.get(f"c{i}") is None
        assert repo.get(f"f{i}") is None


PARTIAL_INDEXES = ("ix_events_ready", "ix_events_leased", "ix_events_finished")


def _is_full_scan(detail: str) -> bool:
    # COUNT(*) has no range to SEARCH on, so it walks the partial index:
    # "SCAN webhook_events USING COVERING INDEX ix_events_ready". That
    # touches only unfinished rows, so it is fine. Scanning the table or a
    # full index (e.g. the idempotency_key one) is not.
    if not detail.startswith("SCAN webhook_events"):
        return False
    return not any(f"INDEX {ix}" in detail for ix in PARTIAL_INDEXES)


def test_full_scan_detection():
    assert _is_full_scan("SCAN webhook_events")
    assert _is_full_scan(
        "SCAN webhook_events USING COVERING INDEX"
        " sqlite_autoindex_webhook_events_1"
    )
    assert not _is_full_scan(
        "SCAN webhook_events USING COVERING INDEX ix_events_leased"
    )
    assert not _is_full_scan(
        "SEARCH webhook_events USING INDEX ix_events_finished (status=?)"
    )


def test_stats_and_purge_queries_use_indexes(repo):
    conn = repo._pool.get()
    params = {"now": 0, "cutoff": 0, "limit": 1}
    cases = [
        (STATS_READY_SQL, "ix_events_ready"),
        (STATS_PROCESSING_SQL, "ix_events_leased"),
        (OLDEST_READY_SQL, "ix_events_ready"),
        (OLDEST_LEASED_SQL, "ix_events_leased"),
        (PURGE_PAYLOAD_SQL, "ix_events_finished"),
        (PURGE_COMPLETED_SQL, "ix_events_finished"),
        (PURGE_FAILED_SQL, "ix_events_finished"),
    ]
    for sql, index in cases:
        plan = conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
        details = [row["detail"] for row in plan]
        assert not any(_is_full_scan(d) for d in details), (sql, details)
        assert any(index in d for d in details), (sql, details)

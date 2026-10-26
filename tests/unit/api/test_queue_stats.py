import logging
import threading

from webhook_receiver.api.queue_stats import QueueStatsCache
from webhook_receiver.common.models import QueueStats

ZERO = QueueStats(pending=0, retrying=0, processing=0, oldest_due_age_ms=0)


def test_cache_returns_zeros_before_refresh(repo):
    cache = QueueStatsCache(repo, 1)
    assert cache.get() == ZERO
    assert cache.get().backlog == 0


def test_refresh_reads_repository(repo, monkeypatch):
    stats = QueueStats(
        pending=3, retrying=2, processing=1, oldest_due_age_ms=500
    )
    monkeypatch.setattr(repo, "queue_stats", lambda: stats)
    cache = QueueStatsCache(repo, 1)
    cache.refresh()
    assert cache.get() == stats
    assert cache.get().backlog == 5


def test_cache_get_does_not_query(repo, monkeypatch):
    cache = QueueStatsCache(repo, 1)
    cache.refresh()

    def boom():
        raise AssertionError("get() must not query the repository")

    monkeypatch.setattr(repo, "queue_stats", boom)
    assert cache.get() == ZERO


def test_refresh_failure_keeps_last_value_and_logs(repo, monkeypatch, caplog):
    good = QueueStats(pending=1, retrying=0, processing=0, oldest_due_age_ms=0)
    monkeypatch.setattr(repo, "queue_stats", lambda: good)
    cache = QueueStatsCache(repo, 1)
    cache.refresh()

    def boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(repo, "queue_stats", boom)
    with caplog.at_level(logging.WARNING):
        cache.refresh()
    assert cache.get() == good
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_background_thread_refreshes_survives_errors_and_stops(
    repo, monkeypatch
):
    calls = 0
    second_call = threading.Event()
    stats = QueueStats(
        pending=7, retrying=0, processing=0, oldest_due_age_ms=0
    )

    def flaky():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient")
        second_call.set()
        return stats

    monkeypatch.setattr(repo, "queue_stats", flaky)
    cache = QueueStatsCache(repo, 0.01)
    cache.start()
    try:
        assert second_call.wait(timeout=5)
    finally:
        cache.stop()
    assert cache.get() == stats
    thread = cache._thread
    assert thread is not None
    assert thread.daemon
    assert not thread.is_alive()


def test_stop_is_prompt_with_long_interval(repo):
    cache = QueueStatsCache(repo, 3600)
    cache.start()
    cache.stop()
    assert cache._thread is not None
    assert not cache._thread.is_alive()


def test_stop_without_start_is_noop(repo):
    QueueStatsCache(repo, 1).stop()

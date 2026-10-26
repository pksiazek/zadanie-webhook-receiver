import asyncio
import logging
import re
import sqlite3
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest

from tests.fakes import FakeClock
from webhook_receiver.common.clock import Clock, SystemClock
from webhook_receiver.common.config import Settings
from webhook_receiver.common.db import ConnectionPool, init_schema
from webhook_receiver.common.models import (
    Event,
    PurgeResult,
    RetentionPolicy,
    Status,
)
from webhook_receiver.common.repository import EventRepository
from webhook_receiver.common.telemetry import (
    WorkerMetrics,
    current_traceparent,
    get_tracer,
    metrics_payload,
)
from webhook_receiver.worker import app as worker_app
from webhook_receiver.worker.app import Worker, create_worker

DAY_MS = 86_400_000
TIMEOUT = 5.0

FAST = {
    "simulated_min_seconds": 0.01,
    "simulated_max_seconds": 0.02,
    "lease_seconds": 1,
    "backoff_base_seconds": 0.01,
    "backoff_max_seconds": 0.02,
    "poll_interval_seconds": 0.01,
    "worker_concurrency": 4,
}


def _metric(series: str) -> float:
    """Value of an exact series line (name plus labels), 0 if absent."""
    text = metrics_payload()[0].decode()
    pattern = re.compile(rf"^{re.escape(series)} (\S+)$", re.MULTILINE)
    match = pattern.search(text)
    return float(match.group(1)) if match else 0.0


def _attempts_total(outcome: str) -> float:
    return _metric(f'webhook_attempts_total{{outcome="{outcome}"}}')


class RecordingRepo(EventRepository):
    """Real repository that also records calls the tests care about."""

    def __init__(self, pool: ConnectionPool, clock: Clock) -> None:
        super().__init__(pool, clock)
        self.purges: list[RetentionPolicy] = []
        self.claim_errors = 0
        self.complete_delay = 0.0
        self.complete_threads: list[threading.Thread] = []

    def purge(self, policy: RetentionPolicy) -> PurgeResult:
        self.purges.append(policy)
        return super().purge(policy)

    def claim(self, limit: int, lease_ms: int) -> list[Event]:
        if self.claim_errors > 0:
            self.claim_errors -= 1
            raise sqlite3.OperationalError("database is locked")
        return super().claim(limit, lease_ms)

    def complete(self, event_id: int, attempt: int, result: dict) -> bool:
        self.complete_threads.append(threading.current_thread())
        time.sleep(self.complete_delay)
        return super().complete(event_id, attempt, result)


@dataclass
class WorkerEnv:
    repo: RecordingRepo
    settings: Callable[..., Settings]

    def make(self, clock: Clock | None = None, **overrides) -> Worker:
        return Worker(
            self.repo,
            self.settings(**overrides),
            clock or SystemClock(),
            WorkerMetrics(),
            worker_id="test-worker",
        )

    def insert(self, key: str, payload: dict | None = None, tp=None):
        event, _ = self.repo.insert_or_get(
            key, "shop", "order.created", payload or {}, tp
        )
        return event

    def get(self, key: str) -> Event:
        event = self.repo.get(key)
        assert event is not None
        return event


@pytest.fixture
def worker_env(tmp_path) -> Iterator[WorkerEnv]:
    pool = ConnectionPool(tmp_path / "w.db")
    init_schema(pool.get())

    def settings(**overrides) -> Settings:
        return Settings(db_path=tmp_path / "w.db", **{**FAST, **overrides})

    try:
        yield WorkerEnv(RecordingRepo(pool, SystemClock()), settings)
    finally:
        pool.close_all()


@asynccontextmanager
async def running(worker: Worker) -> AsyncIterator[asyncio.Task]:
    task = asyncio.create_task(worker.run())
    try:
        yield task
    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=TIMEOUT)


async def until(predicate: Callable[[], bool], timeout=TIMEOUT) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout=timeout)


def _status_is(env: WorkerEnv, key: str, status: Status):
    return lambda: env.get(key).status == status


async def test_processes_event_to_completed(worker_env):
    worker_env.insert("evt-1")
    before = _attempts_total("success")
    async with running(worker_env.make()):
        await until(_status_is(worker_env, "evt-1", Status.COMPLETED))
    event = worker_env.get("evt-1")
    assert event.attempts == 1
    assert event.result["processed_by"] == "test-worker"
    assert event.result["attempt"] == 1
    assert 10 <= event.result["duration_ms"] <= 20
    assert _attempts_total("success") == before + 1


async def test_fail_once_retries_then_completes(worker_env):
    worker_env.insert("evt-1", {"simulate": "fail_once"})
    retries = _attempts_total("retry")
    async with running(worker_env.make()):
        await until(_status_is(worker_env, "evt-1", Status.COMPLETED))
    event = worker_env.get("evt-1")
    assert event.attempts == 2
    assert event.result["attempt"] == 2
    assert event.last_error is None
    assert _attempts_total("retry") == retries + 1


async def test_fail_always_ends_failed_after_max_attempts(worker_env):
    worker_env.insert("evt-1", {"simulate": "fail_always"})
    failed = _attempts_total("failed")
    async with running(worker_env.make(max_attempts=3)):
        await until(_status_is(worker_env, "evt-1", Status.FAILED))
    event = worker_env.get("evt-1")
    assert event.attempts == 3
    assert "fail_always" in event.last_error
    assert event.finished_at is not None
    assert _attempts_total("failed") == failed + 1


async def test_refills_slot_immediately(worker_env):
    for i in range(3):
        worker_env.insert(f"evt-{i}")
    worker = worker_env.make(worker_concurrency=1, poll_interval_seconds=5)
    started = time.monotonic()
    async with running(worker):
        await until(
            lambda: all(
                worker_env.get(f"evt-{i}").status == Status.COMPLETED
                for i in range(3)
            ),
            timeout=1.0,
        )
    assert time.monotonic() - started < 1.0


async def test_recovers_expired_lease_from_dead_worker(worker_env):
    worker_env.insert("evt-1")
    # A worker that claimed the event and died: its 50 ms lease expires.
    (dead,) = worker_env.repo.claim(1, 50)
    assert dead.attempts == 1
    recoveries = _metric("webhook_lease_recoveries_total")
    async with running(worker_env.make()):
        await until(_status_is(worker_env, "evt-1", Status.COMPLETED))
    event = worker_env.get("evt-1")
    assert event.attempts == 2
    assert event.result["attempt"] == 2
    assert _metric("webhook_lease_recoveries_total") == recoveries + 1


@pytest.mark.parametrize("payload", [{}, {"simulate": "fail_always"}])
async def test_stale_completion_is_rejected_and_counted(
    worker_env, caplog, payload
):
    worker_env.insert("evt-1", payload)
    (stale,) = worker_env.repo.claim(1, 50)
    await asyncio.sleep(0.06)
    worker_env.repo.recover_expired(5)
    (current,) = worker_env.repo.claim(1, 60_000)
    assert current.attempts == 2
    before = _metric("webhook_stale_results_total")
    worker = worker_env.make()
    try:
        with caplog.at_level(logging.INFO):
            await asyncio.wait_for(worker.process(stale), timeout=TIMEOUT)
    finally:
        # process() outside run(): close the executor run() would close.
        await worker._close_executor()
    assert _metric("webhook_stale_results_total") == before + 1
    event = worker_env.get("evt-1")
    assert event.status == Status.PROCESSING
    assert event.attempts == 2
    assert event.result is None
    stale_logs = [
        r for r in caplog.records if getattr(r, "event", "") == "stale_result"
    ]
    assert len(stale_logs) == 1
    assert stale_logs[0].idempotency_key == "evt-1"


async def test_stop_finishes_in_flight(worker_env):
    worker_env.insert("evt-1")
    worker_env.insert("evt-2")
    worker = worker_env.make(
        worker_concurrency=1,
        simulated_min_seconds=0.3,
        simulated_max_seconds=0.3,
    )
    task = asyncio.create_task(worker.run())
    await until(lambda: worker_env.get("evt-1").status == Status.PROCESSING)
    await asyncio.sleep(0.05)
    worker.stop()
    await asyncio.wait_for(task, timeout=TIMEOUT)
    assert worker_env.get("evt-1").status == Status.COMPLETED
    # The slot freed during the grace period, but nothing new was claimed.
    second = worker_env.get("evt-2")
    assert second.status == Status.PENDING
    assert second.attempts == 0
    assert not worker.healthy


async def test_stop_cancels_tasks_after_grace(worker_env):
    worker_env.insert("evt-1")
    worker = worker_env.make(
        simulated_min_seconds=0.5,
        simulated_max_seconds=0.5,
        shutdown_grace_seconds=0.05,
    )
    task = asyncio.create_task(worker.run())
    await until(lambda: worker_env.get("evt-1").status == Status.PROCESSING)
    started = time.monotonic()
    worker.stop()
    await asyncio.wait_for(task, timeout=TIMEOUT)
    assert time.monotonic() - started < 0.4
    # Left for lease expiry and recovery by another worker.
    event = worker_env.get("evt-1")
    assert event.status == Status.PROCESSING
    assert event.attempts == 1


async def test_process_span_is_child_of_post_span(worker_env, span_exporter):
    with get_tracer().start_as_current_span("POST /webhooks") as post:
        tp = current_traceparent()
    payload = {"simulate": "fail_once", "email": "x@y.pl"}
    worker_env.insert("evt-1", payload, tp)
    async with running(worker_env.make()):
        await until(_status_is(worker_env, "evt-1", Status.COMPLETED))
    spans = [
        s
        for s in span_exporter.get_finished_spans()
        if s.name == "webhook.process"
    ]
    assert len(spans) == 2
    post_ctx = post.get_span_context()
    for span in spans:
        assert span.parent.span_id == post_ctx.span_id
        assert span.context.trace_id == post_ctx.trace_id
        assert span.attributes["idempotency_key"] == "evt-1"
        assert span.attributes["source"] == "shop"
        assert span.attributes["event_type"] == "order.created"
        assert span.attributes["worker.id"] == "test-worker"
        values = " ".join(str(v) for v in span.attributes.values())
        assert "x@y.pl" not in values
    first, second = sorted(spans, key=lambda s: s.attributes["attempt"])
    assert first.attributes["attempt"] == 1
    assert first.attributes["outcome"] == "retry"
    assert "fail_once" in first.attributes["error"]
    assert second.attributes["attempt"] == 2
    assert second.attributes["outcome"] == "success"


async def test_retention_runs_on_interval(worker_env):
    clock = FakeClock()
    worker = worker_env.make(clock=clock, retention_interval_seconds=600)
    purges = worker_env.repo.purges
    async with running(worker):
        await until(lambda: len(purges) == 1)
        clock.advance(599_999)
        await asyncio.sleep(0.05)
        assert len(purges) == 1
        clock.advance(1)
        await until(lambda: len(purges) == 2)


async def test_retention_policy_comes_from_settings(worker_env):
    worker = worker_env.make(
        retention_payload_days=1.5,
        retention_completed_days=2,
        retention_failed_days=3,
        retention_batch_size=7,
    )
    result = await worker.run_retention_once()
    assert result == PurgeResult(0, 0, 0)
    assert worker_env.repo.purges == [
        RetentionPolicy(
            payload_ms=int(1.5 * DAY_MS),
            completed_ms=2 * DAY_MS,
            failed_ms=3 * DAY_MS,
            batch_size=7,
        )
    ]


async def test_loop_survives_database_error(worker_env, caplog):
    worker_env.repo.claim_errors = 1
    worker_env.insert("evt-1")
    with caplog.at_level(logging.ERROR):
        async with running(worker_env.make()):
            await until(_status_is(worker_env, "evt-1", Status.COMPLETED))
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "database is locked" in str(errors[0].exc_info[1])


async def test_healthy_while_running(worker_env):
    worker = worker_env.make()
    assert not worker.healthy
    async with running(worker):
        await until(lambda: worker.healthy)
    assert not worker.healthy


def test_create_worker_uses_host_and_pid(worker_env):
    import os
    import socket

    worker = create_worker(worker_env.settings(), worker_env.repo)
    assert worker.worker_id == f"{socket.gethostname()}-{os.getpid()}"


async def test_unexpected_error_is_logged_and_event_recovered(
    worker_env, caplog, monkeypatch, span_exporter
):
    real_simulate = worker_app.simulate
    calls = 0

    async def flaky_simulate(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return await real_simulate(*args, **kwargs)

    monkeypatch.setattr(worker_app, "simulate", flaky_simulate)
    worker_env.insert("evt-1", {"email": "x@y.pl"})
    with caplog.at_level(logging.INFO):
        async with running(worker_env.make()):
            await until(_status_is(worker_env, "evt-1", Status.COMPLETED))
    assert worker_env.get("evt-1").attempts == 2
    crashed = [
        r for r in caplog.records if getattr(r, "event", "") == "attempt_error"
    ]
    assert len(crashed) == 1
    record = crashed[0]
    assert record.idempotency_key == "evt-1"
    assert record.attempt == 1
    assert record.source == "shop"
    assert "boom" in str(record.exc_info[1])
    assert not hasattr(record, "payload")
    first = next(
        s
        for s in span_exporter.get_finished_spans()
        if s.name == "webhook.process" and s.attributes["attempt"] == 1
    )
    assert first.attributes["outcome"] == "error"
    assert [e.name for e in first.events] == ["exception"]


async def test_no_db_thread_outlives_run(worker_env):
    # The attempt is cancelled after the grace period while its
    # complete() call is still running in a database thread.
    worker_env.repo.complete_delay = 0.3
    worker_env.insert("evt-1")
    worker = worker_env.make(shutdown_grace_seconds=0.05)
    task = asyncio.create_task(worker.run())
    await until(lambda: worker_env.repo.complete_threads != [])
    worker.stop()
    await asyncio.wait_for(task, timeout=TIMEOUT)
    (db_thread,) = worker_env.repo.complete_threads
    assert db_thread is not threading.main_thread()
    assert not db_thread.is_alive()

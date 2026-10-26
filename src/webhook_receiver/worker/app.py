"""Async worker: claims due events and processes them in parallel slots.

The ``worker`` package depends only on ``common`` (spec §16a). All
SQLite calls run on a thread pool owned by the worker, so the event loop
never blocks and no database thread outlives ``run()``; the connection
pool hands each thread its own connection.
"""

import asyncio
import logging
import os
import socket
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from random import Random
from typing import Any

from opentelemetry.trace import Status as SpanStatus
from opentelemetry.trace import StatusCode

from webhook_receiver.common.clock import Clock, SystemClock
from webhook_receiver.common.config import Settings
from webhook_receiver.common.models import Event, PurgeResult, RetentionPolicy
from webhook_receiver.common.repository import EventRepository
from webhook_receiver.common.telemetry import (
    WorkerMetrics,
    context_from_traceparent,
    get_tracer,
)
from webhook_receiver.worker.backoff import backoff_ms
from webhook_receiver.worker.simulation import SimulatedFailure, simulate

DAY_MS = 86_400_000
# The loop counts as healthy while its last turn is this many poll
# intervals old at most.
HEALTHY_TURNS = 10

_log = logging.getLogger("webhook_receiver.worker")

# A coroutine function: the poll timer is passed to asyncio.create_task.
Sleep = Callable[[float], Coroutine[Any, Any, None]]
# Database threads beyond one per slot: recovery/claim and retention.
EXTRA_DB_THREADS = 2


def _ms(seconds: float) -> int:
    return round(seconds * 1000)


class Worker:
    """Processing loop with ``worker_concurrency`` slots (spec §9)."""

    def __init__(
        self,
        repo: EventRepository,
        settings: Settings,
        clock: Clock,
        metrics: WorkerMetrics,
        *,
        worker_id: str,
        rng: Random | None = None,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.worker_id = worker_id
        self._repo = repo
        self._settings = settings
        self._clock = clock
        self._metrics = metrics
        self._rng = rng or Random()
        self._sleep = sleep
        self._stop = asyncio.Event()
        self._in_flight: set[asyncio.Task[None]] = set()
        self._retention: asyncio.Task[None] | None = None
        self._next_retention_ms: int | None = None
        self._running = False
        self._last_turn = 0.0
        self._executor: ThreadPoolExecutor | None = None
        self._policy = RetentionPolicy(
            payload_ms=int(settings.retention_payload_days * DAY_MS),
            completed_ms=int(settings.retention_completed_days * DAY_MS),
            failed_ms=int(settings.retention_failed_days * DAY_MS),
            batch_size=settings.retention_batch_size,
        )

    @property
    def healthy(self) -> bool:
        """Liveness only: the loop is running and turned recently.

        It says nothing about database health. Under SQLite lock
        contention one call may wait up to ``busy_timeout`` (5 s), so a
        single turn can exceed the ``10 x poll_interval`` window and
        briefly report unhealthy; the Docker healthcheck should allow a
        few retries before declaring the container unhealthy.
        """
        limit = HEALTHY_TURNS * self._settings.poll_interval_seconds
        return self._running and time.monotonic() - self._last_turn < limit

    def stop(self) -> None:
        """Stop claiming; ``run`` then drains in-flight work and returns."""
        self._stop.set()

    async def run(self) -> None:
        """Loop until ``stop()``, then shut down gracefully."""
        self._running = True
        stop_wait = asyncio.create_task(self._stop.wait())
        try:
            while not self._stop.is_set():
                self._last_turn = time.monotonic()
                claimed = await self._turn()
                free = self._settings.worker_concurrency - len(self._in_flight)
                if claimed > 0 and free > 0:
                    # Work may remain: claim again without waiting.
                    continue
                # Nothing claimable, or every slot busy: wake on a freed
                # slot, stop() or the poll timer, whichever comes first.
                # The timer also keeps recovery and the heartbeat going
                # while all slots are busy; it never delays a refill.
                timer = asyncio.create_task(
                    self._sleep(self._settings.poll_interval_seconds)
                )
                await asyncio.wait(
                    {*self._in_flight, stop_wait, timer},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                timer.cancel()
        finally:
            self._running = False
            stop_wait.cancel()
            try:
                await self._drain()
            finally:
                await self._close_executor()

    async def _db[T](self, fn: Callable[..., T], *args: Any) -> T:
        """Run a blocking repository call on the worker's own threads."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._settings.worker_concurrency
                + EXTRA_DB_THREADS,
                thread_name_prefix="worker-db",
            )
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn, *args)

    async def _close_executor(self) -> None:
        # A call cancelled after the grace period may still be running in
        # its thread; wait for it so the caller can safely close the
        # connection pool. The wait is bounded by that repository call,
        # which may be a multi-batch purge running through all remaining
        # batches of every tier; each batch is its own transaction, so a
        # SIGKILL during it is safe. Waiting runs off the loop so it
        # stays responsive meanwhile.
        executor, self._executor = self._executor, None
        if executor is not None:
            await asyncio.to_thread(executor.shutdown, wait=True)

    async def _turn(self) -> int:
        """Recover leases, claim into free slots; returns rows claimed."""
        try:
            await self._recover()
            self._maybe_start_retention()
            free = self._settings.worker_concurrency - len(self._in_flight)
            if free <= 0:
                return 0
            events = await self._db(
                self._repo.claim, free, _ms(self._settings.lease_seconds)
            )
        except Exception:
            # Transient database trouble must not kill the worker; leases
            # protect any work that was claimed meanwhile.
            _log.exception("worker turn failed", extra={"event": "error"})
            return 0
        for event in events:
            task = asyncio.create_task(self.process(event))
            self._in_flight.add(task)
            task.add_done_callback(self._task_done)
        return len(events)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        # process() handles its own errors; nothing to report here.
        self._in_flight.discard(task)

    async def _recover(self) -> None:
        result = await self._db(
            self._repo.recover_expired, self._settings.max_attempts
        )
        count = result.retried + result.failed
        if count:
            self._metrics.lease_recoveries(count)
            _log.warning(
                "recovered expired leases",
                extra={
                    "event": "lease_recovered",
                    "retried": result.retried,
                    "failed": result.failed,
                    "worker_id": self.worker_id,
                },
            )

    async def _drain(self) -> None:
        pending = set(self._in_flight)
        if self._retention is not None:
            pending.add(self._retention)
        if pending:
            _, pending = await asyncio.wait(
                pending, timeout=self._settings.shutdown_grace_seconds
            )
        for task in pending:
            task.cancel()
        # Cancelled attempts keep their lease; it expires and another
        # worker recovers the event (spec §9).
        await asyncio.gather(*pending, return_exceptions=True)
        self._in_flight.clear()
        self._retention = None
        if pending:
            _log.warning(
                "shutdown grace exceeded, attempts cancelled",
                extra={"event": "shutdown_cancelled", "count": len(pending)},
            )

    # --- processing -----------------------------------------------------

    async def process(self, event: Event) -> None:
        """Run one attempt of ``event`` and store its fenced outcome."""
        attempt = event.attempts
        attrs = {
            "idempotency_key": event.idempotency_key,
            "source": event.source,
            "attempt": attempt,
            "worker_id": self.worker_id,
        }
        with get_tracer().start_as_current_span(
            "webhook.process",
            context=context_from_traceparent(event.traceparent),
            attributes={
                "attempt": attempt,
                "idempotency_key": event.idempotency_key,
                "source": event.source,
                "event_type": event.event_type,
                "worker.id": self.worker_id,
            },
        ) as span:
            _log.info("event claimed", extra={**attrs, "event": "claimed"})
            started = time.monotonic()
            error: str | None = None
            outcome_data: dict = {}
            try:
                outcome_data = await simulate(
                    event.payload or {},
                    attempt,
                    self._settings.simulated_min_seconds,
                    self._settings.simulated_max_seconds,
                    self._rng,
                    self._sleep,
                )
            except SimulatedFailure as exc:
                error = str(exc)
            except Exception as exc:
                # Not a simulated outcome: leave the event in processing;
                # its lease expires and it is recovered and retried.
                span.record_exception(exc)
                span.set_attribute("outcome", "error")
                span.set_status(SpanStatus(StatusCode.ERROR, str(exc)))
                _log.exception(
                    "attempt failed unexpectedly",
                    extra={**attrs, "event": "attempt_error"},
                )
                return
            duration = time.monotonic() - started
            try:
                outcome, stored = await self._store(event, error, outcome_data)
            except Exception:
                # The lease expires and the event is recovered later.
                span.set_status(SpanStatus(StatusCode.ERROR, "store failed"))
                _log.exception(
                    "storing attempt result failed",
                    extra={**attrs, "event": "store_error"},
                )
                return
            span.set_attribute("outcome", outcome)
            if error is not None:
                span.set_attribute("error", error)
                span.set_status(SpanStatus(StatusCode.ERROR, error))
            if not stored:
                self._metrics.stale_result()
                _log.warning(
                    "stale attempt result rejected",
                    extra={**attrs, "event": "stale_result"},
                )
                return
            self._metrics.attempt(outcome)
            self._metrics.processing_duration(duration, outcome)
            _log.info(
                "attempt finished",
                extra={
                    **attrs,
                    "event": "attempt",
                    "outcome": outcome,
                    "error": error,
                    "duration_ms": _ms(duration),
                },
            )

    async def _store(
        self, event: Event, error: str | None, data: dict
    ) -> tuple[str, bool]:
        """Persist the attempt; returns (outcome, accepted by fencing)."""
        attempt = event.attempts
        if error is None:
            result = {
                "processed_by": self.worker_id,
                "duration_ms": data["duration_ms"],
                "attempt": attempt,
            }
            stored = await self._db(
                self._repo.complete, event.id, attempt, result
            )
            return "success", stored
        if attempt < self._settings.max_attempts:
            delay = backoff_ms(
                attempt,
                _ms(self._settings.backoff_base_seconds),
                _ms(self._settings.backoff_max_seconds),
                self._rng,
            )
            retry_at: int | None = self._clock.now_ms() + delay
            outcome = "retry"
        else:
            retry_at = None
            outcome = "failed"
        stored = await self._db(
            self._repo.record_failure, event.id, attempt, error, retry_at
        )
        return outcome, stored

    # --- retention ------------------------------------------------------

    def _maybe_start_retention(self) -> None:
        """Start a purge in the background when the interval elapsed.

        Runs at the first turn and then every
        ``retention_interval_seconds`` by the injected clock. A purge in
        progress is never doubled up and never blocks slot refills.
        """
        now = self._clock.now_ms()
        due = self._next_retention_ms
        if due is not None and now < due:
            return
        if self._retention is not None and not self._retention.done():
            return
        self._next_retention_ms = now + _ms(
            self._settings.retention_interval_seconds
        )
        self._retention = asyncio.create_task(self._retention_task())

    async def _retention_task(self) -> None:
        try:
            await self.run_retention_once()
        except Exception:
            _log.exception("retention failed", extra={"event": "retention"})

    async def run_retention_once(self) -> PurgeResult:
        result = await self._db(self._repo.purge, self._policy)
        _log.info(
            "retention finished",
            extra={
                "event": "retention",
                "payloads_cleared": result.payloads_cleared,
                "completed_deleted": result.completed_deleted,
                "failed_deleted": result.failed_deleted,
            },
        )
        return result


def create_worker(
    settings: Settings,
    repo: EventRepository,
    *,
    clock: Clock | None = None,
) -> Worker:
    """Build a worker; the id matches ``service.instance.id`` (§12)."""
    return Worker(
        repo,
        settings,
        clock or SystemClock(),
        WorkerMetrics(),
        worker_id=f"{socket.gethostname()}-{os.getpid()}",
    )

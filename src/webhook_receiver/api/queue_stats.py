"""Periodically refreshed, in-memory snapshot of queue statistics.

Request handlers and gauge callbacks read the snapshot via ``get()``;
only the background refresher touches the database (spec §10, §12).
"""

import logging
import threading

from webhook_receiver.common.models import QueueStats
from webhook_receiver.common.repository import EventRepository

_log = logging.getLogger("webhook_receiver.queue_stats")

_ZERO = QueueStats(pending=0, retrying=0, processing=0, oldest_due_age_ms=0)


class QueueStatsCache:
    """Snapshot of ``repo.queue_stats()`` refreshed by a daemon thread.

    The cache is one-shot: call ``start()`` once and ``stop()`` once.
    Restarting is not supported; a second ``start()`` raises
    ``RuntimeError`` (also after ``stop()``).
    """

    def __init__(self, repo: EventRepository, refresh_seconds: float) -> None:
        if refresh_seconds <= 0:
            raise ValueError("refresh_seconds must be > 0")
        self._repo = repo
        self._refresh_seconds = refresh_seconds
        self._stats = _ZERO
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def get(self) -> QueueStats:
        """Last refreshed value (zeros before the first refresh)."""
        # A single attribute read of an immutable object is atomic.
        return self._stats

    def refresh(self) -> None:
        """Run one ``queue_stats()`` query; keep the old value on error."""
        try:
            self._stats = self._repo.queue_stats()
        except Exception:
            _log.warning("queue stats refresh failed", exc_info=True)

    def start(self) -> None:
        """Refresh once synchronously, then keep refreshing in a thread.

        The synchronous refresh means backpressure sees the real backlog
        from the first request after startup, not the zero placeholder.
        """
        if self._thread is not None:
            raise RuntimeError("QueueStatsCache already started")
        self._stop.clear()
        self.refresh()
        self._thread = threading.Thread(
            target=self._run, name="queue-stats-cache", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.wait(self._refresh_seconds):
            self.refresh()

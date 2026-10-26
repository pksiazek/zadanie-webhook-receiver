"""Helpers for end-to-end tests: real SQLite, real worker, real HTTP app.

The API runs in-process over ``httpx.ASGITransport`` (``TestClient``
warns under ``-W error``); a separate worker runs as a real
``python -m webhook_receiver worker`` subprocess.
"""

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI

from webhook_receiver.common.config import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = {"X-Source": "e2e"}
# Upper bound for any single wait in these tests.
WAIT_SECONDS = 10.0
POLL_SECONDS = 0.05

# Fast simulation: the five gist events finish well under a second.
FAST = {
    "simulated_min_seconds": 0.05,
    "simulated_max_seconds": 0.1,
    "lease_seconds": 2,
    "poll_interval_seconds": 0.05,
    "backoff_base_seconds": 0.1,
    "backoff_max_seconds": 0.2,
    "queue_stats_refresh_seconds": 0.2,
}


def make_settings(db_path: Path, **overrides: Any) -> Settings:
    return Settings(db_path=db_path, **{**FAST, **overrides})


def settings_env(settings: Settings) -> dict[str, str]:
    """Settings as the env of a worker subprocess (minimal, explicit)."""
    env = {
        name: os.environ[name]
        for name in ("PATH", "HOME", "LANG", "TMPDIR", "VIRTUAL_ENV")
        if name in os.environ
    }
    fields = (
        "db_path",
        "simulated_min_seconds",
        "simulated_max_seconds",
        "lease_seconds",
        "poll_interval_seconds",
        "backoff_base_seconds",
        "backoff_max_seconds",
    )
    for field in fields:
        env[field.upper()] = str(getattr(settings, field))
    env.update(
        {
            "WORKER_METRICS_PORT": "0",
            "OTEL_TRACES_EXPORTER": "none",
            "LOG_LEVEL": "INFO",
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def worker_id(pid: int) -> str:
    """The id a worker with this pid writes as ``processed_by``."""
    return f"{socket.gethostname()}-{pid}"


@asynccontextmanager
async def serve(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """Run the app's lifespan and yield a client bound to it."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client


async def post(
    client: httpx.AsyncClient, webhook: dict, expected: int = 202
) -> dict:
    response = await client.post("/webhooks", json=webhook, headers=SRC)
    assert response.status_code == expected, response.text
    return response.json()


async def wait_for_status(
    client: httpx.AsyncClient,
    key: str,
    status: str,
    timeout: float = WAIT_SECONDS,
    diagnostics: Callable[[], str] | None = None,
) -> dict:
    """Poll ``GET /webhooks/{key}`` until ``status`` or fail on timeout."""
    deadline = time.monotonic() + timeout
    body: Any = None
    while True:
        response = await client.get(f"/webhooks/{key}")
        body = response.json()
        if response.status_code == 200 and body["status"] == status:
            return body
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"{key!r} did not reach {status!r} within {timeout}s;"
                f" last response {response.status_code}: {body}"
                f"{diagnostics() if diagnostics else ''}"
            )
        await asyncio.sleep(POLL_SECONDS)


class WorkerProcess:
    """A ``python -m webhook_receiver worker`` subprocess."""

    def __init__(self, settings: Settings, log_path: Path) -> None:
        self.log_path = log_path
        self._log = log_path.open("wb")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "webhook_receiver", "worker"],
            cwd=REPO_ROOT,
            env=settings_env(settings),
            stdin=subprocess.DEVNULL,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )

    @property
    def worker_id(self) -> str:
        return worker_id(self.proc.pid)

    def log_text(self) -> str:
        self._log.flush()
        return self.log_path.read_text(errors="replace")

    def diagnostics(self) -> str:
        """Log tail appended to assertion messages."""
        text = self.log_text()
        code = self.proc.poll()
        return f"\nworker exit code: {code}\nworker log:\n{text[-4000:]}"

    async def wait(self, timeout: float = WAIT_SECONDS) -> int:
        try:
            return await asyncio.to_thread(self.proc.wait, timeout)
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(
                f"worker did not exit within {timeout}s{self.diagnostics()}"
            ) from exc

    async def terminate(self) -> int:
        self.proc.send_signal(signal.SIGTERM)
        return await self.wait()

    async def kill(self) -> int:
        self.proc.kill()
        return await self.wait()

    def close(self) -> None:
        """Always reap the process and close its log (``finally``)."""
        try:
            if self.proc.poll() is None:
                self.proc.kill()
            self.proc.wait(timeout=WAIT_SECONDS)
        finally:
            self._log.close()


@contextmanager
def worker_process(
    settings: Settings, log_path: Path
) -> Iterator[WorkerProcess]:
    worker = WorkerProcess(settings, log_path)
    try:
        yield worker
    finally:
        worker.close()

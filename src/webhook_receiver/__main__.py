"""Entry point: ``python -m webhook_receiver api|worker`` (spec §16a).

The only module that composes the ``api`` and ``worker`` packages: in
``EMBEDDED_WORKER`` mode the API process runs a worker in its lifespan.
"""

import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import uvicorn

from webhook_receiver.api.app import create_app
from webhook_receiver.common.clock import SystemClock
from webhook_receiver.common.config import Settings
from webhook_receiver.common.db import (
    ConnectionPool,
    check_sqlite_version,
    init_schema,
)
from webhook_receiver.common.logging_setup import setup_logging
from webhook_receiver.common.repository import EventRepository
from webhook_receiver.common.telemetry import setup_telemetry
from webhook_receiver.worker.app import Worker, create_worker
from webhook_receiver.worker.http import start_worker_http_server

USAGE = "usage: python -m webhook_receiver {api|worker}"

_log = logging.getLogger("webhook_receiver.main")


def _report_crash(task: asyncio.Task[None]) -> None:
    """Log a crashed embedded worker at once, not only at shutdown."""
    if not task.cancelled() and task.exception() is not None:
        _log.error(
            "embedded worker crashed",
            exc_info=task.exception(),
            extra={"event": "embedded_worker_crashed"},
        )


def embedded_worker_lifespan(
    settings: Settings,
) -> Callable[[EventRepository], AbstractAsyncContextManager[None]]:
    """Run a worker in the API process for the lifetime of the app.

    Its metrics land in the same process registry, so the API's
    ``/metrics`` serves them; no auxiliary server is started.
    """

    @asynccontextmanager
    async def lifespan(repo: EventRepository) -> AsyncIterator[None]:
        worker = create_worker(settings, repo)
        task = asyncio.create_task(worker.run())
        task.add_done_callback(_report_crash)
        try:
            yield
        finally:
            worker.stop()
            # A crash was already logged by _report_crash; the API still
            # shuts down cleanly.
            with contextlib.suppress(Exception):
                await task

    return lifespan


def _run_api(settings: Settings) -> int:
    setup_logging(settings.log_level)
    setup_telemetry("webhook-api")
    extra = None
    if settings.embedded_worker:
        extra = embedded_worker_lifespan(settings)
    uvicorn.run(
        create_app(settings, extra_lifespan=extra),
        host="0.0.0.0",
        port=settings.api_port,
        workers=1,
        log_config=None,
    )
    return 0


async def _serve(worker: Worker) -> None:
    loop = asyncio.get_running_loop()
    signals = (signal.SIGTERM, signal.SIGINT)
    for sig in signals:
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)


def _run_worker(settings: Settings) -> int:
    setup_logging(settings.log_level)
    check_sqlite_version()
    pool = ConnectionPool(settings.db_path)
    try:
        init_schema(pool.get())
        setup_telemetry("webhook-worker")
        repo = EventRepository(pool, SystemClock())
        worker = create_worker(settings, repo)
        server = start_worker_http_server(settings.worker_metrics_port, worker)
        try:
            asyncio.run(_serve(worker))
        finally:
            server.shutdown()
            server.server_close()
    finally:
        pool.close_all()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    commands = {"api": _run_api, "worker": _run_worker}
    if len(args) != 1 or args[0] not in commands:
        print(USAGE, file=sys.stderr)
        return 2
    return commands[args[0]](Settings())


if __name__ == "__main__":
    sys.exit(main())

import asyncio
import logging
import os
import signal
import threading

import httpx
import pytest
from fastapi import FastAPI

from webhook_receiver import __main__ as cli
from webhook_receiver.worker.http import start_worker_http_server

FAST_ENV = {
    "SIMULATED_MIN_SECONDS": "0.01",
    "SIMULATED_MAX_SECONDS": "0.02",
    "LEASE_SECONDS": "1",
    "POLL_INTERVAL_SECONDS": "0.01",
    "QUEUE_STATS_REFRESH_SECONDS": "3600",
}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "m.db"))
    for name, value in FAST_ENV.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


@pytest.fixture
def uvicorn_calls(monkeypatch) -> list[tuple[tuple, dict]]:
    calls: list[tuple[tuple, dict]] = []
    monkeypatch.setattr(
        cli.uvicorn, "run", lambda *a, **kw: calls.append((a, kw))
    )
    return calls


@pytest.mark.parametrize("argv", [["nope"], [], ["api", "extra"]])
def test_main_unknown_command_returns_2(argv, capsys):
    assert cli.main(argv) == 2
    assert "usage" in capsys.readouterr().err


def test_main_api_runs_uvicorn(env, uvicorn_calls):
    assert cli.main(["api"]) == 0
    ((args, kwargs),) = uvicorn_calls
    assert isinstance(args[0], FastAPI)
    assert kwargs == {
        "host": "0.0.0.0",
        "port": 8000,
        "workers": 1,
        "log_config": None,
    }


async def test_main_api_embedded_worker_processes_events(env, uvicorn_calls):
    env.setenv("EMBEDDED_WORKER", "true")
    env.setenv("API_PORT", "8123")
    assert cli.main(["api"]) == 0
    ((args, kwargs),) = uvicorn_calls
    assert kwargs["port"] == 8123
    app = args[0]
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            r = await client.post(
                "/webhooks",
                json={
                    "idempotency_key": "evt-1",
                    "event_type": "t",
                    "payload": {},
                },
                headers={"X-Source": "shop"},
            )
            assert r.status_code == 202

            async def completed() -> None:
                while True:
                    body = (await client.get("/webhooks/evt-1")).json()
                    if body["status"] == "completed":
                        return
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(completed(), timeout=5)


def test_main_worker_stops_on_sigterm(env, monkeypatch):
    env.setenv("WORKER_METRICS_PORT", "0")
    servers = []

    def start(port, worker):
        server = start_worker_http_server(port, worker)
        servers.append((server, worker))
        return server

    monkeypatch.setattr(cli, "start_worker_http_server", start)
    errors: list[str] = []

    def send_sigterm_when_healthy() -> None:
        # The loop installs its signal handlers before run() starts, so
        # once the worker reports healthy SIGTERM reaches worker.stop().
        for _ in range(500):
            if servers and servers[0][1].healthy:
                os.kill(os.getpid(), signal.SIGTERM)
                return
            threading.Event().wait(0.01)
        errors.append("worker never became healthy")
        if servers:
            servers[0][1].stop()

    killer = threading.Thread(target=send_sigterm_when_healthy)
    killer.start()
    try:
        assert cli.main(["worker"]) == 0
    finally:
        killer.join(timeout=10)
    assert errors == []
    ((server, worker),) = servers
    assert not worker.healthy
    # The auxiliary server was shut down and its socket closed.
    assert server.socket.fileno() == -1


class CrashingWorker:
    def __init__(self) -> None:
        self.stopped = False

    async def run(self) -> None:
        raise RuntimeError("worker exploded")

    def stop(self) -> None:
        self.stopped = True


async def test_embedded_worker_crash_is_logged_immediately(
    env, monkeypatch, caplog
):
    worker = CrashingWorker()
    monkeypatch.setattr(cli, "create_worker", lambda settings, repo: worker)
    lifespan = cli.embedded_worker_lifespan(cli.Settings())

    def crash_logged() -> bool:
        return any(
            getattr(r, "event", "") == "embedded_worker_crashed"
            for r in caplog.records
        )

    with caplog.at_level(logging.ERROR):
        async with lifespan(object()):

            async def wait_for_log() -> None:
                while not crash_logged():
                    await asyncio.sleep(0.005)

            # Logged while the app is still running, not at shutdown.
            await asyncio.wait_for(wait_for_log(), timeout=5)
    (record,) = [
        r
        for r in caplog.records
        if getattr(r, "event", "") == "embedded_worker_crashed"
    ]
    assert "worker exploded" in str(record.exc_info[1])
    assert worker.stopped

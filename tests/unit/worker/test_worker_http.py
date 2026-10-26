import threading
import urllib.error
import urllib.request
from collections.abc import Iterator

import pytest

from webhook_receiver.worker.http import start_worker_http_server


class StubWorker:
    healthy = True


@pytest.fixture
def served() -> Iterator[tuple[StubWorker, str]]:
    worker = StubWorker()
    server = start_worker_http_server(0, worker)
    try:
        yield worker, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def _get(url: str) -> tuple[int, str, str]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = resp.read().decode()
            return resp.status, resp.headers["Content-Type"], body
    except urllib.error.HTTPError as exc:
        with exc:
            return exc.code, exc.headers["Content-Type"], exc.read().decode()


def test_worker_http_health_and_metrics(served):
    worker, base = served
    status, _, body = _get(base + "/health")
    assert (status, body) == (200, '{"status": "ok"}')
    worker.healthy = False
    status, _, body = _get(base + "/health")
    assert (status, body) == (503, '{"status": "unhealthy"}')
    status, content_type, body = _get(base + "/metrics")
    assert status == 200
    assert content_type.startswith("text/plain")
    assert "# TYPE" in body
    status, _, _ = _get(base + "/nope")
    assert status == 404


def test_worker_http_runs_in_daemon_thread(served):
    names = {t.name: t for t in threading.enumerate()}
    assert names["worker-http"].daemon

"""Auxiliary HTTP server of the worker process: /health and /metrics.

``prometheus_client.start_http_server`` serves only metrics, while the
Docker healthcheck needs ``/health`` too (spec §9), hence this small
server on a daemon thread.
"""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Protocol

from webhook_receiver.common.telemetry import metrics_payload


class _HasHealth(Protocol):
    @property
    def healthy(self) -> bool: ...


def start_worker_http_server(
    port: int, worker: _HasHealth
) -> ThreadingHTTPServer:
    """Serve on all interfaces; port 0 picks a free port (tests).

    The caller stops it with ``shutdown()`` and ``server_close()``.
    """

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (stdlib hook name)
            if self.path == "/health":
                if worker.healthy:
                    self._send(200, b'{"status": "ok"}', "application/json")
                else:
                    self._send(
                        503, b'{"status": "unhealthy"}', "application/json"
                    )
            elif self.path == "/metrics":
                self._send(200, *metrics_payload())
            else:
                self._send(404, b'{"detail": "not found"}', "application/json")

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            # Healthchecks and scrapes would flood stderr otherwise.
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    threading.Thread(
        target=server.serve_forever, name="worker-http", daemon=True
    ).start()
    return server

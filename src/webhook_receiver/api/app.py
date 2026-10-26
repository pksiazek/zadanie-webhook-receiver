"""FastAPI application: intake, status and operational endpoints (§6).

The ``api`` package depends only on ``common``. The embedded worker mode
is composed by ``__main__`` through ``extra_lifespan``.
"""

import logging
import sqlite3
from collections.abc import AsyncIterator, Callable
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
)
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

from webhook_receiver.api.body_limit import BodySizeLimitMiddleware
from webhook_receiver.api.queue_stats import QueueStatsCache
from webhook_receiver.api.ratelimit import RateLimiter
from webhook_receiver.api.schemas import AcceptedOut, StatusOut, WebhookIn
from webhook_receiver.common.clock import Clock, SystemClock
from webhook_receiver.common.config import Settings
from webhook_receiver.common.db import (
    ConnectionPool,
    check_sqlite_version,
    init_schema,
)
from webhook_receiver.common.logging_setup import setup_logging
from webhook_receiver.common.models import Event
from webhook_receiver.common.repository import EventRepository
from webhook_receiver.common.telemetry import (
    ApiMetrics,
    current_traceparent,
    metrics_payload,
    setup_telemetry,
)

SERVICE_NAME = "webhook-api"
MAX_SOURCE_LENGTH = 64
# Metric label for requests without a usable X-Source: a fixed value
# keeps cardinality bounded and never echoes a rejected header.
NO_SOURCE = ""
# The instrumentor joins these with "|" and re.search-es the result in
# "<scheme>://<Host header><path>" (no query string). Anchoring on the
# whole URL keeps keys such as "health-check" and client-chosen Host
# headers such as "metrics.example" traced (spec §12).
OTEL_EXCLUDED_URLS = ",".join(
    rf"^https?://[^/]*/{path}\Z" for path in ("health", "ready", "metrics")
)

_log = logging.getLogger("webhook_receiver.api")

ExtraLifespan = Callable[[EventRepository], AbstractAsyncContextManager[None]]


def status_url(key: str) -> str:
    return "/webhooks/" + quote(key, safe="")


def _readiness(pool: ConnectionPool) -> list[str]:
    """Names of failed readiness checks (empty when ready).

    If the database cannot be queried, the schema cannot be confirmed
    either, so both checks are reported as failed.
    """
    try:
        conn = pool.get()
        conn.execute("SELECT 1").fetchone()
        row = conn.execute(
            "SELECT 1 FROM sqlite_master"
            " WHERE type = 'table' AND name = 'webhook_events'"
        ).fetchone()
    except sqlite3.Error:
        return ["database", "schema"]
    return [] if row is not None else ["schema"]


def _detail(status_code: int, detail: Any, **headers: str) -> JSONResponse:
    return JSONResponse(
        {"detail": detail}, status_code=status_code, headers=headers or None
    )


def create_app(
    settings: Settings,
    *,
    clock: Clock | None = None,
    extra_lifespan: ExtraLifespan | None = None,
) -> FastAPI:
    """Build the API; storage is opened and closed by the lifespan."""
    clock = clock or SystemClock()
    setup_telemetry(SERVICE_NAME)
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        check_sqlite_version()
        pool = ConnectionPool(settings.db_path)
        async with AsyncExitStack() as stack:
            stack.callback(pool.close_all)
            init_schema(pool.get())
            repo = EventRepository(pool, clock)
            stats = QueueStatsCache(repo, settings.queue_stats_refresh_seconds)
            stats.start()
            stack.callback(stats.stop)
            app.state.pool = pool
            app.state.repo = repo
            app.state.stats = stats
            app.state.metrics = ApiMetrics(stats.get)
            if extra_lifespan is not None:
                await stack.enter_async_context(extra_lifespan(repo))
            yield

    app = FastAPI(title="Webhook receiver", lifespan=lifespan)
    app.state.settings = settings
    app.state.limiter = RateLimiter(
        settings.rate_limit_per_second,
        settings.rate_limit_burst,
        clock,
        settings.rate_limit_max_sources,
    )

    def outcome(event: str, source: str, key: str | None = None) -> None:
        app.state.metrics.received(source, event)
        extra = {"event": event, "source": source}
        if key is not None:
            extra["idempotency_key"] = key
        _log.info("webhook %s", event, extra=extra)

    def status_response(
        event: Event, duplicate: bool | None = None
    ) -> Response:
        body = StatusOut.from_event(event, settings.max_attempts, duplicate)
        return JSONResponse(body.model_dump(mode="json"))

    @app.post("/webhooks")
    async def post_webhook(request: Request) -> Response:
        # Order of checks (spec §6): size (413, middleware) -> X-Source
        # (400) -> body (422) -> rate limit (429) -> backpressure (503).
        span = trace.get_current_span()
        source = request.headers.get("x-source", "")
        # Measured on the latin-1 decoded header, i.e. in bytes for
        # non-ASCII values; stricter than characters, which is harmless.
        if not source or len(source) > MAX_SOURCE_LENGTH:
            outcome("invalid", NO_SOURCE)
            return _detail(
                400,
                "X-Source header is required"
                f" (1-{MAX_SOURCE_LENGTH} characters)",
            )
        # Request-span attributes: identifiers only, never the payload.
        span.set_attribute("source", source)
        try:
            webhook = WebhookIn.model_validate_json(await request.body())
        except ValidationError as exc:
            outcome("invalid", source)
            # Without input/context: never echo submitted data back.
            errors = exc.errors(
                include_url=False, include_input=False, include_context=False
            )
            return _detail(422, errors)
        key = webhook.idempotency_key
        span.set_attribute("idempotency_key", key)

        decision = app.state.limiter.check(source)
        if not decision.allowed:
            outcome("rate_limited", source, key)
            return _detail(
                429,
                "rate limit exceeded for this X-Source",
                **{"Retry-After": str(decision.retry_after_seconds)},
            )

        repo: EventRepository = app.state.repo
        if app.state.stats.get().backlog >= settings.max_backlog:
            existing = await run_in_threadpool(repo.get, key)
            if existing is not None:
                outcome("duplicate", source, key)
                return status_response(existing, duplicate=True)
            outcome("backpressure", source, key)
            retry_after = settings.backpressure_retry_after_seconds
            return _detail(
                503,
                "backlog limit reached, retry later",
                **{"Retry-After": str(retry_after)},
            )

        # Captured here, inside the request span, so the worker's spans
        # join the trace of this request.
        traceparent = current_traceparent()
        event, created = await run_in_threadpool(
            repo.insert_or_get,
            key,
            source,
            webhook.event_type,
            webhook.payload,
            traceparent,
        )
        if not created:
            outcome("duplicate", source, key)
            return status_response(event, duplicate=True)
        outcome("accepted", source, key)
        url = status_url(key)
        body = AcceptedOut(idempotency_key=key, status_url=url)
        return JSONResponse(
            body.model_dump(mode="json"),
            status_code=202,
            headers={"Location": url},
        )

    @app.get("/webhooks/{idempotency_key:path}")
    async def get_webhook(idempotency_key: str) -> Response:
        event = None
        if idempotency_key:
            event = await run_in_threadpool(
                app.state.repo.get, idempotency_key
            )
        if event is None:
            return _detail(404, "event not found")
        return status_response(event)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready() -> Response:
        failed = await run_in_threadpool(_readiness, app.state.pool)
        if failed:
            return JSONResponse(
                {"status": "not ready", "failed": failed}, status_code=503
            )
        return JSONResponse({"status": "ready"})

    @app.get("/metrics")
    async def metrics() -> Response:
        payload, content_type = metrics_payload()
        return Response(payload, media_type=content_type)

    app.add_middleware(
        BodySizeLimitMiddleware, max_bytes=settings.max_body_bytes
    )
    FastAPIInstrumentor.instrument_app(app, excluded_urls=OTEL_EXCLUDED_URLS)
    return app

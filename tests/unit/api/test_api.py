import asyncio
import json
import logging
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest
from opentelemetry.trace import SpanKind
from prometheus_client.parser import text_string_to_metric_families

from tests.fakes import GIST_WEBHOOKS
from webhook_receiver.api.app import create_app
from webhook_receiver.common.config import Settings
from webhook_receiver.common.models import Status

SRC = {"X-Source": "shop"}
OTHER_SRC = {"X-Source": "crm"}


def _webhook(key: str, **extra) -> dict:
    body = {"idempotency_key": key, "event_type": "test.event", "payload": {}}
    body.update(extra)
    return body


@pytest.fixture
def overrides() -> dict:
    """Settings overrides; tests change it with ``parametrize``."""
    return {}


@pytest.fixture
def app(tmp_path, clock, overrides):
    # A long refresh interval: the cache is refreshed once at startup
    # and afterwards only when a test calls ``stats.refresh()``.
    params = {"queue_stats_refresh_seconds": 3600, **overrides}
    settings = Settings(db_path=tmp_path / "a.db", **params)
    return create_app(settings, clock=clock)


@asynccontextmanager
async def _serve(app) -> AsyncIterator[httpx.AsyncClient]:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client


@pytest.fixture
async def client(app) -> AsyncIterator[httpx.AsyncClient]:
    async with _serve(app) as c:
        yield c


def _received(metrics_text: str, source: str, outcome: str) -> float:
    for family in text_string_to_metric_families(metrics_text):
        for sample in family.samples:
            if (
                sample.name == "webhooks_received_total"
                and sample.labels.get("source") == source
                and sample.labels.get("outcome") == outcome
            ):
                return sample.value
    return 0.0


async def _count(client, source: str, outcome: str) -> float:
    return _received((await client.get("/metrics")).text, source, outcome)


# --- POST /webhooks ---------------------------------------------------


async def test_post_new_returns_202_with_location(client):
    r = await client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
    assert r.status_code == 202
    assert r.headers["location"] == "/webhooks/evt-001"
    assert r.json() == {
        "idempotency_key": "evt-001",
        "status": "pending",
        "status_url": "/webhooks/evt-001",
    }


async def test_all_gist_examples_accepted(client, app):
    for body in GIST_WEBHOOKS:
        r = await client.post("/webhooks", json=body, headers=SRC)
        assert r.status_code == 202, body["idempotency_key"]
    for body in GIST_WEBHOOKS:
        event = app.state.repo.get(body["idempotency_key"])
        assert event.event_type == body["event_type"]
        assert event.payload == body["payload"]
        assert event.source == "shop"
        assert event.status is Status.PENDING


async def test_duplicate_returns_200_with_status(client, app):
    first = await client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
    assert first.status_code == 202
    retry = dict(GIST_WEBHOOKS[0], payload={"different": True})
    r = await client.post("/webhooks", json=retry, headers=OTHER_SRC)
    assert r.status_code == 200
    data = r.json()
    assert data["duplicate"] is True
    assert data["status"] == "pending"
    assert data["source"] == "shop"
    assert data["idempotency_key"] == "evt-001"
    assert data["event_type"] == "order.created"
    # The retry is neither stored nor does it replace the original.
    event = app.state.repo.get("evt-001")
    assert event.payload == GIST_WEBHOOKS[0]["payload"]


async def test_missing_or_bad_x_source_is_400_before_validation(client):
    r = await client.post("/webhooks", content=b"not json")
    assert r.status_code == 400
    for value in ("", "x" * 65):
        r = await client.post(
            "/webhooks", json=GIST_WEBHOOKS[0], headers={"X-Source": value}
        )
        assert r.status_code == 400, value
        assert "X-Source" in r.json()["detail"]
    ok = await client.post(
        "/webhooks", json=GIST_WEBHOOKS[0], headers={"X-Source": "x" * 64}
    )
    assert ok.status_code == 202


@pytest.mark.parametrize(
    "content",
    [
        b"not json",
        b"[]",
        json.dumps({"payload": []}).encode(),
        json.dumps({"payload": None}).encode(),
        json.dumps(_webhook("k", payload=[])).encode(),
        json.dumps(_webhook("k", payload=None)).encode(),
        json.dumps({"idempotency_key": "k", "payload": {}}).encode(),
        json.dumps(_webhook("k", event_type="")).encode(),
        json.dumps(_webhook("")).encode(),
        json.dumps(_webhook("x" * 256)).encode(),
        json.dumps(_webhook(1)).encode(),
    ],
)
async def test_post_invalid_bodies_return_422(client, content):
    r = await client.post("/webhooks", content=content, headers=SRC)
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert isinstance(detail, list) and detail
    # Errors describe the problem without echoing the submitted input.
    assert all("input" not in error for error in detail)


async def test_post_key_of_255_chars_and_extra_fields_accepted(client):
    r = await client.post(
        "/webhooks", json=_webhook("x" * 255, extra="ignored"), headers=SRC
    )
    assert r.status_code == 202


async def test_body_over_256_kib_is_413(client):
    big = _webhook("big", payload={"blob": "x" * (256 * 1024)})
    r = await client.post("/webhooks", json=big, headers=SRC)
    assert r.status_code == 413
    # Size is checked first, even before X-Source.
    r = await client.post("/webhooks", json=big)
    assert r.status_code == 413


@pytest.mark.parametrize(
    "overrides",
    [{"rate_limit_burst": 1, "rate_limit_per_second": 1}],
)
async def test_rate_limit_returns_429_with_retry_after(client, clock):
    r = await client.post("/webhooks", json=_webhook("a"), headers=SRC)
    assert r.status_code == 202
    # Invalid bodies are rejected before the rate limit (no token used).
    r = await client.post("/webhooks", content=b"{}", headers=SRC)
    assert r.status_code == 422
    r = await client.post("/webhooks", json=_webhook("b"), headers=SRC)
    assert r.status_code == 429
    assert r.headers["retry-after"] == "1"
    assert "detail" in r.json()
    # Duplicates count against the limit too.
    r = await client.post("/webhooks", json=_webhook("a"), headers=SRC)
    assert r.status_code == 429
    # Limits are per source.
    r = await client.post("/webhooks", json=_webhook("b"), headers=OTHER_SRC)
    assert r.status_code == 202
    clock.advance(1000)
    r = await client.post("/webhooks", json=_webhook("c"), headers=SRC)
    assert r.status_code == 202


@pytest.mark.parametrize("overrides", [{"max_backlog": 1}])
async def test_backpressure_503_for_new_but_200_for_duplicate(client, app):
    r = await client.post("/webhooks", json=_webhook("A"), headers=SRC)
    assert r.status_code == 202
    app.state.stats.refresh()
    assert app.state.stats.get().backlog == 1
    r = await client.post("/webhooks", json=_webhook("B"), headers=SRC)
    assert r.status_code == 503
    assert r.headers["retry-after"] == "5"
    assert "detail" in r.json()
    assert app.state.repo.get("B") is None
    r = await client.post("/webhooks", json=_webhook("A"), headers=SRC)
    assert r.status_code == 200
    assert r.json()["duplicate"] is True
    assert r.json()["idempotency_key"] == "A"


@pytest.mark.parametrize(
    "overrides",
    [{"max_backlog": 1, "rate_limit_burst": 2, "rate_limit_per_second": 1}],
)
async def test_rate_limit_is_checked_before_backpressure(client, app):
    assert (
        await client.post("/webhooks", json=_webhook("A"), headers=SRC)
    ).status_code == 202
    app.state.stats.refresh()
    r = await client.post("/webhooks", json=_webhook("B"), headers=SRC)
    assert r.status_code == 503
    r = await client.post("/webhooks", json=_webhook("B"), headers=SRC)
    assert r.status_code == 429


async def test_post_stores_traceparent_of_request_span(
    client, app, span_exporter
):
    r = await client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
    assert r.status_code == 202
    servers = [
        s
        for s in span_exporter.get_finished_spans()
        if s.kind is SpanKind.SERVER
    ]
    assert len(servers) == 1
    ctx = servers[0].get_span_context()
    expected = (
        f"00-{ctx.trace_id:032x}-{ctx.span_id:016x}-{ctx.trace_flags:02x}"
    )
    assert app.state.repo.get("evt-001").traceparent == expected


async def test_operational_endpoints_are_not_traced(client, span_exporter):
    for path in ("/health", "/ready", "/metrics", "/health?probe=1"):
        assert (await client.get(path)).status_code == 200
    assert span_exporter.get_finished_spans() == ()


def _server_spans(span_exporter) -> list:
    return [
        s
        for s in span_exporter.get_finished_spans()
        if s.kind is SpanKind.SERVER
    ]


@pytest.mark.parametrize(
    "path", ["/webhooks/health-check", "/webhooks/x/health", "/webhooks/ready"]
)
async def test_keys_resembling_operational_paths_are_traced(
    client, span_exporter, path
):
    assert (await client.get(path)).status_code == 404
    assert len(_server_spans(span_exporter)) == 1


@pytest.mark.parametrize("host", ["metrics.example", "health", "ready:8000"])
async def test_host_header_does_not_disable_tracing(
    client, app, span_exporter, host
):
    r = await client.post(
        "/webhooks",
        json=GIST_WEBHOOKS[0],
        headers={**SRC, "Host": host},
    )
    assert r.status_code == 202
    assert len(_server_spans(span_exporter)) == 1
    assert app.state.repo.get("evt-001").traceparent is not None


async def test_request_span_has_source_and_key_attributes(
    client, span_exporter
):
    await client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
    await client.post("/webhooks", content=b"{}", headers=OTHER_SRC)
    accepted, invalid = _server_spans(span_exporter)
    assert accepted.attributes["source"] == "shop"
    assert accepted.attributes["idempotency_key"] == "evt-001"
    assert invalid.attributes["source"] == "crm"
    assert "idempotency_key" not in invalid.attributes
    for span in (accepted, invalid):
        assert "payload" not in span.attributes


async def test_payload_never_reaches_logs_or_spans(
    client, caplog, span_exporter
):
    secret = "Jan Kowalski"
    with caplog.at_level(logging.DEBUG):
        await client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
        await client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
    events = [getattr(r, "event", None) for r in caplog.records]
    assert "accepted" in events and "duplicate" in events
    accepted = next(r for r in caplog.records if r.event == "accepted")
    assert accepted.idempotency_key == "evt-001"
    assert accepted.source == "shop"
    for record in caplog.records:
        assert secret not in json.dumps(vars(record), default=str)
    for span in span_exporter.get_finished_spans():
        assert secret not in json.dumps(dict(span.attributes or {}))


@pytest.mark.parametrize(
    "overrides",
    [{"max_backlog": 1, "rate_limit_burst": 3, "rate_limit_per_second": 1}],
)
async def test_every_outcome_is_counted_and_logged(client, app, caplog):
    outcomes = [
        ("shop", "accepted"),
        ("shop", "duplicate"),
        ("shop", "invalid"),
        ("", "invalid"),
        ("shop", "backpressure"),
        ("shop", "rate_limited"),
    ]
    before = {o: await _count(client, *o) for o in outcomes}
    with caplog.at_level(logging.INFO):
        await client.post("/webhooks", json=_webhook("A"), headers=SRC)
        await client.post("/webhooks", json=_webhook("A"), headers=SRC)
        await client.post("/webhooks", content=b"{}", headers=SRC)
        await client.post("/webhooks", json=_webhook("A"))
        app.state.stats.refresh()
        await client.post("/webhooks", json=_webhook("B"), headers=SRC)
        await client.post("/webhooks", json=_webhook("B"), headers=SRC)
    for o in outcomes:
        assert await _count(client, *o) - before[o] == 1, o
    logged = [getattr(r, "event", None) for r in caplog.records]
    assert logged.count("invalid") == 2
    for name in ("accepted", "duplicate", "backpressure", "rate_limited"):
        assert logged.count(name) == 1, name


async def test_concurrent_posts_same_key(client):
    async def post():
        return await client.post(
            "/webhooks", json=_webhook("same"), headers=SRC
        )

    responses = await asyncio.wait_for(
        asyncio.gather(*(post() for _ in range(20))), timeout=30
    )
    codes = sorted(r.status_code for r in responses)
    assert codes == [200] * 19 + [202]


# --- GET /webhooks/{key} ----------------------------------------------


@pytest.mark.parametrize(
    "key", ["evt/1", "a b", "100%", "q?x#y", "zażółć", "x" * 255]
)
async def test_get_roundtrip_special_keys(client, key):
    r = await client.post("/webhooks", json=_webhook(key), headers=SRC)
    assert r.status_code == 202
    assert r.headers["location"] == r.json()["status_url"]
    assert "/" not in r.json()["status_url"].removeprefix("/webhooks/")
    got = await client.get(r.json()["status_url"])
    assert got.status_code == 200
    assert got.json()["idempotency_key"] == key


async def test_get_unknown_and_empty_key_404(client):
    assert (await client.get("/webhooks/nope")).status_code == 404
    assert (await client.get("/webhooks/")).status_code == 404


async def test_get_new_event_fields(client, clock):
    await client.post("/webhooks", json=GIST_WEBHOOKS[3], headers=SRC)
    r = await client.get("/webhooks/evt-004")
    assert r.status_code == 200
    # FakeClock starts at 1_700_000_000_000 ms = 2023-11-14T22:13:20Z.
    assert r.json() == {
        "idempotency_key": "evt-004",
        "source": "shop",
        "event_type": "user.registered",
        "status": "pending",
        "attempts": 0,
        "max_attempts": 5,
        "next_attempt_at": "2023-11-14T22:13:20.000Z",
        "last_error": None,
        "result": None,
        "created_at": "2023-11-14T22:13:20.000Z",
        "finished_at": None,
        "payload_purged": False,
    }


async def test_get_maps_fields(client, app, clock):
    await client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
    repo = app.state.repo
    (event,) = repo.claim(limit=1, lease_ms=30_000)
    retry_at = clock.now_ms() + 8_250
    assert repo.record_failure(
        event.id, 1, "simulated failure (fail_once)", retry_at
    )
    data = (await client.get("/webhooks/evt-001")).json()
    assert data["status"] == "retrying"
    assert data["attempts"] == 1
    assert data["max_attempts"] == 5
    assert data["next_attempt_at"] == "2023-11-14T22:13:28.250Z"
    assert data["last_error"] == "simulated failure (fail_once)"
    assert data["finished_at"] is None
    assert data["payload_purged"] is False
    assert "duplicate" not in data


async def test_get_completed_and_purged(client, app, clock):
    await client.post("/webhooks", json=GIST_WEBHOOKS[1], headers=SRC)
    repo = app.state.repo
    (event,) = repo.claim(limit=1, lease_ms=30_000)
    clock.advance(3_000)
    assert repo.complete(event.id, 1, {"ok": True})
    data = (await client.get("/webhooks/evt-002")).json()
    assert data["status"] == "completed"
    assert data["next_attempt_at"] is None
    assert data["result"] == {"ok": True}
    assert data["finished_at"] == "2023-11-14T22:13:23.000Z"
    assert data["payload_purged"] is False
    with sqlite3.connect(app.state.settings.db_path) as conn:
        conn.execute("UPDATE webhook_events SET payload_json = NULL")
    conn.close()
    data = (await client.get("/webhooks/evt-002")).json()
    assert data["payload_purged"] is True


# --- operational endpoints --------------------------------------------


async def test_ready_and_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
    r = await client.get("/ready")
    assert r.status_code == 200
    assert r.json() == {"status": "ready"}


async def test_ready_reports_missing_schema(client, app):
    with sqlite3.connect(app.state.settings.db_path) as conn:
        conn.execute("DROP TABLE webhook_events")
    conn.close()
    r = await client.get("/ready")
    assert r.status_code == 503
    assert r.json() == {"status": "not ready", "failed": ["schema"]}
    assert (await client.get("/health")).status_code == 200


async def test_ready_reports_database_failure(client, app, monkeypatch):
    def broken():
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr(app.state.pool, "get", broken)
    r = await client.get("/ready")
    assert r.status_code == 503
    assert r.json() == {
        "status": "not ready",
        "failed": ["database", "schema"],
    }


async def test_metrics_exposes_received_counter(client):
    before = await _count(client, "shop", "accepted")
    await client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
    r = await client.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert _received(r.text, "shop", "accepted") - before == 1
    assert "webhook_queue_depth" in r.text


# --- lifespan ---------------------------------------------------------


async def test_extra_lifespan_runs_inside_cache_lifetime(tmp_path, clock):
    seen = []

    @asynccontextmanager
    async def extra(repo):
        seen.append(("enter", repo, app.state.stats._thread.is_alive()))
        yield
        seen.append(("exit", app.state.stats._thread.is_alive()))

    settings = Settings(db_path=tmp_path / "b.db")
    app = create_app(settings, clock=clock, extra_lifespan=extra)
    async with _serve(app) as c:
        assert (await c.get("/health")).status_code == 200
        assert seen == [("enter", app.state.repo, True)]
    assert seen[-1] == ("exit", True)
    assert not app.state.stats._thread.is_alive()


async def test_startup_failure_releases_cache_and_connections(tmp_path, clock):
    captured = {}

    @asynccontextmanager
    async def failing(repo):
        captured["conn"] = app.state.pool.get()
        captured["thread"] = app.state.stats._thread
        raise RuntimeError("extra lifespan failed")
        yield  # pragma: no cover

    settings = Settings(db_path=tmp_path / "c.db")
    app = create_app(settings, clock=clock, extra_lifespan=failing)
    with pytest.raises(RuntimeError, match="extra lifespan failed"):
        async with app.router.lifespan_context(app):
            pass  # pragma: no cover
    assert not captured["thread"].is_alive()
    with pytest.raises(sqlite3.ProgrammingError):
        captured["conn"].execute("SELECT 1")

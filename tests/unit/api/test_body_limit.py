import asyncio

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from webhook_receiver.api.body_limit import BodySizeLimitMiddleware

TOO_LARGE = {"detail": "request body too large"}


def _make_app(seen: list[bytes]):
    async def echo(request: Request) -> JSONResponse:
        body = await request.body()
        seen.append(body)
        return JSONResponse({"len": len(body)})

    app = Starlette(routes=[Route("/", echo, methods=["POST", "GET"])])
    return BodySizeLimitMiddleware(app, max_bytes=10)


@pytest.fixture
def seen() -> list[bytes]:
    return []


class _Client:
    """Sync facade over httpx.AsyncClient driving the ASGI app in-process.

    (starlette.testclient emits a deprecation warning with httpx, which
    the pristine ``-W error`` run turns into a failure.)
    """

    def __init__(self, app) -> None:
        self._app = app

    def _request(self, method: str, **kwargs) -> httpx.Response:
        async def go() -> httpx.Response:
            transport = httpx.ASGITransport(app=self._app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as c:
                return await c.request(method, "/", **kwargs)

        return asyncio.run(go())

    def post(self, path: str, **kwargs) -> httpx.Response:
        assert path == "/"
        return self._request("POST", **kwargs)

    def get(self, path: str) -> httpx.Response:
        assert path == "/"
        return self._request("GET")


@pytest.fixture
def client(seen):
    return _Client(_make_app(seen))


def test_body_limit_rejects_content_length(client, seen):
    r = client.post("/", content=b"x" * 11)
    assert r.status_code == 413
    assert r.json() == TOO_LARGE
    assert seen == []


def test_body_limit_rejects_content_length_without_reading_body(seen):
    app = _make_app(seen)
    sent = []

    async def receive():
        raise AssertionError("body must not be read")

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [(b"content-length", b"11")],
    }
    asyncio.run(app(scope, receive, send))
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 413
    assert (b"content-type", b"application/json") in sent[0]["headers"]
    assert seen == []


def test_body_limit_rejects_chunked_stream(client, seen):
    async def gen():
        for _ in range(4):
            yield b"abc"

    r = client.post("/", content=gen())
    assert r.status_code == 413
    assert r.json() == TOO_LARGE
    assert seen == []


def test_body_limit_allows_chunked_stream_at_limit(client, seen):
    async def gen():
        yield b"abcde"
        yield b"fghij"

    r = client.post("/", content=gen())
    assert r.status_code == 200
    assert seen == [b"abcdefghij"]


def test_body_limit_allows_small_body(client, seen):
    r = client.post("/", content=b"x" * 10)
    assert r.status_code == 200
    assert r.json() == {"len": 10}
    assert seen == [b"x" * 10]


def test_body_limit_allows_get_without_body(client, seen):
    r = client.get("/")
    assert r.status_code == 200
    assert seen == [b""]


def test_malformed_content_length_falls_back_to_counting(seen):
    app = _make_app(seen)

    def run(chunks):
        sent = []
        queue = [
            {
                "type": "http.request",
                "body": c,
                "more_body": i < len(chunks) - 1,
            }
            for i, c in enumerate(chunks)
        ]

        async def receive():
            return queue.pop(0)

        async def send(message):
            sent.append(message)

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [(b"content-length", b"abc")],
        }
        asyncio.run(app(scope, receive, send))
        return sent[0]["status"]

    assert run([b"12345", b"678901"]) == 413
    assert run([b"12345"]) == 200
    assert seen == [b"12345"]


def test_client_disconnect_while_reading_does_not_call_app(seen):
    app = _make_app(seen)
    sent = []
    messages = [
        {"type": "http.request", "body": b"abc", "more_body": True},
        {"type": "http.disconnect"},
    ]

    async def receive():
        return messages.pop(0)

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": "POST", "path": "/", "headers": []}
    asyncio.run(app(scope, receive, send))
    assert sent == []
    assert seen == []


def test_non_http_scope_passes_through():
    calls = []

    async def inner(scope, receive, send):
        calls.append(scope["type"])

    mw = BodySizeLimitMiddleware(inner, max_bytes=1)

    async def receive():
        raise AssertionError("must not be read")

    async def send(message):
        raise AssertionError("must not send")

    asyncio.run(mw({"type": "lifespan"}, receive, send))
    asyncio.run(mw({"type": "websocket"}, receive, send))
    assert calls == ["lifespan", "websocket"]


def _raw_call(app, headers, chunks):
    """Drive ``app`` with raw ASGI messages; return the response status."""
    sent = []
    queue = [
        {
            "type": "http.request",
            "body": c,
            "more_body": i < len(chunks) - 1,
        }
        for i, c in enumerate(chunks)
    ]

    async def receive():
        if not queue:
            raise AssertionError("body must not be read")
        return queue.pop(0)

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": "POST", "path": "/", "headers": headers}
    asyncio.run(app(scope, receive, send))
    return sent[0]["status"]


@pytest.mark.parametrize(
    "value", [b" 11 ", b"11 ", b"+11", b"1_1", b"-11", b"", b"0x11", b"\xb2"]
)
def test_non_digit_content_length_is_ignored(seen, value):
    # A lenient parse would read these as 11 (> 10) and reject without
    # reading; strict parsing ignores them and the stream count decides.
    app = _make_app(seen)
    assert _raw_call(app, [(b"content-length", value)], [b"12345"]) == 200
    assert seen == [b"12345"]


def test_conflicting_content_lengths_are_ignored(seen):
    app = _make_app(seen)
    headers = [(b"content-length", b"11"), (b"content-length", b"5")]
    assert _raw_call(app, headers, [b"12345"]) == 200
    headers = [(b"content-length", b"5"), (b"content-length", b"11")]
    assert _raw_call(app, headers, [b"12345", b"678901"]) == 413
    assert seen == [b"12345"]


def test_equal_duplicate_content_lengths_are_used(seen):
    app = _make_app(seen)
    headers = [(b"content-length", b"11"), (b"Content-Length", b"11")]
    # No chunks queued: reading the body would raise.
    assert _raw_call(app, headers, []) == 413
    assert seen == []

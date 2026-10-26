"""Pure ASGI middleware rejecting request bodies above a byte limit."""

import json
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

_BODY = json.dumps({"detail": "request body too large"}).encode()


class BodySizeLimitMiddleware:
    """Return ``413`` for bodies larger than ``max_bytes``.

    A declared ``Content-Length`` above the limit is rejected without
    reading the body. Otherwise the body is read up front (at most
    ``max_bytes`` are buffered) and counted across ``http.request``
    messages, so chunked uploads are capped too and the wrapped app
    never sees a partial body. A malformed ``Content-Length`` is not
    trusted: the streaming count still enforces the limit.
    """

    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        if max_bytes < 0:
            raise ValueError("max_bytes must be >= 0")
        self._app = app
        self._max = max_bytes

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        declared = _content_length(scope)
        if declared is not None and declared > self._max:
            await _send_413(send)
            return

        chunks: list[bytes] = []
        total = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            total += len(chunk)
            if total > self._max:
                await _send_413(send)
                return
            chunks.append(chunk)
            if not message.get("more_body", False):
                break

        body = b"".join(chunks)
        replayed = False

        async def replay() -> Message:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {
                    "type": "http.request",
                    "body": body,
                    "more_body": False,
                }
            return await receive()

        await self._app(scope, replay, send)


def _content_length(scope: Scope) -> int | None:
    """Parsed ``Content-Length``, or ``None`` if absent or malformed."""
    for name, value in scope.get("headers", []):
        if name.lower() == b"content-length":
            try:
                length = int(value)
            except ValueError:
                return None
            return length if length >= 0 else None
    return None


async def _send_413(send: Send) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(_BODY)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": _BODY})

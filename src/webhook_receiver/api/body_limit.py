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
    never sees a partial body. A malformed or conflicting
    ``Content-Length`` is not trusted: the streaming count still
    enforces the limit.
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
        if _declared_too_large(scope, self._max):
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


def _declared_too_large(scope: Scope, max_bytes: int) -> bool:
    """Whether a usable ``Content-Length`` exceeds ``max_bytes``.

    Only ASCII digits are accepted (no sign, whitespace or underscores,
    which ``int()`` would tolerate). Several headers are used only if
    they all agree. Anything else is ignored (``False``) and the
    streaming count enforces the limit.
    """
    values = {
        value
        for name, value in scope.get("headers", [])
        if name.lower() == b"content-length"
    }
    if len(values) != 1:
        return False
    (value,) = values
    # bytes.isdigit() is ASCII-only, unlike str.isdigit().
    if not value.isdigit():
        return False
    digits = value.lstrip(b"0") or b"0"
    # Compare lengths first: int() refuses strings longer than CPython's
    # int/str digit limit (4300), and a header that long is too large.
    if len(digits) > len(str(max_bytes)):
        return True
    return int(digits) > max_bytes


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

"""Structured JSON logging to stdout with trace correlation."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from opentelemetry import trace

# Attributes every LogRecord has; anything else came from ``extra``.
_RESERVED = frozenset(
    vars(logging.LogRecord("", 0, "", 0, "", None, None))
) | {"message", "asctime"}


# Event bodies may hold personal data and must never reach the logs.
_REDACTED_KEYS = frozenset({"payload", "payload_json", "body", "result_json"})
_REDACTED = "[redacted]"


def _serializable(value: Any) -> Any:
    """The value if JSON can encode it, otherwise its ``repr``."""
    try:
        json.dumps(value, ensure_ascii=False, default=str)
    except TypeError, ValueError, RecursionError:
        return repr(value)
    return value


class JsonFormatter(logging.Formatter):
    """One JSON object per record, correlated with the current span.

    Fields from ``extra`` are copied as-is, except keys in
    ``_REDACTED_KEYS`` (``payload``, ``payload_json``, ``body``,
    ``result_json``), whose values are replaced with ``"[redacted]"``.
    A value JSON cannot encode (e.g. a circular structure) is logged as
    its ``repr`` so the rest of the line survives.
    """

    def format(self, record: logging.LogRecord) -> str:
        ctx = trace.get_current_span().get_span_context()
        entry: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": (
                trace.format_trace_id(ctx.trace_id) if ctx.is_valid else ""
            ),
            "span_id": (
                trace.format_span_id(ctx.span_id) if ctx.is_valid else ""
            ),
        }
        for key, value in vars(record).items():
            if key in _RESERVED or key in entry:
                continue
            if key in _REDACTED_KEYS:
                entry[key] = _REDACTED
            else:
                entry[key] = _serializable(value)
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        if record.stack_info:
            entry["stack"] = self.formatStack(record.stack_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


class _StdoutHandler(logging.StreamHandler):
    """Writes to whatever ``sys.stdout`` is at emit time."""

    def __init__(self) -> None:
        super().__init__(sys.stdout)

    @property
    def stream(self):
        return sys.stdout

    @stream.setter
    def stream(self, _value) -> None:
        pass


def setup_logging(level: str) -> None:
    """Install a single JSON stdout handler on the root logger."""
    root = logging.getLogger()
    for handler in root.handlers[:]:
        if isinstance(handler, _StdoutHandler):
            root.removeHandler(handler)
    handler = _StdoutHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())

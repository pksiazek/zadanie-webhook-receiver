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


class JsonFormatter(logging.Formatter):
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
            if key not in _RESERVED and key not in entry:
                entry[key] = value
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
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

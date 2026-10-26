import json
import logging
import re

import pytest
from opentelemetry import trace

from webhook_receiver.common.logging_setup import setup_logging
from webhook_receiver.common.telemetry import get_tracer


@pytest.fixture
def restore_root_logger():
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


@pytest.fixture
def log(restore_root_logger):
    setup_logging("INFO")
    return logging.getLogger("webhook_receiver.test")


def _lines(capsys) -> tuple[list[dict], str]:
    captured = capsys.readouterr()
    lines = [json.loads(x) for x in captured.out.splitlines()]
    return lines, captured.err


def test_json_log_has_trace_ids(capsys, span_exporter, log):
    with get_tracer().start_as_current_span("post") as span:
        log.info("x", extra={"idempotency_key": "evt-1", "source": "shop"})
    (entry,), err = _lines(capsys)
    ctx = span.get_span_context()
    assert re.fullmatch(r"[0-9a-f]{32}", entry["trace_id"])
    assert entry["trace_id"] == trace.format_trace_id(ctx.trace_id)
    assert entry["span_id"] == trace.format_span_id(ctx.span_id)
    assert entry["idempotency_key"] == "evt-1"
    assert entry["source"] == "shop"
    assert entry["message"] == "x"
    assert entry["level"] == "INFO"
    assert entry["logger"] == "webhook_receiver.test"
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT.+", entry["ts"])
    assert err == ""


def test_json_log_without_span_has_empty_ids(capsys, log):
    log.warning("no span %s", "here", extra={"event": "retention"})
    (entry,), _ = _lines(capsys)
    assert entry["trace_id"] == ""
    assert entry["span_id"] == ""
    assert entry["message"] == "no span here"
    assert entry["event"] == "retention"
    assert entry["level"] == "WARNING"


def test_non_ascii_is_kept_verbatim(capsys, log):
    log.info("zażółć")
    out = capsys.readouterr().out
    assert "zażółć" in out


def test_level_filters_records(capsys, restore_root_logger):
    setup_logging("WARNING")
    logging.getLogger("x").info("hidden")
    assert capsys.readouterr().out == ""


def test_setup_logging_twice_keeps_single_handler(capsys, log):
    setup_logging("INFO")
    log.info("once")
    entries, _ = _lines(capsys)
    assert len(entries) == 1


def test_exception_is_serialized(capsys, log):
    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("failed")
    (entry,), _ = _lines(capsys)
    assert "ValueError: boom" in entry["exc"]


@pytest.mark.parametrize(
    "key", ["payload", "payload_json", "body", "result_json"]
)
def test_payload_like_extra_is_redacted(capsys, log, key):
    log.info("saved", extra={key: {"email": "a@b.c"}, "source": "shop"})
    (entry,), _ = _lines(capsys)
    assert entry[key] == "[redacted]"
    assert entry["source"] == "shop"
    assert "a@b.c" not in json.dumps(entry)


def test_unserializable_extra_keeps_the_line(capsys, log):
    loop: list = []
    loop.append(loop)
    log.info("odd", extra={"loop": loop, "source": "shop"})
    (entry,), _ = _lines(capsys)
    assert entry["message"] == "odd"
    assert entry["source"] == "shop"
    assert entry["loop"] == repr(loop)


def test_stack_info_is_included(capsys, log):
    log.info("where", stack_info=True)
    (entry,), _ = _lines(capsys)
    assert entry["stack"].startswith("Stack (most recent call last)")

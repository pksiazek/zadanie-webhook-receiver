import re
import sys

import pytest
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from webhook_receiver.common import telemetry
from webhook_receiver.common.models import QueueStats
from webhook_receiver.common.telemetry import (
    ApiMetrics,
    WorkerMetrics,
    context_from_traceparent,
    current_traceparent,
    get_tracer,
    metrics_payload,
    setup_telemetry,
)


def _metrics_text() -> str:
    body, _ = metrics_payload()
    return body.decode()


def _value(text: str, series: str) -> float:
    """Value of an exact series line (name plus labels), 0 if absent."""
    pattern = re.compile(rf"^{re.escape(series)} (\S+)$", re.MULTILINE)
    match = pattern.search(text)
    return float(match.group(1)) if match else 0.0


def test_traceparent_roundtrip(span_exporter):
    with get_tracer().start_as_current_span("post") as parent:
        tp = current_traceparent()
    assert tp.startswith("00-")
    with get_tracer().start_as_current_span(
        "process", context=context_from_traceparent(tp)
    ) as child:
        pass
    assert child.parent.span_id == parent.get_span_context().span_id
    assert child.context.trace_id == parent.get_span_context().trace_id


def test_spans_reach_exporter_with_parent_link(span_exporter):
    with get_tracer().start_as_current_span("post"):
        tp = current_traceparent()
    with get_tracer().start_as_current_span(
        "webhook.process", context=context_from_traceparent(tp)
    ):
        pass
    post, process = span_exporter.get_finished_spans()
    assert process.name == "webhook.process"
    assert process.parent.span_id == post.context.span_id
    assert process.context.trace_id == post.context.trace_id


def test_resource_has_service_attributes(span_exporter):
    with get_tracer().start_as_current_span("x"):
        pass
    (span,) = span_exporter.get_finished_spans()
    attrs = span.resource.attributes
    assert attrs["service.name"] == "test"
    assert re.fullmatch(r".+-\d+", attrs["service.instance.id"])


def test_context_from_missing_traceparent_starts_new_trace(span_exporter):
    with get_tracer().start_as_current_span("outer"):
        with get_tracer().start_as_current_span(
            "root", context=context_from_traceparent(None)
        ) as span:
            pass
    assert span.parent is None


def test_current_traceparent_is_none_without_span():
    assert current_traceparent() is None


def test_setup_telemetry_is_idempotent(span_exporter):
    provider = trace.get_tracer_provider()
    setup_telemetry("other", InMemorySpanExporter())
    assert trace.get_tracer_provider() is provider
    with get_tracer().start_as_current_span("x"):
        pass
    assert len(span_exporter.get_finished_spans()) == 1


def test_metric_names_on_prometheus_endpoint():
    ApiMetrics(lambda: QueueStats(3, 1, 2, 1500)).received("shop", "accepted")
    WorkerMetrics().processing_duration(2.5, "success")
    body, _ = metrics_payload()
    text = body.decode()
    assert "webhooks_received_total{" in text
    assert "webhooks_received_total_total" not in text
    assert "webhook_processing_duration_seconds_bucket" in text
    assert 'webhook_queue_depth{status="pending"} 3.0' in text
    assert 'webhook_queue_depth{status="retrying"} 1.0' in text
    assert 'webhook_queue_depth{status="processing"} 2.0' in text
    assert "webhook_queue_oldest_pending_age_seconds 1.5" in text
    assert "otel_scope_name" not in text


def test_received_counter_counts_by_source_and_outcome():
    series = 'webhooks_received_total{outcome="duplicate",source="crm"}'
    before = _value(_metrics_text(), series)
    metrics = ApiMetrics(lambda: QueueStats(0, 0, 0, 0))
    metrics.received("crm", "duplicate")
    metrics.received("crm", "duplicate")
    assert _value(_metrics_text(), series) == before + 2


def test_new_api_metrics_swaps_stats_source():
    ApiMetrics(lambda: QueueStats(9, 9, 9, 9000))
    ApiMetrics(lambda: QueueStats(4, 0, 0, 0))
    text = _metrics_text()
    assert text.count('webhook_queue_depth{status="pending"}') == 1
    assert 'webhook_queue_depth{status="pending"} 4.0' in text
    assert "webhook_queue_oldest_pending_age_seconds 0.0" in text


def test_processing_duration_uses_second_buckets():
    name = "webhook_processing_duration_seconds_bucket"
    below = f'{name}{{le="0.05",outcome="retry"}}'
    above = f'{name}{{le="0.1",outcome="retry"}}'
    top = f'{name}{{le="60.0",outcome="retry"}}'
    text = _metrics_text()
    before = [_value(text, s) for s in (below, above, top)]
    WorkerMetrics().processing_duration(0.07, "retry")
    text = _metrics_text()
    after = [_value(text, s) for s in (below, above, top)]
    assert after == [before[0], before[1] + 1, before[2] + 1]
    assert f"{top} " in text


def test_worker_counters():
    attempts = 'webhook_attempts_total{outcome="failed"}'
    recoveries = "webhook_lease_recoveries_total"
    stale = "webhook_stale_results_total"
    text = _metrics_text()
    before = [_value(text, s) for s in (attempts, recoveries, stale)]
    metrics = WorkerMetrics()
    metrics.attempt("failed")
    metrics.lease_recoveries(3)
    metrics.stale_result()
    text = _metrics_text()
    after = [_value(text, s) for s in (attempts, recoveries, stale)]
    assert after == [before[0] + 1, before[1] + 3, before[2] + 1]


def test_metrics_payload_content_type():
    _, content_type = metrics_payload()
    assert content_type.startswith("text/plain")


@pytest.fixture
def clean_otel_env(monkeypatch):
    for name in (
        "OTEL_TRACES_EXPORTER",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def _exporter_of(processor):
    assert isinstance(processor, BatchSpanProcessor)
    exporter = processor.span_exporter
    processor.shutdown()
    return exporter


def test_span_processor_uses_given_exporter(clean_otel_env):
    clean_otel_env.setenv("OTEL_TRACES_EXPORTER", "none")
    exporter = InMemorySpanExporter()
    processor = telemetry._span_processor(exporter)
    assert isinstance(processor, SimpleSpanProcessor)
    assert processor.span_exporter is exporter
    processor.shutdown()


def test_span_processor_none(clean_otel_env):
    clean_otel_env.setenv("OTEL_TRACES_EXPORTER", "none")
    clean_otel_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://x:4318")
    assert telemetry._span_processor(None) is None


def test_span_processor_otlp_when_endpoint_set(clean_otel_env):
    clean_otel_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://x:4318")
    processor = telemetry._span_processor(None)
    assert isinstance(processor, BatchSpanProcessor)
    assert isinstance(processor.span_exporter, OTLPSpanExporter)
    processor.shutdown()


def test_span_processor_console_on_stderr_by_default(clean_otel_env):
    processor = telemetry._span_processor(None)
    assert isinstance(processor, BatchSpanProcessor)
    assert isinstance(processor.span_exporter, ConsoleSpanExporter)
    assert processor.span_exporter.out is sys.stderr
    processor.shutdown()


def test_gauges_report_nothing_before_stats_source(monkeypatch):
    ApiMetrics(lambda: QueueStats(1, 1, 1, 1))
    monkeypatch.setattr(telemetry._get_api_instruments(), "stats", None)
    text = _metrics_text()
    assert "webhook_queue_depth{" not in text
    assert "webhook_queue_oldest_pending_age_seconds " not in text


def test_span_processor_otlp_when_traces_endpoint_set(clean_otel_env):
    clean_otel_env.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://x:4318/v1/traces"
    )
    exporter = _exporter_of(telemetry._span_processor(None))
    assert isinstance(exporter, OTLPSpanExporter)


def test_span_processor_explicit_otlp_without_endpoint(clean_otel_env):
    clean_otel_env.setenv("OTEL_TRACES_EXPORTER", "otlp")
    exporter = _exporter_of(telemetry._span_processor(None))
    assert isinstance(exporter, OTLPSpanExporter)


def test_span_processor_explicit_console_beats_endpoint(clean_otel_env):
    clean_otel_env.setenv("OTEL_TRACES_EXPORTER", "console")
    clean_otel_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://x:4318")
    exporter = _exporter_of(telemetry._span_processor(None))
    assert isinstance(exporter, ConsoleSpanExporter)
    assert exporter.out is sys.stderr


def test_failing_stats_source_warns_once(caplog):
    def broken() -> QueueStats:
        raise RuntimeError("cache gone")

    ApiMetrics(broken)
    with caplog.at_level("WARNING"):
        first = _metrics_text()
        second = _metrics_text()
    for text in (first, second):
        assert "webhook_queue_depth{" not in text
        assert "webhook_queue_oldest_pending_age_seconds " not in text
    ours = [
        r for r in caplog.records if r.name == "webhook_receiver.telemetry"
    ]
    assert len(ours) == 1
    assert "cache gone" in ours[0].getMessage()
    assert all(r.name == "webhook_receiver.telemetry" for r in caplog.records)


def test_recovered_stats_source_reports_again(caplog):
    calls = {"n": 0}

    def flaky() -> QueueStats:
        calls["n"] += 1
        if calls["n"] <= 2:  # both gauges fail on the first scrape
            raise RuntimeError("cache gone")
        return QueueStats(5, 0, 0, 0)

    ApiMetrics(flaky)
    with caplog.at_level("WARNING"):
        _metrics_text()
        text = _metrics_text()
    assert 'webhook_queue_depth{status="pending"} 5.0' in text


def test_worker_up_gauge_reports_one():
    WorkerMetrics()
    text = _metrics_text()
    assert _value(text, "webhook_worker_up") == 1.0
    assert "webhook_worker_up_ratio" not in text


@pytest.fixture
def fresh_setup(monkeypatch):
    """Run ``setup_telemetry`` again, capturing the providers it builds.

    The global providers can be set only once per process, so the
    setters are replaced and the built providers shut down afterwards.
    """
    for name in (
        "OTEL_METRICS_EXPORTER",
        "OTEL_TRACES_EXPORTER",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
    captured = {}
    monkeypatch.setattr(telemetry, "_configured", False)
    monkeypatch.setattr(
        telemetry.trace,
        "set_tracer_provider",
        lambda p: captured.setdefault("tracer", p),
    )
    monkeypatch.setattr(
        telemetry.metrics,
        "set_meter_provider",
        lambda p: captured.setdefault("meter", p),
    )

    real_provider = telemetry.MeterProvider

    def recording_provider(**kwargs):
        captured["readers"] = kwargs["metric_readers"]
        return real_provider(**kwargs)

    monkeypatch.setattr(telemetry, "MeterProvider", recording_provider)

    def readers():
        setup_telemetry("fresh")
        return captured["readers"]

    yield monkeypatch, readers
    for kind in ("tracer", "meter"):
        if kind in captured:
            captured[kind].shutdown()


def _reader_types(readers):
    return sorted(type(r).__name__ for r in readers)


def test_setup_metrics_prometheus_only_by_default(fresh_setup):
    _, readers = fresh_setup
    assert _reader_types(readers()) == ["PrometheusMetricReader"]


@pytest.mark.parametrize("value", ["none", "prometheus"])
def test_setup_metrics_without_otlp(fresh_setup, value):
    env, readers = fresh_setup
    env.setenv("OTEL_METRICS_EXPORTER", value)
    env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://x:4318")
    assert _reader_types(readers()) == ["PrometheusMetricReader"]


@pytest.mark.parametrize("value", ["otlp", " OTLP ", "prometheus,otlp"])
def test_setup_metrics_otlp_adds_periodic_reader(fresh_setup, value):
    env, readers = fresh_setup
    env.setenv("OTEL_METRICS_EXPORTER", value)
    env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://x:4318")
    env.setenv("OTEL_METRIC_EXPORT_INTERVAL", "3600000")
    found = readers()
    assert _reader_types(found) == [
        "PeriodicExportingMetricReader",
        "PrometheusMetricReader",
    ]
    (periodic,) = [
        r for r in found if isinstance(r, PeriodicExportingMetricReader)
    ]
    assert isinstance(periodic._exporter, OTLPMetricExporter)
    assert periodic._export_interval_millis == 3600000

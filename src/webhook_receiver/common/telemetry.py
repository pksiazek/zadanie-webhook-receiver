"""OpenTelemetry setup shared by the API and worker processes.

Global providers and the Prometheus registry can be set only once per
process, so setup and metric instruments are process-wide singletons.
Nothing here ever receives or records an event payload.
"""

import logging
import os
import socket
import sys
import threading
from collections.abc import Callable, Iterable

from opentelemetry import metrics, trace
from opentelemetry.context import Context
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import CallbackOptions, Meter, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.trace import Tracer
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from webhook_receiver.common.models import QueueStats

_SCOPE = "webhook_receiver"
# Processing simulation takes seconds; the SDK default buckets are
# millisecond-oriented (0 .. 10000).
_DURATION_BUCKETS_S = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)

_log = logging.getLogger("webhook_receiver.telemetry")
_lock = threading.Lock()
_configured = False
_propagator = TraceContextTextMapPropagator()


def _span_processor(exporter: SpanExporter | None) -> SpanProcessor | None:
    """Pick the span processor; precedence, highest first:

    1. an exporter passed in (tests) -> ``SimpleSpanProcessor``;
    2. ``OTEL_TRACES_EXPORTER=none`` -> no export at all;
    3. ``OTEL_TRACES_EXPORTER=otlp`` or ``console`` -> that exporter
       (OTLP without an endpoint uses the library default endpoint);
    4. ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`` or
       ``OTEL_EXPORTER_OTLP_ENDPOINT`` set -> OTLP over HTTP;
    5. otherwise the console exporter on stderr.
    """
    if exporter is not None:
        return SimpleSpanProcessor(exporter)
    choice = os.environ.get("OTEL_TRACES_EXPORTER", "").strip().lower()
    if choice == "none":
        return None
    if choice not in ("otlp", "console"):
        has_endpoint = bool(
            os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        )
        choice = "otlp" if has_endpoint else "console"
    if choice == "otlp":
        # Imported lazily: only needed when OTLP is configured.
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        return BatchSpanProcessor(OTLPSpanExporter())
    # stdout carries JSON logs; spans go to stderr so streams never mix.
    return BatchSpanProcessor(ConsoleSpanExporter(out=sys.stderr))


def _metric_readers() -> list[MetricReader]:
    """The local Prometheus reader always; OTLP push on request.

    The Prometheus reader backs the processes' own ``/metrics``
    endpoints. ``OTEL_METRICS_EXPORTER`` containing ``otlp`` adds a
    periodic OTLP/HTTP push (to the Collector, spec §12); the exporter
    honours ``OTEL_EXPORTER_OTLP_*`` and ``OTEL_METRIC_EXPORT_INTERVAL``.
    """
    readers: list[MetricReader] = [
        PrometheusMetricReader(scope_info_enabled=False)
    ]
    choices = os.environ.get("OTEL_METRICS_EXPORTER", "").lower().split(",")
    if "otlp" in (c.strip() for c in choices):
        # Imported lazily: only needed when OTLP is configured.
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )

        readers.append(PeriodicExportingMetricReader(OTLPMetricExporter()))
    return readers


def setup_telemetry(
    service_name: str, span_exporter: SpanExporter | None = None
) -> None:
    """Configure tracing and metrics once; later calls are no-ops."""
    global _configured
    with _lock:
        if _configured:
            return
        resource = Resource.create(
            {
                "service.name": service_name,
                "service.instance.id": (
                    f"{socket.gethostname()}-{os.getpid()}"
                ),
            }
        )
        tracer_provider = TracerProvider(resource=resource)
        processor = _span_processor(span_exporter)
        if processor is not None:
            tracer_provider.add_span_processor(processor)
        trace.set_tracer_provider(tracer_provider)
        metrics.set_meter_provider(
            MeterProvider(resource=resource, metric_readers=_metric_readers())
        )
        _configured = True


def get_tracer() -> Tracer:
    return trace.get_tracer(_SCOPE)


def get_meter() -> Meter:
    return metrics.get_meter(_SCOPE)


def current_traceparent() -> str | None:
    """W3C traceparent of the current span, or None without one."""
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier.get("traceparent")


def context_from_traceparent(traceparent: str | None) -> Context:
    """Context whose parent is the given span; empty (new trace) if None."""
    if not traceparent:
        return Context()
    return _propagator.extract({"traceparent": traceparent})


def metrics_payload() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST


class _ApiInstruments:
    """Created once per process; gauges read the latest stats source."""

    def __init__(self) -> None:
        self.stats: Callable[[], QueueStats] | None = None
        self.stats_failing = False
        meter = get_meter()
        self.received = meter.create_counter(
            "webhooks_received", description="Incoming webhook requests"
        )
        meter.create_observable_gauge(
            "webhook_queue_depth",
            callbacks=[self._depth],
            description="Events in non-terminal states",
        )
        meter.create_observable_gauge(
            "webhook_queue_oldest_pending_age",
            callbacks=[self._oldest_age],
            unit="s",
            description="Age of the oldest due, unclaimed work",
        )

    def _read_stats(self) -> QueueStats | None:
        """Current stats, or None; a failure is logged once per outage.

        The callbacks run on every scrape (~1/s), so letting the SDK log
        each exception would flood the logs.
        """
        if self.stats is None:
            return None
        try:
            stats = self.stats()
        except Exception as exc:
            if not self.stats_failing:
                self.stats_failing = True
                _log.warning("queue stats unavailable: %s", exc)
            return None
        self.stats_failing = False
        return stats

    def _depth(self, _options: CallbackOptions) -> Iterable[Observation]:
        stats = self._read_stats()
        if stats is None:
            return []
        return [
            Observation(stats.pending, {"status": "pending"}),
            Observation(stats.retrying, {"status": "retrying"}),
            Observation(stats.processing, {"status": "processing"}),
        ]

    def _oldest_age(self, _options: CallbackOptions) -> Iterable[Observation]:
        stats = self._read_stats()
        if stats is None:
            return []
        return [Observation(stats.oldest_due_age_ms / 1000)]


def _worker_up(_options: CallbackOptions) -> Iterable[Observation]:
    return [Observation(1)]


class _WorkerInstruments:
    def __init__(self) -> None:
        meter = get_meter()
        self.duration = meter.create_histogram(
            "webhook_processing_duration",
            unit="s",
            description="Duration of one processing attempt",
            explicit_bucket_boundaries_advisory=_DURATION_BUCKETS_S,
        )
        self.attempts = meter.create_counter(
            "webhook_attempts", description="Processing attempts by outcome"
        )
        self.lease_recoveries = meter.create_counter(
            "webhook_lease_recoveries",
            description="Expired leases taken over from dead workers",
        )
        self.stale_results = meter.create_counter(
            "webhook_stale_results",
            description="Results rejected due to an attempt mismatch",
        )
        # One series per live worker process; its identity is the
        # resource's service.instance.id, so the gauge has no attributes.
        meter.create_observable_gauge(
            "webhook_worker_up",
            callbacks=[_worker_up],
            description="1 while this worker process is running",
        )


_api_instruments: _ApiInstruments | None = None
_worker_instruments: _WorkerInstruments | None = None


def _get_api_instruments() -> _ApiInstruments:
    global _api_instruments
    with _lock:
        if _api_instruments is None:
            _api_instruments = _ApiInstruments()
        return _api_instruments


def _get_worker_instruments() -> _WorkerInstruments:
    global _worker_instruments
    with _lock:
        if _worker_instruments is None:
            _worker_instruments = _WorkerInstruments()
        return _worker_instruments


class ApiMetrics:
    """API metrics; gauges read only ``stats`` (a cache), never the DB.

    Creating a new instance replaces the stats source of the shared,
    once-per-process gauges.
    """

    def __init__(self, stats: Callable[[], QueueStats]) -> None:
        self._instruments = _get_api_instruments()
        self._instruments.stats = stats
        self._instruments.stats_failing = False

    def received(self, source: str, outcome: str) -> None:
        self._instruments.received.add(
            1, {"source": source, "outcome": outcome}
        )


class WorkerMetrics:
    def __init__(self) -> None:
        self._instruments = _get_worker_instruments()

    def processing_duration(self, seconds: float, outcome: str) -> None:
        self._instruments.duration.record(seconds, {"outcome": outcome})

    def attempt(self, outcome: str) -> None:
        self._instruments.attempts.add(1, {"outcome": outcome})

    def lease_recoveries(self, count: int) -> None:
        self._instruments.lease_recoveries.add(count)

    def stale_result(self) -> None:
        self._instruments.stale_results.add(1)

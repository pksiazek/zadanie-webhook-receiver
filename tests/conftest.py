from collections.abc import Iterator

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from tests.fakes import FakeClock
from webhook_receiver.common.db import ConnectionPool, init_schema
from webhook_receiver.common.repository import EventRepository
from webhook_receiver.common.telemetry import setup_telemetry


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def repo(tmp_path, clock) -> Iterator[EventRepository]:
    pool = ConnectionPool(tmp_path / "t.db")
    init_schema(pool.get())
    try:
        yield EventRepository(pool, clock)
    finally:
        pool.close_all()


@pytest.fixture(scope="session", autouse=True)
def _telemetry_exporter() -> InMemorySpanExporter:
    # OTel global providers can be set only once per process, so the
    # in-memory exporter must be installed before any test runs.
    exporter = InMemorySpanExporter()
    setup_telemetry("test", exporter)
    return exporter


@pytest.fixture
def span_exporter(_telemetry_exporter) -> InMemorySpanExporter:
    _telemetry_exporter.clear()
    return _telemetry_exporter

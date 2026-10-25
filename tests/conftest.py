from collections.abc import Iterator

import pytest

from tests.fakes import FakeClock
from webhook_receiver.common.db import ConnectionPool, init_schema
from webhook_receiver.common.repository import EventRepository


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

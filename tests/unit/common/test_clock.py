import time

from tests.fakes import FakeClock
from webhook_receiver.common.clock import SystemClock


def test_system_clock_returns_epoch_milliseconds():
    before = time.time_ns() // 1_000_000
    value = SystemClock().now_ms()
    after = time.time_ns() // 1_000_000
    assert before <= value <= after


def test_fake_clock_advances():
    clock = FakeClock(start_ms=1000)
    clock.advance(250)
    assert clock.now_ms() == 1250

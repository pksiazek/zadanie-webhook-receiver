from tests.fakes import FakeClock
from webhook_receiver.common.clock import SystemClock


def test_system_clock_returns_epoch_milliseconds():
    assert SystemClock().now_ms() > 1_700_000_000_000


def test_fake_clock_advances():
    clock = FakeClock(start_ms=1000)
    clock.advance(250)
    assert clock.now_ms() == 1250

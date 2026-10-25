import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    """Wall clock returning Unix epoch time in milliseconds."""

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

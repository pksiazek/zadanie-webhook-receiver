class FakeClock:
    """Manually advanced clock for deterministic tests."""

    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self._now_ms = start_ms

    def now_ms(self) -> int:
        return self._now_ms

    def advance(self, ms: int) -> None:
        self._now_ms += ms

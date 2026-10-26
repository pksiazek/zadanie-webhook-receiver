"""Per-source token bucket rate limiter (in-process, thread-safe)."""

import math
import threading
from collections import OrderedDict
from dataclasses import dataclass

from webhook_receiver.common.clock import Clock


@dataclass(frozen=True)
class RateDecision:
    allowed: bool
    retry_after_seconds: int


@dataclass
class _Bucket:
    tokens: float
    updated_ms: int


class RateLimiter:
    """Token bucket per source, bounded by an LRU of ``max_sources``.

    Buckets start full (``burst`` tokens) and refill at
    ``rate_per_second``. When more than ``max_sources`` sources are
    tracked, the least recently used bucket is evicted, so arbitrary
    source values cannot grow memory without bound.
    """

    def __init__(
        self,
        rate_per_second: float,
        burst: int,
        clock: Clock,
        max_sources: int,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        if max_sources < 1:
            raise ValueError("max_sources must be >= 1")
        self._rate = rate_per_second
        self._burst = burst
        self._clock = clock
        self._max_sources = max_sources
        self._buckets: OrderedDict[str, _Bucket] = OrderedDict()
        self._lock = threading.Lock()

    def check(self, source: str) -> RateDecision:
        with self._lock:
            now = self._clock.now_ms()
            bucket = self._buckets.get(source)
            if bucket is None:
                bucket = _Bucket(tokens=float(self._burst), updated_ms=now)
                self._buckets[source] = bucket
                if len(self._buckets) > self._max_sources:
                    self._buckets.popitem(last=False)
            else:
                self._buckets.move_to_end(source)
                elapsed_ms = max(0, now - bucket.updated_ms)
                bucket.tokens = min(
                    float(self._burst),
                    bucket.tokens + elapsed_ms * self._rate / 1000,
                )
                # Never move backwards: a wall-clock step back must not
                # let the same interval be credited twice.
                bucket.updated_ms = max(bucket.updated_ms, now)
            if bucket.tokens >= 1:
                bucket.tokens -= 1
                return RateDecision(allowed=True, retry_after_seconds=0)
            retry = max(1, math.ceil((1 - bucket.tokens) / self._rate))
            return RateDecision(allowed=False, retry_after_seconds=retry)

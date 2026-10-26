import threading

import pytest

from webhook_receiver.api.ratelimit import RateDecision, RateLimiter


def test_allows_burst_then_rejects(clock):
    rl = RateLimiter(50, 100, clock, 10)
    assert all(rl.check("a").allowed for _ in range(100))
    d = rl.check("a")
    assert not d.allowed and d.retry_after_seconds >= 1


def test_allowed_decision_has_zero_retry_after(clock):
    rl = RateLimiter(50, 100, clock, 10)
    assert rl.check("a") == RateDecision(allowed=True, retry_after_seconds=0)


def test_refills_over_time(clock):
    rl = RateLimiter(50, 100, clock, 10)
    for _ in range(100):
        rl.check("a")
    assert not rl.check("a").allowed
    clock.advance(1000)
    assert all(rl.check("a").allowed for _ in range(50))
    assert not rl.check("a").allowed


def test_partial_refill_grants_whole_tokens_only(clock):
    rl = RateLimiter(10, 1, clock, 10)
    assert rl.check("a").allowed
    clock.advance(50)  # half a token
    assert not rl.check("a").allowed
    clock.advance(50)  # now a full token
    assert rl.check("a").allowed


def test_refill_is_capped_at_burst(clock):
    rl = RateLimiter(50, 100, clock, 10)
    rl.check("a")
    clock.advance(60_000)
    assert all(rl.check("a").allowed for _ in range(100))
    assert not rl.check("a").allowed


def test_retry_after_is_time_to_next_token(clock):
    rl = RateLimiter(0.25, 1, clock, 10)
    assert rl.check("a").allowed
    assert rl.check("a") == RateDecision(allowed=False, retry_after_seconds=4)
    clock.advance(1000)  # 0.25 tokens, 0.75 missing -> 3 s
    assert rl.check("a").retry_after_seconds == 3
    clock.advance(2500)  # 0.875 tokens, 0.125 missing -> 0.5 s -> 1
    assert rl.check("a").retry_after_seconds == 1


def test_retry_after_is_at_least_one_second(clock):
    rl = RateLimiter(50, 1, clock, 10)
    rl.check("a")
    assert rl.check("a").retry_after_seconds == 1


def test_sources_are_independent(clock):
    rl = RateLimiter(1, 2, clock, 10)
    assert rl.check("a").allowed
    assert rl.check("a").allowed
    assert not rl.check("a").allowed
    assert rl.check("b").allowed
    assert rl.check("b").allowed
    assert not rl.check("b").allowed


def test_lru_bounds_memory(clock):
    rl = RateLimiter(1, 1, clock, 2)
    for s in "abc":
        rl.check(s)
    assert len(rl._buckets) == 2


def test_lru_evicts_least_recently_used(clock):
    rl = RateLimiter(1, 1, clock, 2)
    rl.check("a")
    rl.check("b")
    rl.check("a")  # touches "a" (even though denied)
    rl.check("c")
    assert list(rl._buckets) == ["a", "c"]
    # "a" kept its drained bucket; evicted "b" starts fresh.
    assert not rl.check("a").allowed
    assert rl.check("b").allowed


def test_concurrent_checks_never_exceed_burst(clock):
    rl = RateLimiter(1, 100, clock, 10)
    n_threads, per_thread = 8, 50
    barrier = threading.Barrier(n_threads, timeout=10)
    allowed = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        count = sum(rl.check("a").allowed for _ in range(per_thread))
        with lock:
            allowed.append(count)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
        assert not t.is_alive()
    assert sum(allowed) == 100


class _SteppingClock:
    """Clock that can be set to any time, including backwards."""

    def __init__(self, now_ms: int) -> None:
        self.now = now_ms

    def now_ms(self) -> int:
        return self.now


def test_clock_step_back_does_not_credit_tokens_twice():
    t0 = 1_700_000_000_000
    c = _SteppingClock(t0)
    rl = RateLimiter(1, 1, c, 10)
    assert rl.check("a").allowed  # bucket empty at t0
    c.now = t0 + 500
    assert not rl.check("a").allowed  # 0.5 tokens
    c.now = t0  # wall clock steps back
    assert not rl.check("a").allowed
    c.now = t0 + 500  # back to the original time: nothing new elapsed
    assert not rl.check("a").allowed
    c.now = t0 + 1000  # monotonic sequence would allow here
    assert rl.check("a").allowed


@pytest.mark.parametrize(
    ("rate", "burst", "max_sources"),
    [(0, 1, 1), (-1, 1, 1), (1, 0, 1), (1, 1, 0)],
)
def test_rejects_invalid_arguments(clock, rate, burst, max_sources):
    with pytest.raises(ValueError):
        RateLimiter(rate, burst, clock, max_sources)

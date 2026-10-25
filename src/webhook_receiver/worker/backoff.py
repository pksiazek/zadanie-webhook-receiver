"""Exponential retry backoff with jitter (spec section 8)."""

from random import Random


def backoff_ms(attempt: int, base_ms: int, max_ms: int, rng: Random) -> int:
    """Return the delay before the next attempt, in milliseconds.

    The delay is ``min(base_ms * 2 ** (attempt - 1), max_ms)`` scaled by a
    random jitter factor in ``[0.8, 1.2]``.
    """
    delay = min(base_ms * 2 ** (attempt - 1), max_ms)
    return round(delay * rng.uniform(0.8, 1.2))

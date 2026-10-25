"""Exponential retry backoff with jitter (spec section 8)."""

from random import Random


def backoff_ms(attempt: int, base_ms: int, max_ms: int, rng: Random) -> int:
    """Return the delay before the next attempt, in milliseconds.

    The delay is ``min(base_ms * 2 ** (attempt - 1), max_ms)`` scaled by a
    random jitter factor in ``[0.8, 1.2]``. ``attempt`` must be >= 1.
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    # 2 ** max_ms.bit_length() > max_ms, so for base_ms >= 1 a larger
    # exponent cannot change the capped result; clamping it avoids building
    # a huge integer for absurd attempt numbers.
    exponent = min(attempt - 1, max_ms.bit_length())
    delay = min(base_ms * 2**exponent, max_ms)
    return round(delay * rng.uniform(0.8, 1.2))

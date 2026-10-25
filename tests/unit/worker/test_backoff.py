from random import Random

import pytest

from webhook_receiver.worker.backoff import backoff_ms


def test_backoff_grows_exponentially_within_jitter():
    rng = Random(0)
    assert 1600 <= backoff_ms(1, 2000, 60_000, rng) <= 2400
    assert 3200 <= backoff_ms(2, 2000, 60_000, rng) <= 4800
    assert 6400 <= backoff_ms(3, 2000, 60_000, rng) <= 9600


def test_backoff_is_capped():
    assert backoff_ms(20, 2000, 60_000, Random(0)) <= 72_000


def test_backoff_cap_applies_before_jitter():
    for seed in range(50):
        value = backoff_ms(20, 2000, 60_000, Random(seed))
        assert 48_000 <= value <= 72_000


def test_backoff_returns_int():
    assert isinstance(backoff_ms(1, 2000, 60_000, Random(0)), int)


def test_backoff_is_deterministic_for_seeded_rng():
    assert backoff_ms(3, 2000, 60_000, Random(42)) == backoff_ms(
        3, 2000, 60_000, Random(42)
    )


def test_backoff_jitter_matches_formula():
    expected = round(4000 * Random(7).uniform(0.8, 1.2))
    assert backoff_ms(2, 2000, 60_000, Random(7)) == expected


@pytest.mark.parametrize("attempt", [0, -1])
def test_backoff_rejects_attempt_below_one(attempt):
    with pytest.raises(ValueError, match="attempt"):
        backoff_ms(attempt, 2000, 60_000, Random(0))


def test_backoff_huge_attempt_is_capped():
    value = backoff_ms(10_000, 2000, 60_000, Random(0))
    assert 48_000 <= value <= 72_000

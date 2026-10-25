from random import Random

import pytest

from webhook_receiver.worker.simulation import SimulatedFailure, simulate


class FakeSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


async def test_simulate_fail_once_only_first_attempt():
    sleep = FakeSleep()
    with pytest.raises(SimulatedFailure, match="fail_once"):
        await simulate({"simulate": "fail_once"}, 1, 2, 5, Random(0), sleep)
    result = await simulate(
        {"simulate": "fail_once"}, 2, 2, 5, Random(0), sleep
    )
    assert "duration_ms" in result
    assert len(sleep.calls) == 2
    assert all(2 <= s <= 5 for s in sleep.calls)


async def test_simulate_fail_once_message_is_verbatim():
    with pytest.raises(SimulatedFailure) as exc_info:
        await simulate(
            {"simulate": "fail_once"}, 1, 2, 5, Random(0), FakeSleep()
        )
    assert str(exc_info.value) == "simulated failure (fail_once)"


async def test_simulate_fail_always():
    sleep = FakeSleep()
    for attempt in (1, 2, 5):
        with pytest.raises(SimulatedFailure) as exc_info:
            await simulate(
                {"simulate": "fail_always"}, attempt, 2, 5, Random(0), sleep
            )
        assert str(exc_info.value) == "simulated failure (fail_always)"
    assert len(sleep.calls) == 3


async def test_simulate_success_without_trigger():
    sleep = FakeSleep()
    result = await simulate({"order": 1}, 1, 2, 5, Random(0), sleep)
    assert result == {"duration_ms": round(sleep.calls[0] * 1000)}
    assert isinstance(result["duration_ms"], int)
    assert 2 <= sleep.calls[0] <= 5


async def test_simulate_ignores_unknown_trigger():
    result = await simulate(
        {"simulate": "something_else"}, 1, 2, 5, Random(0), FakeSleep()
    )
    assert "duration_ms" in result


async def test_simulate_sleep_duration_comes_from_rng():
    sleep = FakeSleep()
    await simulate({}, 1, 2, 5, Random(3), sleep)
    assert sleep.calls == [Random(3).uniform(2, 5)]


@pytest.mark.parametrize("trigger", [1, None, ["fail_always"]])
async def test_simulate_ignores_non_string_trigger(trigger):
    result = await simulate(
        {"simulate": trigger}, 1, 2, 5, Random(0), FakeSleep()
    )
    assert "duration_ms" in result


def test_simulated_failure_is_exception():
    assert issubclass(SimulatedFailure, Exception)

"""Simulated event processing with deterministic failure triggers."""

import asyncio
from collections.abc import Awaitable, Callable
from random import Random
from typing import Any


# Name fixed by the task interface (consumed by the worker loop).
class SimulatedFailure(Exception):  # noqa: N818
    """Raised when the payload requests a simulated processing failure."""


async def simulate(
    payload: dict[str, Any],
    attempt: int,
    min_seconds: float,
    max_seconds: float,
    rng: Random,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    """Pretend to process an event, honouring ``payload["simulate"]``.

    Sleeps for a random duration in ``[min_seconds, max_seconds]``, then
    fails on the first attempt for ``"fail_once"``, on every attempt for
    ``"fail_always"``, and otherwise returns the simulated duration.
    """
    seconds = rng.uniform(min_seconds, max_seconds)
    await sleep(seconds)
    trigger = payload.get("simulate")
    if trigger == "fail_once" and attempt == 1:
        raise SimulatedFailure("simulated failure (fail_once)")
    if trigger == "fail_always":
        raise SimulatedFailure("simulated failure (fail_always)")
    return {"duration_ms": round(seconds * 1000)}

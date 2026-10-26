"""Pure statistics and pass/fail logic of the load test (spec §13).

Everything here is free of I/O so it can be unit tested; ``run`` does
the HTTP traffic and calls into this module.
"""

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

_DEPTH_LINE = re.compile(
    r"^webhook_queue_depth\{(?P<labels>[^}]*)\}\s+(?P<value>\S+)"
)
_STATUS_LABEL = re.compile(r'(?:^|,)\s*status="(?P<status>[^"]*)"')


def percentile(values: Sequence[float], p: float) -> float:
    """Nearest-rank percentile: the smallest value covering ``p`` %."""
    if not values:
        raise ValueError("percentile of an empty sequence")
    if not 0 <= p <= 100:
        raise ValueError("p must be within [0, 100]")
    ordered = sorted(values)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def parse_queue_depth(metrics_text: str) -> dict[str, float]:
    """``webhook_queue_depth`` values by ``status`` from a /metrics page."""
    depth: dict[str, float] = {}
    for line in metrics_text.splitlines():
        match = _DEPTH_LINE.match(line)
        if match is None:
            continue
        status = _STATUS_LABEL.search(match["labels"])
        if status is None:
            continue
        depth[status["status"]] = float(match["value"])
    return depth


def backlog(
    depth: Mapping[str, float], include_processing: bool = False
) -> float:
    """Queued work: ``pending + retrying`` (+ ``processing`` if asked)."""
    statuses = ["pending", "retrying"]
    if include_processing:
        statuses.append("processing")
    return sum(depth.get(status, 0.0) for status in statuses)


def linearity(throughputs: Mapping[int, float]) -> dict[int, float]:
    """``t[n] / (n * t[1])`` for every measured worker count ``n``."""
    base = throughputs.get(1)
    if base is None:
        raise ValueError("linearity needs the 1-worker result")
    if base <= 0:
        raise ValueError("1-worker throughput must be positive")
    return {n: t / (n * base) for n, t in sorted(throughputs.items())}


def completion_rate_per_min(points: Sequence[tuple[float, float]]) -> float:
    """Events finished per minute from ``(seconds, queue size)`` samples.

    Least-squares slope of the falling queue, negated; robust against
    the API's 1 s stats cache repeating a value now and then.
    """
    if len(points) < 2:
        raise ValueError("need at least two samples")
    n = len(points)
    mean_t = sum(t for t, _ in points) / n
    mean_q = sum(q for _, q in points) / n
    var_t = sum((t - mean_t) ** 2 for t, _ in points)
    if var_t == 0:
        raise ValueError("samples must span some time")
    cov = sum((t - mean_t) * (q - mean_q) for t, q in points)
    return -cov / var_t * 60


class ZeroStreak:
    """True once ``required`` consecutive observations were zero.

    The API refreshes queue stats once per second, so a single zero
    sample may be stale; requiring a streak tolerates that.
    """

    def __init__(self, required: int = 2) -> None:
        self._required = required
        self._streak = 0

    def observe(self, value: float) -> bool:
        self._streak = self._streak + 1 if value == 0 else 0
        return self._streak >= self._required


def latency_summary(latencies: Sequence[float]) -> dict[str, float]:
    """count, p50/p95/p99 and max of latencies (seconds)."""
    if not latencies:
        return {"count": 0}
    return {
        "count": len(latencies),
        "p50": percentile(latencies, 50),
        "p95": percentile(latencies, 95),
        "p99": percentile(latencies, 99),
        "max": max(latencies),
    }


@dataclass
class PhaseResult:
    """Outcome of one phase of a scenario."""

    name: str
    latencies: list[float] = field(default_factory=list)
    codes: dict[int, int] = field(default_factory=dict)
    errors: int = 0
    duration_s: float = 0.0
    limit_s: float | None = None
    reached_end: bool = True
    check_p99: bool = True
    max_lag_s: float = 0.0


def format_ms(seconds: float) -> str:
    """Seconds as a compact millisecond string: ``0.5 ms``, ``100 ms``."""
    return f"{seconds * 1000:g} ms"


def phase_failures(phase: PhaseResult, p99_limit_s: float) -> list[str]:
    """Human-readable reasons why ``phase`` failed (empty = PASS)."""
    failures = []
    if phase.check_p99 and phase.latencies:
        p99 = percentile(phase.latencies, 99)
        if p99 >= p99_limit_s:
            failures.append(
                f"{phase.name}: p99 {p99 * 1000:.1f} ms"
                f" >= {format_ms(p99_limit_s)}"
            )
    non_2xx = sum(n for code, n in phase.codes.items() if code // 100 != 2)
    if non_2xx:
        failures.append(f"{phase.name}: {non_2xx} non-2xx responses")
    if phase.errors:
        failures.append(f"{phase.name}: {phase.errors} transport errors")
    if not phase.reached_end:
        failures.append(
            f"{phase.name}: end condition not reached within"
            f" limit {phase.limit_s:.0f} s"
        )
    return failures


def flood_result(
    samples: Sequence[tuple[float, Mapping[str, float]]],
    window: tuple[float, float],
    codes: Mapping[int, int],
    errors: int,
    count: int,
    workers: int,
) -> dict[str, Any]:
    """Throughput and validity of one ``flood`` measurement.

    ``samples`` are ``(seconds, queue depth)`` pairs; only those inside
    ``window`` are used. Throughput is the drain rate of
    ``pending + retrying + processing``. The measurement is valid only
    if every event was accepted (a 429 retried into a 2xx counts once,
    as accepted) and ``pending + retrying`` never hit zero in the
    window: an empty queue starves the workers and understates the rate.
    """
    start, end = window
    inside = [(t, d) for t, d in samples if start <= t <= end]
    points = [(t, backlog(d, include_processing=True)) for t, d in inside]
    rejected = sum(n for c, n in codes.items() if c // 100 != 2 and c != 429)
    accepted = sum(n for c, n in codes.items() if c // 100 == 2)
    queued_at_end = backlog(inside[-1][1]) if inside else 0.0
    starved = any(backlog(d) == 0 for _, d in inside)
    enough = len(points) >= 2
    valid = (
        enough
        and not starved
        and accepted == count
        and rejected == 0
        and errors == 0
    )
    return {
        "workers": workers,
        "throughput_per_min": (
            round(completion_rate_per_min(points), 1) if enough else None
        ),
        "valid": valid,
        "count": count,
        "accepted": accepted,
        "rejected_other": rejected,
        "transport_errors": errors,
        "window_s": [round(start, 1), round(end, 1)],
        "samples_in_window": len(points),
        "queue_at_window_start": points[0][1] if points else None,
        "queue_at_window_end": points[-1][1] if points else None,
        "pending_retrying_at_end": queued_at_end,
    }


def scaling_report(
    throughputs: Mapping[int, float],
    min_linearity: float,
    invalid: Mapping[int, str] | None = None,
    errors: Sequence[str] = (),
) -> tuple[str, bool]:
    """Table N / throughput / linearity and whether every N passes.

    ``invalid`` maps worker counts whose measurement is unusable to a
    reason; they are shown as FAIL rows and fail the report. ``errors``
    (e.g. unreadable result files) are listed and fail it as well.
    """
    invalid = dict(invalid or {})
    lines = [
        f"{'workers':>7}  {'throughput/min':>14}  {'linear/min':>10}"
        f"  {'linearity':>9}  result",
    ]
    base = throughputs.get(1)
    passed = not invalid and not errors and base is not None and base > 0
    ratios = linearity(throughputs) if base is not None and base > 0 else {}
    for n in sorted(set(throughputs) | set(invalid)):
        if n in invalid:
            lines.append(
                f"{n:>7}  {'-':>14}  {'-':>10}  {'-':>9}  FAIL ({invalid[n]})"
            )
        elif n in ratios:
            ok = ratios[n] >= min_linearity
            passed = passed and ok
            lines.append(
                f"{n:>7}  {throughputs[n]:>14.1f}  {n * base:>10.1f}"
                f"  {ratios[n] * 100:>8.1f}%  {'PASS' if ok else 'FAIL'}"
            )
        else:
            lines.append(
                f"{n:>7}  {throughputs[n]:>14.1f}  {'-':>10}  {'-':>9}  -"
            )
    if not ratios:
        lines.append("FAIL no valid 1-worker baseline")
    lines.extend(f"FAIL {error}" for error in errors)
    verdict = "PASS" if passed else "FAIL"
    lines.append(
        f"scaling: {verdict} (every N >= {min_linearity * 100:.0f}% linear)"
    )
    return "\n".join(lines), passed

"""Open-model load generator: ``steady``, ``burst``, ``flood`` (spec §13).

Requests are sent on a fixed schedule, each in its own task, so a slow
response never delays the next send. Queue depth is sampled from the
API's ``/metrics`` once per second.

Output: the final report (``steady``, ``burst``, ``scaling-report``) and
the JSON result of ``flood`` go to stdout; progress goes to stderr.
Payloads are never printed.
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from loadtest.stats import (
    PhaseResult,
    ZeroStreak,
    backlog,
    completion_rate_per_min,
    latency_summary,
    parse_queue_depth,
    percentile,
    phase_failures,
    scaling_report,
)

DEFAULT_BASE_URL = "http://localhost:8000"
# Generous client-side pool: requests must never queue in the client.
LIMITS = httpx.Limits(max_connections=1000, max_keepalive_connections=500)
TIMEOUT = httpx.Timeout(30.0, connect=10.0)
GET_CONCURRENCY = 50
# A scheduler lag above this is reported (generator, not API, is slow).
LAG_WARN_S = 0.010
QUEUE_TRACE_EVERY_S = 10


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", file=sys.stderr)


@dataclass
class Sample:
    t: float
    depth: dict[str, float]


@dataclass
class Sampler:
    """Polls ``/metrics`` every ``interval`` seconds in the background."""

    client: httpx.AsyncClient
    t0: float
    interval: float = 1.0
    samples: list[Sample] = field(default_factory=list)
    failures: int = 0

    def __post_init__(self) -> None:
        self._new = asyncio.Event()

    async def run(self) -> None:
        next_at = time.perf_counter()
        while True:
            try:
                resp = await self.client.get("/metrics")
                depth = parse_queue_depth(resp.text)
                if depth:
                    now = time.perf_counter() - self.t0
                    self.samples.append(Sample(now, depth))
                    self._new.set()
                    self._new = asyncio.Event()
            except httpx.HTTPError:
                self.failures += 1
            next_at += self.interval
            await asyncio.sleep(max(0.0, next_at - time.perf_counter()))

    async def next_sample(self) -> Sample:
        await self._new.wait()
        return self.samples[-1]


class Sender:
    """Sends uniquely keyed webhooks round-robin over the sources."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        sources: int,
        retry_429: bool = False,
    ) -> None:
        self.client = client
        self.run_id = uuid.uuid4().hex[:12]
        self.sources = [f"loadtest-{i + 1}" for i in range(sources)]
        self.retry_429 = retry_429
        self.keys: list[str] = []
        self.retries = 0
        self.tasks: set[asyncio.Task[None]] = set()

    def send(self, phase: PhaseResult, lag: float) -> None:
        seq = len(self.keys)
        key = f"lt-{self.run_id}-{seq}"
        self.keys.append(key)
        source = self.sources[seq % len(self.sources)]
        phase.max_lag_s = max(phase.max_lag_s, lag)
        task = asyncio.create_task(self._post(phase, key, source, seq))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _post(
        self, phase: PhaseResult, key: str, source: str, seq: int
    ) -> None:
        body = {
            "idempotency_key": key,
            "event_type": "loadtest",
            "payload": {"seq": seq},
        }
        while True:
            start = time.perf_counter()
            try:
                resp = await self.client.post(
                    "/webhooks", json=body, headers={"X-Source": source}
                )
            except httpx.HTTPError:
                phase.errors += 1
                return
            phase.latencies.append(time.perf_counter() - start)
            phase.codes[resp.status_code] = (
                phase.codes.get(resp.status_code, 0) + 1
            )
            if not (self.retry_429 and resp.status_code == 429):
                return
            self.retries += 1
            await asyncio.sleep(float(resp.headers.get("Retry-After", "1")))

    async def drain(self) -> None:
        while self.tasks:
            await asyncio.gather(*list(self.tasks))


async def open_loop(
    sender: Sender,
    phase: PhaseResult,
    rate_per_s: float,
    duration_s: float | None = None,
    count: int | None = None,
) -> None:
    """Send at a fixed rate for ``duration_s`` or ``count`` requests.

    Runs until cancelled when neither bound is given.
    """
    if rate_per_s <= 0:
        return
    start = time.perf_counter()
    i = 0
    while True:
        offset = i / rate_per_s
        if duration_s is not None and offset >= duration_s:
            return
        if count is not None and i >= count:
            return
        due = start + offset
        delay = due - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        sender.send(phase, max(0.0, time.perf_counter() - due))
        i += 1


async def wait_until(
    sampler: Sampler, done: Callable[[dict[str, float]], bool], limit: float
) -> bool:
    """Wait for a sample satisfying ``done``; False after ``limit`` s."""

    async def watch() -> None:
        while not done((await sampler.next_sample()).depth):
            pass

    try:
        await asyncio.wait_for(watch(), timeout=max(0.0, limit))
    except TimeoutError:
        return False
    return True


async def run_phase(
    sender: Sender,
    sampler: Sampler,
    phase: PhaseResult,
    rate_per_s: float,
    done: Callable[[dict[str, float]], bool] | None,
) -> None:
    """Send at ``rate_per_s`` until ``done`` holds (or for ``limit_s``)."""
    log(f"phase {phase.name}: {rate_per_s * 60:.0f}/min")
    start = time.perf_counter()
    limit = phase.limit_s if phase.limit_s is not None else float("inf")
    if done is None:
        await open_loop(sender, phase, rate_per_s, duration_s=limit)
    else:
        loop = asyncio.create_task(open_loop(sender, phase, rate_per_s))
        phase.reached_end = await wait_until(sampler, done, limit)
        loop.cancel()
    phase.duration_s = time.perf_counter() - start
    log(
        f"phase {phase.name}: {phase.duration_s:.1f} s,"
        f" end reached: {phase.reached_end}"
    )


def drained() -> Callable[[dict[str, float]], bool]:
    """End condition: queue incl. ``processing`` zero in 2 samples."""
    streak = ZeroStreak(required=2)
    return lambda depth: streak.observe(
        backlog(depth, include_processing=True)
    )


async def verify_completed(
    client: httpx.AsyncClient, keys: Sequence[str]
) -> Counter[str]:
    """Status of every sent key (``GET`` is not rate limited)."""
    statuses: Counter[str] = Counter()
    semaphore = asyncio.Semaphore(GET_CONCURRENCY)

    async def check(key: str) -> None:
        async with semaphore:
            try:
                resp = await client.get(f"/webhooks/{key}")
            except httpx.HTTPError:
                statuses["error"] += 1
                return
        if resp.status_code != 200:
            statuses[f"http_{resp.status_code}"] += 1
        else:
            statuses[resp.json()["status"]] += 1

    await asyncio.gather(*(check(key) for key in keys))
    return statuses


def _ms(seconds: float) -> str:
    return f"{seconds * 1000:.1f}"


def format_report(
    scenario: str,
    params: dict[str, Any],
    phases: Sequence[PhaseResult],
    samples: Sequence[Sample],
    statuses: Counter[str],
    sent: int,
    failures: Sequence[str],
    p99_limit_s: float,
) -> str:
    lines = [f"scenario: {scenario}", f"params: {json.dumps(params)}", ""]
    lines.append(
        "phase      duration_s  limit_s  end   requests   p50_ms   p95_ms"
        "   p99_ms   max_ms  max_lag_ms  codes"
    )
    for p in phases:
        s = latency_summary(p.latencies)
        lat = (
            f"{_ms(s['p50']):>8} {_ms(s['p95']):>8} {_ms(s['p99']):>8}"
            f" {_ms(s['max']):>8}"
            if s["count"]
            else f"{'-':>8} {'-':>8} {'-':>8} {'-':>8}"
        )
        limit = f"{p.limit_s:.0f}" if p.limit_s is not None else "-"
        codes = ",".join(f"{c}:{n}" for c, n in sorted(p.codes.items()))
        if p.errors:
            codes += f",errors:{p.errors}"
        lines.append(
            f"{p.name:<10} {p.duration_s:>10.1f} {limit:>8}"
            f"  {'yes' if p.reached_end else 'NO':<4} {s['count']:>8}"
            f" {lat} {_ms(p.max_lag_s):>11}  {codes or '-'}"
        )
    all_latencies = [x for p in phases for x in p.latencies]
    if all_latencies:
        s = latency_summary(all_latencies)
        lines.append(
            f"all POST: n={s['count']} p50={_ms(s['p50'])} ms"
            f" p95={_ms(s['p95'])} ms p99={_ms(s['p99'])} ms"
            f" max={_ms(s['max'])} ms"
        )
    lags = [p.max_lag_s for p in phases]
    if lags and max(lags) > LAG_WARN_S:
        lines.append(
            f"note: generator scheduling lag up to {_ms(max(lags))} ms"
        )
    total = sum(p.duration_s for p in phases)
    lines.append(f"total duration: {total:.1f} s")
    lines.append("")
    if samples:
        peak = max(samples, key=lambda x: backlog(x.depth))
        lines.append(
            f"queue peak (pending+retrying): {backlog(peak.depth):.0f}"
            f" at t={peak.t:.0f} s"
        )
    lines.append("queue depth over time (every ~10 s):")
    lines.append("     t_s  pending  retrying  processing")
    next_t = 0.0
    for i, sample in enumerate(samples):
        if sample.t >= next_t or i == len(samples) - 1:
            d = sample.depth
            lines.append(
                f"{sample.t:>8.0f} {d.get('pending', 0):>8.0f}"
                f" {d.get('retrying', 0):>9.0f}"
                f" {d.get('processing', 0):>11.0f}"
            )
            next_t = sample.t + QUEUE_TRACE_EVERY_S
    lines.append("")
    lines.append(
        f"completeness: sent={sent} "
        + " ".join(f"{k}={v}" for k, v in sorted(statuses.items()))
    )
    lines.append("")
    lines.append(f"criteria: p99 POST < {p99_limit_s * 1000:.0f} ms,")
    lines.append("  zero non-2xx, phase limits, every key completed")
    for failure in failures:
        lines.append(f"  FAIL {failure}")
    lines.append(f"result: {'FAIL' if failures else 'PASS'}")
    return "\n".join(lines)


async def run_scenario(
    args: argparse.Namespace,
    scenario: str,
    plan: Callable[[Sender, Sampler], Any],
    total_limit_s: float | None = None,
) -> int:
    async with httpx.AsyncClient(
        base_url=args.base_url, limits=LIMITS, timeout=TIMEOUT
    ) as client:
        sender = Sender(client, args.sources)
        t0 = time.perf_counter()
        sampler = Sampler(client, t0)
        sampling = asyncio.create_task(sampler.run())
        log(f"{scenario}: run id {sender.run_id}, {args.base_url}")
        phases: list[PhaseResult] = await plan(sender, sampler)
        await sender.drain()
        elapsed = time.perf_counter() - t0
        sampling.cancel()
        log(f"verifying {len(sender.keys)} keys")
        statuses = await verify_completed(client, sender.keys)

    failures = [
        f for p in phases for f in phase_failures(p, args.p99_ms / 1000)
    ]
    if total_limit_s is not None and elapsed > total_limit_s:
        failures.append(
            f"total {elapsed:.0f} s > hard limit {total_limit_s:.0f} s"
        )
    not_done = len(sender.keys) - statuses.get("completed", 0)
    if not_done:
        failures.append(f"{not_done} keys not completed")
    params = {
        k: v for k, v in vars(args).items() if k not in ("func", "command")
    }
    print(
        format_report(
            scenario,
            params,
            phases,
            sampler.samples,
            statuses,
            len(sender.keys),
            failures,
            args.p99_ms / 1000,
        )
    )
    return 1 if failures else 0


async def steady(args: argparse.Namespace) -> int:
    async def plan(sender: Sender, sampler: Sampler) -> list[PhaseResult]:
        intake = PhaseResult("intake", limit_s=args.minutes * 60)
        await run_phase(sender, sampler, intake, args.rate / 60, None)
        await sender.drain()
        drain = PhaseResult(
            "drain", limit_s=args.drain_timeout, check_p99=False
        )
        await run_phase(sender, sampler, drain, 0, drained())
        return [intake, drain]

    return await run_scenario(args, "steady", plan)


async def burst(args: argparse.Namespace) -> int:
    async def plan(sender: Sender, sampler: Sampler) -> list[PhaseResult]:
        start = time.perf_counter()

        def remaining(limit: float) -> float:
            left = args.total_limit - (time.perf_counter() - start)
            return max(0.0, min(limit, left))

        p1 = PhaseResult("1-burst", limit_s=args.burst_minutes * 60)
        await run_phase(sender, sampler, p1, args.burst_rate / 60, None)
        p2 = PhaseResult(
            "2-low", limit_s=remaining(args.low_limit), check_p99=False
        )
        await run_phase(
            sender,
            sampler,
            p2,
            args.low_rate / 60,
            lambda depth: backlog(depth) <= args.low_until,
        )
        await sender.drain()
        p3 = PhaseResult(
            "3-idle", limit_s=remaining(args.idle_limit), check_p99=False
        )
        await run_phase(sender, sampler, p3, 0, drained())
        return [p1, p2, p3]

    return await run_scenario(
        args, "burst", plan, total_limit_s=args.total_limit
    )


async def flood(args: argparse.Namespace) -> int:
    async with httpx.AsyncClient(
        base_url=args.base_url, limits=LIMITS, timeout=TIMEOUT
    ) as client:
        sender = Sender(client, args.sources, retry_429=True)
        t0 = time.perf_counter()
        sampler = Sampler(client, t0)
        sampling = asyncio.create_task(sampler.run())
        log(f"flood: {args.count} events at {args.rate}/s ({sender.run_id})")
        phase = PhaseResult("flood", check_p99=False)
        await open_loop(sender, phase, args.rate, count=args.count)
        await sender.drain()
        sent_at = time.perf_counter() - t0
        log(f"all accepted after {sent_at:.1f} s; warmup {args.warmup} s")
        await asyncio.sleep(args.warmup)
        window_start = time.perf_counter() - t0
        log(f"measuring for {args.measure} s")
        await asyncio.sleep(args.measure)
        window_end = time.perf_counter() - t0
        sampling.cancel()

    window = [
        (s.t, backlog(s.depth, include_processing=True))
        for s in sampler.samples
        if window_start <= s.t <= window_end
    ]
    rejected = sum(
        n for c, n in phase.codes.items() if c // 100 != 2 and c != 429
    )
    accepted = sum(n for c, n in phase.codes.items() if c // 100 == 2)
    # The queue must stay non-empty for the whole window, otherwise the
    # workers ran out of work and the rate is underestimated.
    queued_at_end = (
        backlog(sampler.samples[-1].depth) if sampler.samples else 0.0
    )
    valid = (
        len(window) >= 2
        and queued_at_end > 0
        and accepted == args.count
        and rejected == 0
        and phase.errors == 0
    )
    result = {
        "workers": args.workers,
        "throughput_per_min": (
            round(completion_rate_per_min(window), 1)
            if len(window) >= 2
            else None
        ),
        "valid": valid,
        "count": args.count,
        "accepted": accepted,
        "retried_429": sender.retries,
        "rejected_other": rejected,
        "transport_errors": phase.errors,
        "all_sent_after_s": round(sent_at, 1),
        "window_s": [round(window_start, 1), round(window_end, 1)],
        "samples_in_window": len(window),
        "queue_at_window_start": window[0][1] if window else None,
        "queue_at_window_end": window[-1][1] if window else None,
        "pending_retrying_at_end": queued_at_end,
        "post_p99_ms": (
            round(percentile(phase.latencies, 99) * 1000, 1)
            if phase.latencies
            else None
        ),
    }
    print(json.dumps(result))
    log(f"throughput {result['throughput_per_min']}/min, valid={valid}")
    return 0 if valid else 1


def report_scaling(args: argparse.Namespace) -> int:
    throughputs: dict[int, float] = {}
    invalid = []
    for path in args.files:
        data = json.loads(Path(path).read_text())
        throughputs[int(data["workers"])] = float(data["throughput_per_min"])
        if not data.get("valid", False):
            invalid.append(path)
    text, passed = scaling_report(throughputs, args.min_linearity)
    print(text)
    for path in invalid:
        print(f"FAIL {path}: measurement marked invalid")
    return 0 if passed and not invalid else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="loadtest")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--base-url",
            default=os.environ.get("BASE_URL", DEFAULT_BASE_URL),
        )
        p.add_argument("--sources", type=int, default=5)
        p.add_argument("--p99-ms", type=float, default=100.0)

    p = sub.add_parser("steady", help="1000/min, then drain")
    common(p)
    p.add_argument("--rate", type=float, default=1000.0, help="per minute")
    p.add_argument("--minutes", type=float, default=3.0)
    p.add_argument("--drain-timeout", type=float, default=120.0)
    p.set_defaults(func=steady)

    p = sub.add_parser("burst", help="3000/min, 100/min, 0 (spec §13)")
    common(p)
    p.add_argument("--burst-rate", type=float, default=3000.0)
    p.add_argument("--burst-minutes", type=float, default=2.0)
    p.add_argument("--low-rate", type=float, default=100.0)
    p.add_argument("--low-until", type=float, default=100.0)
    p.add_argument("--low-limit", type=float, default=600.0)
    p.add_argument("--idle-limit", type=float, default=120.0)
    p.add_argument("--total-limit", type=float, default=900.0)
    p.set_defaults(func=burst)

    p = sub.add_parser("flood", help="fill the queue, measure drain rate")
    common(p)
    p.add_argument("--count", type=int, default=3000)
    p.add_argument("--rate", type=float, default=200.0, help="per second")
    p.add_argument("--warmup", type=float, default=10.0)
    p.add_argument("--measure", type=float, default=120.0)
    p.add_argument("--workers", type=int, required=True)
    p.set_defaults(func=flood)

    p = sub.add_parser("scaling-report", help="table from flood results")
    p.add_argument("files", nargs="+")
    p.add_argument("--min-linearity", type=float, default=0.85)
    p.set_defaults(func=report_scaling)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.func is report_scaling:
        return report_scaling(args)
    return asyncio.run(args.func(args))


if __name__ == "__main__":
    sys.exit(main())

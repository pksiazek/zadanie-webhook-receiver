import asyncio
import io
import json
import time
from collections import Counter

import httpx
import pytest

from loadtest import run
from loadtest.stats import PhaseResult


def test_parser_defaults_follow_spec(monkeypatch):
    monkeypatch.delenv("BASE_URL", raising=False)
    parser = run.build_parser()
    steady = parser.parse_args(["steady"])
    assert (steady.rate, steady.minutes, steady.drain_timeout) == (
        1000.0,
        3.0,
        120.0,
    )
    assert steady.base_url == "http://localhost:8000"
    assert steady.sources == 5
    assert steady.p99_ms == 100.0
    burst = parser.parse_args(["burst"])
    assert (burst.burst_rate, burst.burst_minutes) == (3000.0, 2.0)
    assert (burst.low_rate, burst.low_until, burst.low_limit) == (
        100.0,
        100.0,
        600.0,
    )
    assert (burst.idle_limit, burst.total_limit) == (120.0, 900.0)
    flood = parser.parse_args(["flood", "--workers", "2"])
    assert (flood.count, flood.rate, flood.warmup, flood.measure) == (
        3000,
        200.0,
        10.0,
        120.0,
    )


def test_base_url_from_environment(monkeypatch):
    monkeypatch.setenv("BASE_URL", "http://api:8000")
    assert run.build_parser().parse_args(["steady"]).base_url == (
        "http://api:8000"
    )


def _write(tmp_path, workers, throughput, valid=True):
    path = tmp_path / f"scaling-{workers}.json"
    path.write_text(
        json.dumps(
            {
                "workers": workers,
                "throughput_per_min": throughput,
                "valid": valid,
            }
        )
    )
    return str(path)


def test_scaling_report_exit_codes(tmp_path, capsys):
    files = [_write(tmp_path, n, 340.0 * n) for n in (1, 2, 3)]
    assert run.main(["scaling-report", *files]) == 0
    assert "scaling: PASS" in capsys.readouterr().out

    files[2] = _write(tmp_path, 3, 600.0)
    assert run.main(["scaling-report", *files]) == 1


def test_scaling_report_fails_on_invalid_measurement(tmp_path, capsys):
    files = [_write(tmp_path, n, 340.0 * n) for n in (1, 2)]
    files.append(_write(tmp_path, 3, 1020.0, valid=False))
    assert run.main(["scaling-report", *files]) == 1
    assert "invalid" in capsys.readouterr().out


def test_scaling_report_handles_null_throughput(tmp_path, capsys):
    files = [_write(tmp_path, n, 340.0 * n) for n in (1, 2)]
    files.append(_write(tmp_path, 3, None, valid=False))
    assert run.main(["scaling-report", *files]) == 1
    out = capsys.readouterr().out
    assert "FAIL" in out
    assert "scaling-3.json" in out


def test_scaling_report_handles_unreadable_file(tmp_path, capsys):
    files = [_write(tmp_path, n, 340.0 * n) for n in (1, 2)]
    empty = tmp_path / "scaling-3.json"
    empty.write_text("")
    files.append(str(empty))
    assert run.main(["scaling-report", *files]) == 1
    out = capsys.readouterr().out
    assert "scaling-3.json" in out
    assert "unreadable" in out
    assert "scaling: FAIL" in out


def test_format_report_shows_failures():
    phase = PhaseResult(
        "intake", latencies=[0.01, 0.02], codes={202: 1, 429: 1}
    )
    text = run.format_report(
        "steady",
        {"rate": 1000},
        [phase],
        [run.Sample(0.0, {"pending": 3.0})],
        Counter(completed=2),
        2,
        ["intake: 1 non-2xx responses"],
        0.1,
        metrics_failures=3,
    )
    assert "metrics sample failures: 3" in text
    assert "202:1,429:1" in text
    assert "FAIL intake: 1 non-2xx responses" in text
    assert text.endswith("result: FAIL")


class _CountingSender:
    def __init__(self):
        self.dues = []

    def send(self, phase, due):
        self.dues.append(due)


async def test_open_loop_sends_count_at_rate():
    sender = _CountingSender()
    await run.open_loop(sender, PhaseResult("p"), 1000.0, count=20)
    assert len(sender.dues) == 20
    gaps = [b - a for a, b in zip(sender.dues, sender.dues[1:], strict=False)]
    assert all(gap == pytest.approx(0.001) for gap in gaps)


async def test_open_loop_duration_bound():
    sender = _CountingSender()
    await run.open_loop(sender, PhaseResult("p"), 1000.0, duration_s=0.02)
    assert len(sender.dues) == 20


async def test_sender_lag_measured_when_request_starts():
    async with httpx.AsyncClient(
        transport=_api({}), base_url="http://t"
    ) as client:
        sender = run.Sender(client, sources=1)
        phase = PhaseResult("p")
        # Planned 50 ms ago: the task starts late by at least that much.
        sender.send(phase, time.perf_counter() - 0.05)
        assert phase.max_lag_s == 0.0
        await sender.drain()
    assert phase.max_lag_s >= 0.05


def _api(statuses):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={})
        key = request.url.path.rsplit("/", 1)[1]
        if key not in statuses:
            return httpx.Response(404, json={})
        return httpx.Response(200, json={"status": statuses[key]})

    return httpx.MockTransport(handler)


async def test_verify_completed_counts_statuses():
    transport = _api({"a": "completed", "b": "pending"})
    async with httpx.AsyncClient(
        transport=transport, base_url="http://t"
    ) as client:
        result = await run.verify_completed(client, ["a", "b", "c"])
    assert result == Counter(completed=1, pending=1, http_404=1)


async def test_sender_records_codes_without_retry():
    async with httpx.AsyncClient(
        transport=_api({}), base_url="http://t"
    ) as client:
        sender = run.Sender(client, sources=2)
        phase = PhaseResult("p")
        for _ in range(4):
            sender.send(phase, time.perf_counter())
        await sender.drain()
    assert phase.codes == {202: 4}
    assert len(phase.latencies) == 4
    assert len(set(sender.keys)) == 4


async def test_sender_retries_429_after_retry_after():
    calls = Counter()

    def handler(request: httpx.Request) -> httpx.Response:
        calls[request.content] += 1
        if calls[request.content] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(202, json={})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://t"
    ) as client:
        sender = run.Sender(client, sources=1, retry_429=True)
        phase = PhaseResult("p")
        sender.send(phase, time.perf_counter())
        await sender.drain()
    assert phase.codes == {429: 1, 202: 1}
    assert sender.retries == 1


@pytest.mark.parametrize("argv", [["flood"], ["unknown"]])
def test_parser_rejects_bad_arguments(argv):
    with pytest.raises(SystemExit):
        run.build_parser().parse_args(argv)


def test_format_report_sub_millisecond_threshold():
    text = run.format_report(
        "steady", {}, [], [], Counter(), 0, [], 0.0005, metrics_failures=0
    )
    assert "p99 POST < 0.5 ms" in text
    assert "metrics sample failures: 0" in text


async def test_sampler_counts_non_200_as_failure():
    responses = iter(
        [
            httpx.Response(500, text="oops"),
            httpx.Response(
                200, text='webhook_queue_depth{status="pending"} 4.0\n'
            ),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return next(responses)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://t"
    ) as client:
        sampler = run.Sampler(client, time.perf_counter(), interval=0.001)
        task = asyncio.create_task(sampler.run())
        sample = await asyncio.wait_for(sampler.next_sample(), 1)
        task.cancel()
    assert sample.depth == {"pending": 4.0}
    assert sampler.failures == 1


def test_scaling_report_reads_json_lines_from_stdin(monkeypatch, capsys):
    lines = [
        json.dumps({"workers": 1, "throughput_per_min": 340.0, "valid": True}),
        "",
        json.dumps({"workers": 2, "throughput_per_min": 680.0, "valid": True}),
        json.dumps({"workers": 3, "throughput_per_min": None, "valid": False}),
    ]
    monkeypatch.setattr("sys.stdin", io.StringIO("\n".join(lines) + "\n"))
    assert run.main(["scaling-report", "-"]) == 1
    out = capsys.readouterr().out
    assert "stdin:4: measurement invalid" in out
    assert "99.0%" not in out
    assert "100.0%  PASS" in out


def test_scaling_report_missing_file(tmp_path, capsys):
    files = [_write(tmp_path, n, 340.0 * n) for n in (1, 2)]
    files.append(str(tmp_path / "nope.json"))
    assert run.main(["scaling-report", *files]) == 1
    assert "nope.json: unreadable" in capsys.readouterr().out


def test_scaling_report_uses_source_field_as_label(monkeypatch, capsys):
    rows = [
        {
            "source": "docs/results/scaling-1.json",
            "workers": 1,
            "throughput_per_min": 340.0,
            "valid": True,
        },
        {
            "source": "docs/results/scaling-2.json",
            "workers": 2,
            "throughput_per_min": None,
            "valid": False,
            "error": "stack up failed",
        },
    ]
    text = "".join(json.dumps(row) + "\n" for row in rows)
    monkeypatch.setattr("sys.stdin", io.StringIO(text))
    assert run.main(["scaling-report", "-"]) == 1
    out = capsys.readouterr().out
    assert "docs/results/scaling-2.json: measurement invalid" in out
    assert "stack up failed" in out
    assert "stdin:" not in out


def test_scaling_report_rejects_duplicate_workers(tmp_path, capsys):
    files = [_write(tmp_path, n, 340.0 * n) for n in (1, 2)]
    dup = tmp_path / "again.json"
    dup.write_text(
        json.dumps({"workers": 2, "throughput_per_min": 1.0, "valid": True})
    )
    files.append(str(dup))
    assert run.main(["scaling-report", *files]) == 1
    out = capsys.readouterr().out
    assert "again.json: duplicate workers=2" in out
    assert "scaling-2.json" in out
    # The first value is kept, not overwritten by the duplicate.
    assert "680.0" in out
    assert "scaling: FAIL" in out

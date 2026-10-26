import json
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
    )
    assert "202:1,429:1" in text
    assert "FAIL intake: 1 non-2xx responses" in text
    assert text.endswith("result: FAIL")


class _CountingSender:
    def __init__(self):
        self.lags = []

    def send(self, phase, lag):
        self.lags.append(lag)


async def test_open_loop_sends_count_at_rate():
    sender = _CountingSender()
    await run.open_loop(sender, PhaseResult("p"), 1000.0, count=20)
    assert len(sender.lags) == 20
    assert all(lag >= 0 for lag in sender.lags)


async def test_open_loop_duration_bound():
    sender = _CountingSender()
    await run.open_loop(sender, PhaseResult("p"), 1000.0, duration_s=0.02)
    assert len(sender.lags) == 20


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
            sender.send(phase, 0.0)
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
        sender.send(phase, 0.0)
        await sender.drain()
    assert phase.codes == {429: 1, 202: 1}
    assert sender.retries == 1


@pytest.mark.parametrize("argv", [["flood"], ["unknown"]])
def test_parser_rejects_bad_arguments(argv):
    with pytest.raises(SystemExit):
        run.build_parser().parse_args(argv)

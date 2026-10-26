import pytest

from loadtest.stats import (
    PhaseResult,
    ZeroStreak,
    backlog,
    completion_rate_per_min,
    latency_summary,
    linearity,
    parse_queue_depth,
    percentile,
    phase_failures,
    scaling_report,
)


def test_percentile_nearest_rank():
    assert percentile([1, 2, 3, 4], 50) == 2
    assert percentile(list(range(1, 101)), 99) == 99


def test_percentile_is_order_independent():
    assert percentile([4, 1, 3, 2], 75) == 3


def test_percentile_bounds():
    values = [5.0, 1.0, 9.0]
    assert percentile(values, 100) == 9.0
    assert percentile(values, 0) == 1.0
    assert percentile([7.0], 99) == 7.0


def test_percentile_rejects_empty_and_out_of_range():
    with pytest.raises(ValueError):
        percentile([], 50)
    with pytest.raises(ValueError):
        percentile([1.0], 101)
    with pytest.raises(ValueError):
        percentile([1.0], -1)


def test_parse_queue_depth():
    text = 'webhook_queue_depth{status="pending"} 12.0\n'
    assert parse_queue_depth(text)["pending"] == 12.0


def test_parse_queue_depth_ignores_other_series_and_comments():
    text = (
        "# HELP webhook_queue_depth Events in non-terminal states\n"
        "# TYPE webhook_queue_depth gauge\n"
        'webhook_queue_depth{status="pending"} 3.0\n'
        'webhook_queue_depth{otel_scope_name="x",status="retrying"} 2\n'
        'webhook_queue_depth{status="processing",job="api"} 1e1\n'
        "webhook_queue_depth_other 99.0\n"
        'webhooks_received_total{outcome="accepted",source="s"} 5.0\n'
        "webhook_queue_oldest_pending_age_seconds 0.0\n"
    )
    assert parse_queue_depth(text) == {
        "pending": 3.0,
        "retrying": 2.0,
        "processing": 10.0,
    }


def test_parse_queue_depth_empty_when_absent():
    assert parse_queue_depth("# nothing here\n") == {}


def test_backlog_with_and_without_processing():
    depth = {"pending": 3.0, "retrying": 2.0, "processing": 10.0}
    assert backlog(depth) == 5.0
    assert backlog(depth, include_processing=True) == 15.0
    assert backlog({}) == 0.0


def test_linearity():
    assert linearity({1: 340, 2: 680, 3: 900})[3] == pytest.approx(0.882, 0.01)
    assert linearity({1: 340, 2: 680, 3: 900})[1] == 1.0


def test_linearity_requires_one_worker_baseline():
    with pytest.raises(ValueError):
        linearity({2: 680, 3: 900})
    with pytest.raises(ValueError):
        linearity({1: 0.0, 2: 10.0})


def test_zero_streak_needs_consecutive_zero_samples():
    streak = ZeroStreak(required=2)
    assert not streak.observe(0)
    assert not streak.observe(1)
    assert not streak.observe(0)
    assert streak.observe(0)
    assert streak.observe(0)


def test_completion_rate_is_slope_of_falling_queue():
    # 10 events/s drained -> 600/min, regardless of noise-free offset.
    points = [(float(t), 1000.0 - 10 * t) for t in range(0, 61)]
    assert completion_rate_per_min(points) == pytest.approx(600.0)


def test_completion_rate_needs_two_distinct_times():
    with pytest.raises(ValueError):
        completion_rate_per_min([(1.0, 5.0)])
    with pytest.raises(ValueError):
        completion_rate_per_min([(1.0, 5.0), (1.0, 4.0)])


def test_latency_summary():
    summary = latency_summary([0.001 * i for i in range(1, 101)])
    assert summary["count"] == 100
    assert summary["p50"] == pytest.approx(0.050)
    assert summary["p95"] == pytest.approx(0.095)
    assert summary["p99"] == pytest.approx(0.099)
    assert summary["max"] == pytest.approx(0.100)


def test_latency_summary_empty():
    assert latency_summary([]) == {"count": 0}


def _phase(**overrides):
    base = {
        "name": "p1",
        "latencies": [0.01] * 100,
        "codes": {202: 100},
        "errors": 0,
        "duration_s": 120.0,
        "limit_s": 120.0,
        "reached_end": True,
        "check_p99": True,
    }
    return PhaseResult(**{**base, **overrides})


def test_phase_passes_when_all_criteria_met():
    assert phase_failures(_phase(), p99_limit_s=0.1) == []


def test_phase_fails_on_slow_p99():
    phase = _phase(latencies=[0.01] * 98 + [0.2] * 2)
    failures = phase_failures(phase, p99_limit_s=0.1)
    assert len(failures) == 1
    assert "p99" in failures[0]


def test_phase_p99_ignored_when_not_checked():
    phase = _phase(latencies=[0.5] * 10, check_p99=False)
    assert phase_failures(phase, p99_limit_s=0.1) == []


def test_phase_fails_on_non_2xx_and_transport_errors():
    phase = _phase(codes={202: 98, 200: 1, 429: 1}, errors=2)
    failures = phase_failures(phase, p99_limit_s=0.1)
    assert any("non-2xx" in f for f in failures)
    assert any("transport" in f for f in failures)


def test_phase_fails_when_end_not_reached_within_limit():
    phase = _phase(reached_end=False, duration_s=600.0, limit_s=600.0)
    failures = phase_failures(phase, p99_limit_s=0.1)
    assert any("limit" in f for f in failures)


def test_scaling_report_passes_when_linear_enough():
    text, passed = scaling_report({1: 340.0, 2: 680.0, 3: 900.0}, 0.85)
    assert passed
    assert "88.2%" in text
    assert "PASS" in text


def test_scaling_report_fails_below_threshold():
    text, passed = scaling_report({1: 340.0, 2: 500.0, 3: 900.0}, 0.85)
    assert not passed
    assert "FAIL" in text

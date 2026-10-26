import pytest

from loadtest.stats import (
    PhaseResult,
    ZeroStreak,
    backlog,
    completion_rate_per_min,
    flood_result,
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


def test_phase_failure_message_formats_sub_millisecond_limit():
    phase = _phase(latencies=[0.01] * 10)
    (failure,) = phase_failures(phase, p99_limit_s=0.0005)
    assert "0.5 ms" in failure
    assert "< 0 ms" not in failure


def _depth(pending, processing=20.0, retrying=0.0):
    return {
        "pending": float(pending),
        "retrying": float(retrying),
        "processing": float(processing),
    }


def _flood(samples, codes=None, errors=0, count=3000, window=(10.0, 20.0)):
    return flood_result(
        samples,
        window,
        codes if codes is not None else {202: 3000},
        errors,
        count,
        workers=2,
    )


def test_flood_result_valid():
    samples = [(float(t), _depth(1000 - 10 * t)) for t in range(0, 31)]
    result = _flood(samples)
    assert result["valid"] is True
    assert result["workers"] == 2
    assert result["throughput_per_min"] == pytest.approx(600.0)
    assert result["samples_in_window"] == 11
    assert result["queue_at_window_start"] == 1000 - 100 + 20
    assert result["queue_at_window_end"] == 1000 - 200 + 20
    assert result["accepted"] == 3000


def test_flood_result_invalid_when_queue_empties_mid_window():
    samples = [(float(t), _depth(500)) for t in range(10, 15)]
    samples += [(15.0, _depth(0))]
    samples += [(float(t), _depth(50)) for t in range(16, 21)]
    result = _flood(samples)
    assert result["valid"] is False


def test_flood_result_too_few_samples():
    result = _flood([(5.0, _depth(900)), (12.0, _depth(800))])
    assert result["throughput_per_min"] is None
    assert result["valid"] is False
    assert result["samples_in_window"] == 1


def test_flood_result_retried_429_counts_as_accepted():
    samples = [(float(t), _depth(1000 - 10 * t)) for t in range(10, 21)]
    result = _flood(samples, codes={429: 40, 202: 3000})
    assert result["accepted"] == 3000
    assert result["rejected_other"] == 0
    assert result["valid"] is True


def test_flood_result_invalid_on_rejections_or_errors():
    samples = [(float(t), _depth(1000 - 10 * t)) for t in range(10, 21)]
    assert not _flood(samples, codes={202: 2999, 503: 1})["valid"]
    assert not _flood(samples, codes={202: 2999}, errors=1)["valid"]


def test_flood_result_queued_at_end_ignores_processing():
    samples = [(float(t), _depth(100 - 5 * t)) for t in range(10, 20)]
    samples.append((20.0, _depth(0, processing=40.0)))
    result = _flood(samples)
    assert result["pending_retrying_at_end"] == 0.0
    assert result["queue_at_window_end"] == 40.0
    assert result["valid"] is False


def test_scaling_report_marks_invalid_rows():
    text, passed = scaling_report(
        {1: 340.0, 2: 680.0}, 0.85, invalid={3: "measurement invalid"}
    )
    assert not passed
    assert "measurement invalid" in text
    assert "scaling: FAIL" in text


def test_scaling_report_without_valid_baseline():
    text, passed = scaling_report(
        {2: 680.0}, 0.85, invalid={1: "throughput missing"}
    )
    assert not passed
    assert "no valid 1-worker baseline" in text

import time

import pytest

from benchmarks.sqlite_bench import (
    Result,
    _writer,
    format_table,
    fresh_db,
    main,
    row_notes,
    run_writers,
    summarize,
)


def test_benchmarks_smoke(tmp_path, capsys):
    assert (
        main(["all", "--dir", str(tmp_path), "--repeat", "1", "--quick"]) == 0
    )
    out = capsys.readouterr().out
    for bench in ("sync", "claim", "recover", "contention", "retention"):
        assert bench in out
    assert "FULL" in out and "NORMAL" in out
    assert "ix_events_ready" in out  # EXPLAIN QUERY PLAN of the claim


def test_single_subcommand(tmp_path, capsys):
    argv = ["sync", "--dir", str(tmp_path), "--quick", "--repeat", "1"]
    assert main([*argv, "--sync-rate", "1000"]) == 0
    out = capsys.readouterr().out
    assert "sync" in out and "claim" not in out
    assert "FULL insert_or_get, paced 1000 tx/s" in out
    assert main([*argv, "--sync-rate", "0"]) == 0
    assert "paced" not in capsys.readouterr().out


def test_summarize_median_and_p95():
    assert summarize([0.001, 0.002, 0.003, 0.004]) == (4, 2.5, 4.0)
    n, median, p95 = summarize([float(v) / 1000 for v in range(1, 101)])
    assert (n, median, p95) == (100, 50.5, 95.0)


def test_summarize_rejects_empty():
    with pytest.raises(ValueError):
        summarize([])


def test_format_table_aligns_columns():
    rows = [
        Result("sync", "FULL", 1000, 1.25, 2.5, "ms", "800 tx/s"),
        Result("contention", "8 procs", 5, 812.0, 900.0, "tx/s", ""),
    ]
    lines = format_table(rows).splitlines()
    assert lines[0].split() == [
        "benchmark",
        "case",
        "n",
        "median",
        "p95",
        "unit",
        "notes",
    ]
    assert set(lines[1]) == {"-", " "}
    assert "1.250" in lines[2] and "812.0" in lines[3]
    # Numeric columns are right-aligned to the header's column end.
    end = lines[0].index("median") + len("median")
    assert lines[2][:end].endswith("1.250")
    assert lines[3][:end].endswith("812.0")


def test_row_notes_flags_p95_equal_to_max():
    assert row_notes(Result("c", "x", 5, 1.0, 2.0, "tx/s", "a")) == (
        "p95 = max (n=5); a"
    )
    assert row_notes(Result("c", "x", 19, 1.0, 2.0, "ms")) == (
        "p95 = max (n=19)"
    )
    assert row_notes(Result("c", "x", 20, 1.0, 2.0, "ms", "a")) == "a"


def _writer_dying_before_barrier(path, writer_id, seconds, barrier, results):
    if writer_id == 0:
        raise SystemExit(3)  # dies without reporting or reaching the barrier
    _writer(path, writer_id, seconds, barrier, results)


def test_run_writers_fails_fast_when_a_writer_dies(tmp_path):
    path = fresh_db(tmp_path, "dying")
    start = time.monotonic()
    with pytest.raises(RuntimeError, match="writer"):
        run_writers(path, 3, 0.2, target=_writer_dying_before_barrier)
    assert time.monotonic() - start < 30


def test_run_writers_reports_every_writer(tmp_path):
    reports = run_writers(fresh_db(tmp_path, "ok"), 2, 0.2)
    assert sorted(r.writer_id for r in reports) == [0, 1]
    assert all(r.count > 0 and r.end > r.start for r in reports)

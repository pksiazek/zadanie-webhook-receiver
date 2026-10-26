import pytest

from benchmarks.sqlite_bench import Result, format_table, main, summarize


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
    assert main(["sync", "--dir", str(tmp_path), "--quick"]) == 0
    out = capsys.readouterr().out
    assert "sync" in out and "claim" not in out


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

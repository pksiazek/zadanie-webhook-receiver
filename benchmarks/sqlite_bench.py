"""SQLite micro-benchmarks (spec §13a), run by hand, outside CI.

Each benchmark uses a fresh on-disk database under ``--dir`` (never
``:memory:``), the production ``connect()`` / ``EventRepository`` code
paths, and reports n, median and p95. Usage::

    python -m benchmarks.sqlite_bench all --dir /data/bench --repeat 5

Results table goes to stdout, progress to stderr.
"""

import argparse
import json
import multiprocessing
import shutil
import sqlite3
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from loadtest.stats import percentile
from webhook_receiver.common.clock import SystemClock
from webhook_receiver.common.db import ConnectionPool, immediate, init_schema
from webhook_receiver.common.models import RetentionPolicy
from webhook_receiver.common.repository import (
    CLAIM_SQL,
    PURGE_COMPLETED_SQL,
    PURGE_PAYLOAD_SQL,
    RECOVER_FAIL_SQL,
    RECOVER_RETRY_SQL,
    EventRepository,
)

DAY_MS = 86_400_000
LEASE_MS = 30_000  # LEASE_SECONDS default
MAX_ATTEMPTS = 5  # MAX_ATTEMPTS default
BATCH = 1000  # RETENTION_BATCH_SIZE default
CLAIM_LIMIT = 20  # claim(20): WORKER_CONCURRENCY default
LEASED_ROWS = 60  # 3 workers x 20 slots, all leases still valid
# Representative webhook body (~200 bytes of JSON).
PAYLOAD = {
    "order_id": "ord_0123456789",
    "amount": 12345,
    "currency": "PLN",
    "customer": {"id": "cus_42", "email": "someone@example.com"},
    "items": [{"sku": "SKU-1", "qty": 2}, {"sku": "SKU-2", "qty": 1}],
}
PAYLOAD_JSON = json.dumps(PAYLOAD, separators=(",", ":"))
# Start method chosen explicitly: Python 3.14 defaults to forkserver on
# Linux; spawn behaves the same everywhere and never forks a threaded
# parent.
MP_CONTEXT = "spawn"


@dataclass(frozen=True)
class Result:
    """One row of the results table."""

    bench: str
    case: str
    n: int
    median: float
    p95: float
    unit: str
    notes: str = ""


@dataclass(frozen=True)
class Sizes:
    """Workload sizes; ``--quick`` shrinks them for the smoke test."""

    sync_inserts: int
    claim_sizes: tuple[int, ...]
    claims_per_repeat: int
    writer_counts: tuple[int, ...]
    contention_seconds: float
    retention_eligible: int
    retention_other: int


FULL = Sizes(
    sync_inserts=1000,
    claim_sizes=(0, 10_000, 100_000),
    claims_per_repeat=50,
    writer_counts=(1, 2, 4, 8),
    contention_seconds=10.0,
    retention_eligible=10_000,
    retention_other=100_000,
)
QUICK = Sizes(
    sync_inserts=100,
    claim_sizes=(0, 1_000),
    claims_per_repeat=5,
    writer_counts=(1, 2),
    contention_seconds=0.5,
    retention_eligible=2_000,
    retention_other=1_000,
)


# --- pure helpers ---------------------------------------------------------


def summarize(seconds: Sequence[float]) -> tuple[int, float, float]:
    """``(n, median ms, p95 ms)`` of durations given in seconds."""
    if not seconds:
        raise ValueError("no samples")
    ms = [s * 1000 for s in seconds]
    return len(ms), statistics.median(ms), percentile(ms, 95)


def ms_result(
    bench: str, case: str, seconds: Sequence[float], notes: str = ""
) -> Result:
    n, median, p95 = summarize(seconds)
    return Result(bench, case, n, median, p95, "ms", notes)


def format_table(rows: Sequence[Result]) -> str:
    """Fixed-width table: ms with 3 decimals, rates with 1."""
    header = ("benchmark", "case", "n", "median", "p95", "unit", "notes")

    def num(r: Result, value: float) -> str:
        return f"{value:.3f}" if r.unit == "ms" else f"{value:.1f}"

    cells = [
        (
            r.bench,
            r.case,
            str(r.n),
            num(r, r.median),
            num(r, r.p95),
            r.unit,
            r.notes,
        )
        for r in rows
    ]
    widths = [max(len(row[i]) for row in [header, *cells]) for i in range(7)]
    right = {2, 3, 4}  # numeric columns

    def line(row: Sequence[str]) -> str:
        parts = [
            cell.rjust(widths[i]) if i in right else cell.ljust(widths[i])
            for i, cell in enumerate(row)
        ]
        return "  ".join(parts).rstrip()

    rule = "  ".join("-" * w for w in widths)
    return "\n".join([line(header), rule, *(line(c) for c in cells)])


# --- infrastructure -------------------------------------------------------


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def fresh_db(base: Path, name: str) -> Path:
    """Empty directory with a schema-initialised database file."""
    folder = base / name
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    path = folder / "bench.db"
    pool = ConnectionPool(path)
    try:
        init_schema(pool.get())
    finally:
        pool.close_all()
    return path


def timed[T](fn: Callable[..., T], *args: object) -> tuple[float, T]:
    """``(seconds, result)`` of one call of ``fn(*args)``."""
    start = time.perf_counter()
    value = fn(*args)
    return time.perf_counter() - start, value


def bulk_insert(
    conn: sqlite3.Connection,
    prefix: str,
    count: int,
    status: str,
    run_at: int,
    finished_at: int | None = None,
    attempts: int = 0,
) -> None:
    """Populate quickly in one transaction (setup only, never timed)."""
    created = run_at if finished_at is None else finished_at - 1000
    rows = (
        (
            f"{prefix}-{i}",
            PAYLOAD_JSON,
            status,
            attempts,
            run_at,
            created,
            finished_at,
        )
        for i in range(count)
    )
    with immediate(conn):
        conn.executemany(
            "INSERT INTO webhook_events (idempotency_key, source,"
            " event_type, payload_json, status, attempts, run_at,"
            " created_at, finished_at)"
            " VALUES (?, 'bench', 'order.created', ?, ?, ?, ?, ?, ?)",
            rows,
        )


def query_plan(conn: sqlite3.Connection, sql: str, params: dict) -> str:
    rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    return "; ".join(row["detail"] for row in rows)


# --- 1. synchronous=FULL vs NORMAL ----------------------------------------


def bench_sync(base: Path, repeat: int, sizes: Sizes) -> list[Result]:
    results = []
    for mode, expected in (("FULL", 2), ("NORMAL", 1)):
        samples: list[float] = []
        for rep in range(repeat):
            log(f"sync {mode}: run {rep + 1}/{repeat}")
            pool = ConnectionPool(fresh_db(base, f"sync-{mode.lower()}"))
            try:
                conn = pool.get()  # the connection the repository uses
                conn.execute(f"PRAGMA synchronous={mode}")
                actual = conn.execute("PRAGMA synchronous").fetchone()[0]
                if actual != expected:
                    raise RuntimeError(f"synchronous={actual}, want {mode}")
                repo = EventRepository(pool, SystemClock())
                for i in range(sizes.sync_inserts):
                    elapsed, _ = timed(
                        repo.insert_or_get,
                        f"k-{i}",
                        "bench",
                        "order.created",
                        PAYLOAD,
                        None,
                    )
                    samples.append(elapsed)
            finally:
                pool.close_all()
        rate = len(samples) / sum(samples)
        results.append(
            ms_result(
                "sync",
                f"{mode} insert_or_get",
                samples,
                f"{rate:.0f} tx/s serial",
            )
        )
    return results


# --- 2. claim / recover_expired with a growing queue ----------------------


def bench_claim(
    base: Path, repeat: int, sizes: Sizes
) -> tuple[list[Result], list[str]]:
    """``claim(20)`` and ``recover_expired`` at several queue sizes.

    Each size is populated once (``size`` due ``pending`` rows plus
    ``LEASED_ROWS`` valid leases). After every timed ``claim`` the
    claimed rows are put back to ``pending`` (untimed), so every claim
    sees exactly ``size`` ready rows.
    """
    results: list[Result] = []
    plans: list[str] = []
    clock = SystemClock()
    for size in sizes.claim_sizes:
        pool = ConnectionPool(fresh_db(base, f"claim-{size}"))
        try:
            conn = pool.get()
            now = clock.now_ms()
            bulk_insert(conn, "ready", size, "pending", now - 60_000)
            bulk_insert(
                conn,
                "leased",
                LEASED_ROWS,
                "processing",
                now + 3_600_000,
                attempts=1,
            )
            conn.execute("ANALYZE")
            repo = EventRepository(pool, clock)
            claims: list[float] = []
            recovers: list[float] = []
            for rep in range(repeat):
                log(f"claim {size}: run {rep + 1}/{repeat}")
                for _ in range(sizes.claims_per_repeat):
                    elapsed, _ = timed(repo.recover_expired, MAX_ATTEMPTS)
                    recovers.append(elapsed)
                    elapsed, got = timed(repo.claim, CLAIM_LIMIT, LEASE_MS)
                    claims.append(elapsed)
                    if len(got) != min(CLAIM_LIMIT, size):
                        raise RuntimeError(f"claimed {len(got)} of {size}")
                    with immediate(conn):
                        conn.executemany(
                            "UPDATE webhook_events SET status = 'pending',"
                            " attempts = 0, run_at = ? WHERE id = ?",
                            [(now - 60_000, e.id) for e in got],
                        )
            results.append(
                ms_result("claim", f"claim(20) @ {size} ready", claims)
            )
            results.append(
                ms_result(
                    "recover",
                    f"recover_expired @ {size} ready",
                    recovers,
                    f"{LEASED_ROWS} valid leases",
                )
            )
            if size == sizes.claim_sizes[-1]:
                params = {
                    "now": now,
                    "lease": LEASE_MS,
                    "limit": CLAIM_LIMIT,
                    "max": MAX_ATTEMPTS,
                }
                for name, sql in (
                    ("claim", CLAIM_SQL),
                    ("recover retry", RECOVER_RETRY_SQL),
                    ("recover fail", RECOVER_FAIL_SQL),
                ):
                    plans.append(
                        f"{name} @ {size}: {query_plan(conn, sql, params)}"
                    )
        finally:
            pool.close_all()
    return results, plans


# --- 3. write contention between processes --------------------------------


def _writer(
    path: str,
    writer_id: int,
    seconds: float,
    barrier,  # multiprocessing.synchronize.Barrier
    results,  # multiprocessing.Queue
) -> None:
    pool = ConnectionPool(path)
    count = errors = 0
    worst = 0.0  # slowest insert_or_get, failed ones included
    try:
        repo = EventRepository(pool, SystemClock())
        pool.get()  # connect before the start line
        barrier.wait()
        deadline = time.perf_counter() + seconds
        while (start := time.perf_counter()) < deadline:
            try:
                repo.insert_or_get(
                    f"w{writer_id}-{count}-{errors}",
                    "bench",
                    "order.created",
                    PAYLOAD,
                    None,
                )
                count += 1
            except sqlite3.OperationalError as exc:
                # SQLITE_BUSY once busy_timeout (5 s) runs out: the busy
                # handler polls with backoff and is not fair, so a writer
                # can lose the lock race for that long.
                if exc.sqlite_errorname != "SQLITE_BUSY":
                    raise
                errors += 1
            worst = max(worst, time.perf_counter() - start)
    finally:
        pool.close_all()
        results.put((writer_id, count, errors, worst))


def bench_contention(base: Path, repeat: int, sizes: Sizes) -> list[Result]:
    ctx = multiprocessing.get_context(MP_CONTEXT)
    seconds = sizes.contention_seconds
    out = []
    for writers in sizes.writer_counts:
        totals: list[float] = []
        per_proc: list[float] = []
        errors = 0
        worst = 0.0
        for rep in range(repeat):
            log(f"contention {writers} procs: run {rep + 1}/{repeat}")
            path = fresh_db(base, f"contention-{writers}")
            barrier = ctx.Barrier(writers)
            queue = ctx.Queue()
            procs = [
                ctx.Process(
                    target=_writer,
                    args=(str(path), w, seconds, barrier, queue),
                )
                for w in range(writers)
            ]
            for p in procs:
                p.start()
            reports = [queue.get(timeout=seconds + 120) for _ in procs]
            for p in procs:
                p.join()
                if p.exitcode != 0:
                    raise RuntimeError(f"writer exited with {p.exitcode}")
            queue.close()
            queue.join_thread()
            counts = [c for _, c, _, _ in reports]
            errors += sum(e for _, _, e, _ in reports)
            worst = max(worst, *(w for _, _, _, w in reports))
            totals.append(sum(counts) / seconds)
            per_proc.extend(c / seconds for c in counts)
        out.append(
            Result(
                "contention",
                f"{writers} writer proc(s)",
                len(totals),
                statistics.median(totals),
                percentile(totals, 95),
                "tx/s",
                f"per proc median {statistics.median(per_proc):.0f}"
                f" (min {min(per_proc):.0f}, max {max(per_proc):.0f}) tx/s;"
                f" max tx {worst * 1000:.0f} ms; SQLITE_BUSY {errors}",
            )
        )
    return out


# --- 4. retention ---------------------------------------------------------


def _populate_retention(conn: sqlite3.Connection, sizes: Sizes) -> int:
    now = SystemClock().now_ms()
    old = now - 40 * DAY_MS  # past both the 7 d and the 30 d cutoff
    bulk_insert(
        conn, "old", sizes.retention_eligible, "completed", old, old, 1
    )
    recent = now - 3_600_000
    bulk_insert(
        conn, "new", sizes.retention_other, "completed", recent, recent, 1
    )
    conn.execute("ANALYZE")
    return now


def bench_retention(
    base: Path, repeat: int, sizes: Sizes
) -> tuple[list[Result], list[str]]:
    """One batch of 1000 per tier, and a whole ``purge()``.

    Batches execute the repository's own SQL once each under
    ``immediate()`` (exactly what ``purge`` does per batch), so their
    duration is the write-lock hold time. Every eligible batch is
    timed; then ``purge()`` runs on a freshly populated database.
    """
    payload: list[float] = []
    delete: list[float] = []
    totals: list[float] = []
    plans: list[str] = []
    for rep in range(repeat):
        log(f"retention: run {rep + 1}/{repeat}")
        pool = ConnectionPool(fresh_db(base, "retention-batch"))
        try:
            conn = pool.get()
            now = _populate_retention(conn, sizes)
            for sql, samples, days in (
                (PURGE_PAYLOAD_SQL, payload, 7),
                (PURGE_COMPLETED_SQL, delete, 30),
            ):
                params = {"cutoff": now - days * DAY_MS, "limit": BATCH}
                if rep == 0:
                    plans.append(
                        f"{'payload' if days == 7 else 'delete'} batch:"
                        f" {query_plan(conn, sql, params)}"
                    )
                while True:
                    start = time.perf_counter()
                    with immediate(conn):
                        changed = conn.execute(sql, params).rowcount
                    elapsed = time.perf_counter() - start
                    if changed == 0:
                        break
                    samples.append(elapsed)
        finally:
            pool.close_all()

        pool = ConnectionPool(fresh_db(base, "retention-purge"))
        try:
            _populate_retention(pool.get(), sizes)
            repo = EventRepository(pool, SystemClock())
            policy = RetentionPolicy(
                payload_ms=7 * DAY_MS,
                completed_ms=30 * DAY_MS,
                failed_ms=30 * DAY_MS,
                batch_size=BATCH,
            )
            elapsed, purged = timed(repo.purge, policy)
            totals.append(elapsed)
            if purged.completed_deleted != sizes.retention_eligible:
                raise RuntimeError(f"purge deleted {purged}")
        finally:
            pool.close_all()
    eligible = sizes.retention_eligible
    other = sizes.retention_other
    shape = f"{eligible} eligible + {other} recent rows"
    return [
        ms_result("retention", "payload clear batch 1000", payload, shape),
        ms_result("retention", "delete batch 1000", delete, shape),
        ms_result(
            "retention",
            f"purge() {eligible} eligible",
            totals,
            f"{2 * eligible // BATCH} batches; {shape}",
        ),
    ], plans


# --- CLI ------------------------------------------------------------------

BENCHES = ("sync", "claim", "contention", "retention")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("bench", choices=[*BENCHES, "all"])
    parser.add_argument(
        "--dir", type=Path, required=True, help="directory for db files"
    )
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument(
        "--quick", action="store_true", help="tiny sizes (smoke test)"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.repeat <= 0:
        raise SystemExit("--repeat must be positive")
    sizes = QUICK if args.quick else FULL
    base: Path = args.dir
    base.mkdir(parents=True, exist_ok=True)
    selected = BENCHES if args.bench == "all" else (args.bench,)
    rows: list[Result] = []
    plans: list[str] = []
    started = time.perf_counter()
    for bench in selected:
        if bench == "sync":
            rows += bench_sync(base, args.repeat, sizes)
        elif bench == "claim":
            got, plan = bench_claim(base, args.repeat, sizes)
            rows += got
            plans += plan
        elif bench == "contention":
            rows += bench_contention(base, args.repeat, sizes)
        else:
            got, plan = bench_retention(base, args.repeat, sizes)
            rows += got
            plans += plan
    print(
        f"SQLite {sqlite3.sqlite_version}, Python {sys.version.split()[0]},"
        f" repeat {args.repeat}{', quick' if args.quick else ''},"
        f" dir {base}"
    )
    print(format_table(rows))
    if plans:
        print("\nEXPLAIN QUERY PLAN")
        for plan in plans:
            print(f"  {plan}")
    log(f"done in {time.perf_counter() - started:.1f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())

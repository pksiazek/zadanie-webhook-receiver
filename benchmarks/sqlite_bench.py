"""SQLite micro-benchmarks (spec §13a), run by hand, outside CI.

Each benchmark uses a fresh on-disk database under ``--dir`` (never
``:memory:``), the production ``connect()`` / ``EventRepository`` code
paths, and reports n, median and p95. Usage::

    python -m benchmarks.sqlite_bench all --dir /data/bench --repeat 5

Results table goes to stdout, progress to stderr.
"""

import argparse
import json
import math
import multiprocessing
import random
import shutil
import sqlite3
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from queue import Empty

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
SYNC_WARMUP = 20  # untimed inserts before the timed ones
SYNC_RATE = 100.0  # default pace of the paced sync case (tx/s)
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
    claim_warmup: int
    writer_counts: tuple[int, ...]
    contention_seconds: float
    retention_eligible: int
    retention_other: int
    retention_purged: int


FULL = Sizes(
    sync_inserts=1000,
    claim_sizes=(0, 10_000, 100_000),
    claims_per_repeat=50,
    claim_warmup=100,
    writer_counts=(1, 2, 4, 8),
    contention_seconds=10.0,
    retention_eligible=10_000,
    retention_other=100_000,
    retention_purged=100_000,
)
QUICK = Sizes(
    sync_inserts=100,
    claim_sizes=(0, 1_000),
    claims_per_repeat=5,
    claim_warmup=5,
    writer_counts=(1, 2),
    contention_seconds=0.5,
    retention_eligible=2_000,
    retention_other=1_000,
    retention_purged=1_000,
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


def row_notes(r: Result) -> str:
    """Notes, flagged when nearest-rank p95 is simply the maximum."""
    if math.ceil(0.95 * r.n) < r.n:
        return r.notes
    flag = f"p95 = max (n={r.n})"
    return f"{flag}; {r.notes}" if r.notes else flag


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
            row_notes(r),
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


def _sync_samples(
    base: Path, mode: str, expected: int, count: int, rate: float | None
) -> list[float]:
    """Durations of ``count`` timed ``insert_or_get`` calls.

    ``rate`` paces the calls at most ``rate`` per second; ``None`` runs
    them back to back. ``SYNC_WARMUP`` untimed inserts come first.
    """
    pool = ConnectionPool(fresh_db(base, f"sync-{mode.lower()}"))
    try:
        conn = pool.get()  # the connection the repository uses
        conn.execute(f"PRAGMA synchronous={mode}")
        actual = conn.execute("PRAGMA synchronous").fetchone()[0]
        if actual != expected:
            raise RuntimeError(f"synchronous={actual}, want {mode}")
        repo = EventRepository(pool, SystemClock())
        for i in range(SYNC_WARMUP):
            repo.insert_or_get(
                f"warm-{i}", "bench", "order.created", PAYLOAD, None
            )
        samples = []
        next_at = time.perf_counter()
        for i in range(count):
            if rate is not None:
                delay = next_at - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                # Never earlier than the last call's start + 1/rate: a
                # slow call is not followed by a catch-up burst.
                next_at = max(next_at, time.perf_counter()) + 1 / rate
            elapsed, _ = timed(
                repo.insert_or_get,
                f"k-{i}",
                "bench",
                "order.created",
                PAYLOAD,
                None,
            )
            samples.append(elapsed)
        return samples
    finally:
        pool.close_all()


def bench_sync(
    base: Path, repeat: int, sizes: Sizes, rate: float
) -> list[Result]:
    """Flat out, and paced at ``rate`` tx/s (0 = skip the paced case)."""
    results = []
    paces: list[float | None] = [None] + ([rate] if rate > 0 else [])
    for mode, expected in (("FULL", 2), ("NORMAL", 1)):
        for pace in paces:
            label = "flat out" if pace is None else f"paced {pace:g} tx/s"
            samples: list[float] = []
            for rep in range(repeat):
                log(f"sync {mode} {label}: run {rep + 1}/{repeat}")
                samples += _sync_samples(
                    base, mode, expected, sizes.sync_inserts, pace
                )
            p99 = percentile(samples, 99) * 1000
            notes = f"p99 {p99:.3f} ms"
            if pace is None:
                notes += f"; {len(samples) / sum(samples):.0f} tx/s serial"
            results.append(
                ms_result(
                    "sync", f"{mode} insert_or_get, {label}", samples, notes
                )
            )
    return results


# --- 2. claim / recover_expired with a growing queue ----------------------


def _reset_claimed(conn: sqlite3.Connection, ids: list[int], run_at: int):
    """Put claimed rows back to ``pending`` (setup, never timed)."""
    with immediate(conn):
        conn.executemany(
            "UPDATE webhook_events SET status = 'pending',"
            " attempts = 0, run_at = ? WHERE id = ?",
            [(run_at, i) for i in ids],
        )


def _claim_cycle(
    repo: EventRepository,
    conn: sqlite3.Connection,
    size: int,
    ready_at: int,
) -> tuple[float, float, float, float]:
    """One timed ``recover_expired`` + ``claim``, and one split claim.

    Returns ``(recover, claim, exec, commit)`` seconds. The split claim
    runs ``CLAIM_SQL`` under ``immediate()`` like the repository does,
    timing the statement (exec) apart from the COMMIT (fsync).
    """
    recover, _ = timed(repo.recover_expired, MAX_ATTEMPTS)
    claim, got = timed(repo.claim, CLAIM_LIMIT, LEASE_MS)
    if len(got) != min(CLAIM_LIMIT, size):
        raise RuntimeError(f"claimed {len(got)} of {size}")
    _reset_claimed(conn, [e.id for e in got], ready_at)
    params = {
        "now": SystemClock().now_ms(),
        "lease": LEASE_MS,
        "limit": CLAIM_LIMIT,
    }
    with immediate(conn):
        start = time.perf_counter()
        rows = conn.execute(CLAIM_SQL, params).fetchall()
        executed = time.perf_counter()
    committed = time.perf_counter()
    _reset_claimed(conn, [r["id"] for r in rows], ready_at)
    return recover, claim, executed - start, committed - executed


CLAIM_SEED = 15  # fixed seed of the per-round size order


def bench_claim(
    base: Path, repeat: int, sizes: Sizes
) -> tuple[list[Result], list[str]]:
    """``claim(20)`` and ``recover_expired`` at several queue sizes.

    One database per size is populated up front (``size`` due
    ``pending`` rows plus ``LEASED_ROWS`` valid leases) and all stay
    open. After every claim the claimed rows are put back to
    ``pending`` (untimed), so every claim sees exactly ``size`` ready
    rows. Each database first gets ``claim_warmup`` untimed cycles (the
    0-ready case never writes, so otherwise the first size that commits
    pays for a cold fsync path). Timed cycles are then interleaved: in
    every round each size runs one cycle, in a shuffled order (fixed
    seed), so a slow phase of the host hits all sizes alike instead of
    whichever size happens to run during it.

    Besides the whole ``claim()``, the claim statement is timed alone
    (``exec``, no COMMIT) to separate the query cost, which is what the
    indexes govern, from the fsync of the commit.

    Caveat: the reset restores the same ``run_at``, so every claim
    re-takes the same 20 rows (same rowids) and the index pages they
    live on stay hot in the page cache. This shows the cost does not
    depend on the size of the ready set; it is not a draining queue.
    """
    clock = SystemClock()
    pools: dict[int, ConnectionPool] = {}
    ready_at = clock.now_ms() - 60_000
    try:
        for size in sizes.claim_sizes:
            log(f"claim {size}: populate")
            pool = pools[size] = ConnectionPool(
                fresh_db(base, f"claim-{size}")
            )
            conn = pool.get()
            bulk_insert(conn, "ready", size, "pending", ready_at)
            bulk_insert(
                conn,
                "leased",
                LEASED_ROWS,
                "processing",
                clock.now_ms() + 3_600_000,
                attempts=1,
            )
            conn.execute("ANALYZE")
        repos = {n: EventRepository(p, clock) for n, p in pools.items()}
        for size in sizes.claim_sizes:
            log(f"claim {size}: warm-up {sizes.claim_warmup} cycles")
            for _ in range(sizes.claim_warmup):
                _claim_cycle(repos[size], pools[size].get(), size, ready_at)
        samples: dict[int, list[tuple[float, float, float, float]]] = {
            n: [] for n in sizes.claim_sizes
        }
        order = list(sizes.claim_sizes)
        shuffle = random.Random(CLAIM_SEED).shuffle
        for rep in range(repeat):
            log(f"claim (all sizes interleaved): run {rep + 1}/{repeat}")
            for _ in range(sizes.claims_per_repeat):
                shuffle(order)
                for size in order:
                    samples[size].append(
                        _claim_cycle(
                            repos[size], pools[size].get(), size, ready_at
                        )
                    )
        results: list[Result] = []
        for size in sizes.claim_sizes:
            recovers, claims, execs, commits = (
                list(c) for c in zip(*samples[size], strict=True)
            )
            commit_median = statistics.median(commits) * 1000
            results += [
                ms_result("claim", f"claim(20) @ {size} ready", claims),
                ms_result(
                    "claim",
                    f"claim(20) exec @ {size} ready",
                    execs,
                    f"CLAIM_SQL without COMMIT; COMMIT median"
                    f" {commit_median:.3f} ms",
                ),
                ms_result(
                    "recover",
                    f"recover_expired @ {size} ready",
                    recovers,
                    f"{LEASED_ROWS} valid leases, none expired (no write)",
                ),
            ]
        largest = sizes.claim_sizes[-1]
        params = {
            "now": clock.now_ms(),
            "lease": LEASE_MS,
            "limit": CLAIM_LIMIT,
            "max": MAX_ATTEMPTS,
        }
        plans = [
            f"{name} @ {largest}:"
            f" {query_plan(pools[largest].get(), sql, params)}"
            for name, sql in (
                ("claim", CLAIM_SQL),
                ("recover retry", RECOVER_RETRY_SQL),
                ("recover fail", RECOVER_FAIL_SQL),
            )
        ]
        return results, plans
    finally:
        for pool in pools.values():
            pool.close_all()


# --- 3. write contention between processes --------------------------------


BARRIER_TIMEOUT_S = 60.0  # all writers connected by then, or abort


@dataclass(frozen=True)
class WriterReport:
    writer_id: int
    count: int
    busy: int
    late: int  # commits that finished after the deadline (not counted)
    worst_s: float  # slowest insert_or_get, failed ones included
    start: float  # time.monotonic() at barrier release (system-wide)
    end: float


def _writer(
    path: str,
    writer_id: int,
    seconds: float,
    barrier,  # multiprocessing.synchronize.Barrier
    results,  # multiprocessing.Queue
) -> None:
    pool = ConnectionPool(path)
    try:
        repo = EventRepository(pool, SystemClock())
        pool.get()  # connect before the start line
        barrier.wait(timeout=BARRIER_TIMEOUT_S)
        count = busy = late = 0
        worst = 0.0
        begin = time.monotonic()
        deadline = begin + seconds
        while (start := time.monotonic()) < deadline:
            try:
                repo.insert_or_get(
                    f"w{writer_id}-{count}-{busy}-{late}",
                    "bench",
                    "order.created",
                    PAYLOAD,
                    None,
                )
                # Only commits inside [begin, deadline] count: a stalled
                # last transaction must not stretch the window.
                if time.monotonic() <= deadline:
                    count += 1
                else:
                    late += 1
            except sqlite3.OperationalError as exc:
                # SQLITE_BUSY once busy_timeout (5 s) runs out: the busy
                # handler polls with backoff and is not fair, so a writer
                # can lose the lock race for that long.
                if exc.sqlite_errorname != "SQLITE_BUSY":
                    raise
                busy += 1
            worst = max(worst, time.monotonic() - start)
        report = WriterReport(
            writer_id, count, busy, late, worst, begin, time.monotonic()
        )
        results.put(("ok", report))
    except BaseException as exc:
        results.put(("error", f"writer {writer_id}: {exc!r}"))
        raise
    finally:
        pool.close_all()


def run_writers(
    path: Path,
    writers: int,
    seconds: float,
    target: Callable[..., None] = _writer,
) -> list[WriterReport]:
    """Start ``writers`` processes on one barrier and collect reports.

    A writer that fails (or dies) aborts the barrier so the others do
    not wait forever; every process is terminated and joined on the way
    out, and the failure is raised as ``RuntimeError``.
    """
    ctx = multiprocessing.get_context(MP_CONTEXT)
    barrier = ctx.Barrier(writers)
    queue = ctx.Queue()
    procs = [
        ctx.Process(
            target=target, args=(str(path), w, seconds, barrier, queue)
        )
        for w in range(writers)
    ]
    reports: list[WriterReport] = []
    deadline = time.monotonic() + BARRIER_TIMEOUT_S + seconds + 60
    try:
        for p in procs:
            p.start()
        while len(reports) < writers:
            try:
                kind, value = queue.get(timeout=0.5)
            except Empty:
                dead = [p for p in procs if p.exitcode not in (None, 0)]
                if dead:
                    raise RuntimeError(
                        f"writer process exited with {dead[0].exitcode}"
                    ) from None
                if time.monotonic() > deadline:
                    raise RuntimeError("writers did not finish") from None
                continue
            if kind != "ok":
                raise RuntimeError(f"contention {value}")
            reports.append(value)
    except BaseException:
        barrier.abort()
        raise
    finally:
        for p in procs:
            if p.is_alive():
                p.join(timeout=5)
            if p.is_alive():
                p.terminate()
                p.join()
        queue.close()
        queue.join_thread()
    return reports


def bench_contention(base: Path, repeat: int, sizes: Sizes) -> list[Result]:
    """Write throughput of 1..N processes looping ``insert_or_get``.

    Every writer counts only the commits that finish within ``seconds``
    of its barrier release; rates divide by the nominal ``seconds``. A
    transaction stalled past the deadline is not counted, but shows up
    in "max tx" and "overrun" (last writer end minus the deadline).
    """
    seconds = sizes.contention_seconds
    out = []
    for writers in sizes.writer_counts:
        totals: list[float] = []
        per_proc: list[float] = []
        busy = late = 0
        worst = overrun = 0.0
        for rep in range(repeat):
            log(f"contention {writers} procs: run {rep + 1}/{repeat}")
            path = fresh_db(base, f"contention-{writers}")
            reports = run_writers(path, writers, seconds)
            totals.append(sum(r.count for r in reports) / seconds)
            per_proc.extend(r.count / seconds for r in reports)
            busy += sum(r.busy for r in reports)
            late += sum(r.late for r in reports)
            worst = max(worst, *(r.worst_s for r in reports))
            overrun = max(
                overrun, *(r.end - r.start - seconds for r in reports)
            )
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
                f" max tx {worst * 1000:.0f} ms; SQLITE_BUSY {busy};"
                f" late commits {late}; overrun max {overrun:.2f} s",
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


def _populate_purged(conn: sqlite3.Connection, sizes: Sizes) -> int:
    """Eligible payloads among many rows whose payload is already gone.

    Every completed row spends days 7-30 in this state, so after a week
    of traffic it is the normal shape of the table.
    """
    now = SystemClock().now_ms()
    purged = now - 10 * DAY_MS  # between the 7 d and the 30 d cutoff
    bulk_insert(
        conn, "purged", sizes.retention_purged, "completed", purged, purged, 1
    )
    with immediate(conn):
        conn.execute("UPDATE webhook_events SET payload_json = NULL")
    eligible = now - 8 * DAY_MS
    bulk_insert(
        conn, "old", sizes.retention_eligible, "completed", eligible,
        eligible, 1,
    )  # fmt: skip
    conn.execute("ANALYZE")
    return now


def _payload_batches(
    conn: sqlite3.Connection, params: dict
) -> tuple[list[float], float]:
    """Time each payload batch; returns (changing ones, final empty one)."""
    samples: list[float] = []
    while True:
        start = time.perf_counter()
        with immediate(conn):
            changed = conn.execute(PURGE_PAYLOAD_SQL, params).rowcount
        elapsed = time.perf_counter() - start
        if changed == 0:
            return samples, elapsed
        samples.append(elapsed)


def bench_retention(
    base: Path, repeat: int, sizes: Sizes
) -> tuple[list[Result], list[str]]:
    """One batch of 1000 per tier, and a whole ``purge()``.

    Batches execute the repository's own SQL once each under
    ``immediate()`` (exactly what ``purge`` does per batch), so their
    duration is the write-lock hold time. Every eligible batch is
    timed; then ``purge()`` runs on a freshly populated database. A
    third database repeats the payload batches with many already-purged
    rows below the cutoff.
    """
    payload: list[float] = []
    delete: list[float] = []
    totals: list[float] = []
    purged_batches: list[float] = []
    purged_empty: list[float] = []
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
            expected = (sizes.retention_eligible, sizes.retention_eligible, 0)
            got = (
                purged.payloads_cleared,
                purged.completed_deleted,
                purged.failed_deleted,
            )
            if got != expected:
                raise RuntimeError(f"purge returned {purged}")
        finally:
            pool.close_all()

        pool = ConnectionPool(fresh_db(base, "retention-purged"))
        try:
            conn = pool.get()
            now = _populate_purged(conn, sizes)
            params = {"cutoff": now - 7 * DAY_MS, "limit": BATCH}
            if rep == 0:
                plans.append(
                    f"payload batch, {sizes.retention_purged} already"
                    f" purged: {query_plan(conn, PURGE_PAYLOAD_SQL, params)}"
                )
            samples, empty = _payload_batches(conn, params)
            purged_batches += samples
            purged_empty.append(empty)
        finally:
            pool.close_all()
    eligible = sizes.retention_eligible
    other = sizes.retention_other
    shape = f"{eligible} eligible + {other} recent rows"
    purged_shape = (
        f"{eligible} eligible + {sizes.retention_purged} already purged rows"
    )
    return [
        ms_result("retention", "payload clear batch 1000", payload, shape),
        ms_result("retention", "delete batch 1000", delete, shape),
        ms_result(
            "retention",
            f"purge() {eligible} eligible",
            totals,
            f"{2 * eligible // BATCH} batches; {shape}",
        ),
        ms_result(
            "retention",
            "payload clear batch 1000, purged",
            purged_batches,
            purged_shape,
        ),
        ms_result(
            "retention",
            "payload empty batch, purged",
            purged_empty,
            f"last batch, nothing left to clear; {purged_shape}",
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
    parser.add_argument(
        "--sync-rate",
        type=float,
        default=SYNC_RATE,
        help="pace of the paced sync case in tx/s (0 = skip it)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.repeat <= 0:
        raise SystemExit("--repeat must be positive")
    if args.sync_rate < 0:
        raise SystemExit("--sync-rate must not be negative")
    sizes = QUICK if args.quick else FULL
    base: Path = args.dir
    base.mkdir(parents=True, exist_ok=True)
    selected = BENCHES if args.bench == "all" else (args.bench,)
    rows: list[Result] = []
    plans: list[str] = []
    started = time.perf_counter()
    for bench in selected:
        if bench == "sync":
            rows += bench_sync(base, args.repeat, sizes, args.sync_rate)
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

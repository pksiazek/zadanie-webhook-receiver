import json
import sqlite3

from webhook_receiver.common.clock import Clock
from webhook_receiver.common.db import ConnectionPool, immediate
from webhook_receiver.common.models import Event, RecoveryResult, Status

# The WHERE terms below must repeat the partial-index predicates literally
# (see SCHEMA_SQL): SQLite does not do algebra on them.
CLAIM_SQL = (
    "UPDATE webhook_events SET status = 'processing',"
    " attempts = attempts + 1, run_at = :now + :lease"
    " WHERE id IN (SELECT id FROM webhook_events"
    " WHERE status IN ('pending', 'retrying') AND run_at <= :now"
    " ORDER BY run_at LIMIT :limit) RETURNING *"
)
RECOVER_RETRY_SQL = (
    "UPDATE webhook_events SET status = 'retrying', run_at = :now,"
    " last_error = 'lease expired'"
    " WHERE status = 'processing' AND run_at <= :now AND attempts < :max"
)
RECOVER_FAIL_SQL = (
    "UPDATE webhook_events SET status = 'failed', finished_at = :now,"
    " last_error = 'lease expired'"
    " WHERE status = 'processing' AND run_at <= :now AND attempts >= :max"
)


def _loads(raw: str | None) -> dict | None:
    return None if raw is None else json.loads(raw)


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        source=row["source"],
        event_type=row["event_type"],
        payload=_loads(row["payload_json"]),
        status=Status(row["status"]),
        attempts=row["attempts"],
        run_at=row["run_at"],
        last_error=row["last_error"],
        result=_loads(row["result_json"]),
        traceparent=row["traceparent"],
        created_at=row["created_at"],
        finished_at=row["finished_at"],
    )


class EventRepository:
    """SQLite-backed store and queue of webhook events."""

    def __init__(self, pool: ConnectionPool, clock: Clock) -> None:
        self._pool = pool
        self._clock = clock

    def insert_or_get(
        self,
        key: str,
        source: str,
        event_type: str,
        payload: dict,
        traceparent: str | None,
    ) -> tuple[Event, bool]:
        """Insert a new pending event or return the existing one.

        Duplicates are detected by key only; returns (event, created).
        """
        now = self._clock.now_ms()
        payload_json = json.dumps(payload, separators=(",", ":"))
        conn = self._pool.get()
        with immediate(conn):
            row = conn.execute(
                "INSERT INTO webhook_events (idempotency_key, source,"
                " event_type, payload_json, status, attempts, run_at,"
                " traceparent, created_at)"
                " VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)"
                " ON CONFLICT(idempotency_key) DO NOTHING RETURNING *",
                (key, source, event_type, payload_json, now, traceparent, now),
            ).fetchone()
            created = row is not None
            if row is None:
                row = conn.execute(
                    "SELECT * FROM webhook_events WHERE idempotency_key = ?",
                    (key,),
                ).fetchone()
            return _row_to_event(row), created

    def get(self, key: str) -> Event | None:
        # Plain read outside a transaction (consistent snapshot under WAL).
        conn = self._pool.get()
        row = conn.execute(
            "SELECT * FROM webhook_events WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        return None if row is None else _row_to_event(row)

    def claim(self, limit: int, lease_ms: int) -> list[Event]:
        """Atomically reserve up to `limit` due events for processing."""
        if limit <= 0 or lease_ms <= 0:
            # A negative SQLite LIMIT means "no limit".
            raise ValueError("limit and lease_ms must be positive")
        now = self._clock.now_ms()
        conn = self._pool.get()
        with immediate(conn):
            rows = conn.execute(
                CLAIM_SQL, {"now": now, "lease": lease_ms, "limit": limit}
            ).fetchall()
        events = [_row_to_event(r) for r in rows]
        # RETURNING order is unspecified. Within a batch order is not
        # significant (all rows share run_at = now + lease); this is just
        # a stable tie-break.
        events.sort(key=lambda e: (e.created_at, e.id))
        return events

    def recover_expired(self, max_attempts: int) -> RecoveryResult:
        """Requeue or fail events whose lease expired (worker died)."""
        params = {"now": self._clock.now_ms(), "max": max_attempts}
        conn = self._pool.get()
        with immediate(conn):
            retried = conn.execute(RECOVER_RETRY_SQL, params).rowcount
            failed = conn.execute(RECOVER_FAIL_SQL, params).rowcount
        return RecoveryResult(retried=retried, failed=failed)

    def complete(self, event_id: int, attempt: int, result: dict) -> bool:
        """Store the result; False if the attempt is stale (fencing)."""
        now = self._clock.now_ms()
        result_json = json.dumps(result, separators=(",", ":"))
        conn = self._pool.get()
        with immediate(conn):
            cur = conn.execute(
                "UPDATE webhook_events SET status = 'completed',"
                " result_json = ?, finished_at = ?, last_error = NULL"
                " WHERE id = ? AND status = 'processing' AND attempts = ?",
                (result_json, now, event_id, attempt),
            )
            return cur.rowcount == 1

    def record_failure(
        self,
        event_id: int,
        attempt: int,
        error: str,
        retry_at: int | None,
    ) -> bool:
        """Schedule a retry (or fail if `retry_at` is None); fenced."""
        now = self._clock.now_ms()
        conn = self._pool.get()
        with immediate(conn):
            if retry_at is None:
                cur = conn.execute(
                    "UPDATE webhook_events SET status = 'failed',"
                    " finished_at = ?, last_error = ?"
                    " WHERE id = ? AND status = 'processing'"
                    " AND attempts = ?",
                    (now, error, event_id, attempt),
                )
            else:
                cur = conn.execute(
                    "UPDATE webhook_events SET status = 'retrying',"
                    " run_at = ?, last_error = ?"
                    " WHERE id = ? AND status = 'processing'"
                    " AND attempts = ?",
                    (retry_at, error, event_id, attempt),
                )
            return cur.rowcount == 1

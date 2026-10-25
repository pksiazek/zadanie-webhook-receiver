import json
import sqlite3

from webhook_receiver.common.clock import Clock
from webhook_receiver.common.db import ConnectionPool, immediate
from webhook_receiver.common.models import Event, Status


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
        row = (
            self._pool.get()
            .execute(
                "SELECT * FROM webhook_events WHERE idempotency_key = ?",
                (key,),
            )
            .fetchone()
        )
        return None if row is None else _row_to_event(row)

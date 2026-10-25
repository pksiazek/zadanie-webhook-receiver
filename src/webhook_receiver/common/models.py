from dataclasses import dataclass
from enum import StrEnum


class Status(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRYING = "retrying"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class Event:
    id: int
    idempotency_key: str
    source: str
    event_type: str
    payload: dict | None
    status: Status
    attempts: int
    run_at: int
    last_error: str | None
    result: dict | None
    traceparent: str | None
    created_at: int
    finished_at: int | None

    @property
    def payload_purged(self) -> bool:
        return self.finished_at is not None and self.payload is None


@dataclass(frozen=True)
class QueueStats:
    pending: int
    retrying: int
    processing: int
    oldest_due_age_ms: int

    @property
    def backlog(self) -> int:
        return self.pending + self.retrying


@dataclass(frozen=True)
class RecoveryResult:
    retried: int
    failed: int


@dataclass(frozen=True)
class RetentionPolicy:
    payload_ms: int
    completed_ms: int
    failed_ms: int
    batch_size: int


@dataclass(frozen=True)
class PurgeResult:
    payloads_cleared: int
    completed_deleted: int
    failed_deleted: int

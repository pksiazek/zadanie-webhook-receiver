"""Request and response models of the HTTP API (spec §6)."""

from datetime import UTC, datetime, timedelta
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
)

from webhook_receiver.common.models import Event, Status

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_SCHEDULED = frozenset({Status.PENDING, Status.RETRYING})


def iso_utc(ms: int | None) -> str | None:
    """Epoch milliseconds as ISO 8601 UTC, e.g. ``2026-10-25T10:00:08.000Z``.

    Integer arithmetic keeps the milliseconds exact (no float rounding).
    """
    if ms is None:
        return None
    moment = _EPOCH + timedelta(milliseconds=ms)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class WebhookIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    idempotency_key: str = Field(min_length=1, max_length=255)
    event_type: str = Field(min_length=1)
    payload: dict[str, Any]


class AcceptedOut(BaseModel):
    idempotency_key: str
    status: Status = Status.PENDING
    status_url: str


class StatusOut(BaseModel):
    idempotency_key: str
    source: str
    event_type: str
    status: Status
    attempts: int
    max_attempts: int
    next_attempt_at: str | None
    last_error: str | None
    result: dict[str, Any] | None
    created_at: str
    finished_at: str | None
    payload_purged: bool
    duplicate: bool | None = None

    @model_serializer(mode="wrap")
    def _omit_unset_duplicate(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        # Only ``duplicate`` is dropped when None; other nulls stay.
        data = handler(self)
        if data.get("duplicate") is None:
            data.pop("duplicate", None)
        return data

    @classmethod
    def from_event(
        cls, event: Event, max_attempts: int, duplicate: bool | None = None
    ) -> Self:
        return cls(
            idempotency_key=event.idempotency_key,
            source=event.source,
            event_type=event.event_type,
            status=event.status,
            attempts=event.attempts,
            max_attempts=max_attempts,
            next_attempt_at=(
                iso_utc(event.run_at) if event.status in _SCHEDULED else None
            ),
            last_error=event.last_error,
            result=event.result,
            created_at=iso_utc(event.created_at),
            finished_at=iso_utc(event.finished_at),
            payload_purged=event.payload_purged,
            duplicate=duplicate,
        )

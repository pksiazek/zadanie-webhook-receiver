from pathlib import Path
from typing import Annotated, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings

Port = Annotated[int, Field(ge=0, le=65535)]
PositiveInt = Annotated[int, Field(gt=0)]
PositiveFloat = Annotated[float, Field(gt=0)]


class Settings(BaseSettings):
    """Runtime configuration; env var name is the upper-cased field name."""

    db_path: Path = Path("data/webhooks.db")
    api_port: Port = 8000
    max_attempts: PositiveInt = 5
    lease_seconds: PositiveFloat = 30
    backoff_base_seconds: PositiveFloat = 2
    backoff_max_seconds: PositiveFloat = 60
    worker_concurrency: PositiveInt = 20
    poll_interval_seconds: PositiveFloat = 0.5
    shutdown_grace_seconds: PositiveFloat = 10
    simulated_min_seconds: PositiveFloat = 2
    simulated_max_seconds: PositiveFloat = 5
    rate_limit_per_second: PositiveFloat = 50
    rate_limit_burst: PositiveInt = 100
    rate_limit_max_sources: PositiveInt = 10_000
    max_backlog: PositiveInt = 100_000
    backpressure_retry_after_seconds: PositiveInt = 5
    queue_stats_refresh_seconds: PositiveFloat = 1
    retention_interval_seconds: PositiveFloat = 600
    retention_payload_days: PositiveFloat = 7
    retention_completed_days: PositiveFloat = 30
    retention_failed_days: PositiveFloat = 30
    retention_batch_size: PositiveInt = 1000
    max_body_bytes: PositiveInt = 262_144
    worker_metrics_port: Port = 9100
    embedded_worker: bool = False
    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_consistency(self) -> Self:
        if self.lease_seconds <= self.simulated_max_seconds:
            raise ValueError(
                "lease_seconds must be greater than simulated_max_seconds"
            )
        if self.simulated_min_seconds > self.simulated_max_seconds:
            raise ValueError(
                "simulated_min_seconds must not exceed simulated_max_seconds"
            )
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError(
                "backoff_max_seconds must not be below backoff_base_seconds"
            )
        return self

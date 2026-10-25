from pathlib import Path
from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Runtime configuration; env var name is the upper-cased field name."""

    db_path: Path = Path("data/webhooks.db")
    api_port: int = 8000
    max_attempts: int = 5
    lease_seconds: float = 30
    backoff_base_seconds: float = 2
    backoff_max_seconds: float = 60
    worker_concurrency: int = 20
    poll_interval_seconds: float = 0.5
    shutdown_grace_seconds: float = 10
    simulated_min_seconds: float = 2
    simulated_max_seconds: float = 5
    rate_limit_per_second: float = 50
    rate_limit_burst: int = 100
    rate_limit_max_sources: int = 10_000
    max_backlog: int = 100_000
    backpressure_retry_after_seconds: int = 5
    queue_stats_refresh_seconds: float = 1
    retention_interval_seconds: float = 600
    retention_payload_days: float = 7
    retention_completed_days: float = 30
    retention_failed_days: float = 30
    retention_batch_size: int = 1000
    max_body_bytes: int = 262_144
    worker_metrics_port: int = 9100
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
        return self

import pytest

from webhook_receiver.common.config import Settings


def test_defaults_match_spec():
    s = Settings()
    assert s.max_attempts == 5 and s.lease_seconds == 30
    assert s.worker_concurrency == 20 and s.max_backlog == 100_000
    assert s.max_body_bytes == 262_144


def test_env_override(monkeypatch):
    monkeypatch.setenv("MAX_ATTEMPTS", "3")
    assert Settings().max_attempts == 3


def test_lease_must_exceed_simulation():
    with pytest.raises(ValueError):
        Settings(lease_seconds=5, simulated_max_seconds=5)


def test_simulated_min_must_not_exceed_max():
    with pytest.raises(ValueError):
        Settings(simulated_min_seconds=4, simulated_max_seconds=3)

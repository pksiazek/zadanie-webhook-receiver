import pytest
from pydantic import ValidationError

from webhook_receiver.common.config import Settings


@pytest.fixture(autouse=True)
def _clean_settings_env(monkeypatch):
    """Drop any exported variable that would feed a Settings field."""
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)


def test_defaults_match_spec():
    s = Settings()
    assert s.max_attempts == 5 and s.lease_seconds == 30
    assert s.worker_concurrency == 20 and s.max_backlog == 100_000
    assert s.max_body_bytes == 262_144


def test_env_override(monkeypatch):
    monkeypatch.setenv("MAX_ATTEMPTS", "3")
    assert Settings().max_attempts == 3


def test_settings_do_not_read_env_file():
    assert Settings.model_config.get("env_file") is None


def test_lease_must_exceed_simulation():
    with pytest.raises(ValueError):
        Settings(lease_seconds=5, simulated_max_seconds=5)


def test_simulated_min_must_not_exceed_max():
    with pytest.raises(ValueError):
        Settings(simulated_min_seconds=4, simulated_max_seconds=3)


def test_zero_concurrency_rejected():
    with pytest.raises(ValueError):
        Settings(worker_concurrency=0)


def test_negative_lease_rejected():
    with pytest.raises(ValueError):
        Settings(lease_seconds=-1)


@pytest.mark.parametrize("port", [0, 8080, 65535])
def test_valid_ports_accepted(port):
    s = Settings(api_port=port, worker_metrics_port=port)
    assert s.api_port == port and s.worker_metrics_port == port


@pytest.mark.parametrize("field", ["api_port", "worker_metrics_port"])
@pytest.mark.parametrize("port", [-1, 65536])
def test_invalid_ports_rejected(field, port):
    with pytest.raises(ValueError):
        Settings(**{field: port})


def test_backoff_max_must_not_be_below_base():
    with pytest.raises(ValueError):
        Settings(backoff_base_seconds=10, backoff_max_seconds=5)


@pytest.mark.parametrize("value", ["verbose", "", "TRACE"])
def test_invalid_log_level_rejected(monkeypatch, value):
    monkeypatch.setenv("LOG_LEVEL", value)
    with pytest.raises(ValidationError):
        Settings()


def test_log_level_accepts_lowercase(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "debug")
    assert Settings().log_level == "DEBUG"

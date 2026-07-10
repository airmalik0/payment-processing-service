"""Конфигурация: backoff и валидация."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from payments.config import Settings


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "database_url": "postgresql+asyncpg://u:p@localhost:5432/db",
        "retry_base_delay_seconds": 1.0,
        "retry_multiplier": 5.0,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_retry_delays_are_exponential() -> None:
    s = _settings()
    assert s.retry_delay_seconds(1) == pytest.approx(1.0)
    assert s.retry_delay_seconds(2) == pytest.approx(5.0)
    assert s.retry_delay_seconds(3) == pytest.approx(25.0)


def test_retry_delay_rejects_zero_attempt() -> None:
    with pytest.raises(ValueError, match="начинается с 1"):
        _settings().retry_delay_seconds(0)


def test_rejects_sync_driver() -> None:
    with pytest.raises(ValidationError, match="asyncpg"):
        _settings(database_url="postgresql://u:p@localhost:5432/db")


def test_rejects_inverted_delay_range() -> None:
    with pytest.raises(ValidationError, match="GATEWAY_MAX"):
        _settings(gateway_min_delay_seconds=5.0, gateway_max_delay_seconds=2.0)


def test_success_rate_bounds() -> None:
    with pytest.raises(ValidationError):
        _settings(gateway_success_rate=1.5)

"""Конфигурация сервиса. Единственное место, где читается окружение."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки всех процессов: api, consumer, outbox-relay."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # --- API ---
    api_key: SecretStr = Field(default=SecretStr("local-dev-api-key"))
    log_level: str = "INFO"
    log_json: bool = True

    # --- PostgreSQL ---
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    db_pool_size: int = 10
    db_max_overflow: int = 5
    db_echo: bool = False

    # --- RabbitMQ ---
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    consumer_prefetch_count: int = 10

    # --- Эмуляция платёжного шлюза ---
    gateway_min_delay_seconds: float = 2.0
    gateway_max_delay_seconds: float = 5.0
    gateway_success_rate: float = Field(default=0.9, ge=0.0, le=1.0)

    # --- Webhook ---
    webhook_timeout_seconds: float = 5.0
    webhook_signing_secret: SecretStr = SecretStr("local-dev-webhook-secret")

    # --- Retry / DLQ ---
    max_retries: int = Field(default=3, ge=1)
    retry_base_delay_seconds: float = Field(default=1.0, gt=0)
    retry_multiplier: float = Field(default=5.0, gt=1)

    # --- Outbox relay ---
    outbox_poll_interval_seconds: float = Field(default=0.2, gt=0)
    outbox_batch_size: int = Field(default=50, ge=1)

    @field_validator("database_url")
    @classmethod
    def _require_async_driver(cls, value: str) -> str:
        """asyncpg-драйвер обязателен: синхронный psycopg молча заблокирует event loop."""
        if not value.startswith("postgresql+asyncpg://"):
            msg = "DATABASE_URL должен использовать драйвер postgresql+asyncpg"
            raise ValueError(msg)
        return value

    @field_validator("gateway_max_delay_seconds")
    @classmethod
    def _check_delay_range(cls, value: float, info: object) -> float:
        data = getattr(info, "data", {})
        minimum = data.get("gateway_min_delay_seconds")
        if minimum is not None and value < minimum:
            msg = "GATEWAY_MAX_DELAY_SECONDS не может быть меньше GATEWAY_MIN_DELAY_SECONDS"
            raise ValueError(msg)
        return value

    def retry_delay_seconds(self, attempt: int) -> float:
        """Задержка перед попыткой `attempt` (1-based): 1с, 5с, 25с при базе 1 и множителе 5."""
        if attempt < 1:
            msg = "Номер попытки начинается с 1"
            raise ValueError(msg)
        return self.retry_base_delay_seconds * (self.retry_multiplier ** (attempt - 1))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Синглтон настроек. lru_cache — чтобы .env читался один раз за процесс."""
    return Settings()

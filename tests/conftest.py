"""Общие фикстуры.

Интеграционным тестам нужна живая БД. URL берётся из `TEST_DATABASE_URL`
(по умолчанию — локальный Postgres из docker compose). Если БД недоступна,
интеграционные тесты пропускаются, а не падают — юнит-тесты идут всегда.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payments.config import Settings
from payments.db.base import Base

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://payments:payments@localhost:5432/payments",
)


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Настройки для тестов: детерминированные, без внешних задержек."""
    return Settings(
        database_url=TEST_DATABASE_URL,
        api_key="test-api-key",
        webhook_signing_secret="test-webhook-secret",
        gateway_min_delay_seconds=0.0,
        gateway_max_delay_seconds=0.0,
        gateway_success_rate=1.0,
        max_retries=3,
        retry_base_delay_seconds=1.0,
        retry_multiplier=5.0,
        log_json=False,
    )


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    """Один engine на сессию. Схема создаётся из моделей и удаляется в конце.

    Пропускает интеграционные тесты, если БД недоступна.
    """
    eng = create_async_engine(settings.database_url, pool_pre_ping=True)
    try:
        async with eng.connect() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
            await conn.commit()
    except Exception as error:
        await eng.dispose()
        pytest.skip(f"Тестовая БД недоступна ({TEST_DATABASE_URL}): {error}")

    yield eng

    async with eng.connect() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.commit()
    await eng.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def db_connection(engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    """Соединение с внешней транзакцией: всё, что сделал тест, откатывается.

    Тест может даже вызвать `commit()` — он освобождает savepoint, а внешний
    rollback в финале всё равно возвращает БД в исходное состояние. Тесты
    изолированы друг от друга без пересоздания схемы.
    """
    async with engine.connect() as connection:
        transaction = await connection.begin()
        yield connection
        await transaction.rollback()


@pytest_asyncio.fixture(loop_scope="session")
async def session_factory(
    db_connection: AsyncConnection,
) -> async_sessionmaker[AsyncSession]:
    """Фабрика сессий поверх тестового соединения (savepoint-режим)."""
    return async_sessionmaker(
        bind=db_connection,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )


@pytest_asyncio.fixture(loop_scope="session")
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Одна сессия на тест."""
    async with session_factory() as sess:
        yield sess


@pytest.fixture
def sample_payment_payload() -> dict[str, object]:
    """Валидное тело запроса на создание платежа."""
    return {
        "amount": Decimal("100.50"),
        "currency": "RUB",
        "description": "Тестовый платёж",
        "metadata": {"order_id": "A-1"},
        "webhook_url": "https://example.test/webhook",
    }

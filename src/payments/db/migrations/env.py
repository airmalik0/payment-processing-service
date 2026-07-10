"""Alembic: онлайн-миграции через async engine."""

from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig

import sqlalchemy as sa
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context
from payments.db.base import Base
from payments.db.models import OutboxMessage, Payment  # noqa: F401 — регистрация в metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# Произвольная константа-«namespace» сервиса для pg_advisory_lock.
MIGRATION_LOCK_ID = 8_452_119_003_744_211


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        msg = "Переменная окружения DATABASE_URL не задана"
        raise RuntimeError(msg)
    return url


def run_migrations_offline() -> None:
    """Генерация SQL без подключения к БД."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)

    with context.begin_transaction():
        # Alembic не сериализует конкурентные `upgrade head` сам: при одновременном
        # старте нескольких контейнеров они дерутся даже за создание alembic_version.
        # Advisory-lock уровня транзакции отпускается на COMMIT.
        connection.execute(
            sa.text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": MIGRATION_LOCK_ID}
        )
        context.run_migrations()


async def run_async_migrations() -> None:
    # URL кладём прямо в секцию, минуя config.set_main_option / configparser:
    # символ `%` в пароле сломал бы интерполяцию configparser.
    configuration = config.get_section(config.config_ini_section, {}) or {}
    configuration["sqlalchemy.url"] = _database_url()

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

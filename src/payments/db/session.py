"""Асинхронный engine и фабрика сессий."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payments.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    """Engine с пулом, пригодным для долгоживущего процесса."""
    return create_async_engine(
        settings.database_url,
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        # Соединение могло быть закрыто со стороны Postgres, пока процесс простаивал.
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """`expire_on_commit=False`: после commit объекты остаются пригодными для чтения."""
    return async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


@asynccontextmanager
async def session_scope(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Транзакция на блок: commit при успехе, rollback при исключении."""
    async with session_factory() as session:
        try:
            yield session
        except BaseException:
            await session.rollback()
            raise

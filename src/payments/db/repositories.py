"""Доступ к данным. Никакой бизнес-логики — только запросы."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from payments.db.models import OutboxMessage, Payment

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

# Код ошибки PostgreSQL для нарушения уникальности.
UNIQUE_VIOLATION = "23505"


class PaymentRepository:
    """Чтение и запись платежей."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, payment: Payment) -> None:
        """Добавляет платёж в сессию. Транзакцией управляет вызывающий."""
        self._session.add(payment)

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        """Читает платёж под блокировкой строки.

        Два потребителя, получившие дубликат одного события, сериализуются здесь:
        второй ждёт коммита первого и увидит уже финальный статус.
        """
        result = await self._session.execute(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()


class OutboxRepository:
    """Очередь исходящих событий в таблице."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, message: OutboxMessage) -> None:
        self._session.add(message)

    async def fetch_batch_for_publishing(
        self, *, batch_size: int, now: datetime
    ) -> Sequence[OutboxMessage]:
        """Забирает пачку неопубликованных событий, блокируя строки за собой.

        `FOR UPDATE ... SKIP LOCKED` — то, что позволяет держать несколько реплик
        relay: соседний процесс просто пропустит занятые строки вместо ожидания.
        """
        result = await self._session.execute(
            select(OutboxMessage)
            .where(
                OutboxMessage.published_at.is_(None),
                OutboxMessage.next_retry_at <= now,
            )
            .order_by(OutboxMessage.created_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        return result.scalars().all()

    async def count_unpublished(self) -> int:
        result = await self._session.execute(
            select(func.count())
            .select_from(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
        )
        return int(result.scalar_one())

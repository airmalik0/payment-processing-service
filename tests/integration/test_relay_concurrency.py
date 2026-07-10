"""Relay под конкуренцией: две реплики не публикуют одно событие дважды."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from payments.config import Settings
from payments.db.models import OutboxMessage
from payments.outbox.relay import OutboxRelay

pytestmark = pytest.mark.integration


class CountingBroker:
    def __init__(self) -> None:
        self.published: list[str] = []

    async def publish(self, message: Any, *, message_id: str, **kwargs: Any) -> None:
        self.published.append(message_id)


async def test_two_relays_do_not_double_publish(settings: Settings, engine: AsyncEngine) -> None:
    """FOR UPDATE SKIP LOCKED: параллельные релеи делят работу, не дублируя её.

    Идёт мимо savepoint-изоляции — нужны настоящие параллельные транзакции.
    Сам подчищает вставленные строки.
    """
    real_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    tag = f"concurrency-{uuid.uuid4()}"

    ids = []
    async with real_factory() as seed:
        for _ in range(20):
            message = OutboxMessage(
                id=uuid.uuid4(),
                aggregate_type=tag,  # метка, чтобы отличить свои строки от чужих
                aggregate_id=uuid.uuid4(),
                event_type="payment.created",
                routing_key="payments.new",
                payload={"payment_id": str(uuid.uuid4())},
            )
            seed.add(message)
            ids.append(message.id)
        await seed.commit()

    broker_a = CountingBroker()
    broker_b = CountingBroker()
    relay_a = OutboxRelay(broker=broker_a, session_factory=real_factory, settings=settings)  # type: ignore[arg-type]
    relay_b = OutboxRelay(broker=broker_b, session_factory=real_factory, settings=settings)  # type: ignore[arg-type]

    try:
        # Гоняем оба релея наперегонки, пока не разгребут все свои сообщения.
        for _ in range(20):
            await asyncio.gather(
                relay_a.publish_pending_batch(),
                relay_b.publish_pending_batch(),
            )
            async with real_factory() as check:
                remaining = await check.scalar(
                    select(OutboxMessage)
                    .where(OutboxMessage.aggregate_type == tag)
                    .where(OutboxMessage.published_at.is_(None))
                    .limit(1)
                )
            if remaining is None:
                break

        published_by_us = [
            mid for mid in (broker_a.published + broker_b.published) if uuid.UUID(mid) in set(ids)
        ]
        # Ровно 20 публикаций, без дублей: каждое событие ушло однажды.
        assert len(published_by_us) == 20
        assert len(set(published_by_us)) == 20
    finally:
        async with real_factory() as cleanup:
            await cleanup.execute(delete(OutboxMessage).where(OutboxMessage.aggregate_type == tag))
            await cleanup.commit()

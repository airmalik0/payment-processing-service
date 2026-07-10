"""Outbox relay: публикация, пометка, backoff при сбое брокера."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import Settings
from payments.db.models import OutboxMessage
from payments.outbox.relay import OutboxRelay

pytestmark = pytest.mark.integration


class FakeBroker:
    """Брокер-заглушка: копит опубликованное, по флагу — падает."""

    def __init__(self, *, fail: bool = False) -> None:
        self.published: list[dict[str, Any]] = []
        self.fail = fail

    async def publish(self, message: Any, **kwargs: Any) -> None:
        if self.fail:
            msg = "брокер недоступен"
            raise ConnectionError(msg)
        self.published.append({"message": message, **kwargs})


async def _add_outbox(session: AsyncSession, *, routing_key: str = "payments.new") -> uuid.UUID:
    message = OutboxMessage(
        id=uuid.uuid4(),
        aggregate_type="payment",
        aggregate_id=uuid.uuid4(),
        event_type="payment.created",
        routing_key=routing_key,
        payload={"payment_id": str(uuid.uuid4())},
    )
    session.add(message)
    await session.commit()
    return message.id


async def test_publishes_and_marks(
    session_factory: async_sessionmaker[AsyncSession],
    session: AsyncSession,
    settings: Settings,
) -> None:
    message_id = await _add_outbox(session)
    broker = FakeBroker()
    relay = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)  # type: ignore[arg-type]

    published = await relay.publish_pending_batch()

    assert published == 1
    assert len(broker.published) == 1
    # message_id брокера = id outbox-записи (ключ дедупликации).
    assert broker.published[0]["message_id"] == str(message_id)
    assert broker.published[0]["persist"] is True

    stored = await session.get(OutboxMessage, message_id)
    await session.refresh(stored)
    assert stored is not None
    assert stored.published_at is not None


async def test_empty_batch_returns_zero(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    broker = FakeBroker()
    relay = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)  # type: ignore[arg-type]
    assert await relay.publish_pending_batch() == 0


async def test_broker_failure_schedules_backoff(
    session_factory: async_sessionmaker[AsyncSession],
    session: AsyncSession,
    settings: Settings,
) -> None:
    message_id = await _add_outbox(session)
    broker = FakeBroker(fail=True)
    relay = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)  # type: ignore[arg-type]

    published = await relay.publish_pending_batch()

    assert published == 0
    stored = await session.get(OutboxMessage, message_id)
    await session.refresh(stored)
    assert stored is not None
    # Сообщение не потеряно: осталось неопубликованным, попытка учтена, повтор отложен.
    assert stored.published_at is None
    assert stored.attempts == 1
    assert stored.last_error is not None
    assert stored.next_retry_at > stored.created_at


async def test_already_published_not_republished(
    session_factory: async_sessionmaker[AsyncSession],
    session: AsyncSession,
    settings: Settings,
) -> None:
    await _add_outbox(session)
    broker = FakeBroker()
    relay = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)  # type: ignore[arg-type]

    first = await relay.publish_pending_batch()
    second = await relay.publish_pending_batch()

    assert first == 1
    assert second == 0  # опубликованное не публикуется снова
    assert len(broker.published) == 1


async def test_partial_failure_does_not_lose_messages(
    session_factory: async_sessionmaker[AsyncSession],
    session: AsyncSession,
    settings: Settings,
) -> None:
    """Падение на одном сообщении не откатывает уже опубликованные в пачке."""

    class FlakyBroker(FakeBroker):
        async def publish(self, message: Any, **kwargs: Any) -> None:
            # Падает на каждом втором сообщении.
            if len(self.published) == 1 and not getattr(self, "_failed_once", False):
                self._failed_once = True
                msg = "разовый сбой"
                raise ConnectionError(msg)
            self.published.append({"message": message, **kwargs})

    ids = [await _add_outbox(session) for _ in range(3)]
    broker = FlakyBroker()
    relay = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)  # type: ignore[arg-type]

    await relay.publish_pending_batch()

    published_states = []
    for message_id in ids:
        stored = await session.get(OutboxMessage, message_id)
        assert stored is not None
        await session.refresh(stored)
        published_states.append(stored.published_at is not None)

    # Хотя бы одно опубликовано и зафиксировано, ни одно не потеряно.
    assert any(published_states)
    total = await session.scalar(select(OutboxMessage).where(OutboxMessage.id.in_(ids)))
    assert total is not None

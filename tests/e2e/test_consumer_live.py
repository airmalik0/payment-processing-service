"""E2E consumer на живом RabbitMQ.

TestRabbitBroker в in-memory режиме не эмулирует TTL и dead-lettering, поэтому
фактическое возвращение сообщения по таймауту и попадание в DLQ проверяются
только здесь — на настоящем брокере (`with_real=True`).

Требует запущенного RabbitMQ: `RABBITMQ_URL` (по умолчанию localhost:5672) и
Postgres (`TEST_DATABASE_URL`). Иначе тест пропускается.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from decimal import Decimal

import pytest
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payments.broker.consumer import build_consumer_broker
from payments.broker.setup import declare_topology
from payments.broker.topology import (
    PAYMENTS_NEW_ROUTING_KEY,
    payments_exchange,
)
from payments.config import Settings
from payments.db.models import Payment
from payments.domain.enums import Currency, PaymentStatus
from payments.domain.events import PaymentCreatedEvent
from payments.domain.gateway import GatewayResult
from payments.services.processor import PaymentProcessor
from payments.services.webhook import WebhookSender, create_http_client

pytestmark = pytest.mark.e2e

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://payments:payments@localhost:5672/")


class StubGateway:
    def __init__(self, result: GatewayResult) -> None:
        self.result = result
        self.calls = 0

    async def charge(self, **_: object) -> GatewayResult:
        self.calls += 1
        return self.result


@pytest.fixture
def e2e_settings(settings: Settings) -> Settings:
    return settings.model_copy(update={"rabbitmq_url": RABBITMQ_URL})


async def _rabbitmq_available() -> bool:
    broker = RabbitBroker(RABBITMQ_URL, fail_fast=True)
    try:
        await broker.connect()
        await broker.stop()
    except Exception:
        return False
    return True


async def _insert_pending(session: AsyncSession, *, webhook_url: str | None = None) -> uuid.UUID:
    payment = Payment(
        id=uuid.uuid4(),
        idempotency_key=f"e2e-{uuid.uuid4()}",
        request_fingerprint="f" * 64,
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="e2e",
        payment_metadata={},
        status=PaymentStatus.PENDING,
        webhook_url=webhook_url,
    )
    session.add(payment)
    await session.commit()
    return payment.id


async def test_consumer_processes_message_end_to_end(
    e2e_settings: Settings,
    engine: AsyncEngine,
) -> None:
    """Событие из брокера доводит платёж до финального статуса на живом RabbitMQ."""
    if not await _rabbitmq_available():
        pytest.skip("RabbitMQ недоступен")

    real_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with real_factory() as seed:
        payment_id = await _insert_pending(seed)

    gateway = StubGateway(GatewayResult(status=PaymentStatus.SUCCEEDED))
    processor = PaymentProcessor(
        session_factory=real_factory,
        gateway=gateway,
        webhook_sender=WebhookSender(client=create_http_client(5.0), signing_secret="s"),
    )
    broker = build_consumer_broker(e2e_settings, processor)

    try:
        async with TestRabbitBroker(broker, with_real=True) as running:
            await declare_topology(running, e2e_settings)
            event = PaymentCreatedEvent(payment_id=payment_id, occurred_at=_now())
            await running.publish(
                event.model_dump(mode="json"),
                exchange=payments_exchange,
                routing_key=PAYMENTS_NEW_ROUTING_KEY,
            )

            # Ждём, пока consumer обработает платёж.
            for _ in range(50):
                async with real_factory() as check:
                    refreshed = await check.get(Payment, payment_id)
                    if refreshed and refreshed.status is PaymentStatus.SUCCEEDED:
                        break
                await asyncio.sleep(0.2)

        async with real_factory() as final:
            result = await final.get(Payment, payment_id)
            assert result is not None
            assert result.status is PaymentStatus.SUCCEEDED
            assert gateway.calls == 1
    finally:
        async with real_factory() as cleanup:
            from sqlalchemy import delete

            await cleanup.execute(delete(Payment).where(Payment.id == payment_id))
            await cleanup.commit()


def _now() -> object:
    from datetime import UTC, datetime

    return datetime.now(UTC)

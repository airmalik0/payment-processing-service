"""Обработка платежа: идемпотентность, статусы, доставка webhook."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from decimal import Decimal

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import Payment
from payments.domain.enums import Currency, PaymentStatus
from payments.domain.errors import PaymentNotFoundError
from payments.domain.gateway import GatewayResult
from payments.services.processor import PaymentProcessor
from payments.services.webhook import WebhookSender

pytestmark = pytest.mark.integration


class StubGateway:
    """Детерминированный шлюз: считает вызовы и возвращает заданный результат."""

    def __init__(self, result: GatewayResult) -> None:
        self.result = result
        self.calls = 0

    async def charge(
        self, *, payment_id: object, amount: object, currency: object
    ) -> GatewayResult:
        self.calls += 1
        return self.result


def _webhook_sender(handler: Callable[[httpx.Request], httpx.Response]) -> WebhookSender:
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, timeout=5.0)
    return WebhookSender(client=client, signing_secret="secret")


async def _insert_payment(session: AsyncSession, *, webhook_url: str | None = None) -> uuid.UUID:
    payment = Payment(
        id=uuid.uuid4(),
        idempotency_key=f"key-{uuid.uuid4()}",
        request_fingerprint="f" * 64,
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Платёж",
        payment_metadata={},
        status=PaymentStatus.PENDING,
        webhook_url=webhook_url,
    )
    session.add(payment)
    await session.commit()
    return payment.id


async def test_processes_pending_payment_to_succeeded(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    payment_id = await _insert_payment(session)
    gateway = StubGateway(GatewayResult(status=PaymentStatus.SUCCEEDED))
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=gateway,
        webhook_sender=_webhook_sender(lambda _: httpx.Response(200)),
    )

    await processor.process(payment_id)

    refreshed = await session.get(Payment, payment_id)
    assert refreshed is not None
    await session.refresh(refreshed)
    assert refreshed.status is PaymentStatus.SUCCEEDED
    assert refreshed.processed_at is not None
    assert gateway.calls == 1


async def test_failed_payment_records_reason(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    payment_id = await _insert_payment(session)
    gateway = StubGateway(
        GatewayResult(status=PaymentStatus.FAILED, failure_reason="card_declined")
    )
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=gateway,
        webhook_sender=_webhook_sender(lambda _: httpx.Response(200)),
    )

    await processor.process(payment_id)

    refreshed = await session.get(Payment, payment_id)
    await session.refresh(refreshed)
    assert refreshed is not None
    assert refreshed.status is PaymentStatus.FAILED
    assert refreshed.failure_reason == "card_declined"


async def test_reprocessing_does_not_call_gateway_twice(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    """Ключевой тест идемпотентности: повторная доставка не списывает деньги дважды."""
    payment_id = await _insert_payment(session)
    gateway = StubGateway(GatewayResult(status=PaymentStatus.SUCCEEDED))
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=gateway,
        webhook_sender=_webhook_sender(lambda _: httpx.Response(200)),
    )

    await processor.process(payment_id)
    await processor.process(payment_id)
    await processor.process(payment_id)

    # Шлюз вызван ровно один раз, сколько бы сообщений ни пришло.
    assert gateway.calls == 1


async def test_delivers_webhook_and_marks_delivered(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    calls = {"count": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return httpx.Response(200)

    payment_id = await _insert_payment(session, webhook_url="https://ok.test/hook")
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=StubGateway(GatewayResult(status=PaymentStatus.SUCCEEDED)),
        webhook_sender=_webhook_sender(handler),
    )

    await processor.process(payment_id)

    refreshed = await session.get(Payment, payment_id)
    await session.refresh(refreshed)
    assert refreshed is not None
    assert refreshed.webhook_delivered_at is not None
    assert refreshed.webhook_attempts == 1
    assert calls["count"] == 1


async def test_delivered_webhook_not_sent_again(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    """После успешной доставки повтор обработки не шлёт webhook снова."""
    calls = {"count": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return httpx.Response(200)

    payment_id = await _insert_payment(session, webhook_url="https://ok.test/hook")
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=StubGateway(GatewayResult(status=PaymentStatus.SUCCEEDED)),
        webhook_sender=_webhook_sender(handler),
    )

    await processor.process(payment_id)
    await processor.process(payment_id)

    assert calls["count"] == 1


async def test_failing_webhook_raises_and_counts_attempt(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    from payments.domain.errors import WebhookTransientError

    payment_id = await _insert_payment(session, webhook_url="https://down.test/hook")
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=StubGateway(GatewayResult(status=PaymentStatus.SUCCEEDED)),
        webhook_sender=_webhook_sender(lambda _: httpx.Response(503)),
    )

    with pytest.raises(WebhookTransientError):
        await processor.process(payment_id)

    refreshed = await session.get(Payment, payment_id)
    await session.refresh(refreshed)
    assert refreshed is not None
    # Платёж всё равно финализирован — статус от шлюза сохранён, несмотря на сбой webhook.
    assert refreshed.status is PaymentStatus.SUCCEEDED
    assert refreshed.webhook_attempts == 1
    assert refreshed.webhook_delivered_at is None
    assert refreshed.webhook_last_error is not None


async def test_retry_after_webhook_failure_does_not_recharge(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    """Сбой webhook → повтор доставляет только webhook, шлюз не трогает."""
    from payments.domain.errors import WebhookTransientError

    responses = iter([httpx.Response(503), httpx.Response(200)])

    def handler(_: httpx.Request) -> httpx.Response:
        return next(responses)

    payment_id = await _insert_payment(session, webhook_url="https://flaky.test/hook")
    gateway = StubGateway(GatewayResult(status=PaymentStatus.SUCCEEDED))
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=gateway,
        webhook_sender=_webhook_sender(handler),
    )

    with pytest.raises(WebhookTransientError):
        await processor.process(payment_id)
    await processor.process(payment_id)  # второй раз webhook отвечает 200

    refreshed = await session.get(Payment, payment_id)
    await session.refresh(refreshed)
    assert refreshed is not None
    assert refreshed.webhook_delivered_at is not None
    assert refreshed.webhook_attempts == 2
    # Главное: шлюз вызван один раз, платёж не списан повторно.
    assert gateway.calls == 1


async def test_missing_payment_raises_permanent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=StubGateway(GatewayResult(status=PaymentStatus.SUCCEEDED)),
        webhook_sender=_webhook_sender(lambda _: httpx.Response(200)),
    )

    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid.uuid4())

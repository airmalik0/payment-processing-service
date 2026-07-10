"""Consumer: гарантия ack/nack при сбое маршрутизации в retry/DLQ.

При `AckPolicy.MANUAL` сообщение подтверждает сам обработчик. Если публикация в
retry/DLQ упадёт, сообщение не должно зависнуть unacked — обработчик обязан
вернуть его в очередь через `nack(requeue=True)`.

Тестируется `handle_delivery` напрямую (вынесен из замыкания подписчика), с
мок-сообщением и мок-router — без запуска брокера.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock

import pytest

from payments.broker.consumer import handle_delivery
from payments.domain.errors import PaymentNotFoundError, WebhookTransientError
from payments.domain.events import PaymentCreatedEvent
from payments.services.processor import PaymentProcessor

pytestmark = pytest.mark.integration


def _message() -> AsyncMock:
    message = AsyncMock()
    message.headers = {}
    message.message_id = str(uuid.uuid4())
    message.body = (
        PaymentCreatedEvent(payment_id=uuid.uuid4(), occurred_at=datetime.now(UTC))
        .model_dump_json()
        .encode()
    )
    return message


class _StubProcessor:
    """Заглушка процессора: либо ничего не делает, либо бросает заданную ошибку."""

    def __init__(self, error: Exception | None) -> None:
        self._error = error

    async def process(self, payment_id: uuid.UUID) -> None:
        if self._error is not None:
            raise self._error


def _processor(error: Exception | None) -> PaymentProcessor:
    # Утиный дубль — сигнатура `process` совпадает; каст ради строгих типов.
    return cast(PaymentProcessor, _StubProcessor(error))


async def test_nack_when_retry_publish_fails() -> None:
    """Публикация в retry падает → nack(requeue=True), сообщение не зависает."""
    router = AsyncMock()
    router.send_to_retry = AsyncMock(side_effect=ConnectionError("broker down"))
    message = _message()

    await handle_delivery(
        message,
        processor=_processor(WebhookTransientError("boom")),
        router=router,
        max_retries=3,
    )

    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


async def test_nack_when_ack_itself_fails() -> None:
    """Даже если сам ack упал (обрыв канала) — сообщение возвращается через nack."""
    router = AsyncMock()
    message = _message()
    message.ack = AsyncMock(side_effect=ConnectionError("channel gone"))

    # processor без ошибки → путь к ack, который здесь падает.
    await handle_delivery(message, processor=_processor(None), router=router, max_retries=3)

    message.nack.assert_awaited_once_with(requeue=True)


async def test_successful_delivery_acks() -> None:
    """Happy path: успешная обработка → ack, без nack."""
    router = AsyncMock()
    message = _message()

    await handle_delivery(message, processor=_processor(None), router=router, max_retries=3)

    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


async def test_permanent_error_routes_to_dlq_and_acks() -> None:
    """Permanent-ошибка → публикация в DLQ и ack (не retry)."""
    router = AsyncMock()
    message = _message()

    await handle_delivery(
        message,
        processor=_processor(PaymentNotFoundError(uuid.uuid4())),
        router=router,
        max_retries=3,
    )

    router.send_to_dlq.assert_awaited_once()
    router.send_to_retry.assert_not_awaited()
    message.ack.assert_awaited_once()


async def test_exhausted_retries_go_to_dlq() -> None:
    """На последней попытке transient-ошибка уходит в DLQ, а не в новый retry."""
    router = AsyncMock()
    message = _message()
    message.headers = {"x-retry-count": 3}  # уже было 3 попытки

    await handle_delivery(
        message,
        processor=_processor(WebhookTransientError("still failing")),
        router=router,
        max_retries=3,
    )

    router.send_to_dlq.assert_awaited_once()
    router.send_to_retry.assert_not_awaited()
    message.ack.assert_awaited_once()

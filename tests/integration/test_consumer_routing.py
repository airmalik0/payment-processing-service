"""Consumer: маршрутизация в retry/DLQ через TestRabbitBroker.

TestRabbitBroker не эмулирует TTL и dead-lettering, поэтому здесь проверяется
не фактическое возвращение сообщения по таймауту (это в e2e на живом RabbitMQ),
а решение consumer'а: в какой обменник и с какими заголовками он публикует
сбойное сообщение. Публикацию перехватываем моком брокера.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock

import pytest

from payments.broker.retry import RetryRouter
from payments.broker.topology import (
    DLX_EXCHANGE_NAME,
    RETRY_COUNT_HEADER,
    RETRY_EXCHANGE_NAME,
)
from payments.domain.errors import PaymentNotFoundError, WebhookTransientError

pytestmark = pytest.mark.integration


def _mock_broker() -> Any:
    broker = AsyncMock()
    broker.publish = AsyncMock()
    return broker


async def test_transient_error_goes_to_retry_exchange() -> None:
    broker = _mock_broker()
    router = RetryRouter(broker, max_retries=3)

    await router.send_to_retry(
        body=b'{"x":1}',
        headers={},
        attempt=1,
        error=WebhookTransientError("503"),
    )

    broker.publish.assert_awaited_once()
    _, kwargs = broker.publish.call_args
    assert kwargs["exchange"].name == RETRY_EXCHANGE_NAME
    assert kwargs["routing_key"] == "retry.1"
    assert kwargs["headers"][RETRY_COUNT_HEADER] == 1
    assert kwargs["persist"] is True


async def test_retry_increments_attempt_in_routing_key() -> None:
    broker = _mock_broker()
    router = RetryRouter(broker, max_retries=3)

    await router.send_to_retry(
        body=b"{}", headers={RETRY_COUNT_HEADER: 1}, attempt=2, error=ValueError("x")
    )

    _, kwargs = broker.publish.call_args
    assert kwargs["routing_key"] == "retry.2"
    assert kwargs["headers"][RETRY_COUNT_HEADER] == 2


async def test_permanent_error_goes_to_dlx() -> None:
    broker = _mock_broker()
    router = RetryRouter(broker, max_retries=3)

    await router.send_to_dlq(
        body=b"{}",
        headers={},
        attempt=3,
        error=PaymentNotFoundError(uuid.uuid4()),
    )

    _, kwargs = broker.publish.call_args
    assert kwargs["exchange"].name == DLX_EXCHANGE_NAME
    assert kwargs["persist"] is True


async def test_broker_owned_headers_stripped() -> None:
    """x-death и подобные не переносятся: RabbitMQ выставит их заново."""
    broker = _mock_broker()
    router = RetryRouter(broker, max_retries=3)

    await router.send_to_retry(
        body=b"{}",
        headers={"x-death": [{"count": 1}], "x-custom": "keep", RETRY_COUNT_HEADER: 0},
        attempt=1,
        error=ValueError("x"),
    )

    _, kwargs = broker.publish.call_args
    headers = kwargs["headers"]
    assert "x-death" not in headers
    assert headers["x-custom"] == "keep"  # пользовательские заголовки сохраняются

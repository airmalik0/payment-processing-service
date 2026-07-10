"""Доставка webhook: подпись и классификация ответов на transient/permanent."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from payments.db.models import Payment
from payments.domain.enums import Currency, PaymentStatus
from payments.domain.errors import WebhookPermanentError, WebhookTransientError
from payments.domain.events import webhook_event_id
from payments.services.webhook import (
    EVENT_ID_HEADER,
    SIGNATURE_HEADER,
    WebhookSender,
    sign_payload,
)

SECRET = "test-secret"


def _payment(webhook_url: str = "https://example.test/hook") -> Payment:
    return Payment(
        id=uuid.uuid4(),
        idempotency_key="key",
        request_fingerprint="f" * 64,
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Тест",
        payment_metadata={"order": 1},
        status=PaymentStatus.SUCCEEDED,
        failure_reason=None,
        webhook_url=webhook_url,
        webhook_attempts=0,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        processed_at=datetime.now(UTC),
    )


def _sender(handler: object) -> WebhookSender:
    transport = httpx.MockTransport(handler)  # type: ignore[arg-type]
    client = httpx.AsyncClient(transport=transport, timeout=5.0)
    return WebhookSender(client=client, signing_secret=SECRET)


async def test_delivers_on_2xx_and_signs_payload() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.content
        captured["signature"] = request.headers.get(SIGNATURE_HEADER)
        captured["event_id"] = request.headers.get(EVENT_ID_HEADER)
        return httpx.Response(200)

    payment = _payment()
    await _sender(handler).send(payment)

    body = captured["body"]
    assert isinstance(body, bytes)
    # Подпись должна проверяться получателем ровно тем же секретом и телом.
    assert captured["signature"] == sign_payload(body, SECRET)
    assert captured["event_id"] == str(webhook_event_id(payment.id))


@pytest.mark.parametrize("status_code", [500, 502, 503, 429, 408])
async def test_transient_statuses_raise_transient(status_code: int) -> None:
    sender = _sender(lambda _: httpx.Response(status_code))
    with pytest.raises(WebhookTransientError):
        await sender.send(_payment())


@pytest.mark.parametrize("status_code", [400, 403, 404, 410, 422])
async def test_client_errors_raise_permanent(status_code: int) -> None:
    sender = _sender(lambda _: httpx.Response(status_code))
    with pytest.raises(WebhookPermanentError):
        await sender.send(_payment())


async def test_timeout_is_transient() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    with pytest.raises(WebhookTransientError):
        await _sender(handler).send(_payment())


async def test_connection_error_is_transient() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(WebhookTransientError):
        await _sender(handler).send(_payment())


def test_signature_format() -> None:
    sig = sign_payload(b"body", SECRET)
    assert sig.startswith("sha256=")
    assert len(sig) == len("sha256=") + 64


def test_event_id_is_stable() -> None:
    pid = uuid.uuid4()
    assert webhook_event_id(pid) == webhook_event_id(pid)

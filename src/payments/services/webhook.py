"""Доставка webhook-уведомлений клиенту."""

from __future__ import annotations

import hashlib
import hmac
from http import HTTPStatus
from typing import TYPE_CHECKING

import httpx

from payments.domain.errors import WebhookPermanentError, WebhookTransientError
from payments.domain.events import WebhookPayload, webhook_event_id
from payments.observability.logging import get_logger

if TYPE_CHECKING:
    from payments.db.models import Payment

logger = get_logger(__name__)

# Коды, при которых повтор осмыслен: сервер клиента жив, но сейчас не может принять.
RETRYABLE_STATUS_CODES = frozenset(
    {
        HTTPStatus.REQUEST_TIMEOUT,  # 408
        HTTPStatus.TOO_MANY_REQUESTS,  # 429
    }
)

SIGNATURE_HEADER = "X-Webhook-Signature"
EVENT_ID_HEADER = "X-Webhook-Event-Id"


def sign_payload(body: bytes, secret: str) -> str:
    """HMAC-SHA256 тела уведомления. Получатель проверяет, что webhook пришёл от нас."""
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def build_payload(payment: Payment) -> WebhookPayload:
    """Собирает тело уведомления из текущего состояния платежа."""
    return WebhookPayload(
        event=f"payment.{payment.status.value}",
        event_id=webhook_event_id(payment.id),
        payment_id=payment.id,
        status=payment.status,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        failure_reason=payment.failure_reason,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


class WebhookSender:
    """Отправляет уведомление и переводит исход в язык transient/permanent ошибок.

    Повторов внутри нет: ими управляет брокер через retry-очереди с задержкой.
    Спящий воркер занимал бы слот prefetch и терял бы отсчёт при рестарте.
    """

    def __init__(self, *, client: httpx.AsyncClient, signing_secret: str) -> None:
        self._client = client
        self._signing_secret = signing_secret

    async def send(self, payment: Payment) -> None:
        if not payment.webhook_url:
            msg = "У платежа не задан webhook_url"
            raise ValueError(msg)

        payload = build_payload(payment)
        body = payload.model_dump_json().encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            EVENT_ID_HEADER: str(payload.event_id),
            SIGNATURE_HEADER: sign_payload(body, self._signing_secret),
        }

        log = logger.bind(payment_id=str(payment.id), webhook_url=payment.webhook_url)

        try:
            response = await self._client.post(payment.webhook_url, content=body, headers=headers)
        except httpx.TimeoutException as error:
            raise WebhookTransientError(f"Таймаут при доставке webhook: {error}") from error
        except httpx.TransportError as error:
            raise WebhookTransientError(f"Сетевая ошибка при доставке webhook: {error}") from error

        status = response.status_code

        if 200 <= status < 300:
            log.info("webhook_delivered", status_code=status)
            return

        if status >= 500 or status in RETRYABLE_STATUS_CODES:
            log.warning("webhook_transient_failure", status_code=status)
            raise WebhookTransientError(
                f"Webhook ответил {status}, повтор осмыслен", status_code=status
            )

        # Прочие 4xx: повтор вернёт тот же ответ. В DLQ, разбираться руками.
        log.error("webhook_permanent_failure", status_code=status)
        raise WebhookPermanentError(
            f"Webhook ответил {status}, повтор бессмысленен", status_code=status
        )


def create_http_client(timeout_seconds: float) -> httpx.AsyncClient:
    """Клиент с ограниченным временем ожидания: висящий webhook не должен держать воркер."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=10),
        follow_redirects=False,
    )

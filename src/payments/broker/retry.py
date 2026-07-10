"""Маршрутизация сбойных сообщений: retry-очереди с задержкой или DLQ."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from payments.broker.topology import (
    DLQ_ROUTING_KEY,
    ERROR_HEADER,
    ERROR_TYPE_HEADER,
    ORIGINAL_ROUTING_KEY_HEADER,
    PAYMENTS_NEW_ROUTING_KEY,
    RETRY_COUNT_HEADER,
    dlx_exchange,
    retry_exchange,
    retry_routing_key,
)
from payments.observability.logging import get_logger

if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker

logger = get_logger(__name__)

MAX_ERROR_LENGTH = 500

# Заголовки, которые проставляет сам RabbitMQ при dead-lettering. Копировать их
# в новое сообщение не нужно: брокер выставит их заново.
_BROKER_OWNED_HEADERS = frozenset(
    {
        "x-death",
        "x-first-death-exchange",
        "x-first-death-queue",
        "x-first-death-reason",
        "x-last-death-exchange",
        "x-last-death-queue",
        "x-last-death-reason",
    }
)


def current_attempt(headers: dict[str, Any]) -> int:
    """Сколько раз сообщение уже отправлялось на повтор. Первая доставка — 0."""
    raw = headers.get(RETRY_COUNT_HEADER, 0)
    try:
        attempt = int(raw)
    except (TypeError, ValueError):
        return 0
    return max(attempt, 0)


def _clean_headers(headers: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in headers.items() if key not in _BROKER_OWNED_HEADERS}


class RetryRouter:
    """Отправляет сообщение на повтор или хоронит его в DLQ.

    Публикация выполняется до `ack` исходного сообщения: если процесс упадёт
    между ними, брокер вернёт исходное сообщение и оно будет обработано ещё раз.
    Дубликат безопасен — обработка идемпотентна; потеря — нет.
    """

    # Ограничение на подтверждение публикации: зависший confirm не должен
    # навечно вешать обработчик (у relay тот же приём).
    PUBLISH_TIMEOUT_SECONDS = 10.0

    def __init__(self, broker: RabbitBroker, *, max_retries: int) -> None:
        self._broker = broker
        self._max_retries = max_retries

    async def send_to_retry(
        self,
        *,
        body: Any,
        headers: dict[str, Any],
        attempt: int,
        error: Exception,
        message_id: str | None = None,
    ) -> None:
        """Кладёт сообщение в очередь-отстойник попытки `attempt` (1-based)."""
        next_headers = _clean_headers(headers) | {
            RETRY_COUNT_HEADER: attempt,
            ERROR_HEADER: str(error)[:MAX_ERROR_LENGTH],
            ERROR_TYPE_HEADER: type(error).__name__,
            ORIGINAL_ROUTING_KEY_HEADER: PAYMENTS_NEW_ROUTING_KEY,
        }

        await self._broker.publish(
            body,
            exchange=retry_exchange,
            routing_key=retry_routing_key(attempt),
            headers=next_headers,
            message_id=message_id,
            persist=True,
            timeout=self.PUBLISH_TIMEOUT_SECONDS,
        )
        logger.warning(
            "message_scheduled_for_retry",
            attempt=attempt,
            max_retries=self._max_retries,
            error=str(error)[:MAX_ERROR_LENGTH],
            error_type=type(error).__name__,
        )

    async def send_to_dlq(
        self,
        *,
        body: Any,
        headers: dict[str, Any],
        attempt: int,
        error: Exception,
        message_id: str | None = None,
    ) -> None:
        """Отправляет сообщение в DLQ: попытки исчерпаны либо ошибка неустранима."""
        dlq_headers = _clean_headers(headers) | {
            RETRY_COUNT_HEADER: attempt,
            ERROR_HEADER: str(error)[:MAX_ERROR_LENGTH],
            ERROR_TYPE_HEADER: type(error).__name__,
            ORIGINAL_ROUTING_KEY_HEADER: PAYMENTS_NEW_ROUTING_KEY,
        }

        await self._broker.publish(
            body,
            exchange=dlx_exchange,
            routing_key=DLQ_ROUTING_KEY,
            headers=dlq_headers,
            message_id=message_id,
            persist=True,
            timeout=self.PUBLISH_TIMEOUT_SECONDS,
        )
        logger.error(
            "message_sent_to_dlq",
            attempt=attempt,
            error=str(error)[:MAX_ERROR_LENGTH],
            error_type=type(error).__name__,
        )

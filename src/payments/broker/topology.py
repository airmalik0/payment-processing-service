"""Топология RabbitMQ: обменники, очереди, задержки ретраев, DLQ.

Задержка ретраев реализована очередями с `x-message-ttl` и dead-lettering, а не
`asyncio.sleep` в обработчике. Спящий обработчик держит слот prefetch, а при
рестарте процесса отсчёт задержки теряется вместе с ним. Сообщение, лежащее в
очереди с TTL, переживает рестарт и никого не блокирует.

Задержки: 1с → 5с → 25с. На каждый шаг — своя очередь. Одна очередь с
per-message TTL не годится: RabbitMQ проверяет срок только у головы очереди,
поэтому сообщение с TTL 25с задержало бы стоящее за ним сообщение с TTL 1с.

    relay ──► [payments] ──payments.new──► (payments.new) ──► consumer
                  ▲                                              │ transient error
                  │ TTL истёк                                    ▼
        (payments.new.retry.N) ◄──retry.N── [payments.retry]  attempt < 3
                                                                 │ attempt = 3
                                                                 │ permanent error
                                                                 ▼
                                          [payments.dlx] ──► (payments.new.dlq)
"""

from __future__ import annotations

from typing import Final

from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

# Единый источник ключа маршрутизации — доменный слой. Явный ре-экспорт (`as`),
# чтобы брокерские модули брали его отсюда, а не из двух мест.
from payments.domain.events import PAYMENTS_NEW_ROUTING_KEY as PAYMENTS_NEW_ROUTING_KEY

# --- Имена ---
PAYMENTS_EXCHANGE_NAME: Final = "payments"
RETRY_EXCHANGE_NAME: Final = "payments.retry"
DLX_EXCHANGE_NAME: Final = "payments.dlx"

PAYMENTS_NEW_QUEUE_NAME: Final = "payments.new"
DLQ_NAME: Final = "payments.new.dlq"

# Ключ маршрутизации основной очереди — из доменного слоя (единый источник).
DLQ_ROUTING_KEY: Final = PAYMENTS_NEW_ROUTING_KEY

RETRY_COUNT_HEADER: Final = "x-retry-count"
ERROR_HEADER: Final = "x-error"
ERROR_TYPE_HEADER: Final = "x-error-type"
ORIGINAL_ROUTING_KEY_HEADER: Final = "x-original-routing-key"

# --- Обменники ---
payments_exchange = RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
retry_exchange = RabbitExchange(RETRY_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
dlx_exchange = RabbitExchange(DLX_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)

# --- Основная очередь ---
payments_new_queue = RabbitQueue(
    PAYMENTS_NEW_QUEUE_NAME,
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
)

# --- Очередь несостоявшихся сообщений ---
dlq_queue = RabbitQueue(DLQ_NAME, durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_routing_key(attempt: int) -> str:
    """Ключ маршрутизации для попытки номер `attempt` (1-based)."""
    return f"retry.{attempt}"


def retry_queue_name(attempt: int) -> str:
    return f"{PAYMENTS_NEW_QUEUE_NAME}.retry.{attempt}"


def build_retry_queue(attempt: int, delay_seconds: float) -> RabbitQueue:
    """Очередь-«отстойник»: полежав `delay_seconds`, сообщение само вернётся в `payments.new`.

    `x-message-ttl` истекает → RabbitMQ отправляет сообщение в
    `x-dead-letter-exchange` с ключом `x-dead-letter-routing-key`. У очереди нет
    потребителей: она существует ровно ради этой задержки.
    """
    return RabbitQueue(
        retry_queue_name(attempt),
        durable=True,
        routing_key=retry_routing_key(attempt),
        arguments={
            "x-message-ttl": int(delay_seconds * 1000),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )


def build_retry_queues(
    *, max_retries: int, base_delay: float, multiplier: float
) -> list[RabbitQueue]:
    """Очереди для всех попыток: задержка растёт как `base * multiplier^(n-1)`."""
    return [
        build_retry_queue(attempt, base_delay * (multiplier ** (attempt - 1)))
        for attempt in range(1, max_retries + 1)
    ]

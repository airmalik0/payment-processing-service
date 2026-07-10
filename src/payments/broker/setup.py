"""Создание брокера и декларация топологии.

Очереди ретраев и DLQ не имеют подписчиков, поэтому FastStream не объявит их
сам — он декларирует только то, на что кто-то подписан. Объявляем и связываем
их явно, иначе публикация в `payments.retry` уходила бы в никуда.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from faststream.rabbit import Channel, RabbitBroker

from payments.broker.topology import (
    DLQ_ROUTING_KEY,
    PAYMENTS_NEW_ROUTING_KEY,
    build_retry_queues,
    dlq_queue,
    dlx_exchange,
    payments_exchange,
    payments_new_queue,
    retry_exchange,
    retry_routing_key,
)
from payments.observability.logging import get_logger

if TYPE_CHECKING:
    from payments.config import Settings

logger = get_logger(__name__)


def create_broker(settings: Settings) -> RabbitBroker:
    """Брокер с ограничением prefetch: воркер не набирает больше, чем успевает обработать."""
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=settings.consumer_prefetch_count),
        graceful_timeout=30.0,
        # Логи брокера идут через structlog вместе с остальными.
        log_level=20,
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Идемпотентно создаёт обменники, очереди и связи между ними."""
    payments_ex = await broker.declare_exchange(payments_exchange)
    retry_ex = await broker.declare_exchange(retry_exchange)
    dlx_ex = await broker.declare_exchange(dlx_exchange)

    main_queue = await broker.declare_queue(payments_new_queue)
    await main_queue.bind(payments_ex, routing_key=PAYMENTS_NEW_ROUTING_KEY)

    retry_queues = build_retry_queues(
        max_retries=settings.max_retries,
        base_delay=settings.retry_base_delay_seconds,
        multiplier=settings.retry_multiplier,
    )
    for attempt, queue in enumerate(retry_queues, start=1):
        declared = await broker.declare_queue(queue)
        await declared.bind(retry_ex, routing_key=retry_routing_key(attempt))

    dead_letter_queue = await broker.declare_queue(dlq_queue)
    await dead_letter_queue.bind(dlx_ex, routing_key=DLQ_ROUTING_KEY)

    logger.info(
        "topology_declared",
        retry_queues=len(retry_queues),
        delays=[settings.retry_delay_seconds(n) for n in range(1, settings.max_retries + 1)],
    )

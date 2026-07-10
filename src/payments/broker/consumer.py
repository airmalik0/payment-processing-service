"""Consumer платежей: единственный обработчик, делающий всё.

Решает `AckPolicy.MANUAL`: сообщение подтверждается вручную и только после того,
как его судьба зафиксирована — обработка успешна, либо оно переопубликовано в
retry/DLQ. Автоматический ack здесь опасен: подтверди FastStream сообщение до
переезда в retry-очередь, и при падении публикации сообщение потерялось бы.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from faststream import AckPolicy
from faststream.rabbit.annotations import RabbitMessage

from payments.broker.retry import RetryRouter, current_attempt
from payments.broker.setup import create_broker, declare_topology
from payments.broker.topology import payments_exchange, payments_new_queue
from payments.domain.errors import PermanentError
from payments.domain.events import PaymentCreatedEvent
from payments.observability.logging import get_logger

if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker

    from payments.config import Settings
    from payments.services.processor import PaymentProcessor

logger = get_logger(__name__)


async def handle_delivery(
    message: RabbitMessage,
    *,
    processor: PaymentProcessor,
    router: RetryRouter,
    max_retries: int,
) -> None:
    """Обрабатывает одну доставку сообщения `payments.new`.

    Вынесено из замыкания подписчика, чтобы ветвление ack/nack можно было
    протестировать без запуска брокера.

    При `AckPolicy.MANUAL` FastStream сам не подтверждает сообщение. Если
    маршрутизация в retry/DLQ или ack бросят исключение (обрыв канала,
    PRECONDITION_FAILED), сообщение осталось бы unacked навсегда, заняв слот
    prefetch до consumer_timeout. Внешний try возвращает такое сообщение в
    очередь через `nack(requeue=True)` — оно обработается ещё раз, а дубликат
    безопасен благодаря идемпотентности.
    """
    attempt = current_attempt(message.headers)
    log = logger.bind(message_id=message.message_id, attempt=attempt)
    raw_body = message.body

    try:
        # 1. Разобрать событие. Битый payload неисправим повтором — сразу в DLQ.
        try:
            event = PaymentCreatedEvent.model_validate_json(raw_body)
        except ValueError as error:
            log.error("malformed_event", error=str(error))
            await router.send_to_dlq(
                body=raw_body,
                headers=message.headers,
                attempt=attempt,
                error=error,
                message_id=message.message_id,
            )
            await message.ack()
            return

        # 2. Обработать платёж.
        try:
            await processor.process(event.payment_id)

        except PermanentError as error:
            # Ошибка не исчезнет при повторе (нет платежа, 4xx от webhook) → DLQ.
            await router.send_to_dlq(
                body=raw_body,
                headers=message.headers,
                attempt=attempt,
                error=error,
                message_id=message.message_id,
            )
            await message.ack()

        except Exception as error:
            # Transient-ошибки и всё неизвестное. Незнакомую ошибку считаем
            # transient: у неё есть шанс пройти, а окончательный приговор вынесет
            # DLQ после исчерпания попыток.
            next_attempt = attempt + 1
            if next_attempt > max_retries:
                await router.send_to_dlq(
                    body=raw_body,
                    headers=message.headers,
                    attempt=attempt,
                    error=error,
                    message_id=message.message_id,
                )
            else:
                await router.send_to_retry(
                    body=raw_body,
                    headers=message.headers,
                    attempt=next_attempt,
                    error=error,
                    message_id=message.message_id,
                )
            await message.ack()

        else:
            # Успех: платёж финализирован, webhook (если был) доставлен.
            await message.ack()
            log.info("message_processed_successfully")

    except Exception:
        logger.exception("message_handling_failed_requeue", message_id=message.message_id)
        await message.nack(requeue=True)


def build_consumer_broker(settings: Settings, processor: PaymentProcessor) -> RabbitBroker:
    """Собирает брокер с подписчиком на `payments.new`.

    Зависимости (processor) переданы явно, а не через глобальные — так же
    подключается и подмена в тестах.
    """
    broker = create_broker(settings)
    router = RetryRouter(broker, max_retries=settings.max_retries)

    @broker.subscriber(
        payments_new_queue,
        payments_exchange,
        ack_policy=AckPolicy.MANUAL,
    )
    async def handle_payment_created(message: RabbitMessage) -> None:
        await handle_delivery(
            message, processor=processor, router=router, max_retries=settings.max_retries
        )

    return broker


async def start_consumer(settings: Settings, processor: PaymentProcessor) -> RabbitBroker:
    """Поднимает брокер, декларирует топологию и запускает потребление."""
    broker = build_consumer_broker(settings, processor)
    await broker.connect()
    await declare_topology(broker, settings)
    await broker.start()
    return broker

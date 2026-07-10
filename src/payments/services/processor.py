"""Обработка платежа: вызов шлюза и уведомление клиента.

Идемпотентность — главное свойство этого модуля. Сообщение из RabbitMQ может
прийти повторно (дубликат из outbox, возврат неподтверждённого сообщения,
повторная попытка доставки webhook). Обработчик обязан вести себя так, будто
это первая доставка, но не списать деньги дважды.

Достигается разделением двух шагов:

  * шлюз вызывается, только если платёж всё ещё `pending`;
  * webhook отправляется, только если он ещё не доставлен.

При повторе первый шаг пропускается — платёж уже в финальном статусе.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from payments.db.repositories import PaymentRepository
from payments.domain.enums import PaymentStatus
from payments.domain.errors import PaymentNotFoundError
from payments.observability.logging import get_logger

if TYPE_CHECKING:
    import structlog
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from payments.domain.gateway import PaymentGateway
    from payments.services.webhook import WebhookSender

logger = get_logger(__name__)

MAX_ERROR_LENGTH = 500


class PaymentProcessor:
    """Доводит платёж до финального статуса и уведомляет клиента."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook_sender

    async def process(self, payment_id: uuid.UUID) -> None:
        """Обрабатывает платёж.

        Поднимает `TransientError` — сообщение уйдёт в retry-очередь;
        `PermanentError` — сразу в DLQ. Успех означает: платёж финализирован и,
        если был указан `webhook_url`, уведомление доставлено.

        Две независимые транзакции. Результат шлюза фиксируется первой — до
        отправки webhook. Иначе падение процесса во время доставки webhook (до
        5с) потеряло бы уже полученный ответ шлюза, и повторная доставка вызвала
        бы шлюз второй раз. Разделение сужает окно повторного списания с «вся
        доставка webhook» до «момент между ответом шлюза и commit».
        """
        log = logger.bind(payment_id=str(payment_id))
        await self._finalize_payment(payment_id, log)
        await self._deliver_webhook(payment_id, log)

    async def _finalize_payment(
        self, payment_id: uuid.UUID, log: structlog.stdlib.BoundLogger
    ) -> None:
        """Транзакция 1: доводит платёж до финального статуса через шлюз и коммитит.

        Блокировка строки удерживается только на время вызова шлюза (2-5с) и
        сериализует дубликаты события: второй потребитель дождётся коммита
        первого и увидит уже финальный статус — шлюз вызовется ровно один раз.
        """
        async with self._session_factory() as session:
            payments = PaymentRepository(session)
            payment = await payments.get_for_update(payment_id)
            if payment is None:
                # Платежа нет и не появится: повторять бессмысленно.
                raise PaymentNotFoundError(payment_id)

            if payment.status is not PaymentStatus.PENDING:
                log.info("payment_already_processed", status=payment.status.value)
                return

            result = await self._gateway.charge(
                payment_id=payment.id, amount=payment.amount, currency=payment.currency
            )
            payment.status = result.status
            payment.failure_reason = result.failure_reason
            payment.processed_at = datetime.now(UTC)
            await session.commit()
            log.info("payment_processed", status=payment.status.value)

    async def _deliver_webhook(
        self, payment_id: uuid.UUID, log: structlog.stdlib.BoundLogger
    ) -> None:
        """Транзакция 2: отправляет webhook, если он задан и ещё не доставлен.

        Блокировка строки сериализует конкурентные доставки: только один
        потребитель отправит webhook, остальные увидят проставленный
        `webhook_delivered_at`. Ошибка уходит наверх *после* commit — счётчик
        попыток и диагностика сохраняются, даже если сообщение уйдёт в retry.
        """
        async with self._session_factory() as session:
            payments = PaymentRepository(session)
            payment = await payments.get_for_update(payment_id)
            if payment is None:  # pragma: no cover — платёж только что финализирован
                raise PaymentNotFoundError(payment_id)

            if not payment.webhook_url or payment.webhook_delivered_at is not None:
                await session.commit()
                return

            payment.webhook_attempts += 1
            try:
                await self._webhook.send(payment)
            except Exception as error:
                payment.webhook_last_error = str(error)[:MAX_ERROR_LENGTH]
                await session.commit()
                raise

            payment.webhook_delivered_at = datetime.now(UTC)
            payment.webhook_last_error = None
            await session.commit()
            log.info("webhook_marked_delivered", attempts=payment.webhook_attempts)

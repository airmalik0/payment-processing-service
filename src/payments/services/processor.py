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

    from payments.db.models import Payment
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
        """
        log = logger.bind(payment_id=str(payment_id))

        async with self._session_factory() as session:
            payments = PaymentRepository(session)

            # Блокировка строки удерживается на всё время обработки, включая вызов
            # шлюза. Это цена защиты от двойного списания, когда дубликат события
            # достался двум потребителям одновременно. Время удержания ограничено
            # задержкой шлюза и таймаутом webhook, число блокировок — prefetch_count.
            payment = await payments.get_for_update(payment_id)
            if payment is None:
                # Платежа нет и не появится: повторять бессмысленно.
                raise PaymentNotFoundError(payment_id)

            await self._charge_if_pending(payment, log)

            if payment.webhook_url and payment.webhook_delivered_at is None:
                await self._deliver_webhook(session, payment, log)
                return

            await session.commit()

    async def _charge_if_pending(self, payment: Payment, log: structlog.stdlib.BoundLogger) -> None:
        """Шлюз вызывается ровно один раз за платёж — при повторной доставке шаг пропускается."""
        if payment.status is not PaymentStatus.PENDING:
            log.info("payment_already_processed", status=payment.status.value)
            return

        result = await self._gateway.charge(
            payment_id=payment.id, amount=payment.amount, currency=payment.currency
        )
        payment.status = result.status
        payment.failure_reason = result.failure_reason
        payment.processed_at = datetime.now(UTC)
        log.info("payment_processed", status=payment.status.value)

    async def _deliver_webhook(
        self, session: AsyncSession, payment: Payment, log: structlog.stdlib.BoundLogger
    ) -> None:
        """Отправляет уведомление; при неудаче фиксирует диагностику и пробрасывает ошибку.

        Ошибка уходит наверх *после* commit: статус платежа, полученный от шлюза,
        и счётчик попыток обязаны сохраниться, даже если сообщение уйдёт в retry.
        """
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

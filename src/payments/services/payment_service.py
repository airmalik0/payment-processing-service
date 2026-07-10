"""Приём платежа: идемпотентность и атомарная запись события в outbox."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import IntegrityError

from payments.db.models import OutboxMessage, Payment
from payments.db.repositories import UNIQUE_VIOLATION, OutboxRepository, PaymentRepository
from payments.domain.enums import Currency, PaymentStatus
from payments.domain.errors import IdempotencyConflictError
from payments.domain.events import PAYMENT_CREATED, PaymentCreatedEvent
from payments.domain.fingerprint import request_fingerprint
from payments.observability.logging import get_logger

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)

PAYMENTS_NEW_ROUTING_KEY = "payments.new"
AGGREGATE_TYPE = "payment"


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    """Входные данные для создания платежа."""

    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str | None

    def fingerprint(self) -> str:
        return request_fingerprint(
            {
                "amount": self.amount,
                "currency": self.currency.value,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": self.webhook_url,
            }
        )


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    """Платёж и признак того, что это повтор по тому же ключу идемпотентности."""

    payment: Payment
    replayed: bool


class PaymentService:
    """Бизнес-операции над платежами на стороне API."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        """Создаёт платёж и событие `payment.created` одной транзакцией.

        Повторный вызов с тем же `Idempotency-Key`:
          * то же тело   → возвращается существующий платёж (`replayed=True`);
          * другое тело  → `IdempotencyConflictError`.
        """
        fingerprint = command.fingerprint()

        existing = await self._payments.get_by_idempotency_key(command.idempotency_key)
        if existing is not None:
            return self._replay(existing, fingerprint, command.idempotency_key)

        payment = Payment(
            id=uuid.uuid4(),
            idempotency_key=command.idempotency_key,
            request_fingerprint=fingerprint,
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            payment_metadata=command.metadata,
            status=PaymentStatus.PENDING,
            webhook_url=command.webhook_url,
        )
        created_at = datetime.now(UTC)
        event = PaymentCreatedEvent(payment_id=payment.id, occurred_at=created_at)

        self._payments.add(payment)
        self._outbox.add(
            OutboxMessage(
                id=uuid.uuid4(),
                aggregate_type=AGGREGATE_TYPE,
                aggregate_id=payment.id,
                event_type=PAYMENT_CREATED,
                routing_key=PAYMENTS_NEW_ROUTING_KEY,
                payload=event.model_dump(mode="json"),
            )
        )

        try:
            # Платёж и событие фиксируются вместе: брокер узнает о платеже
            # тогда и только тогда, когда платёж действительно сохранён.
            await self._session.commit()
        except IntegrityError as error:
            await self._session.rollback()
            if not _is_idempotency_key_violation(error):
                raise
            # Гонка: параллельный запрос с тем же ключом успел закоммититься первым.
            winner = await self._payments.get_by_idempotency_key(command.idempotency_key)
            if winner is None:  # pragma: no cover — возможно только при ручном удалении строки
                raise
            logger.info(
                "idempotency_race_lost",
                idempotency_key=command.idempotency_key,
                payment_id=str(winner.id),
            )
            return self._replay(winner, fingerprint, command.idempotency_key)

        logger.info(
            "payment_created",
            payment_id=str(payment.id),
            amount=str(payment.amount),
            currency=payment.currency.value,
        )
        return CreatePaymentResult(payment=payment, replayed=False)

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._payments.get(payment_id)

    @staticmethod
    def _replay(payment: Payment, fingerprint: str, idempotency_key: str) -> CreatePaymentResult:
        """Повтор допустим лишь при совпадении тела запроса.

        Молча вернуть чужой платёж — хуже, чем ошибка: клиент решит, что провёл новый.
        """
        if payment.request_fingerprint != fingerprint:
            raise IdempotencyConflictError(idempotency_key)
        return CreatePaymentResult(payment=payment, replayed=True)


def _is_idempotency_key_violation(error: IntegrityError) -> bool:
    """Отличает нарушение уникальности `idempotency_key` от любого другого IntegrityError.

    `error.orig` — это адаптер SQLAlchemy поверх asyncpg, у него есть `sqlstate`,
    но нет имени constraint. Настоящее исключение asyncpg лежит в `orig.__cause__`.
    """
    original = getattr(error, "orig", None)
    sqlstate = getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)
    if sqlstate != UNIQUE_VIOLATION:
        return False

    cause = getattr(original, "__cause__", None)
    constraint = getattr(cause, "constraint_name", None)
    if constraint is not None:
        return "idempotency_key" in constraint
    return "idempotency_key" in str(error).lower()

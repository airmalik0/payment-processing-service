"""Таблицы `payments` и `outbox_messages`."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from payments.db.base import Base
from payments.domain.enums import Currency, PaymentStatus


def _enum(python_enum: type, name: str) -> Enum:
    """Native PostgreSQL enum, где хранятся *значения* Python-перечисления, а не имена.

    Без `values_callable` SQLAlchemy положит в БД 'PENDING' вместо 'pending'.
    """
    return Enum(
        python_enum,
        name=name,
        native_enum=True,
        values_callable=lambda enum_cls: [member.value for member in enum_cls],
    )


class Payment(Base):
    """Платёж. Живёт от `pending` до одного из финальных статусов."""

    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)

    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(_enum(Currency, "currency"), nullable=False)
    description: Mapped[str] = mapped_column(String(1024), nullable=False)

    # Колонка в БД называется `metadata`, но это имя занято в SQLAlchemy Declarative.
    payment_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )

    status: Mapped[PaymentStatus] = mapped_column(
        _enum(PaymentStatus, "payment_status"),
        nullable=False,
        server_default=PaymentStatus.PENDING.value,
    )
    failure_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)

    webhook_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    webhook_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    webhook_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        Index("ix_payments_status_created_at", "status", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<Payment id={self.id} status={self.status} amount={self.amount} {self.currency}>"


class OutboxMessage(Base):
    """Событие, ожидающее публикации в брокер.

    Пишется той же транзакцией, что и платёж: либо в БД появляется и платёж, и
    событие, либо ничего. `id` используется как `message_id` в RabbitMQ — это
    даёт потребителю стабильный ключ дедупликации.
    """

    __tablename__ = "outbox_messages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    routing_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    next_retry_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # Relay сканирует только неопубликованный хвост: частичный индекс не растёт
        # вместе с историей опубликованных событий.
        Index(
            "ix_outbox_unpublished",
            "next_retry_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    def __repr__(self) -> str:
        state = "published" if self.published_at else "pending"
        return f"<OutboxMessage id={self.id} {self.event_type} {state}>"

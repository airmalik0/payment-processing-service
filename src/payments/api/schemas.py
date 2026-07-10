"""Схемы запросов и ответов HTTP-слоя."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from payments.db.models import Payment
from payments.domain.enums import Currency, PaymentStatus
from payments.domain.fingerprint import MONEY_EXPONENT

# Сумма — Decimal, а не float: двоичная плавающая точка не представляет 0.1 точно.
Amount = Annotated[Decimal, Field(gt=0, le=Decimal("9999999999999999.99"))]


class CreatePaymentRequest(BaseModel):
    """Тело `POST /api/v1/payments`."""

    model_config = ConfigDict(extra="forbid")

    amount: Amount = Field(description="Сумма платежа, положительная, до двух знаков")
    currency: Currency = Field(description="Валюта платежа")
    description: str = Field(min_length=1, max_length=1024, description="Описание платежа")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Произвольные данные")
    webhook_url: HttpUrl | None = Field(
        default=None, description="URL для уведомления о результате обработки"
    )

    @field_validator("amount")
    @classmethod
    def _no_sub_cent_precision(cls, value: Decimal) -> Decimal:
        """Отбрасывать копейки молча нельзя — это чужие деньги."""
        if value != value.quantize(MONEY_EXPONENT):
            msg = "Сумма не может содержать более двух знаков после запятой"
            raise ValueError(msg)
        return value

    @field_validator("metadata")
    @classmethod
    def _metadata_must_be_json_object(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(value) > 50:
            msg = "Метаданные не могут содержать более 50 ключей"
            raise ValueError(msg)
        return value


class CreatePaymentResponse(BaseModel):
    """Ответ `202 Accepted`: платёж принят, но ещё не обработан."""

    model_config = ConfigDict(json_schema_extra={"description": "Платёж принят к обработке"})

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    """Полное состояние платежа для `GET /api/v1/payments/{payment_id}`."""

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    failure_reason: str | None
    webhook_url: str | None
    webhook_attempts: int
    webhook_delivered_at: datetime | None
    webhook_last_error: str | None
    idempotency_key: str
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> PaymentResponse:
        """Сборка вручную: в модели колонка `metadata` живёт под именем `payment_metadata`."""
        return cls(
            id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.payment_metadata,
            status=payment.status,
            failure_reason=payment.failure_reason,
            webhook_url=payment.webhook_url,
            webhook_attempts=payment.webhook_attempts,
            webhook_delivered_at=payment.webhook_delivered_at,
            webhook_last_error=payment.webhook_last_error,
            idempotency_key=payment.idempotency_key,
            created_at=payment.created_at,
            updated_at=payment.updated_at,
            processed_at=payment.processed_at,
        )


class ErrorResponse(BaseModel):
    """Единый формат ошибки."""

    detail: str
    code: str

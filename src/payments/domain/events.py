"""Контракты событий, проходящих через брокер, и тела webhook-уведомления.

Событие в очереди намеренно «тонкое»: оно несёт `payment_id`, а не копию
платежа. Consumer читает актуальное состояние из БД. Толстое событие устарело
бы к моменту обработки и открыло бы путь к рассинхронизации.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field

from payments.domain.enums import Currency, PaymentStatus

PAYMENT_CREATED: Final = "payment.created"

# Единый источник ключа маршрутизации события `payment.created`. Живёт в
# доменном слое (без зависимости от faststream), чтобы и API (запись в outbox),
# и брокерская топология ссылались на одну строку и не разъезжались.
PAYMENTS_NEW_ROUTING_KEY: Final = "payments.new"

# Пространство имён для стабильных идентификаторов webhook-событий (UUIDv5).
# Один платёж → один event_id, сколько бы раз доставка ни повторялась.
WEBHOOK_EVENT_NAMESPACE: Final = uuid.UUID("6f1a2c34-8b5e-4d2a-9f60-1c7d5f0a3b21")


class PaymentCreatedEvent(BaseModel):
    """Событие `payments.new`. Публикуется outbox-relay, потребляется consumer'ом."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_type: str = Field(default=PAYMENT_CREATED)
    payment_id: uuid.UUID
    occurred_at: datetime


class WebhookPayload(BaseModel):
    """Тело POST-запроса, уходящего на `webhook_url` клиента."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event: str
    event_id: uuid.UUID
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    failure_reason: str | None
    created_at: datetime
    processed_at: datetime | None


def webhook_event_id(payment_id: uuid.UUID) -> uuid.UUID:
    """Стабильный идентификатор webhook-события — ключ дедупликации для получателя."""
    return uuid.uuid5(WEBHOOK_EVENT_NAMESPACE, str(payment_id))

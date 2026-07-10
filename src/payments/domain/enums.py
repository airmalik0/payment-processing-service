"""Перечисления предметной области."""

from __future__ import annotations

from enum import StrEnum


class PaymentStatus(StrEnum):
    """Жизненный цикл платежа: pending → succeeded | failed. Обратных переходов нет."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_final(self) -> bool:
        return self is not PaymentStatus.PENDING


class Currency(StrEnum):
    """Поддерживаемые валюты."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"

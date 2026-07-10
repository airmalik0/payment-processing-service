"""Эмуляция внешнего платёжного шлюза.

Задержка 2–5 секунд, 90% успешных платежей и 10% отказов — по условию задания.
Отказ шлюза (`failed`) — это бизнес-результат, а не ошибка обработки: платёж
переходит в финальный статус, и клиент уведомляется webhook'ом так же, как при
успехе. Ошибкой обработки считается только невозможность довести платёж до
результата (см. `errors.py`).
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Protocol

from payments.domain.enums import PaymentStatus

_FAILURE_REASONS = (
    "insufficient_funds",
    "card_declined",
    "issuer_unavailable",
    "fraud_suspected",
)


@dataclass(frozen=True, slots=True)
class GatewayResult:
    """Ответ шлюза: финальный статус и, при отказе, его причина."""

    status: PaymentStatus
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.status.is_final:
            msg = "Шлюз обязан вернуть финальный статус"
            raise ValueError(msg)
        if (self.status is PaymentStatus.FAILED) != (self.failure_reason is not None):
            msg = "failure_reason задаётся тогда и только тогда, когда статус failed"
            raise ValueError(msg)


class PaymentGateway(Protocol):
    """Контракт шлюза. Позволяет подменить эмуляцию детерминированной реализацией в тестах."""

    async def charge(
        self, *, payment_id: object, amount: object, currency: object
    ) -> GatewayResult: ...


class SimulatedGateway:
    """Эмулятор: спит `min..max` секунд и отвечает успехом с вероятностью `success_rate`."""

    def __init__(
        self,
        *,
        min_delay_seconds: float,
        max_delay_seconds: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        if min_delay_seconds < 0 or max_delay_seconds < min_delay_seconds:
            msg = "Некорректный диапазон задержки шлюза"
            raise ValueError(msg)
        if not 0.0 <= success_rate <= 1.0:
            msg = "success_rate должен лежать в [0, 1]"
            raise ValueError(msg)

        self._min_delay = min_delay_seconds
        self._max_delay = max_delay_seconds
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(
        self, *, payment_id: object, amount: object, currency: object
    ) -> GatewayResult:
        del payment_id, amount, currency  # эмулятору не нужны; сигнатура — часть контракта

        delay = self._rng.uniform(self._min_delay, self._max_delay)
        await asyncio.sleep(delay)

        if self._rng.random() < self._success_rate:
            return GatewayResult(status=PaymentStatus.SUCCEEDED)

        return GatewayResult(
            status=PaymentStatus.FAILED,
            failure_reason=self._rng.choice(_FAILURE_REASONS),
        )

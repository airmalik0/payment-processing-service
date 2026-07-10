"""Ошибки предметной области.

Ключевое различие — `TransientError` против `PermanentError`. От него зависит,
попадёт ли сообщение в retry-очередь или сразу в DLQ: три повтора запроса,
который вернул `404`, не сделают ресурс существующим.
"""

from __future__ import annotations


class PaymentsError(Exception):
    """Базовая ошибка сервиса."""


class TransientError(PaymentsError):
    """Ошибка, которая может пройти сама: таймаут, 5xx, недоступность БД."""


class PermanentError(PaymentsError):
    """Ошибка, которая не исчезнет при повторе: битый payload, 4xx, нет платежа."""


class PaymentNotFoundError(PermanentError):
    """Платёж, упомянутый в сообщении, отсутствует в БД."""

    def __init__(self, payment_id: object) -> None:
        super().__init__(f"Платёж {payment_id} не найден")
        self.payment_id = payment_id


class MalformedMessageError(PermanentError):
    """Сообщение не соответствует контракту события."""


class WebhookTransientError(TransientError):
    """Webhook недоступен временно: 5xx, 429, 408, таймаут, отказ соединения."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class WebhookPermanentError(PermanentError):
    """Webhook отверг уведомление окончательно: 4xx, кроме 429 и 408."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class IdempotencyConflictError(PaymentsError):
    """Тот же Idempotency-Key прислан с другим телом запроса."""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"Idempotency-Key {idempotency_key!r} уже использован с другим телом запроса"
        )
        self.idempotency_key = idempotency_key

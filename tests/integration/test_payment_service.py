"""PaymentService: детерминированная проверка ветки гонки idempotency-ключа."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import Payment
from payments.domain.enums import Currency
from payments.domain.errors import IdempotencyConflictError
from payments.services.payment_service import CreatePaymentCommand, PaymentService

pytestmark = pytest.mark.integration


def _command(key: str = "svc-key", amount: str = "100.00") -> CreatePaymentCommand:
    return CreatePaymentCommand(
        idempotency_key=key,
        amount=Decimal(amount),
        currency=Currency.RUB,
        description="Платёж",
        metadata={},
        webhook_url=None,
    )


async def test_create_returns_payment(session: AsyncSession) -> None:
    service = PaymentService(session)
    result = await service.create(_command())
    assert result.replayed is False
    assert result.payment.status.value == "pending"


async def test_replay_same_body(session: AsyncSession) -> None:
    service = PaymentService(session)
    first = await service.create(_command(key="svc-replay"))
    second = await service.create(_command(key="svc-replay"))
    assert second.replayed is True
    assert first.payment.id == second.payment.id


async def test_conflict_different_body(session: AsyncSession) -> None:
    service = PaymentService(session)
    await service.create(_command(key="svc-conflict", amount="100.00"))
    with pytest.raises(IdempotencyConflictError):
        await service.create(_command(key="svc-conflict", amount="200.00"))


async def test_race_branch_recovers_existing_payment(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    """Ветка гонки: pre-check промахнулся, строка уже есть → IntegrityError, читаем победителя.

    Воспроизводим детерминированно: сначала честно создаём платёж, затем
    заставляем предварительный SELECT вернуть None, хотя строка в БД есть.
    commit гарантированно упрётся в unique-constraint, и сервис обязан вернуть
    существующий платёж, а не упасть.
    """
    key = "svc-race"

    # Победитель гонки — реальный платёж в БД.
    winner_service = PaymentService(session)
    winner = await winner_service.create(_command(key=key))

    # Проигравший: тот же ключ и то же тело, но предварительный SELECT «слеп».
    loser_session_cm = session_factory()
    loser_session = await loser_session_cm.__aenter__()
    try:
        loser_service = PaymentService(loser_session)

        # Слепим только предварительную проверку (первый вызов). Повторное чтение
        # в ветке восстановления обязано отработать по-настоящему и найти победителя.
        real_lookup = loser_service._payments.get_by_idempotency_key
        calls = {"n": 0}

        async def blind_once(idempotency_key: str) -> Payment | None:
            calls["n"] += 1
            if calls["n"] == 1:
                return None
            return await real_lookup(idempotency_key)

        loser_service._payments.get_by_idempotency_key = blind_once  # type: ignore[method-assign]

        result = await loser_service.create(_command(key=key))

        # Второй платёж не создан: вернулся победитель.
        assert result.replayed is True
        assert result.payment.id == winner.payment.id
    finally:
        await loser_session_cm.__aexit__(None, None, None)

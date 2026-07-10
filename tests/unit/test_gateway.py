"""Эмулятор платёжного шлюза."""

from __future__ import annotations

import random
import uuid
from decimal import Decimal

import pytest

from payments.domain.enums import Currency, PaymentStatus
from payments.domain.gateway import GatewayResult, SimulatedGateway


async def _charge(gateway: SimulatedGateway) -> GatewayResult:
    return await gateway.charge(
        payment_id=uuid.uuid4(), amount=Decimal("10.00"), currency=Currency.RUB
    )


async def test_always_succeeds_at_rate_one() -> None:
    gateway = SimulatedGateway(
        min_delay_seconds=0, max_delay_seconds=0, success_rate=1.0, rng=random.Random(1)
    )
    result = await _charge(gateway)
    assert result.status is PaymentStatus.SUCCEEDED
    assert result.failure_reason is None


async def test_always_fails_at_rate_zero() -> None:
    gateway = SimulatedGateway(
        min_delay_seconds=0, max_delay_seconds=0, success_rate=0.0, rng=random.Random(1)
    )
    result = await _charge(gateway)
    assert result.status is PaymentStatus.FAILED
    assert result.failure_reason is not None


async def test_success_rate_is_approximately_honored() -> None:
    gateway = SimulatedGateway(
        min_delay_seconds=0, max_delay_seconds=0, success_rate=0.9, rng=random.Random(42)
    )
    results = [await _charge(gateway) for _ in range(2000)]
    successes = sum(1 for r in results if r.status is PaymentStatus.SUCCEEDED)
    assert 0.85 < successes / len(results) < 0.95


def test_gateway_result_rejects_pending() -> None:
    with pytest.raises(ValueError, match="финальный"):
        GatewayResult(status=PaymentStatus.PENDING)


def test_gateway_result_requires_reason_for_failure() -> None:
    with pytest.raises(ValueError, match="failure_reason"):
        GatewayResult(status=PaymentStatus.FAILED, failure_reason=None)


def test_gateway_result_forbids_reason_on_success() -> None:
    with pytest.raises(ValueError, match="failure_reason"):
        GatewayResult(status=PaymentStatus.SUCCEEDED, failure_reason="oops")


def test_invalid_delay_range_rejected() -> None:
    with pytest.raises(ValueError, match="диапазон"):
        SimulatedGateway(min_delay_seconds=5, max_delay_seconds=1, success_rate=1.0)

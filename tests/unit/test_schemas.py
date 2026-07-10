"""Валидация схем запроса."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from payments.api.schemas import CreatePaymentRequest


def _request(**overrides: object) -> CreatePaymentRequest:
    base: dict[str, object] = {
        "amount": Decimal("100.00"),
        "currency": "RUB",
        "description": "Платёж",
    }
    base.update(overrides)
    return CreatePaymentRequest(**base)


def test_valid_request() -> None:
    req = _request(metadata={"k": "v"}, webhook_url="https://ok.test/hook")
    assert req.amount == Decimal("100.00")
    assert req.currency.value == "RUB"


def test_rejects_negative_amount() -> None:
    with pytest.raises(ValidationError):
        _request(amount=Decimal("-1.00"))


def test_rejects_zero_amount() -> None:
    with pytest.raises(ValidationError):
        _request(amount=Decimal("0.00"))


def test_rejects_sub_cent_precision() -> None:
    with pytest.raises(ValidationError, match="двух знаков"):
        _request(amount=Decimal("1.005"))


def test_rejects_unknown_currency() -> None:
    with pytest.raises(ValidationError):
        _request(currency="GBP")


def test_rejects_empty_description() -> None:
    with pytest.raises(ValidationError):
        _request(description="")


def test_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        _request(unexpected="field")


def test_rejects_invalid_webhook_url() -> None:
    with pytest.raises(ValidationError):
        _request(webhook_url="not-a-url")


def test_metadata_defaults_to_empty() -> None:
    assert _request().metadata == {}


def test_rejects_too_many_metadata_keys() -> None:
    with pytest.raises(ValidationError, match="50"):
        _request(metadata={str(i): i for i in range(51)})

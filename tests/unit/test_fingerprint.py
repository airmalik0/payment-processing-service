"""Отпечаток запроса: устойчивость к порядку ключей и записи чисел."""

from __future__ import annotations

from decimal import Decimal

from payments.domain.fingerprint import normalize_amount, request_fingerprint


def test_fingerprint_stable_across_key_order() -> None:
    a = request_fingerprint(
        {"amount": Decimal("10.00"), "currency": "USD", "meta": {"x": 1, "y": 2}}
    )
    b = request_fingerprint(
        {"meta": {"y": 2, "x": 1}, "currency": "USD", "amount": Decimal("10.00")}
    )
    assert a == b


def test_fingerprint_normalizes_amount_scale() -> None:
    # 10.5 и 10.50 — одна сумма, отпечаток обязан совпасть.
    a = request_fingerprint({"amount": Decimal("10.5")})
    b = request_fingerprint({"amount": Decimal("10.50")})
    assert a == b


def test_fingerprint_differs_on_amount() -> None:
    a = request_fingerprint({"amount": Decimal("10.00")})
    b = request_fingerprint({"amount": Decimal("10.01")})
    assert a != b


def test_fingerprint_differs_on_metadata() -> None:
    a = request_fingerprint({"amount": Decimal("10.00"), "meta": {"x": 1}})
    b = request_fingerprint({"amount": Decimal("10.00"), "meta": {"x": 2}})
    assert a != b


def test_fingerprint_is_hex_sha256() -> None:
    fp = request_fingerprint({"amount": Decimal("1.00")})
    assert len(fp) == 64
    assert all(c in "0123456789abcdef" for c in fp)


def test_normalize_amount() -> None:
    assert normalize_amount(Decimal("10.5")) == "10.50"
    assert normalize_amount(Decimal("10")) == "10.00"
    assert normalize_amount(Decimal("0.1")) == "0.10"

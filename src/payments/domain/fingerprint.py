"""Отпечаток тела запроса для проверки идемпотентности.

Один и тот же `Idempotency-Key` с другим телом — почти всегда ошибка клиента
(переиспользовал ключ). Чтобы отличить честный повтор от такой ошибки, вместе
с ключом сохраняется SHA-256 канонизированного тела.

Канонизация обязана быть устойчивой к порядку ключей JSON и к записи чисел:
`{"a":1,"b":2}` и `{"b":2,"a":1}` — одно тело, `100.5` и `100.50` — одна сумма.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

MONEY_EXPONENT = Decimal("0.01")


def normalize_amount(amount: Decimal) -> str:
    """Приводит сумму к каноническому виду с двумя знаками: 100.5 → '100.50'."""
    return str(amount.quantize(MONEY_EXPONENT))


def _canonical(value: Any) -> Any:
    """Рекурсивно приводит структуру к виду, независимому от порядка ключей."""
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, Decimal):
        return normalize_amount(value)
    return value


def request_fingerprint(payload: dict[str, Any]) -> str:
    """SHA-256 канонизированного тела запроса в hex (64 символа)."""
    canonical = json.dumps(
        _canonical(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

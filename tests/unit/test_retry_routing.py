"""Логика ретраев: подсчёт попыток и очистка заголовков."""

from __future__ import annotations

from payments.broker.retry import current_attempt
from payments.broker.topology import (
    RETRY_COUNT_HEADER,
    build_retry_queues,
    retry_queue_name,
    retry_routing_key,
)


def test_current_attempt_defaults_to_zero() -> None:
    assert current_attempt({}) == 0


def test_current_attempt_reads_header() -> None:
    assert current_attempt({RETRY_COUNT_HEADER: 2}) == 2


def test_current_attempt_tolerates_garbage() -> None:
    assert current_attempt({RETRY_COUNT_HEADER: "not-a-number"}) == 0
    assert current_attempt({RETRY_COUNT_HEADER: -5}) == 0


def test_retry_routing_keys() -> None:
    assert retry_routing_key(1) == "retry.1"
    assert retry_routing_key(3) == "retry.3"


def test_retry_queue_names() -> None:
    assert retry_queue_name(1) == "payments.new.retry.1"
    assert retry_queue_name(2) == "payments.new.retry.2"


def test_build_retry_queues_have_expected_ttl() -> None:
    queues = build_retry_queues(max_retries=3, base_delay=1.0, multiplier=5.0)
    assert len(queues) == 3
    ttls = [q.arguments["x-message-ttl"] for q in queues]
    # 1с, 5с, 25с в миллисекундах.
    assert ttls == [1000, 5000, 25000]


def test_retry_queues_dead_letter_back_to_main() -> None:
    queues = build_retry_queues(max_retries=2, base_delay=1.0, multiplier=5.0)
    for queue in queues:
        assert queue.arguments["x-dead-letter-exchange"] == "payments"
        assert queue.arguments["x-dead-letter-routing-key"] == "payments.new"

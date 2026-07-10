"""API платежей: приём, идемпотентность, аутентификация, атомарность outbox."""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payments.api.app import create_app
from payments.api.dependencies import get_db_session
from payments.config import Settings
from payments.db.models import OutboxMessage, Payment

pytestmark = pytest.mark.integration

VALID_BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Тестовый платёж",
    "metadata": {"order_id": "A-1"},
    "webhook_url": "https://example.test/webhook",
}


async def test_create_payment_returns_202(app_client: AsyncClient) -> None:
    response = await app_client.post(
        "/api/v1/payments", json=VALID_BODY, headers={"Idempotency-Key": "k-202"}
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert "payment_id" in body
    assert "created_at" in body
    assert response.headers["Idempotent-Replay"] == "false"


async def test_create_writes_payment_and_outbox_atomically(
    app_client: AsyncClient, session: AsyncSession
) -> None:
    response = await app_client.post(
        "/api/v1/payments", json=VALID_BODY, headers={"Idempotency-Key": "k-atomic"}
    )
    payment_id = response.json()["payment_id"]

    # И платёж, и ровно одно outbox-событие должны появиться одной транзакцией.
    payment = await session.get(Payment, payment_id)
    assert payment is not None

    outbox_count = await session.scalar(
        select(func.count())
        .select_from(OutboxMessage)
        .where(OutboxMessage.aggregate_id == payment_id)
    )
    assert outbox_count == 1

    event = await session.scalar(
        select(OutboxMessage).where(OutboxMessage.aggregate_id == payment_id)
    )
    assert event is not None
    assert event.routing_key == "payments.new"
    assert event.event_type == "payment.created"
    assert event.published_at is None
    assert event.payload["payment_id"] == payment_id


async def test_get_payment_returns_details(app_client: AsyncClient) -> None:
    created = await app_client.post(
        "/api/v1/payments", json=VALID_BODY, headers={"Idempotency-Key": "k-get"}
    )
    payment_id = created.json()["payment_id"]

    response = await app_client.get(f"/api/v1/payments/{payment_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == payment_id
    assert body["amount"] == "100.50"
    assert body["currency"] == "RUB"
    assert body["metadata"] == {"order_id": "A-1"}
    assert body["status"] == "pending"


async def test_get_unknown_payment_returns_404(app_client: AsyncClient) -> None:
    response = await app_client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
    assert response.json()["code"] == "not_found"


# --- Идемпотентность ---


async def test_same_key_same_body_replays(app_client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "k-replay"}
    first = await app_client.post("/api/v1/payments", json=VALID_BODY, headers=headers)
    second = await app_client.post("/api/v1/payments", json=VALID_BODY, headers=headers)

    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert second.headers["Idempotent-Replay"] == "true"


async def test_same_key_creates_single_payment(
    app_client: AsyncClient, session: AsyncSession
) -> None:
    headers = {"Idempotency-Key": "k-single"}
    await app_client.post("/api/v1/payments", json=VALID_BODY, headers=headers)
    await app_client.post("/api/v1/payments", json=VALID_BODY, headers=headers)

    count = await session.scalar(
        select(func.count()).select_from(Payment).where(Payment.idempotency_key == "k-single")
    )
    assert count == 1


async def test_same_key_different_body_conflicts(app_client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "k-conflict"}
    await app_client.post("/api/v1/payments", json=VALID_BODY, headers=headers)

    changed = {**VALID_BODY, "amount": "200.00"}
    response = await app_client.post("/api/v1/payments", json=changed, headers=headers)
    assert response.status_code == 409
    assert response.json()["code"] == "idempotency_conflict"


async def test_missing_idempotency_key_rejected(app_client: AsyncClient) -> None:
    response = await app_client.post("/api/v1/payments", json=VALID_BODY)
    assert response.status_code == 400


async def test_concurrent_same_key_creates_one_payment(
    settings: Settings, engine: AsyncEngine
) -> None:
    """Гонка одновременных запросов с одним ключом создаёт ровно один платёж.

    Разрешается на уровне СУБД: проигравший ловит IntegrityError и читает
    победившую строку. Проверка «SELECT потом INSERT» тут не спасла бы.

    Тесту нужны настоящие параллельные соединения, поэтому он идёт мимо
    savepoint-изоляции (одно общее соединение) и пишет по-настоящему, затем сам
    подчищает за собой.
    """
    from sqlalchemy import delete

    real_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    key = "k-race-concurrent"

    app = create_app(settings)

    async def override_session() -> object:
        async with real_factory() as sess:
            yield sess

    app.dependency_overrides[get_db_session] = override_session
    transport = ASGITransport(app=app)

    try:
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"X-API-Key": settings.api_key.get_secret_value()},
        ) as client:
            headers = {"Idempotency-Key": key}
            responses = await asyncio.gather(
                client.post("/api/v1/payments", json=VALID_BODY, headers=headers),
                client.post("/api/v1/payments", json=VALID_BODY, headers=headers),
                client.post("/api/v1/payments", json=VALID_BODY, headers=headers),
            )

        assert all(r.status_code == 202 for r in responses)
        assert len({r.json()["payment_id"] for r in responses}) == 1

        async with real_factory() as check:
            count = await check.scalar(
                select(func.count()).select_from(Payment).where(Payment.idempotency_key == key)
            )
        assert count == 1
    finally:
        async with real_factory() as cleanup:
            payment_ids = (
                await cleanup.scalars(select(Payment.id).where(Payment.idempotency_key == key))
            ).all()
            await cleanup.execute(
                delete(OutboxMessage).where(OutboxMessage.aggregate_id.in_(payment_ids))
            )
            await cleanup.execute(delete(Payment).where(Payment.idempotency_key == key))
            await cleanup.commit()


# --- Аутентификация ---


async def test_missing_api_key_rejected(app_client: AsyncClient) -> None:
    response = await app_client.post(
        "/api/v1/payments",
        json=VALID_BODY,
        headers={"Idempotency-Key": "k", "X-API-Key": ""},
    )
    assert response.status_code == 401


async def test_wrong_api_key_rejected(app_client: AsyncClient) -> None:
    response = await app_client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000",
        headers={"X-API-Key": "wrong-key"},
    )
    assert response.status_code == 401


async def test_health_needs_no_auth(app_client: AsyncClient) -> None:
    response = await app_client.get("/health", headers={"X-API-Key": ""})
    assert response.status_code == 200


# --- Валидация ---


async def test_negative_amount_rejected(app_client: AsyncClient) -> None:
    body = {**VALID_BODY, "amount": "-1.00"}
    response = await app_client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": "k-neg"}
    )
    assert response.status_code == 422
    assert response.json()["code"] == "validation_error"


async def test_unknown_currency_rejected(app_client: AsyncClient) -> None:
    body = {**VALID_BODY, "currency": "GBP"}
    response = await app_client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": "k-cur"}
    )
    assert response.status_code == 422

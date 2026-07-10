"""Эндпоинты платежей."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Response, status

from payments.api.dependencies import (
    IdempotencyKeyDep,
    PaymentServiceDep,
    verify_api_key,
)
from payments.api.schemas import (
    CreatePaymentRequest,
    CreatePaymentResponse,
    ErrorResponse,
    PaymentResponse,
)
from payments.domain.errors import IdempotencyConflictError
from payments.services.payment_service import CreatePaymentCommand

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses={
        401: {"model": ErrorResponse, "description": "Неверный или отсутствующий API-ключ"},
    },
)

REPLAY_HEADER = "Idempotent-Replay"


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=CreatePaymentResponse,
    summary="Создать платёж",
    responses={
        202: {"description": "Платёж принят к обработке"},
        400: {"model": ErrorResponse, "description": "Отсутствует Idempotency-Key"},
        409: {"model": ErrorResponse, "description": "Ключ идемпотентности переиспользован"},
        422: {"model": ErrorResponse, "description": "Тело запроса не прошло валидацию"},
    },
)
async def create_payment(
    request: CreatePaymentRequest,
    idempotency_key: IdempotencyKeyDep,
    service: PaymentServiceDep,
    response: Response,
) -> CreatePaymentResponse:
    """Принимает платёж и ставит его в очередь на обработку.

    Ответ `202 Accepted` означает: платёж сохранён и событие о нём записано в
    outbox той же транзакцией. Обработка произойдёт асинхронно, о результате
    сервис сообщит на `webhook_url`.

    Повтор с тем же `Idempotency-Key` и тем же телом вернёт тот же платёж и
    заголовок `Idempotent-Replay: true`, не создавая второй.
    """
    command = CreatePaymentCommand(
        idempotency_key=idempotency_key,
        amount=request.amount,
        currency=request.currency,
        description=request.description,
        metadata=request.metadata,
        webhook_url=str(request.webhook_url) if request.webhook_url else None,
    )

    try:
        result = await service.create(command)
    except IdempotencyConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(error),
        ) from error

    response.headers[REPLAY_HEADER] = "true" if result.replayed else "false"

    return CreatePaymentResponse(
        payment_id=result.payment.id,
        status=result.payment.status,
        created_at=result.payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    summary="Получить платёж",
    responses={404: {"model": ErrorResponse, "description": "Платёж не найден"}},
)
async def get_payment(payment_id: uuid.UUID, service: PaymentServiceDep) -> PaymentResponse:
    """Возвращает текущее состояние платежа, включая статус доставки webhook."""
    payment = await service.get(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Платёж {payment_id} не найден",
        )
    return PaymentResponse.from_model(payment)

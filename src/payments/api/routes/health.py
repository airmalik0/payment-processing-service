"""Health-пробы. Без аутентификации: оркестратор не носит секретов."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Request, Response, status
from pydantic import BaseModel

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    database: Literal["up", "down"]


@router.get("/health", summary="Liveness")
async def health() -> dict[str, str]:
    """Процесс жив. Не проверяет зависимости — иначе перезапуск при сбое БД."""
    return {"status": "ok"}


@router.get(
    "/health/ready",
    summary="Readiness",
    response_model=HealthResponse,
    responses={503: {"description": "Сервис не готов принимать трафик"}},
)
async def readiness(request: Request, response: Response) -> HealthResponse:
    """Готовность принимать трафик: нужна живая БД (в неё пишутся платежи и outbox).

    RabbitMQ здесь намеренно не проверяется: API продолжает принимать платежи
    при мёртвом брокере — события копятся в outbox и уйдут, когда он вернётся.
    """
    from payments.api.app import check_database

    database_up = await check_database(request.app.state.engine)
    if not database_up:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthResponse(status="degraded", database="down")

    return HealthResponse(status="ok", database="up")

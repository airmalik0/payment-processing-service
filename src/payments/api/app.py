"""Сборка FastAPI-приложения."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from starlette.exceptions import HTTPException as StarletteHTTPException

from payments.api.routes import health, payments
from payments.config import Settings, get_settings
from payments.db.session import create_engine, create_session_factory
from payments.observability.logging import configure_logging, get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Engine создаётся один раз на процесс и закрывается при остановке."""
    settings: Settings = app.state.settings
    engine = create_engine(settings)
    app.state.engine = engine
    app.state.session_factory = create_session_factory(engine)

    logger.info("api_started")
    try:
        yield
    finally:
        await engine.dispose()
        logger.info("api_stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Фабрика приложения. Настройки передаются явно — так их подменяют тесты."""
    settings = settings or get_settings()
    configure_logging(level=settings.log_level, json_output=settings.log_json)

    app = FastAPI(
        title="Payment Processing Service",
        description=(
            "Асинхронный процессинг платежей: приём по HTTP, обработка через очередь, "
            "уведомление клиента webhook'ом. Все эндпоинты `/api/v1/*` требуют заголовок "
            "`X-API-Key`."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = settings

    app.include_router(payments.router)
    app.include_router(health.router)

    _register_exception_handlers(app)
    return app


_ERROR_CODES = {
    status.HTTP_400_BAD_REQUEST: "bad_request",
    status.HTTP_401_UNAUTHORIZED: "unauthorized",
    status.HTTP_404_NOT_FOUND: "not_found",
    status.HTTP_409_CONFLICT: "idempotency_conflict",
}


def _register_exception_handlers(app: FastAPI) -> None:
    """Единый формат ошибок: `{"detail": ..., "code": ...}`."""

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "detail": exc.detail,
                "code": _ERROR_CODES.get(exc.status_code, "http_error"),
            },
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,  # Unprocessable Content
            content={
                "detail": "Тело запроса не прошло валидацию",
                "code": "validation_error",
                "errors": _serialize_validation_errors(exc),
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
        # Внутренние детали наружу не уходят: они в логе, у клиента — код ошибки.
        logger.exception("unhandled_error", path=request.url.path, error=str(exc))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Внутренняя ошибка сервиса", "code": "internal_error"},
        )


def _serialize_validation_errors(exc: RequestValidationError) -> list[dict[str, Any]]:
    """Ошибки Pydantic содержат несериализуемые объекты в `ctx` — убираем их."""
    serialized: list[dict[str, Any]] = []
    for error in exc.errors():
        serialized.append(
            {
                "field": ".".join(str(part) for part in error["loc"]),
                "message": error["msg"],
                "type": error["type"],
            }
        )
    return serialized


async def check_database(engine: Any) -> bool:
    """Пингует БД для health-пробы."""
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception:
        return False
    return True

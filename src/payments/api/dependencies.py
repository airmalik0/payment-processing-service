"""Зависимости FastAPI: аутентификация, сессия БД, идемпотентный ключ."""

from __future__ import annotations

import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from payments.config import Settings, get_settings
from payments.services.payment_service import PaymentService

API_KEY_HEADER = "X-API-Key"
IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
MAX_IDEMPOTENCY_KEY_LENGTH = 255


def get_app_settings(request: Request) -> Settings:
    """Настройки конкретного приложения.

    Читаются из `app.state`, куда их кладёт `create_app`. Так тесты и любой
    вызов с явными настройками не разъезжаются с глобальным синглтоном `.env`.
    """
    settings: Settings | None = getattr(request.app.state, "settings", None)
    return settings if settings is not None else get_settings()


async def get_db_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Сессия на запрос. Фабрика создана на старте приложения и живёт в state."""
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        try:
            yield session
        except BaseException:
            await session.rollback()
            raise


SettingsDep = Annotated[Settings, Depends(get_app_settings)]


async def verify_api_key(
    settings: SettingsDep,
    x_api_key: Annotated[str | None, Header(alias=API_KEY_HEADER)] = None,
) -> None:
    """Статический ключ доступа.

    `compare_digest` вместо `==`: обычное сравнение строк завершается на первом
    несовпавшем байте, и по времени ответа ключ восстанавливается посимвольно.

    Неверный ключ даёт 401, а не 403: сервис не подтверждает существование ключей.
    """
    expected = settings.api_key.get_secret_value()

    if x_api_key is None or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный или отсутствующий API-ключ",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )


async def get_idempotency_key(
    idempotency_key: Annotated[str | None, Header(alias=IDEMPOTENCY_KEY_HEADER)] = None,
) -> str:
    """`Idempotency-Key` обязателен: без него повтор запроса создаст второй платёж."""
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Заголовок {IDEMPOTENCY_KEY_HEADER} обязателен",
        )
    key = idempotency_key.strip()
    if len(key) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{IDEMPOTENCY_KEY_HEADER} длиннее {MAX_IDEMPOTENCY_KEY_LENGTH} символов",
        )
    return key


async def get_payment_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> PaymentService:
    return PaymentService(session)


SessionDep = Annotated[AsyncSession, Depends(get_db_session)]
PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
IdempotencyKeyDep = Annotated[str, Depends(get_idempotency_key)]

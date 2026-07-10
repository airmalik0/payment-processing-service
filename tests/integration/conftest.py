"""Фикстуры интеграционного слоя: приложение поверх тестовой сессии."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.api.app import create_app
from payments.api.dependencies import get_db_session
from payments.config import Settings

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture(loop_scope="session")
async def app_client(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    """HTTP-клиент к приложению, где сессия БД идёт через тестовую транзакцию.

    Все запросы теста делят одно соединение с внешней транзакцией, поэтому
    записанное одним запросом видно следующему, а в финале всё откатывается.
    """
    app = create_app(settings)

    async def override_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as sess:
            yield sess

    app.dependency_overrides[get_db_session] = override_session

    # lifespan создал бы собственный engine и обошёл тестовую транзакцию —
    # подменяем и state, чтобы health-проба и прочее видели ту же БД.
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"X-API-Key": settings.api_key.get_secret_value()},
        ) as client:
            yield client


@pytest.fixture
def auth_headers(settings: Settings) -> dict[str, str]:
    return {"X-API-Key": settings.api_key.get_secret_value()}

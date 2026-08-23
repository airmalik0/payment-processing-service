# syntax=docker/dockerfile:1

# --- Стадия сборки: ставим зависимости в изолированный venv ---
FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

# uv — быстрый установщик; копируем бинарь из официального образа.
COPY --from=ghcr.io/astral-sh/uv:0.11.0 /uv /uvx /bin/

WORKDIR /app

# Сначала только манифест и lock — слой зависимостей кэшируется, пока они не
# менялись. Код копируется после, поэтому его правки не инвалидируют установку
# зависимостей. `--frozen` требует актуального uv.lock (воспроизводимая сборка).
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-dev

COPY src ./src
RUN uv sync --frozen --no-dev

# --- Стадия для тестов: то же окружение плюс dev-зависимости и сами тесты ---
# Нужна, чтобы проверяющий мог прогнать весь набор тестов одной командой
# (`make test`), не устанавливая локально Python, uv и драйверы.
FROM builder AS dev

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app/src"

# dev-зависимости объявлены как extra `dev`, поэтому именно --extra:
# обычный `uv sync` ставит только dependency-groups.
RUN uv sync --frozen --extra dev
COPY tests ./tests
COPY alembic.ini ./
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["entrypoint.sh"]
CMD ["test"]

# --- Стадия рантайма: только venv и код, без инструментов сборки ---
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app/src"

# Непривилегированный пользователь: процесс в контейнере не должен быть root.
RUN groupadd --system app && useradd --system --gid app --home /app app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY src ./src
COPY alembic.ini ./
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh && chown -R app:app /app

USER app

ENTRYPOINT ["entrypoint.sh"]
CMD ["api"]

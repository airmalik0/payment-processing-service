# syntax=docker/dockerfile:1

# --- Стадия сборки: ставим зависимости в изолированный venv ---
FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# uv — быстрый установщик; копируем бинарь из официального образа.
COPY --from=ghcr.io/astral-sh/uv:0.11.0 /uv /uvx /bin/

WORKDIR /app

# Сначала только манифест — слой с зависимостями кэшируется, пока pyproject не менялся.
COPY pyproject.toml README.md ./
COPY src ./src

RUN uv venv /opt/venv && \
    VIRTUAL_ENV=/opt/venv uv pip install --no-cache .

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

#!/usr/bin/env bash
# Единая точка входа для всех процессов. Первый аргумент выбирает роль.
set -euo pipefail

role="${1:-api}"

run_migrations() {
  echo "[entrypoint] Применяю миграции БД..."
  # env.py берёт advisory-lock, поэтому одновременный запуск нескольких
  # контейнеров с миграциями безопасен.
  alembic upgrade head
  echo "[entrypoint] Миграции применены."
}

# Тесты пересоздают схему, поэтому работают в отдельной БД, а не в рабочей.
# Создаём её, если её ещё нет.
ensure_test_database() {
  python - <<'PY'
import asyncio
import os
from urllib.parse import urlsplit, urlunsplit

import asyncpg

url = os.environ.get("TEST_DATABASE_URL", "")
if not url:
    raise SystemExit(0)

parts = urlsplit(url.replace("postgresql+asyncpg://", "postgresql://"))
database = parts.path.lstrip("/")
admin_url = urlunsplit((parts.scheme, parts.netloc, "/postgres", "", ""))


async def main() -> None:
    connection = await asyncpg.connect(admin_url)
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", database
        )
        if not exists:
            await connection.execute(f'CREATE DATABASE "{database}"')
            print(f"[entrypoint] Создана тестовая БД {database}.")
    finally:
        await connection.close()


asyncio.run(main())
PY
}

case "$role" in
  migrate)
    run_migrations
    ;;
  api)
    exec python -m payments.entrypoints.api
    ;;
  consumer)
    exec python -m payments.entrypoints.consumer
    ;;
  relay)
    exec python -m payments.entrypoints.relay
    ;;
  test)
    # Прогон тестов внутри контейнера: `docker compose run --rm tests [аргументы pytest]`.
    ensure_test_database
    shift || true
    exec pytest "$@"
    ;;
  lint)
    ruff check .
    ruff format --check .
    exec mypy src tests
    ;;
  *)
    echo "[entrypoint] Неизвестная роль: $role (ожидается migrate|api|consumer|relay|test|lint)" >&2
    exit 1
    ;;
esac

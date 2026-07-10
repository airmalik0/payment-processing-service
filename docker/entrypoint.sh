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
  *)
    echo "[entrypoint] Неизвестная роль: $role (ожидается migrate|api|consumer|relay)" >&2
    exit 1
    ;;
esac

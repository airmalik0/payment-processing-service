"""Точка входа API-процесса."""

from __future__ import annotations

import uvicorn

from payments.api.app import create_app

app = create_app()


def main() -> None:
    uvicorn.run(
        "payments.entrypoints.api:app",
        host="0.0.0.0",  # noqa: S104 — сервис в контейнере, доступ снаружи ограничен сетью compose
        port=8000,
        log_config=None,  # логирование настроено через structlog в create_app
        access_log=False,
        workers=1,
    )


if __name__ == "__main__":
    main()

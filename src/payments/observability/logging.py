"""Структурное логирование. JSON в проде, человекочитаемый вывод локально."""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(*, level: str = "INFO", json_output: bool = True) -> None:
    """Настраивает structlog и приводит логи стандартной библиотеки к тому же формату."""
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer(ensure_ascii=False)
        if json_output
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())

    # uvicorn дублирует записи через собственные хендлеры — снимаем их.
    for noisy in ("uvicorn", "uvicorn.access", "uvicorn.error", "faststream"):
        logger = logging.getLogger(noisy)
        logger.handlers.clear()
        logger.propagate = True

    # SQLAlchemy и aio_pika слишком многословны на INFO.
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("aiormq").setLevel(logging.WARNING)


def get_logger(name: str, **initial_values: Any) -> structlog.stdlib.BoundLogger:
    """Логгер с привязанным контекстом."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    if initial_values:
        return logger.bind(**initial_values)
    return logger

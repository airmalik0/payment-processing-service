"""Точка входа consumer-процесса.

Запускается как самостоятельный asyncio-скрипт, а не через `faststream run`:
consumer'у нужны собственные ресурсы (пул БД, HTTP-клиент, эмулятор шлюза) с
управляемым временем жизни, а не только брокер.
"""

from __future__ import annotations

import asyncio
import signal

from payments.broker.consumer import start_consumer
from payments.config import get_settings
from payments.db.session import create_engine, create_session_factory
from payments.domain.gateway import SimulatedGateway
from payments.observability.logging import configure_logging, get_logger
from payments.services.processor import PaymentProcessor
from payments.services.webhook import WebhookSender, create_http_client

logger = get_logger(__name__)


async def run() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level, json_output=settings.log_json)

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    http_client = create_http_client(settings.webhook_timeout_seconds)

    gateway = SimulatedGateway(
        min_delay_seconds=settings.gateway_min_delay_seconds,
        max_delay_seconds=settings.gateway_max_delay_seconds,
        success_rate=settings.gateway_success_rate,
    )
    webhook_sender = WebhookSender(
        client=http_client,
        signing_secret=settings.webhook_signing_secret.get_secret_value(),
    )
    processor = PaymentProcessor(
        session_factory=session_factory, gateway=gateway, webhook_sender=webhook_sender
    )

    broker = await start_consumer(settings, processor)
    logger.info("consumer_started")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await stop.wait()
    finally:
        logger.info("consumer_stopping")
        await broker.stop()  # дожидается дообработки в пределах graceful_timeout
        await http_client.aclose()
        await engine.dispose()
        logger.info("consumer_stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()

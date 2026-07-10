"""Точка входа outbox-relay-процесса."""

from __future__ import annotations

import asyncio
import signal

from payments.broker.setup import create_broker, declare_topology
from payments.config import get_settings
from payments.db.session import create_engine, create_session_factory
from payments.observability.logging import configure_logging, get_logger
from payments.outbox.relay import OutboxRelay

logger = get_logger(__name__)


async def run() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level, json_output=settings.log_json)

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    broker = create_broker(settings)
    await broker.connect()
    # Relay объявляет топологию сам: он может стартовать раньше consumer'а,
    # а публиковать в необъявленный обменник нельзя.
    await declare_topology(broker, settings)

    relay = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)

    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        relay.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_shutdown)

    logger.info("relay_process_started")
    try:
        await relay.run_forever()
    finally:
        logger.info("relay_process_stopping")
        await broker.stop()
        await engine.dispose()
        logger.info("relay_process_stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()

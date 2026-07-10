"""Outbox relay: публикует события из таблицы в RabbitMQ.

Замыкает Outbox pattern. API пишет платёж и событие одной транзакцией; relay
доставляет событие в брокер и помечает его опубликованным. Порядок шагов —
«сначала опубликовать, потом пометить» — даёт at-least-once: если публикация
подтверждена, а пометка не закоммитилась, событие уйдёт повторно. Дубликат
безопасен (обработка идемпотентна), потеря — нет.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from payments.broker.topology import payments_exchange
from payments.db.repositories import OutboxRepository
from payments.observability.logging import get_logger

if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from payments.config import Settings
    from payments.db.models import OutboxMessage

logger = get_logger(__name__)

MAX_ERROR_LENGTH = 500


def _looks_like_broker_down(error: Exception) -> bool:
    """Похоже ли, что публикация упала из-за недоступности брокера, а не одного сообщения.

    Различаем «весь брокер лёг» (обрыв соединения, таймаут подтверждения) от
    единичной проблемы конкретного сообщения. В первом случае обработку пачки
    прерываем, чтобы не ждать таймаут на каждой оставшейся строке.
    """
    return isinstance(error, ConnectionError | TimeoutError | OSError)


class OutboxRelay:
    """Периодически публикует неопубликованные события."""

    def __init__(
        self,
        *,
        broker: RabbitBroker,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._broker = broker
        self._session_factory = session_factory
        self._settings = settings
        self._stopping = asyncio.Event()

    def request_stop(self) -> None:
        """Просит цикл завершиться после текущей итерации."""
        self._stopping.set()

    async def run_forever(self) -> None:
        """Основной цикл. Завершается по `request_stop()`."""
        logger.info(
            "outbox_relay_started",
            poll_interval=self._settings.outbox_poll_interval_seconds,
            batch_size=self._settings.outbox_batch_size,
        )
        while not self._stopping.is_set():
            try:
                published = await self.publish_pending_batch()
            except Exception:
                logger.exception("outbox_batch_failed")
                published = 0

            # Пусто — ждём интервал; была работа — сразу за следующей пачкой.
            if published == 0:
                await self._wait(self._settings.outbox_poll_interval_seconds)

        logger.info("outbox_relay_stopped")

    async def publish_pending_batch(self) -> int:
        """Публикует одну пачку событий. Возвращает число опубликованных.

        Пачка обрабатывается в одной транзакции и коммитится целиком. Сбой на
        одном сообщении не роняет остальные — `_publish_one` глотает ошибку и
        помечает сообщение под повтор. Если же брокер недоступен (сбой
        соединения), обработка пачки прерывается сразу: нет смысла ждать таймаут
        публикации на каждой из оставшихся строк, держа их заблокированными.

        Строки заблокированы `FOR UPDATE SKIP LOCKED` на всё время транзакции,
        поэтому соседняя реплика relay возьмёт другие строки, а не эти же.
        """
        now = datetime.now(UTC)
        published = 0

        async with self._session_factory() as session:
            outbox = OutboxRepository(session)
            messages = await outbox.fetch_batch_for_publishing(
                batch_size=self._settings.outbox_batch_size, now=now
            )
            if not messages:
                return 0

            for message in messages:
                broker_alive = await self._publish_one(session, message)
                if message.published_at is not None:
                    published += 1
                if not broker_alive:
                    # Брокер недоступен — остальные публикации тоже упадут по
                    # таймауту. Коммитим уже сделанное и выходим.
                    break

            await session.commit()

        if published:
            logger.info("outbox_batch_published", count=published, total=len(messages))
        return published

    async def _publish_one(self, session: AsyncSession, message: OutboxMessage) -> bool:
        """Публикует одно событие и помечает его в транзакции пачки.

        Publisher confirms включены на канале по умолчанию: успешный `publish`
        означает, что брокер принял сообщение на диск (`persist=True`), а не
        просто запись в сокет. Ошибка публикации откладывает повтор с backoff.

        Возвращает `False`, если сбой похож на недоступность брокера — сигнал
        вызывающему прервать пачку.
        """
        del session  # сессия управляется вызывающим; параметр — для явности контракта
        try:
            await self._broker.publish(
                message.payload,
                exchange=payments_exchange,
                routing_key=message.routing_key,
                message_id=str(message.id),
                correlation_id=str(message.aggregate_id),
                headers={"x-event-type": message.event_type},
                persist=True,
                mandatory=True,
                timeout=10.0,
            )
        except Exception as error:
            message.attempts += 1
            message.last_error = str(error)[:MAX_ERROR_LENGTH]
            message.next_retry_at = self._next_retry_at(message.attempts)
            logger.warning(
                "outbox_publish_failed",
                message_id=str(message.id),
                attempts=message.attempts,
                error=str(error)[:MAX_ERROR_LENGTH],
            )
            return not _looks_like_broker_down(error)

        message.published_at = datetime.now(UTC)
        message.last_error = None
        return True

    def _next_retry_at(self, attempts: int) -> datetime:
        """Backoff при недоступности брокера, с потолком в минуту."""
        delay = min(2.0**attempts, 60.0)
        return datetime.now(UTC) + timedelta(seconds=delay)

    async def _wait(self, seconds: float) -> None:
        """Ждёт `seconds` или досрочно просыпается при запросе остановки."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)

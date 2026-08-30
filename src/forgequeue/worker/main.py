import asyncio
import signal
from collections.abc import Callable
from typing import Protocol

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.redis import RedisJobBroker, create_redis_client
from forgequeue.core.config import Settings, get_settings
from forgequeue.core.logging import configure_logging
from forgequeue.db.session import (
    create_database_engine,
    create_session_factory,
)
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.runner import Worker


class SignalRegistrar(Protocol):
    def add_signal_handler(
        self,
        signal_number: int,
        callback: Callable[[], None],
    ) -> None: ...


def register_shutdown_signals(
    stop_event: asyncio.Event,
    *,
    loop: SignalRegistrar | None = None,
) -> None:
    event_loop = loop if loop is not None else asyncio.get_running_loop()

    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(shutdown_signal, stop_event.set)


def create_worker(
    settings: Settings,
    redis_client: Redis,
    session_factory: async_sessionmaker[AsyncSession],
) -> Worker:
    broker = RedisJobBroker(
        redis_client,
        stream_name=settings.redis_jobs_stream,
        group_name=settings.redis_worker_group,
    )
    processor = JobProcessor(broker, session_factory)
    return Worker(broker, processor)


async def run_worker() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    stop_event = asyncio.Event()
    register_shutdown_signals(stop_event)

    engine = create_database_engine(settings)
    try:
        redis_client = create_redis_client(settings)
        try:
            session_factory = create_session_factory(engine)
            worker = create_worker(settings, redis_client, session_factory)
            await worker.run_forever(
                stop_event,
                block_ms=settings.redis_worker_block_ms,
            )
        finally:
            await redis_client.aclose()
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()

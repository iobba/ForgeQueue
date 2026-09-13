import asyncio

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.redis import RedisJobBroker, create_redis_client
from forgequeue.core.config import Settings, get_settings
from forgequeue.core.logging import configure_logging
from forgequeue.db.session import create_database_engine, create_session_factory
from forgequeue.scheduler.retry_dispatcher import RetryDispatcher
from forgequeue.worker.main import register_shutdown_signals


def create_retry_dispatcher(
    settings: Settings,
    redis_client: Redis,
    session_factory: async_sessionmaker[AsyncSession],
) -> RetryDispatcher:
    broker = RedisJobBroker(
        redis_client,
        stream_name=settings.redis_jobs_stream,
        group_name=settings.redis_worker_group,
    )
    return RetryDispatcher(
        broker,
        session_factory,
        batch_size=settings.retry_dispatcher_batch_size,
        poll_interval_seconds=settings.retry_dispatcher_poll_seconds,
    )


async def run_retry_dispatcher() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    stop_event = asyncio.Event()
    register_shutdown_signals(stop_event)

    engine = create_database_engine(settings)
    try:
        redis_client = create_redis_client(settings)
        try:
            session_factory = create_session_factory(engine)
            dispatcher = create_retry_dispatcher(
                settings,
                redis_client,
                session_factory,
            )
            await dispatcher.run_forever(stop_event)
        finally:
            await redis_client.aclose()
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(run_retry_dispatcher())


if __name__ == "__main__":
    main()

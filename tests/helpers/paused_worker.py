"""Run a real worker process paused after attempt creation for SIGKILL tests."""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

from forgequeue.broker.redis import (
    RedisDeadLetterBroker,
    RedisJobBroker,
    create_redis_client,
)
from forgequeue.core.config import get_settings
from forgequeue.db.session import create_database_engine, create_session_factory
from forgequeue.worker.child_protocol import HandlerRequest, HandlerResponse
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.runner import Worker


class PausedHandlerExecutor:
    def __init__(self, ready_file: Path) -> None:
        self._ready_file = ready_file

    async def execute(
        self,
        request: HandlerRequest,
        *,
        timeout_seconds: float,
    ) -> HandlerResponse:
        del request, timeout_seconds
        # The processor has committed the running attempt before calling execute.
        # Pause before launching a handler so killing this worker leaves no child.
        self._ready_file.touch()
        await asyncio.Event().wait()
        raise AssertionError("The paused executor must never return")


async def run_paused_worker(ready_file: Path, started_at: datetime) -> None:
    settings = get_settings()
    if settings.app_env != "test" or settings.postgres_db != "forgequeue_test":
        raise RuntimeError("The paused worker must use the integration test database")
    if not all(
        name.startswith("forgequeue:test:")
        for name in (settings.redis_jobs_stream, settings.redis_dead_letter_stream)
    ):
        raise RuntimeError("The paused worker must use isolated test streams")

    engine = create_database_engine(settings)
    try:
        redis_client = create_redis_client(settings)
        try:
            broker = RedisJobBroker(
                redis_client,
                stream_name=settings.redis_jobs_stream,
                group_name=settings.redis_worker_group,
            )
            processor = JobProcessor(
                broker,
                RedisDeadLetterBroker(
                    redis_client,
                    stream_name=settings.redis_dead_letter_stream,
                ),
                create_session_factory(engine),
                clock=lambda: started_at,
                executor=PausedHandlerExecutor(ready_file),
            )
            worker = Worker(broker, processor, worker_id="worker-to-kill")
            await worker.run_forever(asyncio.Event(), block_ms=50)
        finally:
            await redis_client.aclose()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(
        run_paused_worker(Path(sys.argv[1]), datetime.fromisoformat(sys.argv[2]))
    )

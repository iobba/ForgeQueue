import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from math import isfinite
from typing import Protocol

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.contextvars import bound_contextvars

from forgequeue.broker.messages import JobMessage
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService

logger = structlog.get_logger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


class JobPublisher(Protocol):
    async def publish(self, message: JobMessage) -> str: ...


class RetryDispatcher:
    def __init__(
        self,
        broker: JobPublisher,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        batch_size: int = 100,
        poll_interval_seconds: float = 1.0,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if not isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be finite and positive")

        self._broker = broker
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._poll_interval_seconds = poll_interval_seconds
        self._clock = clock

    async def run_once(self) -> int:
        due_at = self._clock()

        async with self._session_factory.begin() as session:
            repository = JobRepository(session)
            jobs = await repository.lock_due_retries(
                due_at=due_at,
                limit=self._batch_size,
            )
            service = JobService(repository)

            for job in jobs:
                with bound_contextvars(
                    job_id=str(job.id),
                    job_type=job.job_type,
                ):
                    entry_id = await self._broker.publish(
                        JobMessage(
                            job_id=job.id,
                            job_type=job.job_type,
                            attempt_number=job.attempts + 1,
                        )
                    )
                    await service.queue_retry(job.id, queued_at=due_at)
                    logger.info(
                        "job_retry_dispatched",
                        entry_id=entry_id,
                    )

        return len(jobs)

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        logger.info(
            "retry_dispatcher_started",
            batch_size=self._batch_size,
            poll_interval_seconds=self._poll_interval_seconds,
        )

        try:
            while not stop_event.is_set():
                await self.run_once()
                if stop_event.is_set():
                    break

                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._poll_interval_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("retry_dispatcher_stopped")

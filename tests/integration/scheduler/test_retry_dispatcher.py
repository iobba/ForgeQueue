import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import JobMessage
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus
from forgequeue.scheduler.retry_dispatcher import RetryDispatcher

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


class RecordingPublisher:
    def __init__(self) -> None:
        self.messages: list[JobMessage] = []

    async def publish(self, message: JobMessage) -> str:
        self.messages.append(message)
        return f"{len(self.messages)}-0"


class FailingPublisher:
    async def publish(self, message: JobMessage) -> str:
        del message
        raise ConnectionError("redis unavailable")


class BlockingPublisher(RecordingPublisher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def publish(self, message: JobMessage) -> str:
        self.started.set()
        await self.release.wait()
        return await super().publish(message)


async def create_scheduled_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    next_attempt_at: datetime,
) -> UUID:
    async with session_factory.begin() as session:
        job = await JobRepository(session).create(
            job_type="sum_numbers",
            payload={"numbers": [10, 20, 30]},
            max_attempts=3,
        )
        job.status = JobStatus.RETRY_SCHEDULED
        job.attempts = 1
        job.error_code = "dependency_unavailable"
        job.error_message = "A dependency is temporarily unavailable"
        job.next_attempt_at = next_attempt_at
        return job.id


async def delete_jobs(
    session_factory: async_sessionmaker[AsyncSession],
    job_ids: set[UUID],
) -> None:
    async with session_factory.begin() as session:
        await session.execute(delete(Job).where(Job.id.in_(job_ids)))


async def test_run_once_publishes_only_due_retries_and_marks_them_queued(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    due_at = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    stream_name = f"forgequeue:test:retry-dispatcher:{uuid7()}"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    dispatcher = RetryDispatcher(
        broker,
        database_session_factory,
        clock=lambda: due_at,
    )
    job_ids: set[UUID] = set()

    try:
        await broker.ensure_consumer_group()
        due_job_id = await create_scheduled_job(
            database_session_factory,
            next_attempt_at=due_at,
        )
        future_job_id = await create_scheduled_job(
            database_session_factory,
            next_attempt_at=due_at + timedelta(seconds=1),
        )
        job_ids.update({due_job_id, future_job_id})

        dispatched_count = await dispatcher.run_once()
        deliveries = await broker.read(
            consumer_name="worker-one",
            count=10,
            block_ms=None,
        )

        async with database_session_factory() as session:
            due_job = await JobRepository(session).get(due_job_id)
            future_job = await JobRepository(session).get(future_job_id)

        assert dispatched_count == 1
        assert len(deliveries) == 1
        assert deliveries[0].message == JobMessage(
            job_id=due_job_id,
            job_type="sum_numbers",
        )
        assert due_job is not None
        assert due_job.status is JobStatus.QUEUED
        assert due_job.next_attempt_at is None
        assert due_job.error_code == "dependency_unavailable"
        assert future_job is not None
        assert future_job.status is JobStatus.RETRY_SCHEDULED
        assert future_job.next_attempt_at == due_at + timedelta(seconds=1)
    finally:
        await redis_client.delete(stream_name)
        if job_ids:
            await delete_jobs(database_session_factory, job_ids)


async def test_publish_failure_rolls_back_retry_state(
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    due_at = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    job_id = await create_scheduled_job(
        database_session_factory,
        next_attempt_at=due_at,
    )
    dispatcher = RetryDispatcher(
        FailingPublisher(),
        database_session_factory,
        clock=lambda: due_at,
    )

    try:
        with pytest.raises(ConnectionError, match="redis unavailable"):
            await dispatcher.run_once()

        async with database_session_factory() as session:
            job = await JobRepository(session).get(job_id)

        assert job is not None
        assert job.status is JobStatus.RETRY_SCHEDULED
        assert job.next_attempt_at == due_at
    finally:
        await delete_jobs(database_session_factory, {job_id})


async def test_concurrent_dispatchers_skip_locked_retry(
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    due_at = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    job_id = await create_scheduled_job(
        database_session_factory,
        next_attempt_at=due_at,
    )
    blocking_publisher = BlockingPublisher()
    competing_publisher = RecordingPublisher()
    first_dispatcher = RetryDispatcher(
        blocking_publisher,
        database_session_factory,
        clock=lambda: due_at,
    )
    second_dispatcher = RetryDispatcher(
        competing_publisher,
        database_session_factory,
        clock=lambda: due_at,
    )

    try:
        first_run = asyncio.create_task(first_dispatcher.run_once())
        await asyncio.wait_for(blocking_publisher.started.wait(), timeout=1)

        second_count = await second_dispatcher.run_once()
        blocking_publisher.release.set()
        first_count = await first_run

        async with database_session_factory() as session:
            job = await JobRepository(session).get(job_id)

        assert first_count == 1
        assert second_count == 0
        assert len(blocking_publisher.messages) == 1
        assert competing_publisher.messages == []
        assert job is not None
        assert job.status is JobStatus.QUEUED
        assert job.next_attempt_at is None
    finally:
        blocking_publisher.release.set()
        await delete_jobs(database_session_factory, {job_id})

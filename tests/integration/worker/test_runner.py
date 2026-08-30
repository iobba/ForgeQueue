import asyncio
from typing import cast
from uuid import UUID, uuid7

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import JobMessage
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService
from forgequeue.jobs.status import JobStatus
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.runner import Worker

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


async def create_persisted_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    numbers: list[int],
) -> UUID:
    async with session_factory.begin() as session:
        service = JobService(JobRepository(session))
        job = await service.create_job(
            job_type="sum_numbers",
            payload={"numbers": numbers},
        )
        return job.id


async def test_two_workers_share_deliveries_from_one_consumer_group(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    stream_name = f"forgequeue:test:workers:{uuid7()}"
    group_name = "forgequeue-test-workers"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name=group_name,
    )
    processor = JobProcessor(broker, database_session_factory)
    first_worker = Worker(broker, processor, worker_id="worker-one")
    second_worker = Worker(broker, processor, worker_id="worker-two")
    job_ids: list[UUID] = []

    try:
        await broker.ensure_consumer_group()
        job_ids = [
            await create_persisted_job(
                database_session_factory,
                numbers=[1, 2],
            ),
            await create_persisted_job(
                database_session_factory,
                numbers=[10, 20],
            ),
        ]
        for job_id in job_ids:
            await broker.publish(JobMessage(job_id=job_id, job_type="sum_numbers"))

        handled = await asyncio.gather(
            first_worker.run_once(block_ms=1_000),
            second_worker.run_once(block_ms=1_000),
        )

        async with database_session_factory() as session:
            jobs = [await JobRepository(session).get(job_id) for job_id in job_ids]

        consumers = cast(
            list[dict[str, object]],
            await redis_client.xinfo_consumers(stream_name, group_name),
        )

        assert handled == [True, True]
        assert all(job is not None for job in jobs)
        assert [job.status for job in jobs if job is not None] == [
            JobStatus.COMPLETED,
            JobStatus.COMPLETED,
        ]
        assert [job.result for job in jobs if job is not None] == [
            {"sum": 3},
            {"sum": 30},
        ]
        assert {consumer["name"] for consumer in consumers} == {
            "worker-one",
            "worker-two",
        }
        assert all(consumer["pending"] == 0 for consumer in consumers)
        assert await broker.list_pending() == []
    finally:
        await redis_client.delete(stream_name)
        if job_ids:
            async with database_session_factory.begin() as session:
                await session.execute(delete(Job).where(Job.id.in_(job_ids)))

import asyncio
from datetime import timedelta
from threading import Event
from typing import cast
from uuid import UUID, uuid7

import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import forgequeue.worker.processor as processor_module
from forgequeue.broker.messages import JobMessage
from forgequeue.broker.redis import RedisDeadLetterBroker, RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus
from forgequeue.jobs.leases import AttemptLeasePolicy
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService
from forgequeue.jobs.status import JobStatus
from forgequeue.jobs.submission import JobSubmissionService
from forgequeue.worker.handlers import JobHandler
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
    dead_letter_stream_name = f"forgequeue:test:workers:dead:{uuid7()}"
    group_name = "forgequeue-test-workers"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name=group_name,
    )
    processor = JobProcessor(
        broker,
        RedisDeadLetterBroker(
            redis_client,
            stream_name=dead_letter_stream_name,
        ),
        database_session_factory,
    )
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
            attempts = [
                await JobAttemptRepository(session).list_for_job(job_id)
                for job_id in job_ids
            ]

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
        assert all(len(job_attempts) == 1 for job_attempts in attempts)
        assert {
            job_attempts[0].worker_id for job_attempts in attempts if job_attempts
        } == {"worker-one", "worker-two"}
        assert all(
            job_attempts[0].status is JobAttemptStatus.SUCCEEDED
            for job_attempts in attempts
            if job_attempts
        )
        assert {consumer["name"] for consumer in consumers} == {
            "worker-one",
            "worker-two",
        }
        assert all(consumer["pending"] == 0 for consumer in consumers)
        assert await broker.list_pending() == []
    finally:
        await redis_client.delete(stream_name, dead_letter_stream_name)
        if job_ids:
            async with database_session_factory.begin() as session:
                await session.execute(delete(Job).where(Job.id.in_(job_ids)))


async def test_shutdown_finishes_in_flight_job_and_skips_new_delivery(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream_name = f"forgequeue:test:shutdown:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:shutdown:dead:{uuid7()}"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    processor = JobProcessor(
        broker,
        RedisDeadLetterBroker(redis_client, stream_name=dead_letter_stream_name),
        database_session_factory,
        lease_policy=AttemptLeasePolicy(
            duration=timedelta(seconds=5),
            heartbeat_interval=timedelta(milliseconds=100),
        ),
    )
    worker = Worker(broker, processor, worker_id="worker-shutdown-test")
    stop_event = asyncio.Event()
    handler_started = asyncio.Event()
    release_handler = Event()
    loop = asyncio.get_running_loop()
    job_ids: list[UUID] = []

    def held_handler(payload: dict[str, object]) -> dict[str, object]:
        loop.call_soon_threadsafe(handler_started.set)
        if not release_handler.wait(timeout=10):
            raise RuntimeError("Test handler was not released")
        return {"sum": sum(cast(list[int], payload["numbers"]))}

    def get_test_handler(job_type: str) -> JobHandler:
        assert job_type == "sum_numbers"
        return held_handler

    monkeypatch.setattr(processor_module, "get_handler", get_test_handler)

    try:
        submission = JobSubmissionService(database_session_factory, broker)
        first_job = await submission.submit(
            job_type="sum_numbers",
            payload={"numbers": [10, 20, 30]},
        )
        second_job = await submission.submit(
            job_type="sum_numbers",
            payload={"numbers": [1, 2]},
        )
        job_ids = [first_job.id, second_job.id]

        worker_task = asyncio.create_task(worker.run_forever(stop_event, block_ms=50))
        try:
            await asyncio.wait_for(handler_started.wait(), timeout=5)
            stop_event.set()

            async with database_session_factory() as session:
                running_job = await JobRepository(session).get(first_job.id)
                running_attempts = await JobAttemptRepository(session).list_for_job(
                    first_job.id
                )
            assert running_job is not None
            assert running_job.status is JobStatus.RUNNING
            assert len(running_attempts) == 1
            assert running_attempts[0].status is JobAttemptStatus.RUNNING
            first_heartbeat = running_attempts[0].heartbeat_at
            assert first_heartbeat is not None
            assert not worker_task.done()
            assert len(await broker.list_pending()) == 1

            async with asyncio.timeout(3):
                while True:
                    async with database_session_factory() as session:
                        attempts = await JobAttemptRepository(session).list_for_job(
                            first_job.id
                        )
                    latest_heartbeat = attempts[0].heartbeat_at
                    if (
                        latest_heartbeat is not None
                        and latest_heartbeat > first_heartbeat
                    ):
                        break
                    await asyncio.sleep(0.02)

            release_handler.set()
            await asyncio.wait_for(worker_task, timeout=5)

            async with database_session_factory() as session:
                completed_job = await JobRepository(session).get(first_job.id)
                queued_job = await JobRepository(session).get(second_job.id)
                completed_attempts = await JobAttemptRepository(session).list_for_job(
                    first_job.id
                )
            assert completed_job is not None
            assert completed_job.status is JobStatus.COMPLETED
            assert completed_job.result == {"sum": 60}
            assert completed_attempts[0].status is JobAttemptStatus.SUCCEEDED
            completed_heartbeat = completed_attempts[0].heartbeat_at
            assert completed_heartbeat is not None
            assert completed_heartbeat > first_heartbeat
            assert queued_job is not None
            assert queued_job.status is JobStatus.QUEUED
            assert queued_job.attempts == 0
            assert await broker.list_pending() == []
        finally:
            stop_event.set()
            release_handler.set()
            if not worker_task.done():
                await asyncio.wait_for(worker_task, timeout=5)
    finally:
        await redis_client.delete(stream_name, dead_letter_stream_name)
        if job_ids:
            async with database_session_factory.begin() as session:
                await session.execute(delete(Job).where(Job.id.in_(job_ids)))


async def test_worker_quarantines_and_acknowledges_malformed_delivery(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    stream_name = f"forgequeue:test:poison:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:poison:dead:{uuid7()}"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    dead_letter_broker = RedisDeadLetterBroker(
        redis_client,
        stream_name=dead_letter_stream_name,
    )
    worker = Worker(
        broker,
        JobProcessor(
            broker,
            dead_letter_broker,
            database_session_factory,
        ),
        worker_id="worker-poison-test",
    )
    invalid_fields: dict[FieldT, EncodableT] = {
        "schema_version": "999",
        "payload": "must-not-be-copied",
    }

    try:
        source_entry_id = cast(
            str,
            await redis_client.xadd(stream_name, invalid_fields),
        )
        await broker.ensure_consumer_group()

        handled = await worker.run_once(block_ms=None)

        dead_letter_entries = cast(
            list[tuple[str, dict[str, str]]],
            await redis_client.xrange(dead_letter_stream_name),
        )
        assert handled is True
        assert await broker.list_pending() == []
        assert len(dead_letter_entries) == 1
        assert dead_letter_entries[0][1] == {
            "schema_version": "1",
            "source_entry_id": source_entry_id,
            "reason": "malformed_message",
            "error_code": "malformed_job_message",
        }
        assert "must-not-be-copied" not in str(dead_letter_entries[0][1])
    finally:
        await redis_client.delete(stream_name, dead_letter_stream_name)


async def test_worker_leaves_malformed_delivery_pending_when_quarantine_fails(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    stream_name = f"forgequeue:test:poison-failure:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:poison-failure:dead:{uuid7()}"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    worker = Worker(
        broker,
        JobProcessor(
            broker,
            RedisDeadLetterBroker(
                redis_client,
                stream_name=dead_letter_stream_name,
            ),
            database_session_factory,
        ),
        worker_id="worker-poison-test",
    )
    invalid_fields: dict[FieldT, EncodableT] = {"schema_version": "999"}

    try:
        source_entry_id = cast(
            str,
            await redis_client.xadd(stream_name, invalid_fields),
        )
        await redis_client.set(dead_letter_stream_name, "not-a-stream")
        await broker.ensure_consumer_group()

        with pytest.raises(ResponseError, match="WRONGTYPE"):
            await worker.run_once(block_ms=None)

        pending_deliveries = await broker.list_pending()
        assert [delivery.entry_id for delivery in pending_deliveries] == [
            source_entry_id
        ]
    finally:
        await redis_client.delete(stream_name, dead_letter_stream_name)

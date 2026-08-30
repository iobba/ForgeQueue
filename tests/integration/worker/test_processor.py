from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import UUID, uuid7

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import forgequeue.worker.processor as processor_module
from forgequeue.broker.messages import JobMessage
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService
from forgequeue.jobs.status import JobStatus
from forgequeue.worker.handlers import JobHandler
from forgequeue.worker.processor import (
    INVALID_JOB_PAYLOAD_ERROR_CODE,
    UNSUPPORTED_JOB_TYPE_ERROR_CODE,
    JobMessageMismatchError,
    JobProcessor,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


@dataclass(slots=True)
class ProcessorTestEnvironment:
    processor: JobProcessor
    broker: RedisJobBroker
    session_factory: async_sessionmaker[AsyncSession]
    stream_name: str
    created_job_ids: set[UUID]


@pytest_asyncio.fixture
async def processor_environment(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[ProcessorTestEnvironment]:
    stream_name = f"forgequeue:test:processor:{uuid7()}"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    environment = ProcessorTestEnvironment(
        processor=JobProcessor(broker, database_session_factory),
        broker=broker,
        session_factory=database_session_factory,
        stream_name=stream_name,
        created_job_ids=set(),
    )

    try:
        yield environment
    finally:
        await redis_client.delete(stream_name)
        if environment.created_job_ids:
            async with database_session_factory.begin() as session:
                await session.execute(
                    delete(Job).where(Job.id.in_(environment.created_job_ids))
                )


async def create_persisted_job(
    environment: ProcessorTestEnvironment,
    *,
    job_type: str = "sum_numbers",
    payload: dict[str, object] | None = None,
) -> UUID:
    async with environment.session_factory.begin() as session:
        service = JobService(JobRepository(session))
        job = await service.create_job(
            job_type=job_type,
            payload=payload if payload is not None else {"numbers": [10, 20, 30]},
        )
        job_id = job.id

    environment.created_job_ids.add(job_id)
    return job_id


async def test_process_completes_job_and_acknowledges_delivery(
    processor_environment: ProcessorTestEnvironment,
) -> None:
    job_id = await create_persisted_job(processor_environment)
    await processor_environment.broker.ensure_consumer_group()
    entry_id = await processor_environment.broker.publish(
        JobMessage(job_id=job_id, job_type="sum_numbers")
    )
    deliveries = await processor_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    await processor_environment.processor.process(deliveries[0])

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.COMPLETED
        assert persisted_job.result == {"sum": 60}
        assert persisted_job.started_at is not None
        assert persisted_job.completed_at is not None

    assert deliveries[0].entry_id == entry_id
    assert await processor_environment.broker.list_pending() == []


async def test_process_rolls_back_mismatch_and_leaves_delivery_pending(
    processor_environment: ProcessorTestEnvironment,
) -> None:
    job_id = await create_persisted_job(processor_environment)
    await processor_environment.broker.ensure_consumer_group()
    await processor_environment.broker.publish(
        JobMessage(job_id=job_id, job_type="generate_report")
    )
    deliveries = await processor_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    with pytest.raises(JobMessageMismatchError) as exc_info:
        await processor_environment.processor.process(deliveries[0])

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.QUEUED
        assert persisted_job.started_at is None
        assert persisted_job.completed_at is None

    pending_deliveries = await processor_environment.broker.list_pending()
    assert [delivery.entry_id for delivery in pending_deliveries] == [
        deliveries[0].entry_id
    ]
    assert exc_info.value.job_id == job_id
    assert exc_info.value.message_job_type == "generate_report"
    assert exc_info.value.database_job_type == "sum_numbers"


async def test_process_persists_unsupported_job_type_and_acknowledges(
    processor_environment: ProcessorTestEnvironment,
) -> None:
    job_id = await create_persisted_job(
        processor_environment,
        job_type="generate_report",
        payload={"customer_id": 42},
    )
    await processor_environment.broker.ensure_consumer_group()
    await processor_environment.broker.publish(
        JobMessage(job_id=job_id, job_type="generate_report")
    )
    deliveries = await processor_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    await processor_environment.processor.process(deliveries[0])

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.FAILED
        assert persisted_job.result is None
        assert persisted_job.error_code == UNSUPPORTED_JOB_TYPE_ERROR_CODE
        assert persisted_job.error_message == (
            "No handler is registered for job type 'generate_report'"
        )
        assert persisted_job.started_at is not None
        assert persisted_job.completed_at is not None

    assert await processor_environment.broker.list_pending() == []


async def test_process_persists_invalid_payload_without_exposing_it(
    processor_environment: ProcessorTestEnvironment,
) -> None:
    job_id = await create_persisted_job(
        processor_environment,
        payload={"numbers": ["sensitive-invalid-value"]},
    )
    await processor_environment.broker.ensure_consumer_group()
    await processor_environment.broker.publish(
        JobMessage(job_id=job_id, job_type="sum_numbers")
    )
    deliveries = await processor_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    await processor_environment.processor.process(deliveries[0])

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.FAILED
        assert persisted_job.result is None
        assert persisted_job.error_code == INVALID_JOB_PAYLOAD_ERROR_CODE
        assert persisted_job.error_message == "Stored job payload failed validation"
        assert "sensitive-invalid-value" not in persisted_job.error_message

    assert await processor_environment.broker.list_pending() == []


async def test_process_leaves_unexpected_handler_failure_pending(
    processor_environment: ProcessorTestEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await create_persisted_job(processor_environment)
    await processor_environment.broker.ensure_consumer_group()
    await processor_environment.broker.publish(
        JobMessage(job_id=job_id, job_type="sum_numbers")
    )
    deliveries = await processor_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    def raise_unexpected_error(payload: dict[str, object]) -> dict[str, object]:
        del payload
        raise RuntimeError("unexpected handler bug")

    def get_failing_handler(job_type: str) -> JobHandler:
        del job_type
        return raise_unexpected_error

    monkeypatch.setattr(processor_module, "get_handler", get_failing_handler)

    with pytest.raises(RuntimeError, match="unexpected handler bug"):
        await processor_environment.processor.process(deliveries[0])

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.RUNNING
        assert persisted_job.completed_at is None
        assert persisted_job.error_code is None

    pending_deliveries = await processor_environment.broker.list_pending()
    assert [delivery.entry_id for delivery in pending_deliveries] == [
        deliveries[0].entry_id
    ]

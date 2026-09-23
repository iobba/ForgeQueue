import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event
from time import sleep
from typing import cast
from uuid import UUID, uuid7

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import forgequeue.worker.processor as processor_module
from forgequeue.broker.messages import JobMessage
from forgequeue.broker.redis import RedisDeadLetterBroker, RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempt_service import (
    AttemptLeaseFinalizationRejectedError,
    AttemptLeaseRenewalRejectedError,
)
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind
from forgequeue.jobs.execution_errors import RetryableJobError
from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.retry_policy import RetryPolicy
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
    redis_client: Redis
    session_factory: async_sessionmaker[AsyncSession]
    stream_name: str
    dead_letter_stream_name: str
    created_job_ids: set[UUID]


@pytest_asyncio.fixture
async def processor_environment(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[ProcessorTestEnvironment]:
    stream_name = f"forgequeue:test:processor:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:processor:dead:{uuid7()}"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    dead_letter_broker = RedisDeadLetterBroker(
        redis_client,
        stream_name=dead_letter_stream_name,
    )
    environment = ProcessorTestEnvironment(
        processor=JobProcessor(
            broker,
            dead_letter_broker,
            database_session_factory,
        ),
        broker=broker,
        redis_client=redis_client,
        session_factory=database_session_factory,
        stream_name=stream_name,
        dead_letter_stream_name=dead_letter_stream_name,
        created_job_ids=set(),
    )

    try:
        yield environment
    finally:
        await redis_client.delete(stream_name, dead_letter_stream_name)
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
    max_attempts: int = 1,
) -> UUID:
    async with environment.session_factory.begin() as session:
        service = JobService(JobRepository(session))
        job = await service.create_job(
            job_type=job_type,
            payload=payload if payload is not None else {"numbers": [10, 20, 30]},
            max_attempts=max_attempts,
        )
        job_id = job.id

    environment.created_job_ids.add(job_id)
    return job_id


async def read_dead_letter_entries(
    environment: ProcessorTestEnvironment,
) -> list[tuple[str, dict[str, str]]]:
    return cast(
        list[tuple[str, dict[str, str]]],
        await environment.redis_client.xrange(environment.dead_letter_stream_name),
    )


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

    await processor_environment.processor.process(
        deliveries[0],
        worker_id="worker-one",
    )

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.COMPLETED
        assert persisted_job.attempts == 1
        assert persisted_job.result == {"sum": 60}
        assert persisted_job.started_at is not None
        assert persisted_job.completed_at is not None
        attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert len(attempts) == 1
        assert attempts[0].attempt_number == 1
        assert attempts[0].worker_id == "worker-one"
        assert attempts[0].status is JobAttemptStatus.SUCCEEDED
        assert attempts[0].completed_at is not None
        assert attempts[0].heartbeat_at == attempts[0].started_at
        assert attempts[0].lease_expires_at is not None
        assert attempts[0].heartbeat_at is not None
        assert attempts[0].lease_expires_at - attempts[0].heartbeat_at == (
            timedelta(seconds=60)
        )

    assert deliveries[0].entry_id == entry_id
    assert await processor_environment.broker.list_pending() == []
    assert await read_dead_letter_entries(processor_environment) == []


async def test_process_renews_lease_while_handler_is_running(
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

    def slow_handler(payload: dict[str, object]) -> dict[str, object]:
        del payload
        sleep(0.08)
        return {"sum": 60}

    def get_slow_handler(job_type: str) -> JobHandler:
        del job_type
        return slow_handler

    monkeypatch.setattr(processor_module, "get_handler", get_slow_handler)
    lease_policy = AttemptLeasePolicy(
        duration=timedelta(milliseconds=500),
        heartbeat_interval=timedelta(milliseconds=10),
    )
    processor = JobProcessor(
        processor_environment.broker,
        RedisDeadLetterBroker(
            processor_environment.redis_client,
            stream_name=processor_environment.dead_letter_stream_name,
        ),
        processor_environment.session_factory,
        lease_policy=lease_policy,
    )

    await processor.process(deliveries[0], worker_id="worker-one")

    async with processor_environment.session_factory() as session:
        attempts = await JobAttemptRepository(session).list_for_job(job_id)

    assert len(attempts) == 1
    assert attempts[0].status is JobAttemptStatus.SUCCEEDED
    assert attempts[0].heartbeat_at is not None
    assert attempts[0].heartbeat_at > attempts[0].started_at
    assert attempts[0].lease_expires_at == (
        attempts[0].heartbeat_at + lease_policy.duration
    )
    assert await processor_environment.broker.list_pending() == []


async def test_process_leaves_delivery_pending_when_lease_renewal_is_rejected(
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
    handler_completed = Event()

    def slow_handler(payload: dict[str, object]) -> dict[str, object]:
        del payload
        try:
            sleep(0.05)
            return {"sum": 60}
        finally:
            handler_completed.set()

    async def reject_renewal(
        repository: JobAttemptRepository,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        renewed_lease: AttemptLease,
    ) -> None:
        del (
            repository,
            attempt_id,
            worker_id,
            current_lease,
            renewed_lease,
        )
        return None

    def get_slow_handler(job_type: str) -> JobHandler:
        del job_type
        return slow_handler

    monkeypatch.setattr(processor_module, "get_handler", get_slow_handler)
    monkeypatch.setattr(JobAttemptRepository, "renew_lease", reject_renewal)
    processor = JobProcessor(
        processor_environment.broker,
        RedisDeadLetterBroker(
            processor_environment.redis_client,
            stream_name=processor_environment.dead_letter_stream_name,
        ),
        processor_environment.session_factory,
        lease_policy=AttemptLeasePolicy(
            duration=timedelta(milliseconds=500),
            heartbeat_interval=timedelta(milliseconds=10),
        ),
    )

    with pytest.raises(AttemptLeaseRenewalRejectedError):
        await processor.process(deliveries[0], worker_id="worker-one")

    assert await asyncio.to_thread(handler_completed.wait, 1)
    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        attempts = await JobAttemptRepository(session).list_for_job(job_id)

    assert persisted_job is not None
    assert persisted_job.status is JobStatus.RUNNING
    assert len(attempts) == 1
    assert attempts[0].status is JobAttemptStatus.RUNNING
    assert [
        pending.entry_id
        for pending in await processor_environment.broker.list_pending()
    ] == [deliveries[0].entry_id]


async def test_process_rejects_success_after_lease_ownership_is_lost(
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

    async def reject_success(
        repository: JobAttemptRepository,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        completed_at: datetime,
    ) -> None:
        del repository, attempt_id, worker_id, current_lease, completed_at
        return None

    monkeypatch.setattr(JobAttemptRepository, "succeed_if_owned", reject_success)

    with pytest.raises(AttemptLeaseFinalizationRejectedError):
        await processor_environment.processor.process(
            deliveries[0],
            worker_id="worker-one",
        )

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        attempts = await JobAttemptRepository(session).list_for_job(job_id)

    assert persisted_job is not None
    assert persisted_job.status is JobStatus.RUNNING
    assert len(attempts) == 1
    assert attempts[0].status is JobAttemptStatus.RUNNING
    assert len(await processor_environment.broker.list_pending()) == 1


async def test_process_rejects_failure_after_lease_ownership_is_lost(
    processor_environment: ProcessorTestEnvironment,
    monkeypatch: pytest.MonkeyPatch,
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

    async def reject_failure(
        repository: JobAttemptRepository,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        failure_kind: JobFailureKind,
        error_code: str,
        error_message: str,
        completed_at: datetime,
    ) -> None:
        del (
            repository,
            attempt_id,
            worker_id,
            current_lease,
            failure_kind,
            error_code,
            error_message,
            completed_at,
        )
        return None

    monkeypatch.setattr(JobAttemptRepository, "fail_if_owned", reject_failure)

    with pytest.raises(AttemptLeaseFinalizationRejectedError):
        await processor_environment.processor.process(
            deliveries[0],
            worker_id="worker-one",
        )

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        attempts = await JobAttemptRepository(session).list_for_job(job_id)

    assert persisted_job is not None
    assert persisted_job.status is JobStatus.RUNNING
    assert len(attempts) == 1
    assert attempts[0].status is JobAttemptStatus.RUNNING
    assert len(await processor_environment.broker.list_pending()) == 1
    assert await read_dead_letter_entries(processor_environment) == []


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
        await processor_environment.processor.process(
            deliveries[0],
            worker_id="worker-one",
        )

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.QUEUED
        assert persisted_job.attempts == 0
        assert persisted_job.started_at is None
        assert persisted_job.completed_at is None
        assert await JobAttemptRepository(session).list_for_job(job_id) == []

    pending_deliveries = await processor_environment.broker.list_pending()
    assert [delivery.entry_id for delivery in pending_deliveries] == [
        deliveries[0].entry_id
    ]
    assert exc_info.value.job_id == job_id
    assert exc_info.value.message_job_type == "generate_report"
    assert exc_info.value.database_job_type == "sum_numbers"
    assert await read_dead_letter_entries(processor_environment) == []


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

    await processor_environment.processor.process(
        deliveries[0],
        worker_id="worker-one",
    )

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.FAILED
        assert persisted_job.attempts == 1
        assert persisted_job.result is None
        assert persisted_job.error_code == UNSUPPORTED_JOB_TYPE_ERROR_CODE
        assert persisted_job.error_message == (
            "No handler is registered for job type 'generate_report'"
        )
        assert persisted_job.started_at is not None
        assert persisted_job.completed_at is not None
        attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert len(attempts) == 1
        assert attempts[0].status is JobAttemptStatus.FAILED
        assert attempts[0].failure_kind is JobFailureKind.PERMANENT
        assert attempts[0].error_code == UNSUPPORTED_JOB_TYPE_ERROR_CODE
        assert attempts[0].error_message == persisted_job.error_message
        assert attempts[0].completed_at is not None

    assert await processor_environment.broker.list_pending() == []

    dead_letter_entries = await read_dead_letter_entries(processor_environment)
    assert len(dead_letter_entries) == 1
    assert dead_letter_entries[0][1] == {
        "schema_version": "1",
        "source_entry_id": deliveries[0].entry_id,
        "job_id": str(job_id),
        "job_type": "generate_report",
        "attempt_number": "1",
        "reason": "permanent_failure",
        "failure_kind": "permanent",
        "error_code": UNSUPPORTED_JOB_TYPE_ERROR_CODE,
    }


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

    await processor_environment.processor.process(
        deliveries[0],
        worker_id="worker-one",
    )

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.FAILED
        assert persisted_job.attempts == 1
        assert persisted_job.result is None
        assert persisted_job.error_code == INVALID_JOB_PAYLOAD_ERROR_CODE
        assert persisted_job.error_message == "Stored job payload failed validation"
        assert "sensitive-invalid-value" not in persisted_job.error_message
        attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert len(attempts) == 1
        assert attempts[0].status is JobAttemptStatus.FAILED
        assert attempts[0].failure_kind is JobFailureKind.PERMANENT
        assert attempts[0].error_code == INVALID_JOB_PAYLOAD_ERROR_CODE
        assert attempts[0].error_message is not None
        assert "sensitive-invalid-value" not in attempts[0].error_message

    assert await processor_environment.broker.list_pending() == []

    dead_letter_entries = await read_dead_letter_entries(processor_environment)
    assert len(dead_letter_entries) == 1
    assert dead_letter_entries[0][1]["error_code"] == INVALID_JOB_PAYLOAD_ERROR_CODE
    assert "sensitive-invalid-value" not in str(dead_letter_entries[0][1])


async def test_process_rolls_back_terminal_failure_when_dead_letter_publish_fails(
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
    await processor_environment.redis_client.set(
        processor_environment.dead_letter_stream_name,
        "not-a-stream",
    )

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await processor_environment.processor.process(
            deliveries[0],
            worker_id="worker-one",
        )

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.RUNNING
        assert persisted_job.error_code is None
        assert persisted_job.completed_at is None
        attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert len(attempts) == 1
        assert attempts[0].status is JobAttemptStatus.RUNNING
        assert attempts[0].failure_kind is None
        assert attempts[0].error_code is None

    pending_deliveries = await processor_environment.broker.list_pending()
    assert [delivery.entry_id for delivery in pending_deliveries] == [
        deliveries[0].entry_id
    ]


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
        await processor_environment.processor.process(
            deliveries[0],
            worker_id="worker-one",
        )

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.RUNNING
        assert persisted_job.attempts == 1
        assert persisted_job.completed_at is None
        assert persisted_job.error_code is None
        attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert len(attempts) == 1
        assert attempts[0].status is JobAttemptStatus.RUNNING
        assert attempts[0].worker_id == "worker-one"
        assert attempts[0].completed_at is None
        assert attempts[0].failure_kind is None

    pending_deliveries = await processor_environment.broker.list_pending()
    assert [delivery.entry_id for delivery in pending_deliveries] == [
        deliveries[0].entry_id
    ]


async def test_process_schedules_retryable_failure_with_backoff_and_acknowledges(
    processor_environment: ProcessorTestEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduled_at = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    job_id = await create_persisted_job(
        processor_environment,
        max_attempts=3,
    )
    await processor_environment.broker.ensure_consumer_group()
    await processor_environment.broker.publish(
        JobMessage(job_id=job_id, job_type="sum_numbers")
    )
    deliveries = await processor_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    def raise_retryable_error(payload: dict[str, object]) -> dict[str, object]:
        del payload
        raise RetryableJobError(
            error_code="dependency_unavailable",
            safe_message="A dependency is temporarily unavailable",
        )

    def get_failing_handler(job_type: str) -> JobHandler:
        del job_type
        return raise_retryable_error

    monkeypatch.setattr(processor_module, "get_handler", get_failing_handler)
    processor = JobProcessor(
        processor_environment.broker,
        RedisDeadLetterBroker(
            processor_environment.redis_client,
            stream_name=processor_environment.dead_letter_stream_name,
        ),
        processor_environment.session_factory,
        retry_policy=RetryPolicy(
            base_delay_seconds=10,
            max_delay_seconds=60,
        ),
        jitter_source=lambda: 0.5,
        clock=lambda: scheduled_at,
    )

    await processor.process(deliveries[0], worker_id="worker-one")

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.RETRY_SCHEDULED
        assert persisted_job.attempts == 1
        assert persisted_job.result is None
        assert persisted_job.error_code == "dependency_unavailable"
        assert persisted_job.error_message == (
            "A dependency is temporarily unavailable"
        )
        assert persisted_job.completed_at is None
        assert persisted_job.next_attempt_at == scheduled_at + timedelta(seconds=7.5)
        attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert len(attempts) == 1
        assert attempts[0].status is JobAttemptStatus.FAILED
        assert attempts[0].failure_kind is JobFailureKind.RETRYABLE
        assert attempts[0].error_code == "dependency_unavailable"
        assert attempts[0].completed_at is not None
        assert attempts[0].heartbeat_at == scheduled_at
        assert attempts[0].lease_expires_at == scheduled_at + timedelta(seconds=60)

    assert await processor_environment.broker.list_pending() == []
    assert await read_dead_letter_entries(processor_environment) == []


async def test_process_makes_exhausted_retryable_failure_terminal(
    processor_environment: ProcessorTestEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await create_persisted_job(
        processor_environment,
        max_attempts=1,
    )
    await processor_environment.broker.ensure_consumer_group()
    await processor_environment.broker.publish(
        JobMessage(job_id=job_id, job_type="sum_numbers")
    )
    deliveries = await processor_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    def raise_retryable_error(payload: dict[str, object]) -> dict[str, object]:
        del payload
        raise RetryableJobError(
            error_code="dependency_unavailable",
            safe_message="A dependency is temporarily unavailable",
        )

    def get_failing_handler(job_type: str) -> JobHandler:
        del job_type
        return raise_retryable_error

    def reject_jitter_call() -> float:
        raise AssertionError(
            "Jitter must not be generated after attempts are exhausted"
        )

    monkeypatch.setattr(processor_module, "get_handler", get_failing_handler)
    processor = JobProcessor(
        processor_environment.broker,
        RedisDeadLetterBroker(
            processor_environment.redis_client,
            stream_name=processor_environment.dead_letter_stream_name,
        ),
        processor_environment.session_factory,
        jitter_source=reject_jitter_call,
    )

    await processor.process(deliveries[0], worker_id="worker-one")

    async with processor_environment.session_factory() as session:
        persisted_job = await JobRepository(session).get(job_id)
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.FAILED
        assert persisted_job.attempts == 1
        assert persisted_job.error_code == "dependency_unavailable"
        assert persisted_job.completed_at is not None
        assert persisted_job.next_attempt_at is None
        attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert len(attempts) == 1
        assert attempts[0].status is JobAttemptStatus.FAILED
        assert attempts[0].failure_kind is JobFailureKind.RETRYABLE

    assert await processor_environment.broker.list_pending() == []

    dead_letter_entries = await read_dead_letter_entries(processor_environment)
    assert len(dead_letter_entries) == 1
    assert dead_letter_entries[0][1] == {
        "schema_version": "1",
        "source_entry_id": deliveries[0].entry_id,
        "job_id": str(job_id),
        "job_type": "sum_numbers",
        "attempt_number": "1",
        "reason": "retries_exhausted",
        "failure_kind": "retryable",
        "error_code": "dependency_unavailable",
    }

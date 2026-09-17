from datetime import UTC, datetime
from typing import cast
from uuid import uuid7

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import forgequeue.worker.processor as processor_module
from forgequeue.broker.redis import RedisDeadLetterBroker, RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind
from forgequeue.jobs.execution_errors import RetryableJobError
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.retry_policy import RetryPolicy
from forgequeue.jobs.status import JobStatus
from forgequeue.jobs.submission import JobSubmissionService
from forgequeue.scheduler.retry_dispatcher import RetryDispatcher
from forgequeue.worker.handlers import JobHandler
from forgequeue.worker.processor import JobProcessor

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


async def test_retryable_job_is_dispatched_again_and_succeeds(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduled_at = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    stream_name = f"forgequeue:test:retry-flow:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:retry-flow:dead:{uuid7()}"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    processor = JobProcessor(
        broker,
        RedisDeadLetterBroker(
            redis_client,
            stream_name=dead_letter_stream_name,
        ),
        database_session_factory,
        retry_policy=RetryPolicy(
            base_delay_seconds=5,
            max_delay_seconds=60,
        ),
        jitter_source=lambda: 1.0,
        clock=lambda: scheduled_at,
    )
    dispatcher = RetryDispatcher(
        broker,
        database_session_factory,
        clock=lambda: scheduled_at.replace(second=5),
    )
    handler_calls = 0
    job_id = None

    def fail_once_then_succeed(payload: dict[str, object]) -> dict[str, object]:
        nonlocal handler_calls
        handler_calls += 1
        if handler_calls == 1:
            raise RetryableJobError(
                error_code="dependency_unavailable",
                safe_message="A dependency is temporarily unavailable",
            )
        return {"sum": sum(cast(list[int], payload["numbers"]))}

    def get_test_handler(job_type: str) -> JobHandler:
        del job_type
        return fail_once_then_succeed

    monkeypatch.setattr(processor_module, "get_handler", get_test_handler)

    try:
        await broker.ensure_consumer_group()
        job = await JobSubmissionService(
            database_session_factory,
            broker,
        ).submit(
            job_type="sum_numbers",
            payload={"numbers": [10, 20, 30]},
            max_attempts=3,
        )
        job_id = job.id

        first_deliveries = await broker.read(
            consumer_name="worker-one",
            block_ms=None,
        )
        await processor.process(first_deliveries[0], worker_id="worker-one")

        dispatched_count = await dispatcher.run_once()
        second_deliveries = await broker.read(
            consumer_name="worker-one",
            block_ms=None,
        )
        await processor.process(second_deliveries[0], worker_id="worker-one")

        async with database_session_factory() as session:
            persisted_job = await JobRepository(session).get(job_id)
            attempts = await JobAttemptRepository(session).list_for_job(job_id)

        assert dispatched_count == 1
        assert handler_calls == 2
        assert persisted_job is not None
        assert persisted_job.status is JobStatus.COMPLETED
        assert persisted_job.attempts == 2
        assert persisted_job.result == {"sum": 60}
        assert persisted_job.error_code is None
        assert persisted_job.error_message is None
        assert persisted_job.next_attempt_at is None
        assert [attempt.status for attempt in attempts] == [
            JobAttemptStatus.FAILED,
            JobAttemptStatus.SUCCEEDED,
        ]
        assert attempts[0].failure_kind is JobFailureKind.RETRYABLE
        assert attempts[1].failure_kind is None
        assert await broker.list_pending() == []
    finally:
        await redis_client.delete(stream_name, dead_letter_stream_name)
        if job_id is not None:
            async with database_session_factory.begin() as session:
                await session.execute(delete(Job).where(Job.id == job_id))

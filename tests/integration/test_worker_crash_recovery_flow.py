from datetime import UTC, datetime, timedelta
from typing import NoReturn
from uuid import UUID, uuid7

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.redis import RedisDeadLetterBroker, RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus
from forgequeue.jobs.submission import JobSubmissionService
from forgequeue.scheduler.retry_dispatcher import RetryDispatcher
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.recovery import (
    ReclaimedDeliveryAction,
    ReclaimedDeliveryCoordinator,
    ReclaimedDeliveryReason,
)
from forgequeue.worker.runner import Worker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_crashed_worker_attempt_is_reclaimed_retried_and_completed(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream_name = f"forgequeue:test:crash-recovery:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:crash-recovery:dead:{uuid7()}"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    dead_letter_broker = RedisDeadLetterBroker(
        redis_client,
        stream_name=dead_letter_stream_name,
    )
    crashed_at = datetime.now(UTC) - timedelta(minutes=2)
    crashing_processor = JobProcessor(
        broker,
        dead_letter_broker,
        database_session_factory,
        clock=lambda: crashed_at,
    )
    recovery_processor = JobProcessor(
        broker,
        dead_letter_broker,
        database_session_factory,
    )
    first_worker = Worker(broker, crashing_processor, worker_id="worker-one")
    second_worker = Worker(broker, recovery_processor, worker_id="worker-two")
    job_id: UUID | None = None

    async def interrupt_after_attempt_started(
        *_args: object, **_kwargs: object
    ) -> NoReturn:
        raise RuntimeError("simulated worker interruption")

    monkeypatch.setattr(
        crashing_processor,
        "_run_handler_with_heartbeats",
        interrupt_after_attempt_started,
    )

    try:
        await broker.ensure_consumer_group()
        job = await JobSubmissionService(database_session_factory, broker).submit(
            job_type="sum_numbers",
            payload={"numbers": [10, 20, 30]},
            max_attempts=2,
        )
        job_id = job.id

        with pytest.raises(RuntimeError, match="simulated worker interruption"):
            await first_worker.run_once(block_ms=None)

        pending = await broker.list_pending()
        async with database_session_factory() as session:
            running_job = await JobRepository(session).get(job_id)
            running_attempts = await JobAttemptRepository(session).list_for_job(job_id)

        assert running_job is not None
        assert running_job.status is JobStatus.RUNNING
        assert len(running_attempts) == 1
        assert running_attempts[0].status is JobAttemptStatus.RUNNING
        assert running_attempts[0].worker_id == "worker-one"
        assert len(pending) == 1
        assert pending[0].consumer_name == "worker-one"
        original_entry_id = pending[0].entry_id

        recovery = ReclaimedDeliveryCoordinator(
            broker,
            recovery_processor,
            database_session_factory,
        )
        recovery_batch = await recovery.recover_batch(
            worker_id="worker-two",
            min_idle_ms=0,
            start_id="0-0",
            count=1,
        )

        async with database_session_factory() as session:
            scheduled_job = await JobRepository(session).get(job_id)
            failed_attempts = await JobAttemptRepository(session).list_for_job(job_id)

        assert len(recovery_batch.outcomes) == 1
        assert recovery_batch.outcomes[0].entry_id == original_entry_id
        assert (
            recovery_batch.outcomes[0].decision.action
            is ReclaimedDeliveryAction.RECOVER
        )
        assert (
            recovery_batch.outcomes[0].decision.reason
            is ReclaimedDeliveryReason.JOB_LEASE_EXPIRED
        )
        assert scheduled_job is not None
        assert scheduled_job.status is JobStatus.RETRY_SCHEDULED
        next_attempt_at = scheduled_job.next_attempt_at
        assert next_attempt_at is not None
        assert len(failed_attempts) == 1
        assert failed_attempts[0].status is JobAttemptStatus.FAILED
        assert failed_attempts[0].failure_kind is JobFailureKind.RETRYABLE
        assert failed_attempts[0].error_code == "worker_lease_expired"
        assert await broker.list_pending() == []

        dispatcher = RetryDispatcher(
            broker,
            database_session_factory,
            clock=lambda: next_attempt_at,
        )
        assert await dispatcher.run_once() == 1
        assert await second_worker.run_once(block_ms=None) is True

        async with database_session_factory() as session:
            completed_job = await JobRepository(session).get(job_id)
            attempts = await JobAttemptRepository(session).list_for_job(job_id)

        assert completed_job is not None
        assert completed_job.status is JobStatus.COMPLETED
        assert completed_job.attempts == 2
        assert completed_job.result == {"sum": 60}
        assert [attempt.status for attempt in attempts] == [
            JobAttemptStatus.FAILED,
            JobAttemptStatus.SUCCEEDED,
        ]
        assert [attempt.worker_id for attempt in attempts] == [
            "worker-one",
            "worker-two",
        ]
        assert await broker.list_pending() == []
        assert await redis_client.xlen(stream_name) == 2
        assert await redis_client.xlen(dead_letter_stream_name) == 0
    finally:
        await redis_client.delete(stream_name, dead_letter_stream_name)
        if job_id is not None:
            async with database_session_factory.begin() as session:
                await session.execute(delete(Job).where(Job.id == job_id))

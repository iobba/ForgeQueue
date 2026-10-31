import asyncio
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid7

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from forgequeue.broker.redis import RedisDeadLetterBroker, RedisJobBroker
from forgequeue.core.config import get_settings
from forgequeue.db.models import Job, JobAttempt
from forgequeue.db.session import build_database_url, create_session_factory
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind
from forgequeue.jobs.leases import AttemptLeasePolicy
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus
from forgequeue.jobs.submission import JobSubmissionService
from forgequeue.scheduler.retry_dispatcher import RetryDispatcher
from forgequeue.worker.executor import HandlerSubprocessExecutor
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.recovery import (
    ReclaimedDeliveryAction,
    ReclaimedDeliveryCoordinator,
    ReclaimedDeliveryReason,
)
from forgequeue.worker.runner import Worker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_interrupted_postgres_heartbeat_stops_child_and_job_recovers(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    settings = get_settings()
    if settings.postgres_db != "forgequeue_test":
        raise RuntimeError("Fault injection must use the integration test database")

    stream_name = f"forgequeue:test:postgres-interruption:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:postgres-interruption:dead:{uuid7()}"
    application_name = f"fq-heartbeat-{uuid7()}"
    # Only this worker's connection is eligible for termination. Test control
    # queries use the separate fixture pool and never interrupt other clients.
    worker_engine = create_async_engine(
        build_database_url(settings),
        connect_args={"application_name": application_name},
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    worker_sessions = create_session_factory(worker_engine)
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name="forgequeue-test-workers",
    )
    dead_letter_broker = RedisDeadLetterBroker(
        redis_client, stream_name=dead_letter_stream_name
    )
    pid_file = tmp_path / "held-handler.pid"
    child_script = (
        "import os, sys, time\n"
        "from pathlib import Path\n"
        "sys.stdin.buffer.read()\n"
        "Path(sys.argv[1]).write_text(str(os.getpid()))\n"
        "time.sleep(30)\n"
    )
    observed_at = datetime.now(UTC) - timedelta(minutes=2)
    processor = JobProcessor(
        broker,
        dead_letter_broker,
        worker_sessions,
        lease_policy=AttemptLeasePolicy(
            duration=timedelta(seconds=60),
            heartbeat_interval=timedelta(milliseconds=50),
        ),
        executor=HandlerSubprocessExecutor(
            command=(sys.executable, "-c", child_script, str(pid_file)),
            termination_grace_seconds=0.1,
        ),
        handler_timeout_seconds=20,
        clock=lambda: observed_at,
    )
    first_worker = Worker(broker, processor, worker_id="worker-disconnected")
    worker_task: asyncio.Task[bool] | None = None
    job_id: UUID | None = None

    try:
        await broker.ensure_consumer_group()
        job = await JobSubmissionService(database_session_factory, broker).submit(
            job_type="sum_numbers",
            payload={"numbers": [10, 20, 30]},
            max_attempts=2,
        )
        job_id = job.id
        worker_task = asyncio.create_task(first_worker.run_once(block_ms=None))

        async with asyncio.timeout(10):
            while True:
                if worker_task.done():
                    await worker_task
                    pytest.fail("Worker finished before database fault injection")
                if pid_file.exists():
                    pid_text = await asyncio.to_thread(pid_file.read_text)
                    if pid_text:
                        child_pid = int(pid_text)
                        break
                await asyncio.sleep(0.01)

            # Holding the row lock makes the real heartbeat UPDATE wait inside
            # PostgreSQL, so termination cannot race with an idle connection.
            async with database_session_factory.begin() as blocker:
                attempt = await blocker.scalar(
                    select(JobAttempt)
                    .where(JobAttempt.job_id == job_id)
                    .with_for_update()
                )
                assert attempt is not None
                assert attempt.status is JobAttemptStatus.RUNNING
                assert attempt.heartbeat_at is not None
                assert attempt.lease_expires_at is not None
                attempt_id = attempt.id
                original_heartbeat = attempt.heartbeat_at
                original_expiry = attempt.lease_expires_at
                observed_at = original_heartbeat + timedelta(seconds=1)
                blocking_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
                assert isinstance(blocking_pid, int)

                while True:
                    if worker_task.done():
                        await worker_task
                        pytest.fail("Worker finished before its heartbeat was blocked")
                    # A fresh transaction on each poll avoids a cached activity
                    # snapshot. The unique application name and blocker identify
                    # only this test's in-flight heartbeat connection.
                    async with database_session_factory() as control:
                        terminated = await control.scalar(
                            text(
                                "SELECT pg_terminate_backend(pid) "
                                "FROM pg_stat_activity "
                                "WHERE datname = current_database() "
                                "AND usename = current_user "
                                "AND application_name = :application_name "
                                "AND state = 'active' "
                                "AND wait_event_type = 'Lock' "
                                "AND :blocking_pid = ANY(pg_blocking_pids(pid))"
                            ),
                            {
                                "application_name": application_name,
                                "blocking_pid": blocking_pid,
                            },
                        )
                    if terminated is not None:
                        assert terminated is True
                        break
                    await asyncio.sleep(0.01)

                with pytest.raises(DBAPIError) as interrupted:
                    await worker_task
                assert interrupted.value.connection_invalidated is True

        # Failure to renew the lease must cancel execution and reap the real child.
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
        async with database_session_factory() as session:
            abandoned_job = await JobRepository(session).get(job_id)
            abandoned_attempt = await JobAttemptRepository(session).get(attempt_id)
        assert abandoned_job is not None
        assert abandoned_job.status is JobStatus.RUNNING
        assert abandoned_job.attempts == 1
        assert abandoned_job.result is None
        assert abandoned_job.completed_at is None
        assert abandoned_attempt is not None
        assert abandoned_attempt.status is JobAttemptStatus.RUNNING
        assert abandoned_attempt.heartbeat_at == original_heartbeat
        assert abandoned_attempt.lease_expires_at == original_expiry
        assert abandoned_attempt.completed_at is None
        pending = await broker.list_pending()
        assert len(pending) == 1
        assert pending[0].consumer_name == "worker-disconnected"
        original_entry_id = pending[0].entry_id
        assert await redis_client.xlen(dead_letter_stream_name) == 0

        # Reuse the same pool: SQLAlchemy must replace the invalidated connection.
        async with worker_sessions() as session:
            assert await session.scalar(text("SELECT 1")) == 1
        recovery_processor = JobProcessor(
            broker,
            dead_letter_broker,
            worker_sessions,
            clock=lambda: observed_at,
            jitter_source=lambda: 0.5,
            handler_timeout_seconds=5,
        )
        recovery = ReclaimedDeliveryCoordinator(
            broker,
            recovery_processor,
            worker_sessions,
            clock=lambda: observed_at,
        )
        observed_at = original_expiry
        recovered = await recovery.recover_batch(
            worker_id="worker-two", min_idle_ms=0, count=1
        )
        assert len(recovered.outcomes) == 1
        assert recovered.outcomes[0].entry_id == original_entry_id
        assert recovered.outcomes[0].decision.action is ReclaimedDeliveryAction.RECOVER
        assert (
            recovered.outcomes[0].decision.reason
            is ReclaimedDeliveryReason.JOB_LEASE_EXPIRED
        )
        async with database_session_factory() as session:
            scheduled_job = await JobRepository(session).get(job_id)
            failed_attempt = await JobAttemptRepository(session).get(attempt_id)
        assert scheduled_job is not None
        assert scheduled_job.status is JobStatus.RETRY_SCHEDULED
        next_attempt_at = scheduled_job.next_attempt_at
        assert next_attempt_at is not None
        assert failed_attempt is not None
        assert failed_attempt.status is JobAttemptStatus.FAILED
        assert failed_attempt.failure_kind is JobFailureKind.RETRYABLE
        assert failed_attempt.error_code == "worker_lease_expired"
        assert await broker.list_pending() == []

        observed_at = next_attempt_at
        dispatcher = RetryDispatcher(broker, worker_sessions, clock=lambda: observed_at)
        second_worker = Worker(broker, recovery_processor, worker_id="worker-two")
        assert await dispatcher.run_once() == 1
        assert await second_worker.run_once(block_ms=None) is True

        async with database_session_factory() as session:
            completed_job = await JobRepository(session).get(job_id)
            attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert completed_job is not None
        assert completed_job.status is JobStatus.COMPLETED
        assert completed_job.attempts == 2
        assert completed_job.result == {"sum": 60}
        assert [attempt.attempt_number for attempt in attempts] == [1, 2]
        assert [attempt.status for attempt in attempts] == [
            JobAttemptStatus.FAILED,
            JobAttemptStatus.SUCCEEDED,
        ]
        assert [attempt.worker_id for attempt in attempts] == [
            "worker-disconnected",
            "worker-two",
        ]
        assert attempts[0].id == attempt_id
        assert attempts[0].error_code == "worker_lease_expired"
        assert await broker.list_pending() == []
        assert await redis_client.xlen(stream_name) == 2
        assert await redis_client.xlen(dead_letter_stream_name) == 0
    finally:
        if worker_task is not None:
            if not worker_task.done():
                worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
        await worker_engine.dispose()
        await redis_client.delete(stream_name, dead_letter_stream_name)
        if job_id is not None:
            async with database_session_factory.begin() as session:
                await session.execute(delete(Job).where(Job.id == job_id))

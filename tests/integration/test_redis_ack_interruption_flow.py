import asyncio
from uuid import UUID, uuid7

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.redis import RedisDeadLetterBroker, RedisJobBroker
from forgequeue.core.config import get_settings
from forgequeue.db.models import Job
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus
from forgequeue.jobs.submission import JobSubmissionService
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.recovery import (
    ReclaimedDeliveryAction,
    ReclaimedDeliveryCoordinator,
    ReclaimedDeliveryDecision,
    ReclaimedDeliveryOutcome,
    ReclaimedDeliveryReason,
)
from forgequeue.worker.runner import Worker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_disconnected_redis_ack_recovers_without_reexecuting_completed_job(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    stream_name = f"forgequeue:test:ack-interruption:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:ack-interruption:dead:{uuid7()}"
    group_name = "forgequeue-test-workers"
    control_broker = RedisJobBroker(
        redis_client, stream_name=stream_name, group_name=group_name
    )
    worker_client = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
        client_name=f"forgequeue-test-ack-{uuid7()}",
        single_connection_client=True,
        socket_connect_timeout=2,
        socket_timeout=2,
        # Surface the failed XACK to ForgeQueue instead of letting redis-py
        # reconnect and retry it transparently. This policy is test-only.
        retry=Retry(NoBackoff(), 0),
    )
    worker_broker = RedisJobBroker(
        worker_client, stream_name=stream_name, group_name=group_name
    )
    processor = JobProcessor(
        worker_broker,
        RedisDeadLetterBroker(worker_client, stream_name=dead_letter_stream_name),
        database_session_factory,
        handler_timeout_seconds=5,
    )
    first_worker = Worker(worker_broker, processor, worker_id="worker-one")
    job_id: UUID | None = None

    try:
        await control_broker.ensure_consumer_group()
        job = await JobSubmissionService(
            database_session_factory, control_broker
        ).submit(
            job_type="sum_numbers",
            payload={"numbers": [10, 20, 30]},
            max_attempts=2,
        )
        job_id = job.id
        # redis-py leaves **kwargs untyped on these connection commands; their
        # return types are known, so ignore only the incomplete member signature.
        worker_client_id = await worker_client.client_id()  # pyright: ignore[reportUnknownMemberType]
        real_acknowledge = worker_broker.acknowledge
        attempted_entry_ids: list[str] = []

        async def disconnect_before_ack(entry_id: str) -> int:
            attempted_entry_ids.append(entry_id)
            # Close exactly the dedicated worker connection, then run the real
            # acknowledgement method on that broken socket. No fake exception.
            killed_count = await redis_client.client_kill_filter(  # pyright: ignore[reportUnknownMemberType]
                _id=str(worker_client_id)
            )
            assert killed_count == 1
            return await real_acknowledge(entry_id)

        monkeypatch.setattr(worker_broker, "acknowledge", disconnect_before_ack)
        async with asyncio.timeout(10):
            with pytest.raises(RedisConnectionError):
                await first_worker.run_once(block_ms=None)

        async with database_session_factory() as session:
            completed_job = await JobRepository(session).get(job_id)
            attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert completed_job is not None
        assert completed_job.status is JobStatus.COMPLETED
        assert completed_job.result == {"sum": 60}
        assert completed_job.attempts == 1
        assert completed_job.next_attempt_at is None
        assert completed_job.error_code is None
        assert completed_job.completed_at is not None
        assert len(attempts) == 1
        successful_attempt = attempts[0]
        assert successful_attempt.status is JobAttemptStatus.SUCCEEDED
        assert successful_attempt.worker_id == "worker-one"
        assert successful_attempt.completed_at is not None
        pending = await control_broker.list_pending()
        assert len(pending) == 1
        assert pending[0].consumer_name == "worker-one"
        original_entry_id = pending[0].entry_id
        assert attempted_entry_ids == [original_entry_id]
        assert await redis_client.xlen(dead_letter_stream_name) == 0

        # remove the injection and reconnect the same client for normal recovery.
        monkeypatch.setattr(worker_broker, "acknowledge", real_acknowledge)
        # ping() opens a new connection with a new Redis client ID.
        assert await worker_client.ping() is True  # pyright: ignore[reportUnknownMemberType]
        reconnected_client_id = await worker_client.client_id()  # pyright: ignore[reportUnknownMemberType]
        assert reconnected_client_id != worker_client_id
        recovery = ReclaimedDeliveryCoordinator(
            worker_broker, processor, database_session_factory
        )
        recovered = await recovery.recover_batch(
            worker_id="worker-two", min_idle_ms=0, count=1
        )
        assert recovered.outcomes == [
            ReclaimedDeliveryOutcome(
                entry_id=original_entry_id,
                decision=ReclaimedDeliveryDecision(
                    action=ReclaimedDeliveryAction.ACKNOWLEDGE,
                    reason=ReclaimedDeliveryReason.JOB_TERMINAL,
                ),
            )
        ]

        async with database_session_factory() as session:
            recovered_job = await JobRepository(session).get(job_id)
            recovered_attempts = await JobAttemptRepository(session).list_for_job(
                job_id
            )
        assert recovered_job is not None
        assert recovered_job.status is JobStatus.COMPLETED
        assert recovered_job.result == completed_job.result
        assert recovered_job.completed_at == completed_job.completed_at
        assert recovered_job.attempts == 1
        assert recovered_job.next_attempt_at is None
        assert len(recovered_attempts) == 1
        assert recovered_attempts[0].id == successful_attempt.id
        assert recovered_attempts[0].status is JobAttemptStatus.SUCCEEDED
        assert recovered_attempts[0].worker_id == "worker-one"
        assert recovered_attempts[0].completed_at == successful_attempt.completed_at
        assert await control_broker.list_pending() == []
        assert await redis_client.xlen(stream_name) == 1
        assert await redis_client.xlen(dead_letter_stream_name) == 0
        repeated_recovery = await recovery.recover_batch(
            worker_id="worker-two", min_idle_ms=0, count=1
        )
        assert repeated_recovery.outcomes == []
    finally:
        await worker_client.aclose()
        await redis_client.delete(stream_name, dead_letter_stream_name)
        if job_id is not None:
            async with database_session_factory.begin() as session:
                await session.execute(delete(Job).where(Job.id == job_id))

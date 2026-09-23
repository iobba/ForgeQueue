from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid7

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from redis.typing import EncodableT, FieldT
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import (
    JobDelivery,
    JobMessage,
    MalformedJobDelivery,
)
from forgequeue.broker.redis import RedisDeadLetterBroker, RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempt_service import JobAttemptService
from forgequeue.jobs.attempts import JobAttemptStatus
from forgequeue.jobs.execution_errors import RetryableJobError
from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService
from forgequeue.jobs.status import JobStatus
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.recovery import (
    ReclaimedDeliveryAction,
    ReclaimedDeliveryCoordinator,
    ReclaimedDeliveryDecision,
    ReclaimedDeliveryOutcome,
    ReclaimedDeliveryReason,
    RecoveryBatchResult,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


@dataclass(slots=True)
class RecoveryTestEnvironment:
    coordinator: ReclaimedDeliveryCoordinator
    broker: RedisJobBroker
    redis_client: Redis
    session_factory: async_sessionmaker[AsyncSession]
    stream_name: str
    dead_letter_stream_name: str
    created_job_ids: set[UUID]


@pytest_asyncio.fixture
async def recovery_environment(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[RecoveryTestEnvironment]:
    stream_name = f"forgequeue:test:recovery:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:recovery:dead:{uuid7()}"
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
    )
    environment = RecoveryTestEnvironment(
        coordinator=ReclaimedDeliveryCoordinator(
            broker,
            processor,
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


async def create_job(
    environment: RecoveryTestEnvironment,
    *,
    max_attempts: int = 1,
) -> UUID:
    async with environment.session_factory.begin() as session:
        job = await JobService(JobRepository(session)).create_job(
            job_type="sum_numbers",
            payload={"numbers": [10, 20, 30]},
            max_attempts=max_attempts,
        )
        job_id = job.id

    environment.created_job_ids.add(job_id)
    return job_id


async def publish_and_reclaim(
    environment: RecoveryTestEnvironment,
    message: JobMessage,
) -> JobDelivery:
    await environment.broker.ensure_consumer_group()
    await environment.broker.publish(message)
    original_deliveries = await environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )
    assert len(original_deliveries) == 1

    claimed_batch = await environment.broker.claim_stale(
        consumer_name="worker-two",
        min_idle_ms=0,
    )
    assert len(claimed_batch.deliveries) == 1
    return claimed_batch.deliveries[0]


async def start_attempt_with_lease(
    environment: RecoveryTestEnvironment,
    *,
    job_id: UUID,
    lease: AttemptLease,
    worker_id: str = "worker-one",
) -> UUID:
    async with environment.session_factory.begin() as session:
        job = await JobService(JobRepository(session)).start_job(
            job_id,
            started_at=lease.heartbeat_at,
        )
        attempt = await JobAttemptService(JobAttemptRepository(session)).start_attempt(
            job_id=job_id,
            attempt_number=job.attempts,
            worker_id=worker_id,
            lease=lease,
        )
        return attempt.id


async def test_recover_one_processes_queued_job_and_acknowledges_delivery(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    job_id = await create_job(recovery_environment)
    delivery = await publish_and_reclaim(
        recovery_environment,
        JobMessage(job_id=job_id, job_type="sum_numbers"),
    )

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    async with recovery_environment.session_factory() as session:
        job = await JobRepository(session).get(job_id)
        attempts = await JobAttemptRepository(session).list_for_job(job_id)
    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.PROCESS,
        reason=ReclaimedDeliveryReason.JOB_READY,
    )
    assert job is not None
    assert job.status is JobStatus.COMPLETED
    assert job.result == {"sum": 60}
    assert len(attempts) == 1
    assert attempts[0].worker_id == "worker-two"
    assert attempts[0].status is JobAttemptStatus.SUCCEEDED
    assert await recovery_environment.broker.list_pending() == []


@pytest.mark.parametrize("terminal_status", [JobStatus.COMPLETED, JobStatus.FAILED])
async def test_recover_one_acknowledges_terminal_duplicate(
    recovery_environment: RecoveryTestEnvironment,
    terminal_status: JobStatus,
) -> None:
    job_id = await create_job(recovery_environment)
    async with recovery_environment.session_factory.begin() as session:
        service = JobService(JobRepository(session))
        await service.start_job(job_id)
        if terminal_status is JobStatus.COMPLETED:
            await service.complete_job(job_id, {"sum": 60})
        else:
            await service.fail_job(
                job_id,
                error_code="terminal_failure",
                error_message="Job already reached a terminal failure",
            )
    delivery = await publish_and_reclaim(
        recovery_environment,
        JobMessage(job_id=job_id, job_type="sum_numbers"),
    )

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.ACKNOWLEDGE,
        reason=ReclaimedDeliveryReason.JOB_TERMINAL,
    )
    assert await recovery_environment.broker.list_pending() == []


async def test_recover_one_leaves_running_job_pending(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    job_id = await create_job(recovery_environment)
    lease = AttemptLeasePolicy().issue(heartbeat_at=datetime.now(UTC))
    await start_attempt_with_lease(
        recovery_environment,
        job_id=job_id,
        lease=lease,
    )
    delivery = await publish_and_reclaim(
        recovery_environment,
        JobMessage(job_id=job_id, job_type="sum_numbers"),
    )

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.LEAVE_PENDING,
        reason=ReclaimedDeliveryReason.JOB_RUNNING,
    )
    pending = await recovery_environment.broker.list_pending()
    assert [item.entry_id for item in pending] == [delivery.entry_id]
    assert pending[0].consumer_name == "worker-two"


async def test_recover_one_schedules_retry_for_expired_running_attempt(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    job_id = await create_job(recovery_environment, max_attempts=2)
    lease = AttemptLeasePolicy().issue(
        heartbeat_at=datetime.now(UTC) - timedelta(minutes=2)
    )
    attempt_id = await start_attempt_with_lease(
        recovery_environment,
        job_id=job_id,
        lease=lease,
    )
    delivery = await publish_and_reclaim(
        recovery_environment,
        JobMessage(job_id=job_id, job_type="sum_numbers"),
    )

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    async with recovery_environment.session_factory() as session:
        job = await JobRepository(session).get(job_id)
        attempt = await JobAttemptRepository(session).get(attempt_id)

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.RECOVER,
        reason=ReclaimedDeliveryReason.JOB_LEASE_EXPIRED,
    )
    assert job is not None
    assert job.status is JobStatus.RETRY_SCHEDULED
    assert job.next_attempt_at is not None
    assert job.error_code == "worker_lease_expired"
    assert attempt is not None
    assert attempt.status is JobAttemptStatus.FAILED
    assert attempt.error_code == "worker_lease_expired"
    assert await recovery_environment.broker.list_pending() == []


async def test_recover_one_fails_exhausted_expired_attempt_and_dead_letters(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    job_id = await create_job(recovery_environment, max_attempts=1)
    lease = AttemptLeasePolicy().issue(
        heartbeat_at=datetime.now(UTC) - timedelta(minutes=2)
    )
    attempt_id = await start_attempt_with_lease(
        recovery_environment,
        job_id=job_id,
        lease=lease,
    )
    delivery = await publish_and_reclaim(
        recovery_environment,
        JobMessage(job_id=job_id, job_type="sum_numbers"),
    )

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    async with recovery_environment.session_factory() as session:
        job = await JobRepository(session).get(job_id)
        attempt = await JobAttemptRepository(session).get(attempt_id)
    dead_letters = cast(
        list[tuple[str, dict[str, str]]],
        await recovery_environment.redis_client.xrange(
            recovery_environment.dead_letter_stream_name
        ),
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.RECOVER,
        reason=ReclaimedDeliveryReason.JOB_LEASE_EXPIRED,
    )
    assert job is not None
    assert job.status is JobStatus.FAILED
    assert job.error_code == "worker_lease_expired"
    assert attempt is not None
    assert attempt.status is JobAttemptStatus.FAILED
    assert attempt.error_code == "worker_lease_expired"
    assert await recovery_environment.broker.list_pending() == []
    assert len(dead_letters) == 1
    assert dead_letters[0][1]["reason"] == "retries_exhausted"
    assert dead_letters[0][1]["error_code"] == "worker_lease_expired"


async def test_recover_one_leaves_retry_scheduled_job_pending(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    job_id = await create_job(recovery_environment, max_attempts=2)
    async with recovery_environment.session_factory.begin() as session:
        service = JobService(JobRepository(session))
        await service.start_job(job_id)
        await service.schedule_retry(
            job_id,
            error=RetryableJobError(
                error_code="dependency_unavailable",
                safe_message="A dependency is temporarily unavailable",
            ),
            delay=timedelta(minutes=1),
        )
    delivery = await publish_and_reclaim(
        recovery_environment,
        JobMessage(job_id=job_id, job_type="sum_numbers"),
    )

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.LEAVE_PENDING,
        reason=ReclaimedDeliveryReason.RETRY_SCHEDULED,
    )
    assert [
        item.entry_id for item in await recovery_environment.broker.list_pending()
    ] == [delivery.entry_id]


async def test_recover_one_leaves_missing_job_pending(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    delivery = await publish_and_reclaim(
        recovery_environment,
        JobMessage(job_id=uuid7(), job_type="sum_numbers"),
    )

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.LEAVE_PENDING,
        reason=ReclaimedDeliveryReason.JOB_MISSING,
    )
    assert [
        item.entry_id for item in await recovery_environment.broker.list_pending()
    ] == [delivery.entry_id]


async def test_recover_one_leaves_job_type_mismatch_pending(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    job_id = await create_job(recovery_environment)
    delivery = await publish_and_reclaim(
        recovery_environment,
        JobMessage(job_id=job_id, job_type="generate_report"),
    )

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.LEAVE_PENDING,
        reason=ReclaimedDeliveryReason.JOB_TYPE_MISMATCH,
    )
    assert [
        item.entry_id for item in await recovery_environment.broker.list_pending()
    ] == [delivery.entry_id]


async def test_recover_one_quarantines_malformed_delivery(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    invalid_fields: dict[FieldT, EncodableT] = {"schema_version": "999"}
    await recovery_environment.redis_client.xadd(
        recovery_environment.stream_name,
        invalid_fields,
    )
    await recovery_environment.broker.ensure_consumer_group()
    await recovery_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )
    claimed_batch = await recovery_environment.broker.claim_stale(
        consumer_name="worker-two",
        min_idle_ms=0,
    )
    assert len(claimed_batch.deliveries) == 1
    delivery = claimed_batch.deliveries[0]
    assert isinstance(delivery, MalformedJobDelivery)

    decision = await recovery_environment.coordinator.recover_one(
        delivery,
        worker_id="worker-two",
    )

    dead_letters = cast(
        list[tuple[str, dict[str, str]]],
        await recovery_environment.redis_client.xrange(
            recovery_environment.dead_letter_stream_name
        ),
    )
    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.ACKNOWLEDGE,
        reason=ReclaimedDeliveryReason.MALFORMED_MESSAGE,
    )
    assert await recovery_environment.broker.list_pending() == []
    assert len(dead_letters) == 1
    assert dead_letters[0][1]["reason"] == "malformed_message"


async def test_recover_batch_applies_each_safe_state_decision(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    queued_job_id = await create_job(recovery_environment)
    completed_job_id = await create_job(recovery_environment)
    running_job_id = await create_job(recovery_environment)
    async with recovery_environment.session_factory.begin() as session:
        service = JobService(JobRepository(session))
        await service.start_job(completed_job_id)
        await service.complete_job(completed_job_id, {"sum": 60})
        await service.start_job(running_job_id)

    await recovery_environment.broker.ensure_consumer_group()
    messages = [
        JobMessage(job_id=queued_job_id, job_type="sum_numbers"),
        JobMessage(job_id=completed_job_id, job_type="sum_numbers"),
        JobMessage(job_id=running_job_id, job_type="sum_numbers"),
    ]
    entry_ids = [
        await recovery_environment.broker.publish(message) for message in messages
    ]
    original_deliveries = await recovery_environment.broker.read(
        consumer_name="worker-one",
        count=3,
        block_ms=None,
    )
    assert [delivery.entry_id for delivery in original_deliveries] == entry_ids

    result = await recovery_environment.coordinator.recover_batch(
        worker_id="worker-two",
        min_idle_ms=0,
        count=3,
    )

    assert result == RecoveryBatchResult(
        next_start_id="0-0",
        outcomes=[
            ReclaimedDeliveryOutcome(
                entry_id=entry_ids[0],
                decision=ReclaimedDeliveryDecision(
                    action=ReclaimedDeliveryAction.PROCESS,
                    reason=ReclaimedDeliveryReason.JOB_READY,
                ),
            ),
            ReclaimedDeliveryOutcome(
                entry_id=entry_ids[1],
                decision=ReclaimedDeliveryDecision(
                    action=ReclaimedDeliveryAction.ACKNOWLEDGE,
                    reason=ReclaimedDeliveryReason.JOB_TERMINAL,
                ),
            ),
            ReclaimedDeliveryOutcome(
                entry_id=entry_ids[2],
                decision=ReclaimedDeliveryDecision(
                    action=ReclaimedDeliveryAction.LEAVE_PENDING,
                    reason=ReclaimedDeliveryReason.JOB_RUNNING,
                ),
            ),
        ],
        deleted_entry_ids=[],
    )
    pending = await recovery_environment.broker.list_pending()
    assert [item.entry_id for item in pending] == [entry_ids[2]]
    assert pending[0].consumer_name == "worker-two"


async def test_recover_batch_continues_from_returned_cursor(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    job_ids = [await create_job(recovery_environment) for _ in range(3)]
    await recovery_environment.broker.ensure_consumer_group()
    entry_ids: list[str] = []
    for job_id in job_ids:
        entry_ids.append(
            await recovery_environment.broker.publish(
                JobMessage(job_id=job_id, job_type="sum_numbers")
            )
        )
    await recovery_environment.broker.read(
        consumer_name="worker-one",
        count=3,
        block_ms=None,
    )

    first_result = await recovery_environment.coordinator.recover_batch(
        worker_id="worker-two",
        min_idle_ms=0,
        count=1,
    )
    second_result = await recovery_environment.coordinator.recover_batch(
        worker_id="worker-two",
        min_idle_ms=0,
        start_id=first_result.next_start_id,
        count=2,
    )

    assert len(first_result.outcomes) == 1
    assert first_result.next_start_id != "0-0"
    assert len(second_result.outcomes) == 2
    assert second_result.next_start_id == "0-0"
    assert {
        outcome.entry_id
        for outcome in [*first_result.outcomes, *second_result.outcomes]
    } == set(entry_ids)
    assert await recovery_environment.broker.list_pending() == []


async def test_recover_batch_preserves_deleted_pending_entry_ids(
    recovery_environment: RecoveryTestEnvironment,
) -> None:
    job_id = await create_job(recovery_environment)
    await recovery_environment.broker.ensure_consumer_group()
    entry_id = await recovery_environment.broker.publish(
        JobMessage(job_id=job_id, job_type="sum_numbers")
    )
    await recovery_environment.broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )
    assert (
        await recovery_environment.redis_client.xdel(
            recovery_environment.stream_name,
            entry_id,
        )
        == 1
    )

    result = await recovery_environment.coordinator.recover_batch(
        worker_id="worker-two",
        min_idle_ms=0,
    )

    assert result == RecoveryBatchResult(
        next_start_id="0-0",
        outcomes=[],
        deleted_entry_ids=[entry_id],
    )

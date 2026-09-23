from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempt_service import (
    AttemptLeaseExpirationRejectedError,
    AttemptLeaseFinalizationRejectedError,
    JobAttemptService,
)
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind
from forgequeue.jobs.execution_errors import PermanentJobError, RetryableJobError
from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy
from forgequeue.jobs.repository import JobRepository

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


def make_lease() -> AttemptLease:
    return AttemptLeasePolicy().issue(heartbeat_at=datetime.now(UTC))


async def test_attempt_service_persists_successful_attempt(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )
    repository = JobAttemptRepository(database_session)
    service = JobAttemptService(repository)
    lease = make_lease()
    attempt = await service.start_attempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=lease,
    )
    attempt_id = attempt.id

    completed_at = lease.heartbeat_at + timedelta(seconds=1)
    await service.succeed_owned_attempt(
        attempt_id,
        worker_id="worker-1",
        current_lease=lease,
        completed_at=completed_at,
    )
    await database_session.commit()
    database_session.expunge(attempt)

    persisted_attempt = await repository.get(attempt_id)

    assert persisted_attempt is not None
    assert persisted_attempt.status is JobAttemptStatus.SUCCEEDED
    assert persisted_attempt.completed_at is not None
    assert persisted_attempt.failure_kind is None
    assert persisted_attempt.heartbeat_at == lease.heartbeat_at
    assert persisted_attempt.lease_expires_at == lease.expires_at


async def test_attempt_service_persists_permanent_failure(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": ["invalid"]},
    )
    repository = JobAttemptRepository(database_session)
    service = JobAttemptService(repository)
    lease = make_lease()
    attempt = await service.start_attempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=lease,
    )
    attempt_id = attempt.id

    completed_at = lease.heartbeat_at + timedelta(seconds=1)
    await service.fail_owned_attempt(
        attempt_id,
        PermanentJobError(
            error_code="invalid_job_payload",
            safe_message="Stored job payload failed validation",
        ),
        worker_id="worker-1",
        current_lease=lease,
        completed_at=completed_at,
    )
    await database_session.commit()
    database_session.expunge(attempt)

    persisted_attempt = await repository.get(attempt_id)

    assert persisted_attempt is not None
    assert persisted_attempt.status is JobAttemptStatus.FAILED
    assert persisted_attempt.failure_kind is JobFailureKind.PERMANENT
    assert persisted_attempt.error_code == "invalid_job_payload"
    assert persisted_attempt.error_message == "Stored job payload failed validation"
    assert persisted_attempt.completed_at is not None
    assert persisted_attempt.heartbeat_at == lease.heartbeat_at
    assert persisted_attempt.lease_expires_at == lease.expires_at


async def test_attempt_service_persists_renewed_lease(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )
    repository = JobAttemptRepository(database_session)
    service = JobAttemptService(repository)
    lease = make_lease()
    attempt = await service.start_attempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=lease,
    )
    heartbeat_at = lease.heartbeat_at + timedelta(seconds=15)

    renewed_lease = await service.renew_attempt_lease(
        attempt.id,
        worker_id="worker-1",
        current_lease=lease,
        heartbeat_at=heartbeat_at,
    )
    await database_session.commit()
    database_session.expunge(attempt)

    persisted_attempt = await repository.get(attempt.id)

    assert persisted_attempt is not None
    assert renewed_lease.heartbeat_at == heartbeat_at
    assert persisted_attempt.heartbeat_at == renewed_lease.heartbeat_at
    assert persisted_attempt.lease_expires_at == renewed_lease.expires_at


async def test_attempt_service_rejects_finalization_with_stale_lease(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )
    repository = JobAttemptRepository(database_session)
    service = JobAttemptService(repository)
    original_lease = make_lease()
    attempt = await service.start_attempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=original_lease,
    )
    renewed_lease = await service.renew_attempt_lease(
        attempt.id,
        worker_id="worker-1",
        current_lease=original_lease,
        heartbeat_at=original_lease.heartbeat_at + timedelta(seconds=15),
    )

    with pytest.raises(AttemptLeaseFinalizationRejectedError):
        await service.succeed_owned_attempt(
            attempt.id,
            worker_id="worker-1",
            current_lease=original_lease,
            completed_at=original_lease.heartbeat_at + timedelta(seconds=30),
        )

    persisted_attempt = await repository.get(attempt.id)

    assert persisted_attempt is not None
    assert persisted_attempt.status is JobAttemptStatus.RUNNING
    assert persisted_attempt.heartbeat_at == renewed_lease.heartbeat_at
    assert persisted_attempt.lease_expires_at == renewed_lease.expires_at


async def test_attempt_service_rejects_expiration_after_concurrent_renewal(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )
    repository = JobAttemptRepository(database_session)
    service = JobAttemptService(repository)
    original_lease = make_lease()
    attempt = await service.start_attempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=original_lease,
    )
    renewed_lease = await service.renew_attempt_lease(
        attempt.id,
        worker_id="worker-1",
        current_lease=original_lease,
        heartbeat_at=original_lease.heartbeat_at + timedelta(seconds=15),
    )

    with pytest.raises(AttemptLeaseExpirationRejectedError):
        await service.expire_attempt(
            attempt.id,
            RetryableJobError(
                error_code="worker_lease_expired",
                safe_message="Worker stopped renewing the job attempt lease",
            ),
            current_lease=original_lease,
            expired_at=original_lease.expires_at + timedelta(seconds=1),
        )

    persisted_attempt = await repository.get(attempt.id)

    assert persisted_attempt is not None
    assert persisted_attempt.status is JobAttemptStatus.RUNNING
    assert persisted_attempt.heartbeat_at == renewed_lease.heartbeat_at
    assert persisted_attempt.lease_expires_at == renewed_lease.expires_at

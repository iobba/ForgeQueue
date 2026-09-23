from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus
from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy
from forgequeue.jobs.repository import JobRepository

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


def make_lease(*, minute: int = 0) -> AttemptLease:
    return AttemptLeasePolicy().issue(
        heartbeat_at=datetime(2026, 9, 22, 12, minute, tzinfo=UTC)
    )


async def test_create_and_get_attempt(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
        max_attempts=3,
    )
    repository = JobAttemptRepository(database_session)
    lease = make_lease()

    attempt = await repository.create(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=lease,
    )
    attempt_id = attempt.id
    database_session.expunge(attempt)

    persisted_attempt = await repository.get(attempt_id)

    assert persisted_attempt is not None
    assert persisted_attempt.id.version == 7
    assert persisted_attempt.job_id == job.id
    assert persisted_attempt.attempt_number == 1
    assert persisted_attempt.worker_id == "worker-1"
    assert persisted_attempt.status is JobAttemptStatus.RUNNING
    assert persisted_attempt.started_at == lease.heartbeat_at
    assert persisted_attempt.heartbeat_at == lease.heartbeat_at
    assert persisted_attempt.lease_expires_at == lease.expires_at
    heartbeat_at = persisted_attempt.heartbeat_at
    lease_expires_at = persisted_attempt.lease_expires_at
    assert heartbeat_at is not None
    assert lease_expires_at is not None
    assert lease_expires_at - heartbeat_at == timedelta(seconds=60)


async def test_get_returns_none_for_unknown_attempt(
    database_session: AsyncSession,
) -> None:
    from uuid import uuid7

    repository = JobAttemptRepository(database_session)

    assert await repository.get(uuid7()) is None


async def test_renew_lease_atomically_replaces_expected_lease(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )
    repository = JobAttemptRepository(database_session)
    current_lease = make_lease()
    attempt = await repository.create(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=current_lease,
    )
    renewed_lease = AttemptLeasePolicy().renew(
        current_lease,
        heartbeat_at=current_lease.heartbeat_at + timedelta(seconds=15),
    )

    renewed_attempt = await repository.renew_lease(
        attempt_id=attempt.id,
        worker_id="worker-1",
        current_lease=current_lease,
        renewed_lease=renewed_lease,
    )

    assert renewed_attempt is not None
    assert renewed_attempt.heartbeat_at == renewed_lease.heartbeat_at
    assert renewed_attempt.lease_expires_at == renewed_lease.expires_at

    stale_renewal = await repository.renew_lease(
        attempt_id=attempt.id,
        worker_id="worker-1",
        current_lease=current_lease,
        renewed_lease=renewed_lease,
    )

    assert stale_renewal is None


async def test_renew_lease_rejects_wrong_worker(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )
    repository = JobAttemptRepository(database_session)
    current_lease = make_lease()
    attempt = await repository.create(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=current_lease,
    )
    renewed_lease = AttemptLeasePolicy().renew(
        current_lease,
        heartbeat_at=current_lease.heartbeat_at + timedelta(seconds=15),
    )

    renewed_attempt = await repository.renew_lease(
        attempt_id=attempt.id,
        worker_id="worker-2",
        current_lease=current_lease,
        renewed_lease=renewed_lease,
    )

    assert renewed_attempt is None


async def test_renew_lease_rejects_terminal_attempt(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )
    repository = JobAttemptRepository(database_session)
    current_lease = make_lease()
    attempt = await repository.create(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=current_lease,
    )
    attempt.status = JobAttemptStatus.SUCCEEDED
    attempt.completed_at = current_lease.heartbeat_at + timedelta(seconds=1)
    await database_session.flush()
    renewed_lease = AttemptLeasePolicy().renew(
        current_lease,
        heartbeat_at=current_lease.heartbeat_at + timedelta(seconds=15),
    )

    renewed_attempt = await repository.renew_lease(
        attempt_id=attempt.id,
        worker_id="worker-1",
        current_lease=current_lease,
        renewed_lease=renewed_lease,
    )

    assert renewed_attempt is None


async def test_list_for_job_returns_attempt_history_in_sequence(
    database_session: AsyncSession,
) -> None:
    job_repository = JobRepository(database_session)
    first_job = await job_repository.create(
        job_type="sum_numbers",
        payload={"numbers": [1]},
        max_attempts=3,
    )
    second_job = await job_repository.create(
        job_type="sum_numbers",
        payload={"numbers": [2]},
    )
    repository = JobAttemptRepository(database_session)

    second_attempt = await repository.create(
        job_id=first_job.id,
        attempt_number=2,
        worker_id="worker-2",
        lease=make_lease(minute=2),
    )
    first_attempt = await repository.create(
        job_id=first_job.id,
        attempt_number=1,
        worker_id="worker-1",
        lease=make_lease(minute=1),
    )
    await repository.create(
        job_id=second_job.id,
        attempt_number=1,
        worker_id="other-worker",
        lease=make_lease(minute=3),
    )

    attempts = await repository.list_for_job(first_job.id)

    assert [attempt.id for attempt in attempts] == [
        first_attempt.id,
        second_attempt.id,
    ]
    assert [attempt.attempt_number for attempt in attempts] == [1, 2]

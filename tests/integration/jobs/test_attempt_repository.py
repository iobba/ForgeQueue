import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus
from forgequeue.jobs.repository import JobRepository

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


async def test_create_and_get_attempt(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
        max_attempts=3,
    )
    repository = JobAttemptRepository(database_session)

    attempt = await repository.create(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
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
    assert persisted_attempt.started_at.tzinfo is not None


async def test_get_returns_none_for_unknown_attempt(
    database_session: AsyncSession,
) -> None:
    from uuid import uuid7

    repository = JobAttemptRepository(database_session)

    assert await repository.get(uuid7()) is None


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
    )
    first_attempt = await repository.create(
        job_id=first_job.id,
        attempt_number=1,
        worker_id="worker-1",
    )
    await repository.create(
        job_id=second_job.id,
        attempt_number=1,
        worker_id="other-worker",
    )

    attempts = await repository.list_for_job(first_job.id)

    assert [attempt.id for attempt in attempts] == [
        first_attempt.id,
        second_attempt.id,
    ]
    assert [attempt.attempt_number for attempt in attempts] == [1, 2]

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempt_service import JobAttemptService
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind
from forgequeue.jobs.execution_errors import PermanentJobError
from forgequeue.jobs.repository import JobRepository

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


async def test_attempt_service_persists_successful_attempt(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )
    repository = JobAttemptRepository(database_session)
    service = JobAttemptService(repository)
    attempt = await service.start_attempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
    )
    attempt_id = attempt.id

    await service.succeed_attempt(attempt_id)
    await database_session.commit()
    database_session.expunge(attempt)

    persisted_attempt = await repository.get(attempt_id)

    assert persisted_attempt is not None
    assert persisted_attempt.status is JobAttemptStatus.SUCCEEDED
    assert persisted_attempt.completed_at is not None
    assert persisted_attempt.failure_kind is None


async def test_attempt_service_persists_permanent_failure(
    database_session: AsyncSession,
) -> None:
    job = await JobRepository(database_session).create(
        job_type="sum_numbers",
        payload={"numbers": ["invalid"]},
    )
    repository = JobAttemptRepository(database_session)
    service = JobAttemptService(repository)
    attempt = await service.start_attempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
    )
    attempt_id = attempt.id

    await service.fail_attempt(
        attempt_id,
        PermanentJobError(
            error_code="invalid_job_payload",
            safe_message="Stored job payload failed validation",
        ),
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

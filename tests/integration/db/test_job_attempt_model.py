from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.db.models import Job, JobAttempt
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


async def create_job(database_session: AsyncSession) -> Job:
    job = Job(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
        max_attempts=3,
    )
    database_session.add(job)
    await database_session.flush()
    return job


async def test_persists_running_attempt_with_defaults(
    database_session: AsyncSession,
) -> None:
    job = await create_job(database_session)
    attempt = JobAttempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
    )
    database_session.add(attempt)

    await database_session.flush()
    await database_session.refresh(attempt)

    assert attempt.id is not None
    assert attempt.job_id == job.id
    assert attempt.attempt_number == 1
    assert attempt.worker_id == "worker-1"
    assert attempt.status is JobAttemptStatus.RUNNING
    assert attempt.failure_kind is None
    assert attempt.error_code is None
    assert attempt.error_message is None
    assert attempt.started_at is not None
    assert attempt.completed_at is None


async def test_persists_successful_attempt(
    database_session: AsyncSession,
) -> None:
    job = await create_job(database_session)
    completed_at = datetime.now(UTC)
    attempt = JobAttempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        status=JobAttemptStatus.SUCCEEDED,
        completed_at=completed_at,
    )
    database_session.add(attempt)

    await database_session.flush()

    assert attempt.status is JobAttemptStatus.SUCCEEDED
    assert attempt.completed_at == completed_at


@pytest.mark.parametrize(
    ("failure_kind", "error_code"),
    [
        (JobFailureKind.RETRYABLE, "dependency_unavailable"),
        (JobFailureKind.PERMANENT, "invalid_job_payload"),
    ],
)
async def test_persists_classified_failed_attempt(
    database_session: AsyncSession,
    failure_kind: JobFailureKind,
    error_code: str,
) -> None:
    job = await create_job(database_session)
    attempt = JobAttempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
        status=JobAttemptStatus.FAILED,
        failure_kind=failure_kind,
        error_code=error_code,
        error_message="Sanitized failure description",
        completed_at=datetime.now(UTC),
    )
    database_session.add(attempt)

    await database_session.flush()

    assert attempt.failure_kind is failure_kind
    assert attempt.error_code == error_code


@pytest.mark.parametrize("attempt_number", [0, -1])
async def test_rejects_non_positive_attempt_number(
    database_session: AsyncSession,
    attempt_number: int,
) -> None:
    job = await create_job(database_session)
    database_session.add(
        JobAttempt(
            job_id=job.id,
            attempt_number=attempt_number,
            worker_id="worker-1",
        )
    )

    with pytest.raises(IntegrityError):
        await database_session.flush()


@pytest.mark.parametrize("worker_id", ["", "   "])
async def test_rejects_blank_worker_id(
    database_session: AsyncSession,
    worker_id: str,
) -> None:
    job = await create_job(database_session)
    database_session.add(
        JobAttempt(
            job_id=job.id,
            attempt_number=1,
            worker_id=worker_id,
        )
    )

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_rejects_duplicate_attempt_number_for_job(
    database_session: AsyncSession,
) -> None:
    job = await create_job(database_session)
    database_session.add_all(
        [
            JobAttempt(
                job_id=job.id,
                attempt_number=1,
                worker_id="worker-1",
            ),
            JobAttempt(
                job_id=job.id,
                attempt_number=1,
                worker_id="worker-2",
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        await database_session.flush()


@pytest.mark.parametrize(
    "attempt",
    [
        JobAttempt(
            job_id=uuid7(),
            attempt_number=1,
            worker_id="worker-1",
            status=JobAttemptStatus.RUNNING,
            completed_at=datetime.now(UTC),
        ),
        JobAttempt(
            job_id=uuid7(),
            attempt_number=1,
            worker_id="worker-1",
            status=JobAttemptStatus.SUCCEEDED,
        ),
        JobAttempt(
            job_id=uuid7(),
            attempt_number=1,
            worker_id="worker-1",
            status=JobAttemptStatus.FAILED,
            completed_at=datetime.now(UTC),
        ),
    ],
)
async def test_rejects_attempt_state_with_inconsistent_details(
    database_session: AsyncSession,
    attempt: JobAttempt,
) -> None:
    job = await create_job(database_session)
    attempt.job_id = job.id
    database_session.add(attempt)

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_rejects_completion_before_start(
    database_session: AsyncSession,
) -> None:
    job = await create_job(database_session)
    started_at = datetime.now(UTC)
    database_session.add(
        JobAttempt(
            job_id=job.id,
            attempt_number=1,
            worker_id="worker-1",
            status=JobAttemptStatus.SUCCEEDED,
            started_at=started_at,
            completed_at=started_at - timedelta(seconds=1),
        )
    )

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_database_rejects_invalid_attempt_status(
    database_session: AsyncSession,
) -> None:
    job = await create_job(database_session)

    with pytest.raises(IntegrityError):
        await database_session.execute(
            text(
                """
                INSERT INTO job_attempts (
                    id,
                    job_id,
                    attempt_number,
                    worker_id,
                    status
                )
                VALUES (
                    :attempt_id,
                    :job_id,
                    1,
                    'worker-1',
                    'INVALID'
                )
                """
            ),
            {"attempt_id": uuid7(), "job_id": job.id},
        )


async def test_deleting_job_cascades_to_attempts(
    database_session: AsyncSession,
) -> None:
    job = await create_job(database_session)
    attempt = JobAttempt(
        job_id=job.id,
        attempt_number=1,
        worker_id="worker-1",
    )
    database_session.add(attempt)
    await database_session.flush()
    attempt_id = attempt.id

    await database_session.execute(delete(Job).where(Job.id == job.id))
    await database_session.flush()
    database_session.expunge(attempt)

    persisted_attempt = await database_session.scalar(
        select(JobAttempt).where(JobAttempt.id == attempt_id)
    )

    assert persisted_attempt is None

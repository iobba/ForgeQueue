from datetime import UTC, datetime
from uuid import uuid7

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.db.models import Job
from forgequeue.jobs.status import JobStatus

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


@pytest.mark.parametrize("job_type", ["", "   "])
async def test_rejects_blank_job_type(
    database_session: AsyncSession,
    job_type: str,
) -> None:
    job = Job(
        job_type=job_type,
        payload={},
    )
    database_session.add(job)

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_rejects_non_positive_max_attempts(
    database_session: AsyncSession,
) -> None:
    job = Job(
        job_type="generate_report",
        payload={},
        max_attempts=0,
    )
    database_session.add(job)

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_rejects_negative_attempts(
    database_session: AsyncSession,
) -> None:
    job = Job(
        job_type="generate_report",
        payload={},
        attempts=-1,
        max_attempts=3,
    )
    database_session.add(job)

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_rejects_attempts_above_maximum(
    database_session: AsyncSession,
) -> None:
    job = Job(
        job_type="generate_report",
        payload={},
        attempts=4,
        max_attempts=3,
    )
    database_session.add(job)

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_database_rejects_invalid_status(
    database_session: AsyncSession,
) -> None:
    statement = text(
        """
        INSERT INTO jobs (
            id,
            job_type,
            status,
            payload,
            attempts,
            max_attempts
        )
        VALUES (
            :job_id,
            'generate_report',
            'INVALID',
            CAST('{}' AS jsonb),
            0,
            1
        )
        """
    )

    with pytest.raises(IntegrityError):
        await database_session.execute(
            statement,
            {"job_id": uuid7()},
        )


async def test_rejects_retry_scheduled_without_next_attempt_time(
    database_session: AsyncSession,
) -> None:
    job = Job(
        job_type="generate_report",
        status=JobStatus.RETRY_SCHEDULED,
        payload={},
    )
    database_session.add(job)

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_rejects_next_attempt_time_for_non_scheduled_job(
    database_session: AsyncSession,
) -> None:
    job = Job(
        job_type="generate_report",
        status=JobStatus.RUNNING,
        payload={},
        next_attempt_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
    )
    database_session.add(job)

    with pytest.raises(IntegrityError):
        await database_session.flush()


async def test_accepts_retry_scheduled_with_next_attempt_time(
    database_session: AsyncSession,
) -> None:
    next_attempt_at = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    job = Job(
        job_type="generate_report",
        status=JobStatus.RETRY_SCHEDULED,
        payload={},
        attempts=1,
        max_attempts=3,
        next_attempt_at=next_attempt_at,
    )
    database_session.add(job)

    await database_session.flush()

    assert job.next_attempt_at == next_attempt_at

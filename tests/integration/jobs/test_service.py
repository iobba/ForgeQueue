from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.jobs.execution_errors import RetryableJobError
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService
from forgequeue.jobs.status import JobStatus

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


async def test_start_job_persists_running_state(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    service = JobService(repository)
    job = await service.create_job(
        job_type="generate_report",
        payload={"customer_id": 42},
    )
    job_id = job.id

    started_job = await service.start_job(job_id)
    await database_session.commit()
    database_session.expunge(started_job)

    persisted_job = await repository.get(job_id)

    assert persisted_job is not None
    assert persisted_job.status is JobStatus.RUNNING
    assert persisted_job.attempts == 1
    assert persisted_job.started_at is not None
    assert persisted_job.completed_at is None


async def test_complete_job_persists_result(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    service = JobService(repository)
    job = await service.create_job(
        job_type="generate_report",
        payload={"customer_id": 42},
    )
    job_id = job.id
    await service.start_job(job_id)
    await database_session.commit()
    database_session.expunge(job)
    result: dict[str, object] = {"report_key": "reports/42.pdf"}

    completed_job = await service.complete_job(job_id, result)
    await database_session.commit()
    database_session.expunge(completed_job)

    persisted_job = await repository.get(job_id)

    assert persisted_job is not None
    assert persisted_job.status is JobStatus.COMPLETED
    assert persisted_job.attempts == 1
    assert persisted_job.result == result
    assert persisted_job.error_code is None
    assert persisted_job.error_message is None
    assert persisted_job.started_at is not None
    assert persisted_job.completed_at is not None


async def test_fail_job_persists_error_details(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    service = JobService(repository)
    job = await service.create_job(
        job_type="generate_report",
        payload={"customer_id": 42},
    )
    job_id = job.id
    await service.start_job(job_id)
    await database_session.commit()
    database_session.expunge(job)

    failed_job = await service.fail_job(
        job_id,
        error_code="REPORT_GENERATION_FAILED",
        error_message="PDF rendering failed",
    )
    await database_session.commit()
    database_session.expunge(failed_job)

    persisted_job = await repository.get(job_id)

    assert persisted_job is not None
    assert persisted_job.status is JobStatus.FAILED
    assert persisted_job.attempts == 1
    assert persisted_job.result is None
    assert persisted_job.error_code == "REPORT_GENERATION_FAILED"
    assert persisted_job.error_message == "PDF rendering failed"
    assert persisted_job.started_at is not None
    assert persisted_job.completed_at is not None


async def test_schedule_retry_and_start_next_attempt_persist(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    service = JobService(repository)
    scheduled_at = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    delay = timedelta(seconds=15)
    job = await service.create_job(
        job_type="generate_report",
        payload={"customer_id": 42},
        max_attempts=3,
    )
    job_id = job.id
    await service.start_job(job_id, started_at=scheduled_at)

    scheduled_job = await service.schedule_retry(
        job_id,
        error=RetryableJobError(
            error_code="UPSTREAM_UNAVAILABLE",
            safe_message="The report provider is temporarily unavailable",
        ),
        delay=delay,
        scheduled_at=scheduled_at,
    )
    await database_session.commit()
    database_session.expunge(scheduled_job)

    persisted_scheduled_job = await repository.get(job_id)

    assert persisted_scheduled_job is not None
    assert persisted_scheduled_job.status is JobStatus.RETRY_SCHEDULED
    assert persisted_scheduled_job.attempts == 1
    assert persisted_scheduled_job.error_code == "UPSTREAM_UNAVAILABLE"
    assert persisted_scheduled_job.next_attempt_at == scheduled_at + delay
    assert persisted_scheduled_job.completed_at is None

    started_job = await service.start_job(
        job_id,
        started_at=scheduled_at + delay,
    )
    await database_session.commit()
    database_session.expunge(started_job)

    persisted_running_job = await repository.get(job_id)

    assert persisted_running_job is not None
    assert persisted_running_job.status is JobStatus.RUNNING
    assert persisted_running_job.attempts == 2
    assert persisted_running_job.error_code is None
    assert persisted_running_job.error_message is None
    assert persisted_running_job.next_attempt_at is None

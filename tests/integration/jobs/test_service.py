import pytest
from sqlalchemy.ext.asyncio import AsyncSession

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
    assert persisted_job.result is None
    assert persisted_job.error_code == "REPORT_GENERATION_FAILED"
    assert persisted_job.error_message == "PDF rendering failed"
    assert persisted_job.started_at is not None
    assert persisted_job.completed_at is not None

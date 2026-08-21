from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.db.models import Job
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


async def test_create_persists_job_with_defaults(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    payload: dict[str, object] = {
        "customer_id": 42,
        "options": {"format": "pdf"},
    }

    job = await repository.create(
        job_type="generate_report",
        payload=payload,
    )

    assert job.id.version == 7
    assert job.job_type == "generate_report"
    assert job.payload == payload
    assert job.status is JobStatus.QUEUED
    assert job.attempts == 0
    assert job.max_attempts == 1
    assert job.result is None
    assert job.error_code is None
    assert job.error_message is None
    assert job.started_at is None
    assert job.completed_at is None
    assert job.created_at.tzinfo is not None
    assert job.updated_at.tzinfo is not None


async def test_create_persists_custom_max_attempts(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)

    job = await repository.create(
        job_type="send_email",
        payload={"recipient": "user@example.com"},
        max_attempts=5,
    )

    assert job.max_attempts == 5


async def test_get_returns_existing_job(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    created_job = await repository.create(
        job_type="process_csv",
        payload={"file_key": "uploads/customers.csv"},
    )
    created_job_id = created_job.id

    database_session.expunge(created_job)
    loaded_job = await repository.get(created_job_id)

    assert loaded_job is not None
    assert loaded_job.id == created_job_id
    assert loaded_job.job_type == "process_csv"
    assert loaded_job.payload == {"file_key": "uploads/customers.csv"}


async def test_get_returns_none_for_unknown_id(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)

    loaded_job = await repository.get(uuid7())

    assert loaded_job is None


async def test_list_jobs_returns_newest_first(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    base_time = datetime(2026, 1, 1, tzinfo=UTC)
    created_jobs: list[Job] = []

    for index in range(3):
        job = await repository.create(
            job_type=f"job-{index}",
            payload={"index": index},
        )
        job.created_at = base_time + timedelta(minutes=index)
        created_jobs.append(job)

    await database_session.flush()

    jobs = await repository.list_jobs()

    assert [job.id for job in jobs] == [
        created_jobs[2].id,
        created_jobs[1].id,
        created_jobs[0].id,
    ]


async def test_list_jobs_filters_by_status(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    queued_job = await repository.create(job_type="queued", payload={})
    running_job = await repository.create(job_type="running", payload={})
    completed_job = await repository.create(job_type="completed", payload={})

    running_job.status = JobStatus.RUNNING
    completed_job.status = JobStatus.COMPLETED
    await database_session.flush()

    jobs = await repository.list_jobs(status=JobStatus.RUNNING)

    job_ids = {job.id for job in jobs}
    assert job_ids == {running_job.id}
    assert queued_job.id not in job_ids
    assert completed_job.id not in job_ids


async def test_list_jobs_applies_limit_and_offset(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    base_time = datetime(2026, 1, 1, tzinfo=UTC)
    created_jobs: list[Job] = []

    for index in range(4):
        job = await repository.create(
            job_type=f"job-{index}",
            payload={"index": index},
        )
        job.created_at = base_time + timedelta(minutes=index)
        created_jobs.append(job)

    await database_session.flush()

    jobs = await repository.list_jobs(limit=2, offset=1)

    assert [job.id for job in jobs] == [
        created_jobs[2].id,
        created_jobs[1].id,
    ]


async def test_count_jobs_counts_all_jobs_and_applies_status_filter(
    database_session: AsyncSession,
) -> None:
    repository = JobRepository(database_session)
    initial_total = await repository.count_jobs()
    initial_running = await repository.count_jobs(status=JobStatus.RUNNING)
    queued_job = await repository.create(job_type="queued", payload={})
    running_job = await repository.create(job_type="running", payload={})
    running_job.status = JobStatus.RUNNING
    await database_session.flush()

    assert await repository.count_jobs() == initial_total + 2
    assert await repository.count_jobs(status=JobStatus.RUNNING) == initial_running + 1
    assert queued_job.status is JobStatus.QUEUED

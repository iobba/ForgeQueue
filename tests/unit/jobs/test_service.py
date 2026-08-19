from datetime import UTC, datetime
from uuid import UUID, uuid7

import pytest

from forgequeue.db.models import Job
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobNotFoundError, JobService
from forgequeue.jobs.status import InvalidJobStatusTransition, JobStatus

pytestmark = [
    pytest.mark.unit,
    pytest.mark.asyncio,
]


class FakeJobRepository(JobRepository):
    def __init__(self, job: Job | None) -> None:
        self.job = job
        self.requested_job_id: UUID | None = None
        self.create_call: tuple[str, dict[str, object], int] | None = None

    async def create(
        self,
        *,
        job_type: str,
        payload: dict[str, object],
        max_attempts: int = 1,
    ) -> Job:
        self.create_call = (job_type, payload, max_attempts)

        if self.job is None:
            raise AssertionError("Fake repository has no job to return")

        return self.job

    async def get(self, job_id: UUID) -> Job | None:
        self.requested_job_id = job_id
        return self.job


def make_job(status: JobStatus) -> Job:
    return Job(
        id=uuid7(),
        job_type="generate_report",
        status=status,
        payload={"customer_id": 42},
        attempts=0,
        max_attempts=3,
    )


async def test_create_job_delegates_to_repository() -> None:
    job = make_job(JobStatus.QUEUED)
    repository = FakeJobRepository(job)
    service = JobService(repository)
    payload: dict[str, object] = {"customer_id": 42}

    created_job = await service.create_job(
        job_type="generate_report",
        payload=payload,
        max_attempts=5,
    )

    assert created_job is job
    assert repository.create_call == ("generate_report", payload, 5)


async def test_start_job_moves_queued_job_to_running() -> None:
    job = make_job(JobStatus.QUEUED)
    repository = FakeJobRepository(job)
    service = JobService(repository)
    before = datetime.now(UTC)

    started_job = await service.start_job(job.id)

    after = datetime.now(UTC)
    assert started_job is job
    assert repository.requested_job_id == job.id
    assert job.status is JobStatus.RUNNING
    assert job.started_at is not None
    assert before <= job.started_at <= after


async def test_start_job_raises_when_job_is_missing() -> None:
    repository = FakeJobRepository(None)
    service = JobService(repository)
    job_id = uuid7()

    with pytest.raises(JobNotFoundError) as exc_info:
        await service.start_job(job_id)

    assert exc_info.value.job_id == job_id
    assert repository.requested_job_id == job_id


async def test_start_job_rejects_invalid_transition() -> None:
    job = make_job(JobStatus.COMPLETED)
    repository = FakeJobRepository(job)
    service = JobService(repository)

    with pytest.raises(InvalidJobStatusTransition) as exc_info:
        await service.start_job(job.id)

    assert exc_info.value.current is JobStatus.COMPLETED
    assert exc_info.value.target is JobStatus.RUNNING
    assert job.status is JobStatus.COMPLETED
    assert job.started_at is None


async def test_complete_job_stores_result_and_completion_time() -> None:
    job = make_job(JobStatus.RUNNING)
    job.error_code = "old_error"
    job.error_message = "An error from an earlier attempt"
    repository = FakeJobRepository(job)
    service = JobService(repository)
    result: dict[str, object] = {"report_key": "reports/42.pdf"}
    before = datetime.now(UTC)

    completed_job = await service.complete_job(job.id, result)

    after = datetime.now(UTC)
    assert completed_job is job
    assert job.status is JobStatus.COMPLETED
    assert job.result == result
    assert job.error_code is None
    assert job.error_message is None
    assert job.completed_at is not None
    assert before <= job.completed_at <= after


async def test_complete_job_raises_when_job_is_missing() -> None:
    repository = FakeJobRepository(None)
    service = JobService(repository)
    job_id = uuid7()

    with pytest.raises(JobNotFoundError) as exc_info:
        await service.complete_job(job_id, {"report_key": "reports/42.pdf"})

    assert exc_info.value.job_id == job_id


async def test_complete_job_rejects_invalid_transition() -> None:
    job = make_job(JobStatus.QUEUED)
    repository = FakeJobRepository(job)
    service = JobService(repository)

    with pytest.raises(InvalidJobStatusTransition) as exc_info:
        await service.complete_job(job.id, {"report_key": "reports/42.pdf"})

    assert exc_info.value.current is JobStatus.QUEUED
    assert exc_info.value.target is JobStatus.COMPLETED
    assert job.status is JobStatus.QUEUED
    assert job.result is None
    assert job.completed_at is None


async def test_fail_job_stores_error_and_completion_time() -> None:
    job = make_job(JobStatus.RUNNING)
    job.result = {"partial": True}
    repository = FakeJobRepository(job)
    service = JobService(repository)
    before = datetime.now(UTC)

    failed_job = await service.fail_job(
        job.id,
        error_code="REPORT_GENERATION_FAILED",
        error_message="PDF rendering failed",
    )

    after = datetime.now(UTC)
    assert failed_job is job
    assert job.status is JobStatus.FAILED
    assert job.result is None
    assert job.error_code == "REPORT_GENERATION_FAILED"
    assert job.error_message == "PDF rendering failed"
    assert job.completed_at is not None
    assert before <= job.completed_at <= after


async def test_fail_job_raises_when_job_is_missing() -> None:
    repository = FakeJobRepository(None)
    service = JobService(repository)
    job_id = uuid7()

    with pytest.raises(JobNotFoundError) as exc_info:
        await service.fail_job(
            job_id,
            error_code="REPORT_GENERATION_FAILED",
            error_message="PDF rendering failed",
        )

    assert exc_info.value.job_id == job_id


async def test_fail_job_rejects_invalid_transition() -> None:
    job = make_job(JobStatus.QUEUED)
    repository = FakeJobRepository(job)
    service = JobService(repository)

    with pytest.raises(InvalidJobStatusTransition) as exc_info:
        await service.fail_job(
            job.id,
            error_code="REPORT_GENERATION_FAILED",
            error_message="PDF rendering failed",
        )

    assert exc_info.value.current is JobStatus.QUEUED
    assert exc_info.value.target is JobStatus.FAILED
    assert job.status is JobStatus.QUEUED
    assert job.error_code is None
    assert job.error_message is None
    assert job.completed_at is None

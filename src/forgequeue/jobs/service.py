from datetime import UTC, datetime
from uuid import UUID

from forgequeue.db.models import Job
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus, validate_transition


class JobNotFoundError(LookupError):
    def __init__(self, job_id: UUID) -> None:
        self.job_id = job_id
        super().__init__(f"Job {job_id} was not found")


class JobService:
    def __init__(self, repository: JobRepository) -> None:
        self._repository = repository

    async def create_job(
        self,
        *,
        job_type: str,
        payload: dict[str, object],
        max_attempts: int = 1,
    ) -> Job:
        return await self._repository.create(
            job_type=job_type,
            payload=payload,
            max_attempts=max_attempts,
        )

    async def get_job(self, job_id: UUID) -> Job:
        job = await self._repository.get(job_id=job_id)
        if job is None:
            raise JobNotFoundError(job_id=job_id)

        return job

    async def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        page: int = 1,
        limit: int = 100,
    ) -> tuple[list[Job], int]:
        offset = (page - 1) * limit
        jobs = await self._repository.list_jobs(
            status=status,
            limit=limit,
            offset=offset,
        )
        total = await self._repository.count_jobs(status=status)
        return jobs, total

    async def start_job(self, job_id: UUID) -> Job:
        job = await self._repository.get(job_id=job_id)
        if job is None:
            raise JobNotFoundError(job_id=job_id)

        validate_transition(job.status, JobStatus.RUNNING)

        job.status = JobStatus.RUNNING
        job.started_at = datetime.now(UTC)

        return job

    async def complete_job(
        self,
        job_id: UUID,
        result: dict[str, object],
    ) -> Job:
        job = await self._repository.get(job_id=job_id)
        if job is None:
            raise JobNotFoundError(job_id=job_id)

        validate_transition(job.status, JobStatus.COMPLETED)

        job.status = JobStatus.COMPLETED
        job.result = result
        job.error_code = None
        job.error_message = None
        job.completed_at = datetime.now(UTC)

        return job

    async def fail_job(
        self,
        job_id: UUID,
        *,
        error_code: str,
        error_message: str,
    ) -> Job:
        job = await self._repository.get(job_id=job_id)
        if job is None:
            raise JobNotFoundError(job_id=job_id)

        validate_transition(job.status, JobStatus.FAILED)

        job.status = JobStatus.FAILED
        job.result = None
        job.error_code = error_code
        job.error_message = error_message
        job.completed_at = datetime.now(UTC)

        return job

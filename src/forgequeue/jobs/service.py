from datetime import UTC, datetime, timedelta
from uuid import UUID

from forgequeue.db.models import Job
from forgequeue.jobs.attempts import JobFailureKind
from forgequeue.jobs.execution_errors import JobExecutionError
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus, validate_transition


class JobNotFoundError(LookupError):
    def __init__(self, job_id: UUID) -> None:
        self.job_id = job_id
        super().__init__(f"Job {job_id} was not found")


class JobAttemptsExhaustedError(RuntimeError):
    def __init__(self, job_id: UUID, max_attempts: int) -> None:
        self.job_id = job_id
        self.max_attempts = max_attempts
        super().__init__(f"Job {job_id} has exhausted its {max_attempts} attempts")


class JobRetryNotReadyError(RuntimeError):
    def __init__(self, job_id: UUID, next_attempt_at: datetime) -> None:
        self.job_id = job_id
        self.next_attempt_at = next_attempt_at
        super().__init__(
            f"Job {job_id} is not eligible to retry before "
            f"{next_attempt_at.isoformat()}"
        )


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

    async def start_job(
        self,
        job_id: UUID,
        *,
        started_at: datetime | None = None,
    ) -> Job:
        job = await self._repository.get(job_id=job_id)
        if job is None:
            raise JobNotFoundError(job_id=job_id)

        validate_transition(job.status, JobStatus.RUNNING)
        if job.attempts >= job.max_attempts:
            raise JobAttemptsExhaustedError(job.id, job.max_attempts)

        resolved_started_at = started_at or datetime.now(UTC)
        if (
            job.next_attempt_at is not None
            and resolved_started_at < job.next_attempt_at
        ):
            raise JobRetryNotReadyError(job.id, job.next_attempt_at)

        job.status = JobStatus.RUNNING
        job.attempts += 1
        job.result = None
        job.error_code = None
        job.error_message = None
        job.started_at = resolved_started_at
        job.completed_at = None
        job.next_attempt_at = None

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
        job.next_attempt_at = None

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
        job.next_attempt_at = None

        return job

    async def schedule_retry(
        self,
        job_id: UUID,
        *,
        error: JobExecutionError,
        delay: timedelta,
        scheduled_at: datetime | None = None,
    ) -> Job:
        job = await self._repository.get(job_id=job_id)
        if job is None:
            raise JobNotFoundError(job_id=job_id)

        validate_transition(job.status, JobStatus.RETRY_SCHEDULED)
        if error.failure_kind is not JobFailureKind.RETRYABLE:
            raise ValueError("Only retryable failures can schedule a retry")
        if job.attempts >= job.max_attempts:
            raise JobAttemptsExhaustedError(job.id, job.max_attempts)
        if delay <= timedelta(0):
            raise ValueError("Retry delay must be positive")

        resolved_scheduled_at = scheduled_at or datetime.now(UTC)
        job.status = JobStatus.RETRY_SCHEDULED
        job.result = None
        job.error_code = error.error_code
        job.error_message = error.safe_message
        job.completed_at = None
        job.next_attempt_at = resolved_scheduled_at + delay

        return job

    async def queue_retry(
        self,
        job_id: UUID,
        *,
        queued_at: datetime | None = None,
    ) -> Job:
        job = await self._repository.get(job_id=job_id)
        if job is None:
            raise JobNotFoundError(job_id=job_id)

        validate_transition(job.status, JobStatus.QUEUED)
        resolved_queued_at = queued_at or datetime.now(UTC)
        if job.next_attempt_at is None:
            raise ValueError("A retry-scheduled job must have next_attempt_at")
        if resolved_queued_at < job.next_attempt_at:
            raise JobRetryNotReadyError(job.id, job.next_attempt_at)

        job.status = JobStatus.QUEUED
        job.next_attempt_at = None
        return job

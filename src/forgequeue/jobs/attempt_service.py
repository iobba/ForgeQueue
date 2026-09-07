from datetime import UTC, datetime
from uuid import UUID

from forgequeue.db.models import JobAttempt
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus, validate_attempt_transition
from forgequeue.jobs.execution_errors import JobExecutionError


class JobAttemptNotFoundError(LookupError):
    def __init__(self, attempt_id: UUID) -> None:
        self.attempt_id = attempt_id
        super().__init__(f"Job attempt {attempt_id} was not found")


class JobAttemptService:
    def __init__(self, repository: JobAttemptRepository) -> None:
        self._repository = repository

    async def start_attempt(
        self,
        *,
        job_id: UUID,
        attempt_number: int,
        worker_id: str,
    ) -> JobAttempt:
        return await self._repository.create(
            job_id=job_id,
            attempt_number=attempt_number,
            worker_id=worker_id,
        )

    async def get_attempt(self, attempt_id: UUID) -> JobAttempt:
        attempt = await self._repository.get(attempt_id)
        if attempt is None:
            raise JobAttemptNotFoundError(attempt_id)
        return attempt

    async def list_attempts(self, job_id: UUID) -> list[JobAttempt]:
        return await self._repository.list_for_job(job_id)

    async def succeed_attempt(self, attempt_id: UUID) -> JobAttempt:
        attempt = await self.get_attempt(attempt_id)
        validate_attempt_transition(attempt.status, JobAttemptStatus.SUCCEEDED)

        attempt.status = JobAttemptStatus.SUCCEEDED
        attempt.completed_at = datetime.now(UTC)
        return attempt

    async def fail_attempt(
        self,
        attempt_id: UUID,
        error: JobExecutionError,
    ) -> JobAttempt:
        attempt = await self.get_attempt(attempt_id)
        validate_attempt_transition(attempt.status, JobAttemptStatus.FAILED)

        attempt.status = JobAttemptStatus.FAILED
        attempt.failure_kind = error.failure_kind
        attempt.error_code = error.error_code
        attempt.error_message = error.safe_message
        attempt.completed_at = datetime.now(UTC)
        return attempt

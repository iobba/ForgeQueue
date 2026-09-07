from datetime import UTC, datetime
from uuid import UUID, uuid7

import pytest

from forgequeue.db.models import JobAttempt
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempt_service import (
    JobAttemptNotFoundError,
    JobAttemptService,
)
from forgequeue.jobs.attempts import (
    InvalidJobAttemptStatusTransition,
    JobAttemptStatus,
)
from forgequeue.jobs.execution_errors import PermanentJobError, RetryableJobError

pytestmark = [
    pytest.mark.unit,
    pytest.mark.asyncio,
]


class FakeJobAttemptRepository(JobAttemptRepository):
    def __init__(self, attempt: JobAttempt | None) -> None:
        self.attempt = attempt
        self.create_call: tuple[UUID, int, str] | None = None
        self.requested_attempt_id: UUID | None = None
        self.requested_job_id: UUID | None = None
        self.list_result: list[JobAttempt] = [] if attempt is None else [attempt]

    async def create(
        self,
        *,
        job_id: UUID,
        attempt_number: int,
        worker_id: str,
    ) -> JobAttempt:
        self.create_call = (job_id, attempt_number, worker_id)
        if self.attempt is None:
            raise AssertionError("Fake repository has no attempt to return")
        return self.attempt

    async def get(self, attempt_id: UUID) -> JobAttempt | None:
        self.requested_attempt_id = attempt_id
        return self.attempt

    async def list_for_job(self, job_id: UUID) -> list[JobAttempt]:
        self.requested_job_id = job_id
        return self.list_result


def make_attempt(status: JobAttemptStatus) -> JobAttempt:
    return JobAttempt(
        id=uuid7(),
        job_id=uuid7(),
        attempt_number=1,
        worker_id="worker-1",
        status=status,
    )


async def test_start_attempt_delegates_to_repository() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)

    started_attempt = await service.start_attempt(
        job_id=attempt.job_id,
        attempt_number=2,
        worker_id="worker-2",
    )

    assert started_attempt is attempt
    assert repository.create_call == (attempt.job_id, 2, "worker-2")


async def test_get_attempt_returns_existing_attempt() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)

    returned_attempt = await service.get_attempt(attempt.id)

    assert returned_attempt is attempt
    assert repository.requested_attempt_id == attempt.id


async def test_get_attempt_raises_when_attempt_is_missing() -> None:
    repository = FakeJobAttemptRepository(None)
    service = JobAttemptService(repository)
    attempt_id = uuid7()

    with pytest.raises(JobAttemptNotFoundError) as exc_info:
        await service.get_attempt(attempt_id)

    assert exc_info.value.attempt_id == attempt_id
    assert repository.requested_attempt_id == attempt_id


async def test_list_attempts_delegates_to_repository() -> None:
    first_attempt = make_attempt(JobAttemptStatus.FAILED)
    second_attempt = make_attempt(JobAttemptStatus.SUCCEEDED)
    repository = FakeJobAttemptRepository(first_attempt)
    repository.list_result = [first_attempt, second_attempt]
    service = JobAttemptService(repository)

    attempts = await service.list_attempts(first_attempt.job_id)

    assert attempts == [first_attempt, second_attempt]
    assert repository.requested_job_id == first_attempt.job_id


async def test_succeed_attempt_sets_terminal_state_and_time() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    before = datetime.now(UTC)

    succeeded_attempt = await service.succeed_attempt(attempt.id)

    after = datetime.now(UTC)
    assert succeeded_attempt is attempt
    assert attempt.status is JobAttemptStatus.SUCCEEDED
    assert attempt.completed_at is not None
    assert before <= attempt.completed_at <= after
    assert attempt.failure_kind is None
    assert attempt.error_code is None
    assert attempt.error_message is None


async def test_succeed_attempt_rejects_terminal_attempt() -> None:
    attempt = make_attempt(JobAttemptStatus.FAILED)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)

    with pytest.raises(InvalidJobAttemptStatusTransition):
        await service.succeed_attempt(attempt.id)

    assert attempt.status is JobAttemptStatus.FAILED
    assert attempt.completed_at is None


@pytest.mark.parametrize(
    "error",
    [
        RetryableJobError(
            error_code="dependency_unavailable",
            safe_message="Dependency is temporarily unavailable",
        ),
        PermanentJobError(
            error_code="invalid_job_payload",
            safe_message="Stored job payload failed validation",
        ),
    ],
)
async def test_fail_attempt_stores_classified_safe_details(
    error: RetryableJobError | PermanentJobError,
) -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    before = datetime.now(UTC)

    failed_attempt = await service.fail_attempt(attempt.id, error)

    after = datetime.now(UTC)
    assert failed_attempt is attempt
    assert attempt.status is JobAttemptStatus.FAILED
    assert attempt.failure_kind is error.failure_kind
    assert attempt.error_code == error.error_code
    assert attempt.error_message == error.safe_message
    assert attempt.completed_at is not None
    assert before <= attempt.completed_at <= after


async def test_fail_attempt_rejects_terminal_attempt() -> None:
    attempt = make_attempt(JobAttemptStatus.SUCCEEDED)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    error = PermanentJobError(
        error_code="invalid_job_payload",
        safe_message="Stored job payload failed validation",
    )

    with pytest.raises(InvalidJobAttemptStatusTransition):
        await service.fail_attempt(attempt.id, error)

    assert attempt.status is JobAttemptStatus.SUCCEEDED
    assert attempt.failure_kind is None
    assert attempt.error_code is None
    assert attempt.error_message is None
    assert attempt.completed_at is None

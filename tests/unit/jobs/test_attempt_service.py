from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

import pytest

from forgequeue.db.models import JobAttempt
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempt_service import (
    AttemptLeaseExpirationRejectedError,
    AttemptLeaseFinalizationRejectedError,
    AttemptLeaseRenewalRejectedError,
    JobAttemptNotFoundError,
    JobAttemptService,
)
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind
from forgequeue.jobs.execution_errors import PermanentJobError, RetryableJobError
from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy

pytestmark = [
    pytest.mark.unit,
    pytest.mark.asyncio,
]


class FakeJobAttemptRepository(JobAttemptRepository):
    def __init__(self, attempt: JobAttempt | None) -> None:
        self.attempt = attempt
        self.create_call: tuple[UUID, int, str, AttemptLease] | None = None
        self.renew_call: tuple[UUID, str, AttemptLease, AttemptLease] | None = None
        self.renew_result = attempt
        self.succeed_owned_call: tuple[UUID, str, AttemptLease, datetime] | None = None
        self.succeed_owned_result = attempt
        self.fail_owned_call: (
            tuple[
                UUID,
                str,
                AttemptLease,
                JobFailureKind,
                str,
                str,
                datetime,
            ]
            | None
        ) = None
        self.fail_owned_result = attempt
        self.expire_call: (
            tuple[UUID, AttemptLease, JobFailureKind, str, str, datetime] | None
        ) = None
        self.expire_result = attempt
        self.requested_attempt_id: UUID | None = None
        self.requested_job_id: UUID | None = None
        self.list_result: list[JobAttempt] = [] if attempt is None else [attempt]

    async def create(
        self,
        *,
        job_id: UUID,
        attempt_number: int,
        worker_id: str,
        lease: AttemptLease,
    ) -> JobAttempt:
        self.create_call = (job_id, attempt_number, worker_id, lease)
        if self.attempt is None:
            raise AssertionError("Fake repository has no attempt to return")
        return self.attempt

    async def get(self, attempt_id: UUID) -> JobAttempt | None:
        self.requested_attempt_id = attempt_id
        return self.attempt

    async def renew_lease(
        self,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        renewed_lease: AttemptLease,
    ) -> JobAttempt | None:
        self.renew_call = (
            attempt_id,
            worker_id,
            current_lease,
            renewed_lease,
        )
        return self.renew_result

    async def succeed_if_owned(
        self,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        completed_at: datetime,
    ) -> JobAttempt | None:
        self.succeed_owned_call = (
            attempt_id,
            worker_id,
            current_lease,
            completed_at,
        )
        return self.succeed_owned_result

    async def fail_if_owned(
        self,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        failure_kind: JobFailureKind,
        error_code: str,
        error_message: str,
        completed_at: datetime,
    ) -> JobAttempt | None:
        self.fail_owned_call = (
            attempt_id,
            worker_id,
            current_lease,
            failure_kind,
            error_code,
            error_message,
            completed_at,
        )
        return self.fail_owned_result

    async def expire_if_stale(
        self,
        *,
        attempt_id: UUID,
        current_lease: AttemptLease,
        failure_kind: JobFailureKind,
        error_code: str,
        error_message: str,
        expired_at: datetime,
    ) -> JobAttempt | None:
        self.expire_call = (
            attempt_id,
            current_lease,
            failure_kind,
            error_code,
            error_message,
            expired_at,
        )
        return self.expire_result

    async def list_for_job(self, job_id: UUID) -> list[JobAttempt]:
        self.requested_job_id = job_id
        return self.list_result


def make_attempt(status: JobAttemptStatus) -> JobAttempt:
    lease = make_lease()
    return JobAttempt(
        id=uuid7(),
        job_id=uuid7(),
        attempt_number=1,
        worker_id="worker-1",
        status=status,
        started_at=lease.heartbeat_at,
        heartbeat_at=lease.heartbeat_at,
        lease_expires_at=lease.expires_at,
    )


def make_lease() -> AttemptLease:
    return AttemptLeasePolicy().issue(
        heartbeat_at=datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
    )


async def test_start_attempt_delegates_to_repository() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    lease = make_lease()

    started_attempt = await service.start_attempt(
        job_id=attempt.job_id,
        attempt_number=2,
        worker_id="worker-2",
        lease=lease,
    )

    assert started_attempt is attempt
    assert repository.create_call == (attempt.job_id, 2, "worker-2", lease)


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


async def test_renew_attempt_lease_uses_compare_and_swap() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    current_lease = make_lease()
    heartbeat_at = current_lease.heartbeat_at + timedelta(seconds=15)

    renewed_lease = await service.renew_attempt_lease(
        attempt.id,
        worker_id=attempt.worker_id,
        current_lease=current_lease,
        heartbeat_at=heartbeat_at,
    )

    assert renewed_lease == AttemptLease(
        heartbeat_at=heartbeat_at,
        expires_at=heartbeat_at + timedelta(seconds=60),
    )
    assert repository.renew_call == (
        attempt.id,
        attempt.worker_id,
        current_lease,
        renewed_lease,
    )


async def test_renew_attempt_lease_rejects_changed_database_lease() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    repository.renew_result = None
    service = JobAttemptService(repository)
    current_lease = make_lease()

    with pytest.raises(AttemptLeaseRenewalRejectedError) as exc_info:
        await service.renew_attempt_lease(
            attempt.id,
            worker_id=attempt.worker_id,
            current_lease=current_lease,
            heartbeat_at=current_lease.heartbeat_at + timedelta(seconds=15),
        )

    assert exc_info.value.attempt_id == attempt.id


async def test_renew_attempt_lease_rejects_expired_lease_before_write() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    current_lease = make_lease()

    with pytest.raises(AttemptLeaseRenewalRejectedError):
        await service.renew_attempt_lease(
            attempt.id,
            worker_id=attempt.worker_id,
            current_lease=current_lease,
            heartbeat_at=current_lease.expires_at,
        )

    assert repository.renew_call is None


async def test_list_attempts_delegates_to_repository() -> None:
    first_attempt = make_attempt(JobAttemptStatus.FAILED)
    second_attempt = make_attempt(JobAttemptStatus.SUCCEEDED)
    repository = FakeJobAttemptRepository(first_attempt)
    repository.list_result = [first_attempt, second_attempt]
    service = JobAttemptService(repository)

    attempts = await service.list_attempts(first_attempt.job_id)

    assert attempts == [first_attempt, second_attempt]
    assert repository.requested_job_id == first_attempt.job_id


async def test_succeed_owned_attempt_delegates_with_current_lease() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    lease = make_lease()
    completed_at = lease.heartbeat_at + timedelta(seconds=30)

    succeeded_attempt = await service.succeed_owned_attempt(
        attempt.id,
        worker_id=attempt.worker_id,
        current_lease=lease,
        completed_at=completed_at,
    )

    assert succeeded_attempt is attempt
    assert repository.succeed_owned_call == (
        attempt.id,
        attempt.worker_id,
        lease,
        completed_at,
    )


async def test_succeed_owned_attempt_rejects_expired_lease_before_write() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    lease = make_lease()

    with pytest.raises(AttemptLeaseFinalizationRejectedError):
        await service.succeed_owned_attempt(
            attempt.id,
            worker_id=attempt.worker_id,
            current_lease=lease,
            completed_at=lease.expires_at,
        )

    assert repository.succeed_owned_call is None


async def test_fail_owned_attempt_delegates_safe_failure_details() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    lease = make_lease()
    completed_at = lease.heartbeat_at + timedelta(seconds=30)
    error = PermanentJobError(
        error_code="invalid_job_payload",
        safe_message="Stored job payload failed validation",
    )

    failed_attempt = await service.fail_owned_attempt(
        attempt.id,
        error,
        worker_id=attempt.worker_id,
        current_lease=lease,
        completed_at=completed_at,
    )

    assert failed_attempt is attempt
    assert repository.fail_owned_call == (
        attempt.id,
        attempt.worker_id,
        lease,
        error.failure_kind,
        error.error_code,
        error.safe_message,
        completed_at,
    )


async def test_fail_owned_attempt_rejects_changed_database_lease() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    repository.fail_owned_result = None
    service = JobAttemptService(repository)
    lease = make_lease()
    error = PermanentJobError(
        error_code="invalid_job_payload",
        safe_message="Stored job payload failed validation",
    )

    with pytest.raises(AttemptLeaseFinalizationRejectedError) as exc_info:
        await service.fail_owned_attempt(
            attempt.id,
            error,
            worker_id=attempt.worker_id,
            current_lease=lease,
            completed_at=lease.heartbeat_at + timedelta(seconds=30),
        )

    assert exc_info.value.attempt_id == attempt.id


async def test_expire_attempt_delegates_after_lease_deadline() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    lease = make_lease()
    expired_at = lease.expires_at + timedelta(seconds=1)
    error = RetryableJobError(
        error_code="worker_lease_expired",
        safe_message="The worker stopped renewing its attempt lease",
    )

    expired_attempt = await service.expire_attempt(
        attempt.id,
        error,
        current_lease=lease,
        expired_at=expired_at,
    )

    assert expired_attempt is attempt
    assert repository.expire_call == (
        attempt.id,
        lease,
        error.failure_kind,
        error.error_code,
        error.safe_message,
        expired_at,
    )


async def test_expire_attempt_rejects_active_lease_before_write() -> None:
    attempt = make_attempt(JobAttemptStatus.RUNNING)
    repository = FakeJobAttemptRepository(attempt)
    service = JobAttemptService(repository)
    lease = make_lease()
    error = RetryableJobError(
        error_code="worker_lease_expired",
        safe_message="The worker stopped renewing its attempt lease",
    )

    with pytest.raises(AttemptLeaseExpirationRejectedError):
        await service.expire_attempt(
            attempt.id,
            error,
            current_lease=lease,
            expired_at=lease.expires_at - timedelta(seconds=1),
        )

    assert repository.expire_call is None

from datetime import datetime
from uuid import UUID

from forgequeue.db.models import JobAttempt
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.execution_errors import JobExecutionError
from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy


class JobAttemptNotFoundError(LookupError):
    def __init__(self, attempt_id: UUID) -> None:
        self.attempt_id = attempt_id
        super().__init__(f"Job attempt {attempt_id} was not found")


class AttemptLeaseRenewalRejectedError(RuntimeError):
    def __init__(self, attempt_id: UUID) -> None:
        self.attempt_id = attempt_id
        super().__init__(f"Lease renewal was rejected for job attempt {attempt_id}")


class AttemptLeaseFinalizationRejectedError(RuntimeError):
    def __init__(self, attempt_id: UUID) -> None:
        self.attempt_id = attempt_id
        super().__init__(
            f"Lease-owned finalization was rejected for job attempt {attempt_id}"
        )


class AttemptLeaseExpirationRejectedError(RuntimeError):
    def __init__(self, attempt_id: UUID) -> None:
        self.attempt_id = attempt_id
        super().__init__(f"Lease expiration was rejected for job attempt {attempt_id}")


class JobAttemptService:
    def __init__(
        self,
        repository: JobAttemptRepository,
        *,
        lease_policy: AttemptLeasePolicy | None = None,
    ) -> None:
        self._repository = repository
        self._lease_policy = lease_policy or AttemptLeasePolicy()

    async def start_attempt(
        self,
        *,
        job_id: UUID,
        attempt_number: int,
        worker_id: str,
        lease: AttemptLease,
    ) -> JobAttempt:
        return await self._repository.create(
            job_id=job_id,
            attempt_number=attempt_number,
            worker_id=worker_id,
            lease=lease,
        )

    async def get_attempt(self, attempt_id: UUID) -> JobAttempt:
        attempt = await self._repository.get(attempt_id)
        if attempt is None:
            raise JobAttemptNotFoundError(attempt_id)
        return attempt

    async def renew_attempt_lease(
        self,
        attempt_id: UUID,
        *,
        worker_id: str,
        current_lease: AttemptLease,
        heartbeat_at: datetime,
    ) -> AttemptLease:
        try:
            renewed_lease = self._lease_policy.renew(
                current_lease,
                heartbeat_at=heartbeat_at,
            )
        except ValueError as exc:
            raise AttemptLeaseRenewalRejectedError(attempt_id) from exc

        renewed_attempt = await self._repository.renew_lease(
            attempt_id=attempt_id,
            worker_id=worker_id,
            current_lease=current_lease,
            renewed_lease=renewed_lease,
        )
        if renewed_attempt is None:
            raise AttemptLeaseRenewalRejectedError(attempt_id)
        return renewed_lease

    async def list_attempts(self, job_id: UUID) -> list[JobAttempt]:
        return await self._repository.list_for_job(job_id)

    async def succeed_owned_attempt(
        self,
        attempt_id: UUID,
        *,
        worker_id: str,
        current_lease: AttemptLease,
        completed_at: datetime,
    ) -> JobAttempt:
        self._validate_finalization_time(
            attempt_id,
            current_lease=current_lease,
            completed_at=completed_at,
        )
        attempt = await self._repository.succeed_if_owned(
            attempt_id=attempt_id,
            worker_id=worker_id,
            current_lease=current_lease,
            completed_at=completed_at,
        )
        if attempt is None:
            raise AttemptLeaseFinalizationRejectedError(attempt_id)
        return attempt

    async def fail_owned_attempt(
        self,
        attempt_id: UUID,
        error: JobExecutionError,
        *,
        worker_id: str,
        current_lease: AttemptLease,
        completed_at: datetime,
    ) -> JobAttempt:
        self._validate_finalization_time(
            attempt_id,
            current_lease=current_lease,
            completed_at=completed_at,
        )
        attempt = await self._repository.fail_if_owned(
            attempt_id=attempt_id,
            worker_id=worker_id,
            current_lease=current_lease,
            failure_kind=error.failure_kind,
            error_code=error.error_code,
            error_message=error.safe_message,
            completed_at=completed_at,
        )
        if attempt is None:
            raise AttemptLeaseFinalizationRejectedError(attempt_id)
        return attempt

    async def expire_attempt(
        self,
        attempt_id: UUID,
        error: JobExecutionError,
        *,
        current_lease: AttemptLease,
        expired_at: datetime,
    ) -> JobAttempt:
        if not self._lease_policy.is_expired(
            current_lease,
            observed_at=expired_at,
        ):
            raise AttemptLeaseExpirationRejectedError(attempt_id)

        attempt = await self._repository.expire_if_stale(
            attempt_id=attempt_id,
            current_lease=current_lease,
            failure_kind=error.failure_kind,
            error_code=error.error_code,
            error_message=error.safe_message,
            expired_at=expired_at,
        )
        if attempt is None:
            raise AttemptLeaseExpirationRejectedError(attempt_id)
        return attempt

    def _validate_finalization_time(
        self,
        attempt_id: UUID,
        *,
        current_lease: AttemptLease,
        completed_at: datetime,
    ) -> None:
        if completed_at < current_lease.heartbeat_at or self._lease_policy.is_expired(
            current_lease,
            observed_at=completed_at,
        ):
            raise AttemptLeaseFinalizationRejectedError(attempt_id)

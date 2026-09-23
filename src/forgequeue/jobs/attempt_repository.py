from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.db.models import JobAttempt
from forgequeue.jobs.attempts import JobAttemptStatus, JobFailureKind
from forgequeue.jobs.leases import AttemptLease


class JobAttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        job_id: UUID,
        attempt_number: int,
        worker_id: str,
        lease: AttemptLease,
    ) -> JobAttempt:
        attempt = JobAttempt(
            job_id=job_id,
            attempt_number=attempt_number,
            worker_id=worker_id,
            started_at=lease.heartbeat_at,
            heartbeat_at=lease.heartbeat_at,
            lease_expires_at=lease.expires_at,
        )
        self._session.add(attempt)
        await self._session.flush()
        await self._session.refresh(attempt)
        return attempt

    async def get(self, attempt_id: UUID) -> JobAttempt | None:
        return await self._session.get(JobAttempt, attempt_id)

    async def renew_lease(
        self,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        renewed_lease: AttemptLease,
    ) -> JobAttempt | None:
        statement = (
            update(JobAttempt)
            .where(
                JobAttempt.id == attempt_id,
                JobAttempt.worker_id == worker_id,
                JobAttempt.status == JobAttemptStatus.RUNNING,
                JobAttempt.heartbeat_at == current_lease.heartbeat_at,
                JobAttempt.lease_expires_at == current_lease.expires_at,
            )
            .values(
                heartbeat_at=renewed_lease.heartbeat_at,
                lease_expires_at=renewed_lease.expires_at,
            )
            .returning(JobAttempt)
        )
        return await self._session.scalar(statement)

    async def succeed_if_owned(
        self,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        completed_at: datetime,
    ) -> JobAttempt | None:
        statement = (
            update(JobAttempt)
            .where(
                JobAttempt.id == attempt_id,
                JobAttempt.worker_id == worker_id,
                JobAttempt.status == JobAttemptStatus.RUNNING,
                JobAttempt.heartbeat_at == current_lease.heartbeat_at,
                JobAttempt.lease_expires_at == current_lease.expires_at,
                JobAttempt.lease_expires_at > completed_at,
            )
            .values(
                status=JobAttemptStatus.SUCCEEDED,
                completed_at=completed_at,
            )
            .returning(JobAttempt)
        )
        return await self._session.scalar(statement)

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
        statement = (
            update(JobAttempt)
            .where(
                JobAttempt.id == attempt_id,
                JobAttempt.worker_id == worker_id,
                JobAttempt.status == JobAttemptStatus.RUNNING,
                JobAttempt.heartbeat_at == current_lease.heartbeat_at,
                JobAttempt.lease_expires_at == current_lease.expires_at,
                JobAttempt.lease_expires_at > completed_at,
            )
            .values(
                status=JobAttemptStatus.FAILED,
                failure_kind=failure_kind,
                error_code=error_code,
                error_message=error_message,
                completed_at=completed_at,
            )
            .returning(JobAttempt)
        )
        return await self._session.scalar(statement)

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
        statement = (
            update(JobAttempt)
            .where(
                JobAttempt.id == attempt_id,
                JobAttempt.status == JobAttemptStatus.RUNNING,
                JobAttempt.heartbeat_at == current_lease.heartbeat_at,
                JobAttempt.lease_expires_at == current_lease.expires_at,
                JobAttempt.lease_expires_at <= expired_at,
            )
            .values(
                status=JobAttemptStatus.FAILED,
                failure_kind=failure_kind,
                error_code=error_code,
                error_message=error_message,
                completed_at=expired_at,
            )
            .returning(JobAttempt)
        )
        return await self._session.scalar(statement)

    async def get_for_job_attempt(
        self,
        *,
        job_id: UUID,
        attempt_number: int,
    ) -> JobAttempt | None:
        query = select(JobAttempt).where(
            JobAttempt.job_id == job_id,
            JobAttempt.attempt_number == attempt_number,
        )
        return await self._session.scalar(query)

    async def list_for_job(self, job_id: UUID) -> list[JobAttempt]:
        query = (
            select(JobAttempt)
            .where(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.attempt_number.asc())
        )
        attempts = await self._session.scalars(query)
        return list(attempts)

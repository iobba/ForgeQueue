from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.db.models import JobAttempt


class JobAttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        job_id: UUID,
        attempt_number: int,
        worker_id: str,
    ) -> JobAttempt:
        attempt = JobAttempt(
            job_id=job_id,
            attempt_number=attempt_number,
            worker_id=worker_id,
        )
        self._session.add(attempt)
        await self._session.flush()
        await self._session.refresh(attempt)
        return attempt

    async def get(self, attempt_id: UUID) -> JobAttempt | None:
        return await self._session.get(JobAttempt, attempt_id)

    async def list_for_job(self, job_id: UUID) -> list[JobAttempt]:
        query = (
            select(JobAttempt)
            .where(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.attempt_number.asc())
        )
        attempts = await self._session.scalars(query)
        return list(attempts)

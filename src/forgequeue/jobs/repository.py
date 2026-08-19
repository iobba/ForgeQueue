from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from forgequeue.db.models import Job
from forgequeue.jobs.status import JobStatus


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        job_type: str,
        payload: dict[str, object],
        max_attempts: int = 1,
    ) -> Job:
        new_job = Job(job_type=job_type, payload=payload, max_attempts=max_attempts)
        self._session.add(new_job)
        await self._session.flush()
        await self._session.refresh(new_job)
        return new_job

    async def get(self, job_id: UUID) -> Job | None:
        return await self._session.get(Job, job_id)

    async def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        query = select(Job)
        if status is not None:
            query = query.where(Job.status == status)

        query = query.order_by(Job.created_at.desc(), Job.id.desc())

        query = query.limit(limit).offset(offset)

        jobs = await self._session.scalars(query)

        return list(jobs)

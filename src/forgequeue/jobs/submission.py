from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import JobMessage
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService


class JobSubmissionService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RedisJobBroker,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker

    async def submit(
        self,
        *,
        job_type: str,
        payload: dict[str, object],
        max_attempts: int = 1,
    ) -> Job:
        async with self._session_factory.begin() as session:
            service = JobService(JobRepository(session))
            job = await service.create_job(
                job_type=job_type,
                payload=payload,
                max_attempts=max_attempts,
            )

        await self._broker.publish(
            JobMessage(
                job_id=job.id,
                job_type=job.job_type,
            )
        )
        return job

from typing import cast

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import JobMessage
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus
from forgequeue.jobs.submission import JobSubmissionService

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]


class FailingBroker:
    def __init__(self) -> None:
        self.published_message: JobMessage | None = None

    async def publish(self, message: JobMessage) -> str:
        self.published_message = message
        raise ConnectionError("Redis is unavailable")


async def test_publish_failure_leaves_committed_job_queued(
    database_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    broker = FailingBroker()
    service = JobSubmissionService(
        database_session_factory,
        cast(RedisJobBroker, broker),
    )

    with pytest.raises(ConnectionError, match="Redis is unavailable"):
        await service.submit(
            job_type="sum_numbers",
            payload={"numbers": [10, 20, 30]},
        )

    message = broker.published_message
    assert message is not None

    try:
        async with database_session_factory() as session:
            persisted_job = await JobRepository(session).get(message.job_id)

        assert persisted_job is not None
        assert persisted_job.status is JobStatus.QUEUED
        assert persisted_job.job_type == message.job_type
        assert persisted_job.payload == {"numbers": [10, 20, 30]}
    finally:
        async with database_session_factory.begin() as session:
            await session.execute(delete(Job).where(Job.id == message.job_id))

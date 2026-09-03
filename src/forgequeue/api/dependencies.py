from collections.abc import AsyncIterator
from typing import Annotated, cast

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.redis import RedisJobBroker
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService
from forgequeue.jobs.submission import JobSubmissionService


async def get_database_session(
    request: Request,
) -> AsyncIterator[AsyncSession]:
    session_factory = cast(
        async_sessionmaker[AsyncSession],
        request.app.state.db_session_factory,
    )

    async with session_factory.begin() as session:
        yield session


DatabaseSession = Annotated[
    AsyncSession,
    Depends(get_database_session, scope="function"),
]


def get_job_service(session: DatabaseSession) -> JobService:
    repository = JobRepository(session=session)
    return JobService(repository=repository)


JobServiceDependency = Annotated[
    JobService,
    Depends(get_job_service),
]


def get_job_submission_service(request: Request) -> JobSubmissionService:
    session_factory = cast(
        async_sessionmaker[AsyncSession],
        request.app.state.db_session_factory,
    )
    broker = cast(
        RedisJobBroker,
        request.app.state.job_broker,
    )
    return JobSubmissionService(session_factory, broker)


JobSubmissionServiceDependency = Annotated[
    JobSubmissionService,
    Depends(get_job_submission_service),
]

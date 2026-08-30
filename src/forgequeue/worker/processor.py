from uuid import UUID

import structlog
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import ReceivedJobMessage
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.service import JobService
from forgequeue.worker.handlers import UnsupportedJobTypeError, get_handler

INVALID_JOB_PAYLOAD_ERROR_CODE = "invalid_job_payload"
UNSUPPORTED_JOB_TYPE_ERROR_CODE = "unsupported_job_type"

logger = structlog.get_logger(__name__)


class JobMessageMismatchError(ValueError):
    def __init__(
        self,
        *,
        job_id: UUID,
        message_job_type: str,
        database_job_type: str,
    ) -> None:
        self.job_id = job_id
        self.message_job_type = message_job_type
        self.database_job_type = database_job_type
        super().__init__(
            f"Job {job_id} message type {message_job_type!r} does not match "
            f"database type {database_job_type!r}"
        )


class JobProcessor:
    def __init__(
        self,
        broker: RedisJobBroker,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._broker = broker
        self._session_factory = session_factory

    async def process(self, delivery: ReceivedJobMessage) -> None:
        job_id = delivery.message.job_id

        async with self._session_factory.begin() as session:
            service = JobService(JobRepository(session))
            job = await service.start_job(job_id)

            if job.job_type != delivery.message.job_type:
                raise JobMessageMismatchError(
                    job_id=job_id,
                    message_job_type=delivery.message.job_type,
                    database_job_type=job.job_type,
                )

            job_type = job.job_type
            payload = dict(job.payload)

        logger.info("job_processing_started")

        try:
            handler = get_handler(job_type)
            result = handler(payload)
        except UnsupportedJobTypeError as exc:
            await self._persist_failure(
                job_id,
                error_code=UNSUPPORTED_JOB_TYPE_ERROR_CODE,
                error_message=f"No handler is registered for job type {exc.job_type!r}",
            )
            logger.warning(
                "job_failed",
                error_code=UNSUPPORTED_JOB_TYPE_ERROR_CODE,
            )
        except ValidationError:
            await self._persist_failure(
                job_id,
                error_code=INVALID_JOB_PAYLOAD_ERROR_CODE,
                error_message="Stored job payload failed validation",
            )
            logger.warning(
                "job_failed",
                error_code=INVALID_JOB_PAYLOAD_ERROR_CODE,
            )
        else:
            async with self._session_factory.begin() as session:
                service = JobService(JobRepository(session))
                await service.complete_job(job_id, result)
            logger.info("job_completed")

        acknowledged_count = await self._broker.acknowledge(delivery.entry_id)
        logger.info(
            "job_delivery_acknowledged",
            acknowledged_count=acknowledged_count,
        )

    async def _persist_failure(
        self,
        job_id: UUID,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        async with self._session_factory.begin() as session:
            service = JobService(JobRepository(session))
            await service.fail_job(
                job_id,
                error_code=error_code,
                error_message=error_message,
            )

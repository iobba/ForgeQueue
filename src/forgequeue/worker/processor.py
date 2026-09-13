from collections.abc import Callable
from datetime import UTC, datetime
from random import random
from uuid import UUID

import structlog
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import ReceivedJobMessage
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempt_service import JobAttemptService
from forgequeue.jobs.execution_errors import JobExecutionError, PermanentJobError
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.retry_policy import RetryPolicy
from forgequeue.jobs.service import JobService
from forgequeue.worker.handlers import UnsupportedJobTypeError, get_handler

INVALID_JOB_PAYLOAD_ERROR_CODE = "invalid_job_payload"
UNSUPPORTED_JOB_TYPE_ERROR_CODE = "unsupported_job_type"

logger = structlog.get_logger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


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
        *,
        retry_policy: RetryPolicy | None = None,
        jitter_source: Callable[[], float] = random,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._broker = broker
        self._session_factory = session_factory
        self._retry_policy = retry_policy or RetryPolicy()
        self._jitter_source = jitter_source
        self._clock = clock

    async def process(
        self,
        delivery: ReceivedJobMessage,
        *,
        worker_id: str,
    ) -> None:
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

            attempt_service = JobAttemptService(JobAttemptRepository(session))
            attempt = await attempt_service.start_attempt(
                job_id=job.id,
                attempt_number=job.attempts,
                worker_id=worker_id,
            )
            attempt_id = attempt.id
            attempt_number = attempt.attempt_number
            max_attempts = job.max_attempts
            job_type = job.job_type
            payload = dict(job.payload)

        logger.info("job_processing_started")

        try:
            handler = get_handler(job_type)
            result = handler(payload)
        except UnsupportedJobTypeError as exc:
            error = PermanentJobError(
                error_code=UNSUPPORTED_JOB_TYPE_ERROR_CODE,
                safe_message=(
                    f"No handler is registered for job type {exc.job_type!r}"
                ),
            )
        except ValidationError:
            error = PermanentJobError(
                error_code=INVALID_JOB_PAYLOAD_ERROR_CODE,
                safe_message="Stored job payload failed validation",
            )
        except JobExecutionError as exc:
            error = exc
        else:
            async with self._session_factory.begin() as session:
                service = JobService(JobRepository(session))
                await service.complete_job(job_id, result)
                attempt_service = JobAttemptService(JobAttemptRepository(session))
                await attempt_service.succeed_attempt(attempt_id)
            logger.info("job_completed")
            error = None

        if error is not None:
            next_attempt_at = await self._persist_failure(
                job_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                max_attempts=max_attempts,
                error=error,
            )
            if next_attempt_at is None:
                logger.warning(
                    "job_failed",
                    error_code=error.error_code,
                )
            else:
                logger.warning(
                    "job_retry_scheduled",
                    error_code=error.error_code,
                    next_attempt_at=next_attempt_at.isoformat(),
                )

        acknowledged_count = await self._broker.acknowledge(delivery.entry_id)
        logger.info(
            "job_delivery_acknowledged",
            acknowledged_count=acknowledged_count,
        )

    async def _persist_failure(
        self,
        job_id: UUID,
        *,
        attempt_id: UUID,
        attempt_number: int,
        max_attempts: int,
        error: JobExecutionError,
    ) -> datetime | None:
        should_retry = self._retry_policy.can_retry(
            failure_kind=error.failure_kind,
            attempt_number=attempt_number,
            max_attempts=max_attempts,
        )
        delay = (
            self._retry_policy.delay_after(
                attempt_number,
                jitter_fraction=self._jitter_source(),
            )
            if should_retry
            else None
        )

        async with self._session_factory.begin() as session:
            service = JobService(JobRepository(session))
            if delay is None:
                await service.fail_job(
                    job_id,
                    error_code=error.error_code,
                    error_message=error.safe_message,
                )
                next_attempt_at = None
            else:
                scheduled_job = await service.schedule_retry(
                    job_id,
                    error=error,
                    delay=delay,
                    scheduled_at=self._clock(),
                )
                next_attempt_at = scheduled_job.next_attempt_at

            attempt_service = JobAttemptService(JobAttemptRepository(session))
            await attempt_service.fail_attempt(attempt_id, error)

        return next_attempt_at

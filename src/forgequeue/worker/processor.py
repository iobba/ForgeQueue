import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from random import random
from uuid import UUID

import structlog
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import (
    DeadLetterMessage,
    DeadLetterReason,
    JobDelivery,
    MalformedJobDelivery,
    ReceivedJobMessage,
)
from forgequeue.broker.redis import RedisDeadLetterBroker, RedisJobBroker
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempt_service import JobAttemptService
from forgequeue.jobs.attempts import JobFailureKind
from forgequeue.jobs.execution_errors import (
    JobExecutionError,
    PermanentJobError,
    RetryableJobError,
)
from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.retry_policy import RetryPolicy
from forgequeue.jobs.service import JobService
from forgequeue.worker.handlers import (
    JobHandler,
    JobResult,
    UnsupportedJobTypeError,
    get_handler,
)

INVALID_JOB_PAYLOAD_ERROR_CODE = "invalid_job_payload"
UNSUPPORTED_JOB_TYPE_ERROR_CODE = "unsupported_job_type"
WORKER_LEASE_EXPIRED_ERROR_CODE = "worker_lease_expired"

logger = structlog.get_logger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class HandlerExecution:
    lease: AttemptLease
    result: JobResult | None = None
    error: Exception | None = None


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
        dead_letter_broker: RedisDeadLetterBroker,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        retry_policy: RetryPolicy | None = None,
        lease_policy: AttemptLeasePolicy | None = None,
        jitter_source: Callable[[], float] = random,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._broker = broker
        self._dead_letter_broker = dead_letter_broker
        self._session_factory = session_factory
        self._retry_policy = retry_policy or RetryPolicy()
        self._lease_policy = lease_policy or AttemptLeasePolicy()
        self._jitter_source = jitter_source
        self._clock = clock

    async def process(
        self,
        delivery: JobDelivery,
        *,
        worker_id: str,
    ) -> None:
        if isinstance(delivery, MalformedJobDelivery):
            await self.quarantine_malformed(delivery)
            return

        job_id = delivery.message.job_id
        started_at = self._clock()

        async with self._session_factory.begin() as session:
            service = JobService(JobRepository(session))
            job = await service.start_job(job_id, started_at=started_at)

            if job.job_type != delivery.message.job_type:
                raise JobMessageMismatchError(
                    job_id=job_id,
                    message_job_type=delivery.message.job_type,
                    database_job_type=job.job_type,
                )

            attempt_service = JobAttemptService(JobAttemptRepository(session))
            lease = self._lease_policy.issue(heartbeat_at=started_at)
            attempt = await attempt_service.start_attempt(
                job_id=job.id,
                attempt_number=job.attempts,
                worker_id=worker_id,
                lease=lease,
            )
            attempt_id = attempt.id
            attempt_number = attempt.attempt_number
            max_attempts = job.max_attempts
            job_type = job.job_type
            payload = dict(job.payload)

        logger.info("job_processing_started")

        try:
            handler = get_handler(job_type)
            execution = await self._run_handler_with_heartbeats(
                handler,
                payload,
                attempt_id=attempt_id,
                worker_id=worker_id,
                lease=lease,
            )
            lease = execution.lease
            if execution.error is not None:
                raise execution.error
            result = execution.result
            if result is None:
                raise RuntimeError("Job handler returned no result")
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
            completed_at = self._clock()
            async with self._session_factory.begin() as session:
                attempt_service = JobAttemptService(
                    JobAttemptRepository(session),
                    lease_policy=self._lease_policy,
                )
                await attempt_service.succeed_owned_attempt(
                    attempt_id,
                    worker_id=worker_id,
                    current_lease=lease,
                    completed_at=completed_at,
                )
                service = JobService(JobRepository(session))
                await service.complete_job(job_id, result)
            logger.info("job_completed")
            error = None

        if error is not None:
            failed_at = self._clock()
            next_attempt_at = await self._persist_failure(
                job_id,
                attempt_id=attempt_id,
                worker_id=worker_id,
                current_lease=lease,
                failed_at=failed_at,
                attempt_number=attempt_number,
                max_attempts=max_attempts,
                job_type=job_type,
                source_entry_id=delivery.entry_id,
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

    async def _run_handler_with_heartbeats(
        self,
        handler: JobHandler,
        payload: dict[str, object],
        *,
        attempt_id: UUID,
        worker_id: str,
        lease: AttemptLease,
    ) -> HandlerExecution:
        handler_task = asyncio.create_task(asyncio.to_thread(handler, payload))
        current_lease = lease
        heartbeat_interval = self._lease_policy.heartbeat_interval.total_seconds()

        try:
            while True:
                completed, _ = await asyncio.wait(
                    {handler_task},
                    timeout=heartbeat_interval,
                )
                if handler_task in completed:
                    try:
                        result = await handler_task
                    except Exception as exc:
                        return HandlerExecution(lease=current_lease, error=exc)
                    return HandlerExecution(lease=current_lease, result=result)

                heartbeat_at = self._clock()
                async with self._session_factory.begin() as session:
                    attempt_service = JobAttemptService(
                        JobAttemptRepository(session),
                        lease_policy=self._lease_policy,
                    )
                    current_lease = await attempt_service.renew_attempt_lease(
                        attempt_id,
                        worker_id=worker_id,
                        current_lease=current_lease,
                        heartbeat_at=heartbeat_at,
                    )
                logger.debug(
                    "job_attempt_lease_renewed",
                    lease_expires_at=current_lease.expires_at.isoformat(),
                )
        finally:
            if not handler_task.done():
                handler_task.cancel()
                await asyncio.gather(handler_task, return_exceptions=True)

    async def _persist_failure(
        self,
        job_id: UUID,
        *,
        attempt_id: UUID,
        worker_id: str,
        current_lease: AttemptLease,
        failed_at: datetime,
        attempt_number: int,
        max_attempts: int,
        job_type: str,
        source_entry_id: str,
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
            attempt_service = JobAttemptService(
                JobAttemptRepository(session),
                lease_policy=self._lease_policy,
            )
            await attempt_service.fail_owned_attempt(
                attempt_id,
                error,
                worker_id=worker_id,
                current_lease=current_lease,
                completed_at=failed_at,
            )

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
                    scheduled_at=failed_at,
                )
                next_attempt_at = scheduled_job.next_attempt_at

            if delay is None:
                reason = (
                    DeadLetterReason.PERMANENT_FAILURE
                    if error.failure_kind is JobFailureKind.PERMANENT
                    else DeadLetterReason.RETRIES_EXHAUSTED
                )
                await self._dead_letter_broker.publish(
                    DeadLetterMessage(
                        source_entry_id=source_entry_id,
                        job_id=job_id,
                        job_type=job_type,
                        attempt_number=attempt_number,
                        reason=reason,
                        failure_kind=error.failure_kind.value,
                        error_code=error.error_code,
                    )
                )

        return next_attempt_at

    async def recover_expired_attempt(
        self,
        delivery: ReceivedJobMessage,
        *,
        attempt_id: UUID,
        current_lease: AttemptLease,
        attempt_number: int,
        max_attempts: int,
        expired_at: datetime,
    ) -> datetime | None:
        error = RetryableJobError(
            error_code=WORKER_LEASE_EXPIRED_ERROR_CODE,
            safe_message="The worker stopped renewing its attempt lease",
        )
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
        job_id = delivery.message.job_id

        async with self._session_factory.begin() as session:
            attempt_service = JobAttemptService(
                JobAttemptRepository(session),
                lease_policy=self._lease_policy,
            )
            await attempt_service.expire_attempt(
                attempt_id,
                error,
                current_lease=current_lease,
                expired_at=expired_at,
            )

            service = JobService(JobRepository(session))
            if delay is None:
                await service.fail_job(
                    job_id,
                    error_code=error.error_code,
                    error_message=error.safe_message,
                )
                await self._dead_letter_broker.publish(
                    DeadLetterMessage(
                        source_entry_id=delivery.entry_id,
                        job_id=job_id,
                        job_type=delivery.message.job_type,
                        attempt_number=attempt_number,
                        reason=DeadLetterReason.RETRIES_EXHAUSTED,
                        failure_kind=error.failure_kind.value,
                        error_code=error.error_code,
                    )
                )
                next_attempt_at = None
            else:
                scheduled_job = await service.schedule_retry(
                    job_id,
                    error=error,
                    delay=delay,
                    scheduled_at=expired_at,
                )
                next_attempt_at = scheduled_job.next_attempt_at

        acknowledged_count = await self._broker.acknowledge(delivery.entry_id)
        logger.warning(
            "expired_job_attempt_recovered",
            attempt_number=attempt_number,
            next_attempt_at=(
                next_attempt_at.isoformat() if next_attempt_at is not None else None
            ),
            acknowledged_count=acknowledged_count,
        )
        return next_attempt_at

    async def quarantine_malformed(self, delivery: MalformedJobDelivery) -> None:
        dead_letter_entry_id = await self._dead_letter_broker.publish(
            DeadLetterMessage(
                source_entry_id=delivery.entry_id,
                reason=DeadLetterReason.MALFORMED_MESSAGE,
                error_code=delivery.error_code,
            )
        )
        acknowledged_count = await self._broker.acknowledge(delivery.entry_id)
        logger.warning(
            "malformed_job_delivery_quarantined",
            source_entry_id=delivery.entry_id,
            dead_letter_entry_id=dead_letter_entry_id,
            acknowledged_count=acknowledged_count,
        )

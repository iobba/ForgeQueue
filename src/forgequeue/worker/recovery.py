from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from math import isfinite
from time import monotonic
from typing import Protocol, assert_never

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.messages import JobDelivery, MalformedJobDelivery
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus
from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus
from forgequeue.worker.processor import JobProcessor


def utc_now() -> datetime:
    return datetime.now(UTC)


class ReclaimedDeliveryAction(StrEnum):
    PROCESS = "process"
    RECOVER = "recover"
    ACKNOWLEDGE = "acknowledge"
    LEAVE_PENDING = "leave_pending"


class ReclaimedDeliveryReason(StrEnum):
    JOB_READY = "job_ready"
    JOB_LEASE_EXPIRED = "job_lease_expired"
    JOB_TERMINAL = "job_terminal"
    JOB_RUNNING = "job_running"
    RETRY_SCHEDULED = "retry_scheduled"
    JOB_MISSING = "job_missing"
    JOB_TYPE_MISMATCH = "job_type_mismatch"
    MALFORMED_MESSAGE = "malformed_message"


@dataclass(frozen=True, slots=True)
class ReclaimedDeliveryDecision:
    action: ReclaimedDeliveryAction
    reason: ReclaimedDeliveryReason


@dataclass(frozen=True, slots=True)
class ReclaimedDeliveryOutcome:
    entry_id: str
    decision: ReclaimedDeliveryDecision


@dataclass(frozen=True, slots=True)
class RecoveryBatchResult:
    next_start_id: str
    outcomes: list[ReclaimedDeliveryOutcome]
    deleted_entry_ids: list[str]


class RecoveryBatchCoordinator(Protocol):
    async def recover_batch(
        self,
        *,
        worker_id: str,
        min_idle_ms: int,
        start_id: str,
        count: int,
    ) -> RecoveryBatchResult: ...


def decide_reclaimed_delivery(
    *,
    message_job_type: str,
    job_status: JobStatus | None,
    database_job_type: str | None,
    running_attempt_lease_expired: bool = False,
) -> ReclaimedDeliveryDecision:
    if job_status is None or database_job_type is None:
        return ReclaimedDeliveryDecision(
            action=ReclaimedDeliveryAction.LEAVE_PENDING,
            reason=ReclaimedDeliveryReason.JOB_MISSING,
        )

    if message_job_type != database_job_type:
        return ReclaimedDeliveryDecision(
            action=ReclaimedDeliveryAction.LEAVE_PENDING,
            reason=ReclaimedDeliveryReason.JOB_TYPE_MISMATCH,
        )

    match job_status:
        case JobStatus.QUEUED:
            return ReclaimedDeliveryDecision(
                action=ReclaimedDeliveryAction.PROCESS,
                reason=ReclaimedDeliveryReason.JOB_READY,
            )
        case JobStatus.RUNNING:
            if running_attempt_lease_expired:
                return ReclaimedDeliveryDecision(
                    action=ReclaimedDeliveryAction.RECOVER,
                    reason=ReclaimedDeliveryReason.JOB_LEASE_EXPIRED,
                )
            return ReclaimedDeliveryDecision(
                action=ReclaimedDeliveryAction.LEAVE_PENDING,
                reason=ReclaimedDeliveryReason.JOB_RUNNING,
            )
        case JobStatus.RETRY_SCHEDULED:
            return ReclaimedDeliveryDecision(
                action=ReclaimedDeliveryAction.LEAVE_PENDING,
                reason=ReclaimedDeliveryReason.RETRY_SCHEDULED,
            )
        case JobStatus.COMPLETED | JobStatus.FAILED:
            return ReclaimedDeliveryDecision(
                action=ReclaimedDeliveryAction.ACKNOWLEDGE,
                reason=ReclaimedDeliveryReason.JOB_TERMINAL,
            )

    assert_never(job_status)


class ReclaimedDeliveryCoordinator:
    def __init__(
        self,
        broker: RedisJobBroker,
        processor: JobProcessor,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._broker = broker
        self._processor = processor
        self._session_factory = session_factory
        self._clock = clock

    async def recover_one(
        self,
        delivery: JobDelivery,
        *,
        worker_id: str,
    ) -> ReclaimedDeliveryDecision:
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")

        if isinstance(delivery, MalformedJobDelivery):
            await self._processor.quarantine_malformed(delivery)
            return ReclaimedDeliveryDecision(
                action=ReclaimedDeliveryAction.ACKNOWLEDGE,
                reason=ReclaimedDeliveryReason.MALFORMED_MESSAGE,
            )

        attempt = None
        async with self._session_factory() as session:
            job = await JobRepository(session).get(delivery.message.job_id)
            job_status = job.status if job is not None else None
            database_job_type = job.job_type if job is not None else None
            if (
                job is not None
                and job.status is JobStatus.RUNNING
                and job.attempts >= 1
            ):
                attempt = await JobAttemptRepository(session).get_for_job_attempt(
                    job_id=job.id,
                    attempt_number=job.attempts,
                )

        observed_at = self._clock()
        current_lease = None
        running_attempt_lease_expired = False
        if (
            attempt is not None
            and attempt.status is JobAttemptStatus.RUNNING
            and attempt.heartbeat_at is not None
            and attempt.lease_expires_at is not None
        ):
            current_lease = AttemptLease(
                heartbeat_at=attempt.heartbeat_at,
                expires_at=attempt.lease_expires_at,
            )
            running_attempt_lease_expired = AttemptLeasePolicy.is_expired(
                current_lease,
                observed_at=observed_at,
            )

        decision = decide_reclaimed_delivery(
            message_job_type=delivery.message.job_type,
            job_status=job_status,
            database_job_type=database_job_type,
            running_attempt_lease_expired=running_attempt_lease_expired,
        )

        match decision.action:
            case ReclaimedDeliveryAction.PROCESS:
                await self._processor.process(delivery, worker_id=worker_id)
            case ReclaimedDeliveryAction.RECOVER:
                if job is None or attempt is None or current_lease is None:
                    raise RuntimeError("Expired attempt recovery context is incomplete")
                await self._processor.recover_expired_attempt(
                    delivery,
                    attempt_id=attempt.id,
                    current_lease=current_lease,
                    attempt_number=attempt.attempt_number,
                    max_attempts=job.max_attempts,
                    expired_at=observed_at,
                )
            case ReclaimedDeliveryAction.ACKNOWLEDGE:
                await self._broker.acknowledge(delivery.entry_id)
            case ReclaimedDeliveryAction.LEAVE_PENDING:
                pass

        return decision

    async def recover_batch(
        self,
        *,
        worker_id: str,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int = 10,
    ) -> RecoveryBatchResult:
        claimed_batch = await self._broker.claim_stale(
            consumer_name=worker_id,
            min_idle_ms=min_idle_ms,
            start_id=start_id,
            count=count,
        )
        outcomes: list[ReclaimedDeliveryOutcome] = []

        for delivery in claimed_batch.deliveries:
            decision = await self.recover_one(
                delivery,
                worker_id=worker_id,
            )
            outcomes.append(
                ReclaimedDeliveryOutcome(
                    entry_id=delivery.entry_id,
                    decision=decision,
                )
            )

        return RecoveryBatchResult(
            next_start_id=claimed_batch.next_start_id,
            outcomes=outcomes,
            deleted_entry_ids=claimed_batch.deleted_entry_ids,
        )


class PeriodicRecovery:
    def __init__(
        self,
        coordinator: RecoveryBatchCoordinator,
        *,
        min_idle_ms: int = 60_000,
        batch_size: int = 10,
        poll_interval_seconds: float = 30.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if min_idle_ms < 1:
            raise ValueError("min_idle_ms must be at least 1")
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if batch_size > 1_000:
            raise ValueError("batch_size must not exceed 1000")
        if not isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be finite and positive")

        self._coordinator = coordinator
        self._min_idle_ms = min_idle_ms
        self._batch_size = batch_size
        self._poll_interval_seconds = poll_interval_seconds
        self._clock = clock
        self._start_id = "0-0"
        self._next_run_at: float | None = None

    async def run_if_due(
        self,
        *,
        worker_id: str,
    ) -> RecoveryBatchResult | None:
        now = self._clock()
        if self._next_run_at is not None and now < self._next_run_at:
            return None

        result = await self._coordinator.recover_batch(
            worker_id=worker_id,
            min_idle_ms=self._min_idle_ms,
            start_id=self._start_id,
            count=self._batch_size,
        )
        self._start_id = result.next_start_id

        if self._start_id == "0-0":
            self._next_run_at = self._clock() + self._poll_interval_seconds
        else:
            self._next_run_at = None

        return result

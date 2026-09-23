from dataclasses import dataclass
from math import inf, nan

import pytest

from forgequeue.jobs.status import JobStatus
from forgequeue.worker.recovery import (
    PeriodicRecovery,
    ReclaimedDeliveryAction,
    ReclaimedDeliveryDecision,
    ReclaimedDeliveryReason,
    RecoveryBatchResult,
    decide_reclaimed_delivery,
)

pytestmark = pytest.mark.unit


@dataclass(frozen=True, slots=True)
class RecoverBatchCall:
    worker_id: str
    min_idle_ms: int
    start_id: str
    count: int


class FakeRecoveryCoordinator:
    def __init__(self, results: list[RecoveryBatchResult]) -> None:
        self.results = results
        self.calls: list[RecoverBatchCall] = []

    async def recover_batch(
        self,
        *,
        worker_id: str,
        min_idle_ms: int,
        start_id: str,
        count: int,
    ) -> RecoveryBatchResult:
        self.calls.append(
            RecoverBatchCall(
                worker_id=worker_id,
                min_idle_ms=min_idle_ms,
                start_id=start_id,
                count=count,
            )
        )
        return self.results.pop(0)


class MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def empty_batch(next_start_id: str = "0-0") -> RecoveryBatchResult:
    return RecoveryBatchResult(
        next_start_id=next_start_id,
        outcomes=[],
        deleted_entry_ids=[],
    )


def test_queued_job_is_safe_to_process() -> None:
    decision = decide_reclaimed_delivery(
        message_job_type="sum_numbers",
        job_status=JobStatus.QUEUED,
        database_job_type="sum_numbers",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.PROCESS,
        reason=ReclaimedDeliveryReason.JOB_READY,
    )


@pytest.mark.parametrize("job_status", [JobStatus.COMPLETED, JobStatus.FAILED])
def test_terminal_job_delivery_is_safe_to_acknowledge(job_status: JobStatus) -> None:
    decision = decide_reclaimed_delivery(
        message_job_type="sum_numbers",
        job_status=job_status,
        database_job_type="sum_numbers",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.ACKNOWLEDGE,
        reason=ReclaimedDeliveryReason.JOB_TERMINAL,
    )


def test_running_job_is_left_pending_until_lease_ownership_is_known() -> None:
    decision = decide_reclaimed_delivery(
        message_job_type="sum_numbers",
        job_status=JobStatus.RUNNING,
        database_job_type="sum_numbers",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.LEAVE_PENDING,
        reason=ReclaimedDeliveryReason.JOB_RUNNING,
    )


def test_running_job_with_expired_attempt_lease_is_recovered() -> None:
    decision = decide_reclaimed_delivery(
        message_job_type="sum_numbers",
        job_status=JobStatus.RUNNING,
        database_job_type="sum_numbers",
        running_attempt_lease_expired=True,
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.RECOVER,
        reason=ReclaimedDeliveryReason.JOB_LEASE_EXPIRED,
    )


def test_retry_scheduled_job_is_left_pending_because_delivery_is_ambiguous() -> None:
    decision = decide_reclaimed_delivery(
        message_job_type="sum_numbers",
        job_status=JobStatus.RETRY_SCHEDULED,
        database_job_type="sum_numbers",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.LEAVE_PENDING,
        reason=ReclaimedDeliveryReason.RETRY_SCHEDULED,
    )


@pytest.mark.parametrize(
    ("job_status", "database_job_type"),
    [
        (None, None),
        (JobStatus.QUEUED, None),
    ],
)
def test_missing_job_is_left_pending(
    job_status: JobStatus | None,
    database_job_type: str | None,
) -> None:
    decision = decide_reclaimed_delivery(
        message_job_type="sum_numbers",
        job_status=job_status,
        database_job_type=database_job_type,
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.LEAVE_PENDING,
        reason=ReclaimedDeliveryReason.JOB_MISSING,
    )


@pytest.mark.parametrize("job_status", list(JobStatus))
def test_job_type_mismatch_is_never_processed_or_acknowledged(
    job_status: JobStatus,
) -> None:
    decision = decide_reclaimed_delivery(
        message_job_type="generate_report",
        job_status=job_status,
        database_job_type="sum_numbers",
    )

    assert decision == ReclaimedDeliveryDecision(
        action=ReclaimedDeliveryAction.LEAVE_PENDING,
        reason=ReclaimedDeliveryReason.JOB_TYPE_MISMATCH,
    )


@pytest.mark.asyncio
async def test_periodic_recovery_runs_immediately_then_waits_after_complete_scan() -> (
    None
):
    clock = MutableClock(100.0)
    coordinator = FakeRecoveryCoordinator([empty_batch(), empty_batch()])
    recovery = PeriodicRecovery(
        coordinator,
        min_idle_ms=60_000,
        batch_size=10,
        poll_interval_seconds=30.0,
        clock=clock,
    )

    first_result = await recovery.run_if_due(worker_id="worker-one")
    clock.value = 129.0
    early_result = await recovery.run_if_due(worker_id="worker-one")
    clock.value = 130.0
    second_result = await recovery.run_if_due(worker_id="worker-one")

    assert first_result == empty_batch()
    assert early_result is None
    assert second_result == empty_batch()
    assert coordinator.calls == [
        RecoverBatchCall(
            worker_id="worker-one",
            min_idle_ms=60_000,
            start_id="0-0",
            count=10,
        ),
        RecoverBatchCall(
            worker_id="worker-one",
            min_idle_ms=60_000,
            start_id="0-0",
            count=10,
        ),
    ]


@pytest.mark.asyncio
async def test_periodic_recovery_continues_nonterminal_cursor_without_waiting() -> None:
    clock = MutableClock(100.0)
    coordinator = FakeRecoveryCoordinator(
        [
            empty_batch("1730000000000-0"),
            empty_batch(),
        ]
    )
    recovery = PeriodicRecovery(
        coordinator,
        poll_interval_seconds=30.0,
        clock=clock,
    )

    first_result = await recovery.run_if_due(worker_id="worker-one")
    second_result = await recovery.run_if_due(worker_id="worker-one")
    third_result = await recovery.run_if_due(worker_id="worker-one")

    assert first_result == empty_batch("1730000000000-0")
    assert second_result == empty_batch()
    assert third_result is None
    assert [call.start_id for call in coordinator.calls] == [
        "0-0",
        "1730000000000-0",
    ]


@pytest.mark.parametrize("min_idle_ms", [0, -1])
def test_periodic_recovery_rejects_invalid_idle_threshold(min_idle_ms: int) -> None:
    with pytest.raises(ValueError, match="min_idle_ms must be at least 1"):
        PeriodicRecovery(
            FakeRecoveryCoordinator([]),
            min_idle_ms=min_idle_ms,
        )


@pytest.mark.parametrize("batch_size", [0, -1, 1_001])
def test_periodic_recovery_rejects_invalid_batch_size(batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size must"):
        PeriodicRecovery(
            FakeRecoveryCoordinator([]),
            batch_size=batch_size,
        )


@pytest.mark.parametrize("poll_interval_seconds", [0.0, -1.0, inf, nan])
def test_periodic_recovery_rejects_invalid_poll_interval(
    poll_interval_seconds: float,
) -> None:
    with pytest.raises(
        ValueError,
        match="poll_interval_seconds must be finite and positive",
    ):
        PeriodicRecovery(
            FakeRecoveryCoordinator([]),
            poll_interval_seconds=poll_interval_seconds,
        )

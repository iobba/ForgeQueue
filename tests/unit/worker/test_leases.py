from datetime import UTC, datetime, timedelta

import pytest

from forgequeue.jobs.leases import AttemptLease, AttemptLeasePolicy

pytestmark = pytest.mark.unit


def test_issue_creates_lease_from_heartbeat_time_and_duration() -> None:
    heartbeat_at = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    policy = AttemptLeasePolicy(
        duration=timedelta(seconds=60),
        heartbeat_interval=timedelta(seconds=15),
    )

    lease = policy.issue(heartbeat_at=heartbeat_at)

    assert lease == AttemptLease(
        heartbeat_at=heartbeat_at,
        expires_at=heartbeat_at + timedelta(seconds=60),
    )


def test_renew_replaces_heartbeat_and_expiry_times() -> None:
    started_at = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    renewed_at = started_at + timedelta(seconds=15)
    policy = AttemptLeasePolicy()
    original = policy.issue(heartbeat_at=started_at)

    renewed = policy.renew(original, heartbeat_at=renewed_at)

    assert renewed.heartbeat_at == renewed_at
    assert renewed.expires_at == renewed_at + timedelta(seconds=60)


def test_renew_rejects_heartbeat_that_moves_backwards() -> None:
    started_at = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    policy = AttemptLeasePolicy()
    lease = policy.issue(heartbeat_at=started_at)

    with pytest.raises(ValueError, match="heartbeat_at must not move backwards"):
        policy.renew(
            lease,
            heartbeat_at=started_at - timedelta(microseconds=1),
        )


@pytest.mark.parametrize("offset", [timedelta(0), timedelta(microseconds=1)])
def test_renew_does_not_revive_expired_lease(offset: timedelta) -> None:
    started_at = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    policy = AttemptLeasePolicy()
    lease = policy.issue(heartbeat_at=started_at)

    with pytest.raises(ValueError, match="expired lease cannot be renewed"):
        policy.renew(
            lease,
            heartbeat_at=lease.expires_at + offset,
        )


def test_lease_is_active_before_expiry() -> None:
    heartbeat_at = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    policy = AttemptLeasePolicy()
    lease = policy.issue(heartbeat_at=heartbeat_at)

    assert not policy.is_expired(
        lease,
        observed_at=lease.expires_at - timedelta(microseconds=1),
    )


def test_lease_expires_at_exact_boundary() -> None:
    heartbeat_at = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    policy = AttemptLeasePolicy()
    lease = policy.issue(heartbeat_at=heartbeat_at)

    assert policy.is_expired(lease, observed_at=lease.expires_at)


@pytest.mark.parametrize(
    ("duration", "heartbeat_interval", "message"),
    [
        (timedelta(0), timedelta(seconds=1), "duration must be positive"),
        (timedelta(seconds=1), timedelta(0), "heartbeat_interval must be positive"),
        (
            timedelta(seconds=10),
            timedelta(seconds=10),
            "heartbeat_interval must be shorter than duration",
        ),
        (
            timedelta(seconds=10),
            timedelta(seconds=11),
            "heartbeat_interval must be shorter than duration",
        ),
    ],
)
def test_policy_rejects_invalid_timing(
    duration: timedelta,
    heartbeat_interval: timedelta,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        AttemptLeasePolicy(
            duration=duration,
            heartbeat_interval=heartbeat_interval,
        )


@pytest.mark.parametrize("field_name", ["heartbeat_at", "expires_at"])
def test_lease_rejects_naive_timestamps(field_name: str) -> None:
    aware = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    naive = datetime(2026, 9, 21, 12, 1)
    values = {
        "heartbeat_at": aware,
        "expires_at": aware + timedelta(seconds=60),
        field_name: naive,
    }

    with pytest.raises(ValueError, match=f"{field_name} must be timezone-aware"):
        AttemptLease(**values)


def test_lease_rejects_non_increasing_expiry() -> None:
    heartbeat_at = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="expires_at must be later"):
        AttemptLease(
            heartbeat_at=heartbeat_at,
            expires_at=heartbeat_at,
        )


@pytest.mark.parametrize("operation", ["issue", "is_expired"])
def test_policy_rejects_naive_operation_time(operation: str) -> None:
    policy = AttemptLeasePolicy()
    aware = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    lease = policy.issue(heartbeat_at=aware)
    naive = datetime(2026, 9, 21, 12, 1)

    with pytest.raises(ValueError, match="must be timezone-aware"):
        if operation == "issue":
            policy.issue(heartbeat_at=naive)
        else:
            policy.is_expired(lease, observed_at=naive)

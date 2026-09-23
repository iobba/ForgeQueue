from dataclasses import dataclass
from datetime import datetime, timedelta


def validate_aware_datetime(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class AttemptLease:
    heartbeat_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        validate_aware_datetime(self.heartbeat_at, field_name="heartbeat_at")
        validate_aware_datetime(self.expires_at, field_name="expires_at")
        if self.expires_at <= self.heartbeat_at:
            raise ValueError("expires_at must be later than heartbeat_at")


@dataclass(frozen=True, slots=True)
class AttemptLeasePolicy:
    duration: timedelta = timedelta(seconds=60)
    heartbeat_interval: timedelta = timedelta(seconds=15)

    def __post_init__(self) -> None:
        if self.duration <= timedelta(0):
            raise ValueError("duration must be positive")
        if self.heartbeat_interval <= timedelta(0):
            raise ValueError("heartbeat_interval must be positive")
        if self.heartbeat_interval >= self.duration:
            raise ValueError("heartbeat_interval must be shorter than duration")

    def issue(self, *, heartbeat_at: datetime) -> AttemptLease:
        validate_aware_datetime(heartbeat_at, field_name="heartbeat_at")
        return AttemptLease(
            heartbeat_at=heartbeat_at,
            expires_at=heartbeat_at + self.duration,
        )

    def renew(self, lease: AttemptLease, *, heartbeat_at: datetime) -> AttemptLease:
        validate_aware_datetime(heartbeat_at, field_name="heartbeat_at")
        if heartbeat_at < lease.heartbeat_at:
            raise ValueError("heartbeat_at must not move backwards")
        if heartbeat_at >= lease.expires_at:
            raise ValueError("expired lease cannot be renewed")
        return self.issue(heartbeat_at=heartbeat_at)

    @staticmethod
    def is_expired(lease: AttemptLease, *, observed_at: datetime) -> bool:
        validate_aware_datetime(observed_at, field_name="observed_at")
        return observed_at >= lease.expires_at

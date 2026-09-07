from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class JobAttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobFailureKind(StrEnum):
    RETRYABLE = "retryable"
    PERMANENT = "permanent"


ALLOWED_ATTEMPT_TRANSITIONS: Final[
    Mapping[JobAttemptStatus, frozenset[JobAttemptStatus]]
] = MappingProxyType(
    {
        JobAttemptStatus.RUNNING: frozenset(
            {JobAttemptStatus.SUCCEEDED, JobAttemptStatus.FAILED}
        ),
        JobAttemptStatus.SUCCEEDED: frozenset(),
        JobAttemptStatus.FAILED: frozenset(),
    }
)


class InvalidJobAttemptStatusTransition(ValueError):
    def __init__(
        self,
        current: JobAttemptStatus,
        target: JobAttemptStatus,
    ) -> None:
        self.current = current
        self.target = target
        super().__init__(
            f"Cannot transition attempt from {current.value!r} to {target.value!r}"
        )


def can_transition_attempt(
    current: JobAttemptStatus,
    target: JobAttemptStatus,
) -> bool:
    return target in ALLOWED_ATTEMPT_TRANSITIONS[current]


def validate_attempt_transition(
    current: JobAttemptStatus,
    target: JobAttemptStatus,
) -> None:
    if not can_transition_attempt(current=current, target=target):
        raise InvalidJobAttemptStatusTransition(current, target)

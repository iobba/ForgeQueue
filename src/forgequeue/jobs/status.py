from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


ALLOWED_TRANSITIONS: Final[Mapping[JobStatus, frozenset[JobStatus]]] = MappingProxyType(
    {
        JobStatus.QUEUED: frozenset({JobStatus.RUNNING}),
        JobStatus.RUNNING: frozenset({JobStatus.COMPLETED, JobStatus.FAILED}),
        JobStatus.COMPLETED: frozenset(),
        JobStatus.FAILED: frozenset(),
    }
)


class InvalidJobStatusTransition(ValueError):
    def __init__(self, current: JobStatus, target: JobStatus) -> None:
        self.current = current
        self.target = target
        super().__init__(
            f"Cannot transition job from {current.value!r} to {target.value!r}"
        )


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[current]


def validate_transition(current: JobStatus, target: JobStatus) -> None:
    if not can_transition(current=current, target=target):
        raise InvalidJobStatusTransition(current, target)

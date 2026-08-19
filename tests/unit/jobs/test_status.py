import pytest

from forgequeue.jobs.status import (
    InvalidJobStatusTransition,
    JobStatus,
    can_transition,
    validate_transition,
)

VALID_TRANSITIONS = frozenset(
    {
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.RUNNING, JobStatus.COMPLETED),
        (JobStatus.RUNNING, JobStatus.FAILED),
    }
)

INVALID_TRANSITIONS = tuple(
    (current, target)
    for current in JobStatus
    for target in JobStatus
    if (current, target) not in VALID_TRANSITIONS
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(("current", "target"), VALID_TRANSITIONS)
def test_valid_transition(
    current: JobStatus,
    target: JobStatus,
) -> None:
    assert can_transition(current, target) is True
    validate_transition(current, target)


@pytest.mark.parametrize(("current", "target"), INVALID_TRANSITIONS)
def test_invalid_transition(
    current: JobStatus,
    target: JobStatus,
) -> None:
    assert can_transition(current, target) is False

    with pytest.raises(InvalidJobStatusTransition) as exc_info:
        validate_transition(current, target)

    assert exc_info.value.current is current
    assert exc_info.value.target is target
    assert str(exc_info.value) == (
        f"Cannot transition job from {current.value!r} to {target.value!r}"
    )

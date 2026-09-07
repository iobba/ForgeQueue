import pytest

from forgequeue.jobs.attempts import (
    InvalidJobAttemptStatusTransition,
    JobAttemptStatus,
    can_transition_attempt,
    validate_attempt_transition,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("current", "target", "expected"),
    [
        (JobAttemptStatus.RUNNING, JobAttemptStatus.RUNNING, False),
        (JobAttemptStatus.RUNNING, JobAttemptStatus.SUCCEEDED, True),
        (JobAttemptStatus.RUNNING, JobAttemptStatus.FAILED, True),
        (JobAttemptStatus.SUCCEEDED, JobAttemptStatus.RUNNING, False),
        (JobAttemptStatus.SUCCEEDED, JobAttemptStatus.SUCCEEDED, False),
        (JobAttemptStatus.SUCCEEDED, JobAttemptStatus.FAILED, False),
        (JobAttemptStatus.FAILED, JobAttemptStatus.RUNNING, False),
        (JobAttemptStatus.FAILED, JobAttemptStatus.SUCCEEDED, False),
        (JobAttemptStatus.FAILED, JobAttemptStatus.FAILED, False),
    ],
)
def test_can_transition_attempt(
    current: JobAttemptStatus,
    target: JobAttemptStatus,
    expected: bool,
) -> None:
    assert can_transition_attempt(current, target) is expected


def test_validate_attempt_transition_accepts_legal_transition() -> None:
    validate_attempt_transition(
        JobAttemptStatus.RUNNING,
        JobAttemptStatus.SUCCEEDED,
    )


def test_validate_attempt_transition_describes_illegal_transition() -> None:
    with pytest.raises(InvalidJobAttemptStatusTransition) as exc_info:
        validate_attempt_transition(
            JobAttemptStatus.SUCCEEDED,
            JobAttemptStatus.FAILED,
        )

    assert exc_info.value.current is JobAttemptStatus.SUCCEEDED
    assert exc_info.value.target is JobAttemptStatus.FAILED
    assert str(exc_info.value) == (
        "Cannot transition attempt from 'succeeded' to 'failed'"
    )

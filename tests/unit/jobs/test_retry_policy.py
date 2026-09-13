from datetime import timedelta
from math import inf, nan

import pytest

from forgequeue.jobs.attempts import JobFailureKind
from forgequeue.jobs.retry_policy import RetryPolicy

pytestmark = pytest.mark.unit


def test_retryable_failure_can_retry_while_attempts_remain() -> None:
    policy = RetryPolicy()

    assert policy.can_retry(
        failure_kind=JobFailureKind.RETRYABLE,
        attempt_number=1,
        max_attempts=3,
    )


def test_retryable_failure_cannot_retry_after_final_attempt() -> None:
    policy = RetryPolicy()

    assert not policy.can_retry(
        failure_kind=JobFailureKind.RETRYABLE,
        attempt_number=3,
        max_attempts=3,
    )


def test_permanent_failure_never_retries() -> None:
    policy = RetryPolicy()

    assert not policy.can_retry(
        failure_kind=JobFailureKind.PERMANENT,
        attempt_number=1,
        max_attempts=3,
    )


@pytest.mark.parametrize(
    ("attempt_number", "max_attempts", "message"),
    [
        (0, 3, "attempt_number must be positive"),
        (1, 0, "max_attempts must be positive"),
        (4, 3, "attempt_number must not exceed max_attempts"),
    ],
)
def test_can_retry_rejects_invalid_attempt_numbers(
    attempt_number: int,
    max_attempts: int,
    message: str,
) -> None:
    policy = RetryPolicy()

    with pytest.raises(ValueError, match=message):
        policy.can_retry(
            failure_kind=JobFailureKind.RETRYABLE,
            attempt_number=attempt_number,
            max_attempts=max_attempts,
        )


@pytest.mark.parametrize(
    ("attempt_number", "jitter_fraction", "expected_seconds"),
    [
        (1, 0.0, 2.5),
        (1, 1.0, 5.0),
        (2, 0.5, 7.5),
        (3, 1.0, 20.0),
        (7, 1.0, 300.0),
        (8, 0.0, 150.0),
    ],
)
def test_delay_uses_capped_exponential_equal_jitter(
    attempt_number: int,
    jitter_fraction: float,
    expected_seconds: float,
) -> None:
    policy = RetryPolicy()

    delay = policy.delay_after(
        attempt_number,
        jitter_fraction=jitter_fraction,
    )

    assert delay == timedelta(seconds=expected_seconds)


def test_delay_is_deterministic_for_supplied_jitter() -> None:
    policy = RetryPolicy()

    first = policy.delay_after(4, jitter_fraction=0.42)
    second = policy.delay_after(4, jitter_fraction=0.42)

    assert first == second


def test_large_attempt_number_reaches_cap_without_large_exponentiation() -> None:
    policy = RetryPolicy()

    delay = policy.delay_after(1_000_000, jitter_fraction=1.0)

    assert delay == timedelta(seconds=300)


@pytest.mark.parametrize("attempt_number", [0, -1])
def test_delay_rejects_non_positive_attempt_number(attempt_number: int) -> None:
    with pytest.raises(ValueError, match="attempt_number must be positive"):
        RetryPolicy().delay_after(attempt_number, jitter_fraction=0.5)


@pytest.mark.parametrize("jitter_fraction", [-0.1, 1.1, inf, -inf, nan])
def test_delay_rejects_invalid_jitter_fraction(jitter_fraction: float) -> None:
    with pytest.raises(
        ValueError,
        match="jitter_fraction must be finite and between 0 and 1",
    ):
        RetryPolicy().delay_after(1, jitter_fraction=jitter_fraction)


@pytest.mark.parametrize(
    ("base_delay_seconds", "max_delay_seconds", "message"),
    [
        (0.0, 300.0, "base_delay_seconds must be finite and positive"),
        (-1.0, 300.0, "base_delay_seconds must be finite and positive"),
        (inf, 300.0, "base_delay_seconds must be finite and positive"),
        (nan, 300.0, "base_delay_seconds must be finite and positive"),
        (5.0, 0.0, "max_delay_seconds must be finite and positive"),
        (5.0, inf, "max_delay_seconds must be finite and positive"),
        (
            10.0,
            5.0,
            "max_delay_seconds must be greater than or equal to base_delay_seconds",
        ),
    ],
)
def test_policy_rejects_invalid_configuration(
    base_delay_seconds: float,
    max_delay_seconds: float,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        RetryPolicy(
            base_delay_seconds=base_delay_seconds,
            max_delay_seconds=max_delay_seconds,
        )

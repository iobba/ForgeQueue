import pytest

from forgequeue.jobs.attempts import JobFailureKind
from forgequeue.jobs.execution_errors import PermanentJobError, RetryableJobError

pytestmark = pytest.mark.unit


def test_retryable_error_carries_safe_classified_details() -> None:
    error = RetryableJobError(
        error_code=" dependency_unavailable ",
        safe_message=" Dependency is temporarily unavailable ",
    )

    assert error.failure_kind is JobFailureKind.RETRYABLE
    assert error.error_code == "dependency_unavailable"
    assert error.safe_message == "Dependency is temporarily unavailable"
    assert str(error) == "Dependency is temporarily unavailable"


def test_permanent_error_carries_safe_classified_details() -> None:
    error = PermanentJobError(
        error_code="invalid_job_payload",
        safe_message="Stored job payload failed validation",
    )

    assert error.failure_kind is JobFailureKind.PERMANENT
    assert error.error_code == "invalid_job_payload"
    assert error.safe_message == "Stored job payload failed validation"


@pytest.mark.parametrize(
    ("error_code", "safe_message", "expected_message"),
    [
        ("", "Safe message", "error_code must not be blank"),
        ("   ", "Safe message", "error_code must not be blank"),
        ("a" * 101, "Safe message", "error_code must not exceed 100 characters"),
        ("valid_code", "", "safe_message must not be blank"),
        ("valid_code", "   ", "safe_message must not be blank"),
    ],
)
def test_execution_error_rejects_invalid_public_details(
    error_code: str,
    safe_message: str,
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        PermanentJobError(
            error_code=error_code,
            safe_message=safe_message,
        )

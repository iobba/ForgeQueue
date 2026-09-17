from uuid import uuid7

import pytest
from pydantic import ValidationError

from forgequeue.broker.messages import (
    DeadLetterMessage,
    DeadLetterReason,
    JobMessage,
)

pytestmark = pytest.mark.unit


def test_job_message_accepts_valid_fields_and_applies_version_default() -> None:
    job_id = uuid7()

    message = JobMessage(
        job_id=job_id,
        job_type="sum_numbers",
    )

    assert message.schema_version == "1"
    assert message.job_id == job_id
    assert message.job_type == "sum_numbers"


def test_job_message_serializes_to_redis_compatible_strings_and_round_trips() -> None:
    message = JobMessage(
        job_id=uuid7(),
        job_type="sum_numbers",
    )

    fields = message.model_dump(mode="json")
    restored_message = JobMessage.model_validate(fields)

    assert fields == {
        "schema_version": "1",
        "job_id": str(message.job_id),
        "job_type": "sum_numbers",
    }
    assert all(isinstance(value, str) for value in fields.values())
    assert restored_message == message


def test_job_message_rejects_invalid_job_id() -> None:
    with pytest.raises(ValidationError):
        JobMessage.model_validate(
            {
                "job_id": "not-a-uuid",
                "job_type": "sum_numbers",
            }
        )


def test_job_message_rejects_unsupported_schema_version() -> None:
    with pytest.raises(ValidationError):
        JobMessage.model_validate(
            {
                "schema_version": "2",
                "job_id": str(uuid7()),
                "job_type": "sum_numbers",
            }
        )


@pytest.mark.parametrize("job_type", ["", "x" * 101])
def test_job_message_rejects_invalid_job_type(job_type: str) -> None:
    with pytest.raises(ValidationError):
        JobMessage.model_validate(
            {
                "job_id": str(uuid7()),
                "job_type": job_type,
            }
        )


def test_job_message_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        JobMessage.model_validate(
            {
                "job_id": str(uuid7()),
                "job_type": "sum_numbers",
                "payload": {"numbers": [10, 20, 30]},
            }
        )


def test_job_message_is_immutable() -> None:
    message = JobMessage(
        job_id=uuid7(),
        job_type="sum_numbers",
    )

    with pytest.raises(ValidationError):
        message.job_type = "generate_report"


def test_dead_letter_message_serializes_to_redis_compatible_strings() -> None:
    message = DeadLetterMessage(
        source_entry_id="1730000000000-0",
        job_id=uuid7(),
        job_type="sum_numbers",
        attempt_number=3,
        reason=DeadLetterReason.RETRIES_EXHAUSTED,
        failure_kind="retryable",
        error_code="dependency_unavailable",
    )

    fields = message.model_dump(mode="json")

    assert fields == {
        "schema_version": "1",
        "source_entry_id": "1730000000000-0",
        "job_id": str(message.job_id),
        "job_type": "sum_numbers",
        "attempt_number": 3,
        "reason": "retries_exhausted",
        "failure_kind": "retryable",
        "error_code": "dependency_unavailable",
    }
    assert DeadLetterMessage.model_validate(fields) == message


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("attempt_number", 0),
        ("reason", "unknown"),
        ("failure_kind", "unknown"),
        ("error_code", ""),
        ("error_code", "x" * 101),
    ],
)
def test_dead_letter_message_rejects_invalid_fields(
    field: str,
    value: object,
) -> None:
    fields: dict[str, object] = {
        "source_entry_id": "1730000000000-0",
        "job_id": str(uuid7()),
        "job_type": "sum_numbers",
        "attempt_number": 1,
        "reason": "permanent_failure",
        "failure_kind": "permanent",
        "error_code": "invalid_job_payload",
        field: value,
    }

    with pytest.raises(ValidationError):
        DeadLetterMessage.model_validate(fields)


def test_dead_letter_message_does_not_accept_payload_or_raw_error() -> None:
    fields = {
        "source_entry_id": "1730000000000-0",
        "job_id": str(uuid7()),
        "job_type": "sum_numbers",
        "attempt_number": 1,
        "reason": "permanent_failure",
        "failure_kind": "permanent",
        "error_code": "invalid_job_payload",
        "raw_error": "secret internal details",
    }

    with pytest.raises(ValidationError):
        DeadLetterMessage.model_validate(fields)


@pytest.mark.parametrize(
    ("reason", "failure_kind"),
    [
        ("permanent_failure", "retryable"),
        ("retries_exhausted", "permanent"),
    ],
)
def test_dead_letter_message_rejects_inconsistent_failure_classification(
    reason: str,
    failure_kind: str,
) -> None:
    with pytest.raises(ValidationError, match="requires failure_kind"):
        DeadLetterMessage.model_validate(
            {
                "source_entry_id": "1730000000000-0",
                "job_id": str(uuid7()),
                "job_type": "sum_numbers",
                "attempt_number": 3,
                "reason": reason,
                "failure_kind": failure_kind,
                "error_code": "dependency_unavailable",
            }
        )


def test_dead_letter_message_accepts_malformed_delivery_without_job_context() -> None:
    message = DeadLetterMessage(
        source_entry_id="1730000000000-0",
        reason=DeadLetterReason.MALFORMED_MESSAGE,
        error_code="malformed_job_message",
    )

    assert message.job_id is None
    assert message.job_type is None
    assert message.attempt_number is None
    assert message.failure_kind is None


def test_malformed_dead_letter_rejects_untrusted_job_context() -> None:
    with pytest.raises(ValidationError, match="must not include job context"):
        DeadLetterMessage(
            source_entry_id="1730000000000-0",
            job_id=uuid7(),
            reason=DeadLetterReason.MALFORMED_MESSAGE,
            error_code="malformed_job_message",
        )

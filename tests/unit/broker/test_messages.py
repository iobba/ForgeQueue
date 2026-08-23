from uuid import uuid7

import pytest
from pydantic import ValidationError

from forgequeue.broker.messages import JobMessage

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

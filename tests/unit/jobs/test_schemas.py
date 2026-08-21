from datetime import UTC, datetime
from uuid import uuid7

import pytest
from pydantic import ValidationError

from forgequeue.db.models import Job
from forgequeue.jobs.schemas import (
    ErrorDetail,
    ErrorResponse,
    JobCreateRequest,
    JobListResponse,
    JobResponse,
)
from forgequeue.jobs.status import JobStatus

pytestmark = pytest.mark.unit


def test_create_request_accepts_supported_type_and_applies_defaults() -> None:
    request = JobCreateRequest.model_validate(
        {
            "type": "sum_numbers",
            "payload": {"numbers": [10, 20, 30]},
        }
    )

    assert request.type == "sum_numbers"
    assert request.payload.numbers == [10, 20, 30]
    assert request.payload.model_dump() == {"numbers": [10, 20, 30]}
    assert request.max_attempts == 1


@pytest.mark.parametrize(
    "job_type",
    ["", "   ", " sum_numbers ", "generate_report"],
)
def test_create_request_rejects_unsupported_type(job_type: str) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": job_type,
                "payload": {"numbers": [1]},
            }
        )


def test_create_request_rejects_missing_numbers() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": "sum_numbers",
                "payload": {},
            }
        )


@pytest.mark.parametrize("max_attempts", [0, -1])
def test_create_request_rejects_non_positive_max_attempts(
    max_attempts: int,
) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": "sum_numbers",
                "payload": {"numbers": [1]},
                "max_attempts": max_attempts,
            }
        )


def test_create_request_rejects_non_object_payload() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": "sum_numbers",
                "payload": [10, 20, 30],
            }
        )


def test_create_request_rejects_empty_numbers() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": "sum_numbers",
                "payload": {"numbers": []},
            }
        )


@pytest.mark.parametrize("value", ["10", 10.5, True])
def test_create_request_rejects_non_integer_number(value: object) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": "sum_numbers",
                "payload": {"numbers": [value]},
            }
        )


def test_create_request_rejects_oversized_number_list() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": "sum_numbers",
                "payload": {"numbers": [0] * 10_001},
            }
        )


def test_create_request_rejects_unknown_payload_fields() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": "sum_numbers",
                "payload": {
                    "numbers": [1, 2],
                    "operation": "sum",
                },
            }
        )


def test_create_request_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {
                "type": "sum_numbers",
                "payload": {"numbers": [1]},
                "priority": 5,
            }
        )


def test_job_response_reads_attributes_from_job_model() -> None:
    now = datetime.now(UTC)
    job = Job(
        id=uuid7(),
        job_type="sum_numbers",
        status=JobStatus.COMPLETED,
        payload={"numbers": [10, 20, 30]},
        result={"sum": 60},
        error_code=None,
        error_message=None,
        attempts=1,
        max_attempts=3,
        created_at=now,
        started_at=now,
        completed_at=now,
        updated_at=now,
    )

    response = JobResponse.model_validate(job)

    assert response.id == job.id
    assert response.type == job.job_type
    assert response.status is JobStatus.COMPLETED
    assert response.payload == job.payload
    assert response.result == {"sum": 60}
    assert response.attempts == 1
    assert response.max_attempts == 3
    assert response.created_at == now
    assert response.started_at == now
    assert response.completed_at == now
    assert response.updated_at == now


def test_job_response_serializes_public_field_names_and_values() -> None:
    now = datetime.now(UTC)
    response = JobResponse.model_validate(
        {
            "id": uuid7(),
            "job_type": "sum_numbers",
            "status": JobStatus.QUEUED,
            "payload": {"numbers": [10, 20, 30]},
            "result": None,
            "error_code": None,
            "error_message": None,
            "attempts": 0,
            "max_attempts": 1,
            "created_at": now,
            "started_at": None,
            "completed_at": None,
            "updated_at": now,
        }
    )

    serialized = response.model_dump(mode="json")

    assert serialized["type"] == "sum_numbers"
    assert serialized["status"] == "queued"
    assert "job_type" not in serialized


def test_job_list_response_accepts_valid_page_metadata() -> None:
    response = JobListResponse(
        page=2,
        pages=4,
        total=82,
        items=[],
    )

    assert response.page == 2
    assert response.pages == 4
    assert response.total == 82
    assert response.items == []


@pytest.mark.parametrize(
    ("page", "pages", "total"),
    [
        (0, 0, 0),
        (1, -1, 0),
        (1, 0, -1),
    ],
)
def test_job_list_response_rejects_invalid_page_metadata(
    page: int,
    pages: int,
    total: int,
) -> None:
    with pytest.raises(ValidationError):
        JobListResponse(
            page=page,
            pages=pages,
            total=total,
            items=[],
        )


def test_error_response_uses_stable_envelope() -> None:
    response = ErrorResponse(
        error=ErrorDetail(
            code="job_not_found",
            message="The requested job does not exist",
        )
    )

    assert response.model_dump() == {
        "error": {
            "code": "job_not_found",
            "message": "The requested job does not exist",
        }
    }

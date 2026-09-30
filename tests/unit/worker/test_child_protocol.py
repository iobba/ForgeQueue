from uuid import uuid7

import pytest
from pydantic import ValidationError

from forgequeue.jobs.attempts import JobFailureKind
from forgequeue.worker.child_protocol import (
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    HandlerRequest,
    HandlerResponse,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
)

pytestmark = pytest.mark.unit


def test_request_round_trips_as_bounded_json() -> None:
    request = HandlerRequest(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )

    assert decode_request(encode_request(request)) == request


def test_request_rejects_unknown_version_and_fields() -> None:
    with pytest.raises(ValidationError):
        decode_request(b'{"protocol_version":2,"job_type":"sum_numbers","payload":{}}')
    with pytest.raises(ValidationError):
        decode_request(
            b'{"protocol_version":1,"job_type":"sum_numbers",'
            b'"payload":{},"job_id":"unexpected"}'
        )


def test_request_rejects_oversized_input() -> None:
    with pytest.raises(ValueError, match="size limit"):
        decode_request(b"x" * (MAX_REQUEST_BYTES + 1))
    with pytest.raises(ValueError, match="size limit"):
        encode_request(
            HandlerRequest(
                job_type="sum_numbers", payload={"data": "x" * MAX_REQUEST_BYTES}
            )
        )


def test_success_and_failure_responses_round_trip() -> None:
    success = HandlerResponse(status="succeeded", result={"sum": 60})
    failure = HandlerResponse(
        status="failed",
        failure_kind=JobFailureKind.RETRYABLE,
        error_code="temporary_failure",
        safe_message="Try again later",
    )

    assert decode_response(encode_response(success)) == success
    assert decode_response(encode_response(failure)) == failure


def test_response_rejects_inconsistent_outcome() -> None:
    with pytest.raises(ValidationError, match="success requires"):
        HandlerResponse(status="succeeded")
    with pytest.raises(ValidationError, match="failure requires"):
        HandlerResponse(status="failed", result={"sum": 60})


def test_response_rejects_oversized_or_non_json_result() -> None:
    with pytest.raises(ValueError, match="size limit"):
        encode_response(
            HandlerResponse(
                status="succeeded", result={"data": "x" * MAX_RESPONSE_BYTES}
            )
        )
    with pytest.raises(TypeError):
        encode_response(HandlerResponse(status="succeeded", result={"id": uuid7()}))

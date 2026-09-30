import json
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from forgequeue.jobs.attempts import JobFailureKind

MAX_REQUEST_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 64 * 1024


class HandlerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol_version: Literal[1] = 1
    job_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, object]


class HandlerResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol_version: Literal[1] = 1
    status: Literal["succeeded", "failed"]
    result: dict[str, object] | None = None
    failure_kind: JobFailureKind | None = None
    error_code: str | None = Field(default=None, min_length=1, max_length=100)
    safe_message: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.status == "succeeded":
            if (
                self.result is None
                or self.failure_kind is not None
                or self.error_code is not None
                or self.safe_message is not None
            ):
                raise ValueError("success requires a result and no failure details")
        elif (
            self.result is not None
            or self.failure_kind is None
            or self.error_code is None
            or self.safe_message is None
        ):
            raise ValueError("failure requires details and no result")
        return self


def _encode_bounded(model: BaseModel, *, max_bytes: int) -> bytes:
    encoded = json.dumps(
        model.model_dump(mode="python"),
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > max_bytes:
        raise ValueError("handler protocol message exceeds size limit")
    return encoded


def encode_request(request: HandlerRequest) -> bytes:
    return _encode_bounded(request, max_bytes=MAX_REQUEST_BYTES)


def decode_request(data: bytes) -> HandlerRequest:
    if len(data) > MAX_REQUEST_BYTES:
        raise ValueError("handler request exceeds size limit")
    return HandlerRequest.model_validate_json(data)


def encode_response(response: HandlerResponse) -> bytes:
    return _encode_bounded(response, max_bytes=MAX_RESPONSE_BYTES)


def decode_response(data: bytes) -> HandlerResponse:
    if len(data) > MAX_RESPONSE_BYTES:
        raise ValueError("handler response exceeds size limit")
    return HandlerResponse.model_validate_json(data)

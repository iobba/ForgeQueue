from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StringConstraints

from forgequeue.jobs.status import JobStatus

JobType = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=100,
    ),
]


class SumNumbersPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    numbers: list[StrictInt] = Field(
        min_length=1,
        max_length=10_000,
    )


class JobCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["sum_numbers"]
    payload: SumNumbersPayload
    max_attempts: int = Field(default=1, ge=1)


class JobResponse(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        from_attributes=True,
    )

    id: UUID
    type: JobType = Field(validation_alias="job_type")
    status: JobStatus
    payload: dict[str, object]
    result: dict[str, object] | None
    error_code: str | None
    error_message: str | None
    attempts: int
    max_attempts: int
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    updated_at: datetime


class JobListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page: int = Field(ge=1)
    pages: int = Field(ge=0)
    total: int = Field(ge=0)
    items: list[JobResponse]


class ErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1)
    message: str = Field(min_length=1)


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ErrorDetail

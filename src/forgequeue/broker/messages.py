from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class JobMessage(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    schema_version: Literal["1"] = "1"
    job_id: UUID
    job_type: str = Field(min_length=1, max_length=100)
    attempt_number: int | None = Field(default=None, ge=1)


class DeadLetterReason(StrEnum):
    PERMANENT_FAILURE = "permanent_failure"
    RETRIES_EXHAUSTED = "retries_exhausted"
    MALFORMED_MESSAGE = "malformed_message"


class DeadLetterMessage(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    schema_version: Literal["1"] = "1"
    source_entry_id: str = Field(min_length=1, max_length=128)
    job_id: UUID | None = None
    job_type: str | None = Field(default=None, min_length=1, max_length=100)
    attempt_number: int | None = Field(default=None, ge=1)
    reason: DeadLetterReason
    failure_kind: Literal["retryable", "permanent"] | None = None
    error_code: str = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def validate_reason_context(self) -> Self:
        job_context = (self.job_id, self.job_type, self.attempt_number)
        if self.reason is DeadLetterReason.MALFORMED_MESSAGE:
            if any(value is not None for value in job_context):
                raise ValueError("malformed_message must not include job context")
            if self.failure_kind is not None:
                raise ValueError("malformed_message must not include failure_kind")
            return self

        if any(value is None for value in job_context):
            raise ValueError("terminal job failures require complete job context")

        expected_failure_kind = {
            DeadLetterReason.PERMANENT_FAILURE: "permanent",
            DeadLetterReason.RETRIES_EXHAUSTED: "retryable",
        }[self.reason]
        if self.failure_kind != expected_failure_kind:
            raise ValueError(
                f"{self.reason.value} requires failure_kind {expected_failure_kind!r}"
            )

        return self


@dataclass(frozen=True, slots=True)
class ReceivedJobMessage:
    entry_id: str
    message: JobMessage


@dataclass(frozen=True, slots=True)
class MalformedJobDelivery:
    entry_id: str
    error_code: Literal["malformed_job_message"] = "malformed_job_message"


type JobDelivery = ReceivedJobMessage | MalformedJobDelivery


@dataclass(frozen=True, slots=True)
class ClaimedDeliveryBatch:
    next_start_id: str
    deliveries: list[JobDelivery]
    deleted_entry_ids: list[str]


@dataclass(frozen=True, slots=True)
class ReceivedDeadLetterMessage:
    entry_id: str
    message: DeadLetterMessage


@dataclass(frozen=True, slots=True)
class PendingJobDelivery:
    entry_id: str
    consumer_name: str
    idle_ms: int
    delivery_count: int

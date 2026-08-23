from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class JobMessage(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    schema_version: Literal["1"] = "1"
    job_id: UUID
    job_type: str = Field(min_length=1, max_length=100)


@dataclass(frozen=True, slots=True)
class ReceivedJobMessage:
    entry_id: str
    message: JobMessage


@dataclass(frozen=True, slots=True)
class PendingJobDelivery:
    entry_id: str
    consumer_name: str
    idle_ms: int
    delivery_count: int

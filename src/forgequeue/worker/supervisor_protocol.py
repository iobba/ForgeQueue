"""Private lifecycle confirmation, separate from the handler's JSON result."""

import json
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_STATUS_BYTES = 256


class SupervisorStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    # Required on the wire; strict integers reject JSON true and 1.0 as versions.
    version: int = Field(ge=1, le=1)
    kind: Literal["exited", "stopped", "not_started"]
    returncode: int | None

    @model_validator(mode="after")
    def validate_returncode(self) -> Self:
        if (self.kind == "not_started") != (self.returncode is None):
            raise ValueError("only not_started must have a null returncode")
        return self


def encode_status(status: SupervisorStatus) -> bytes:
    encoded = json.dumps(status.model_dump(), separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_STATUS_BYTES:
        raise ValueError("supervisor status exceeds size limit")
    return encoded


def decode_status(data: bytes) -> SupervisorStatus:
    if len(data) > MAX_STATUS_BYTES:
        raise ValueError("supervisor status exceeds size limit")
    return SupervisorStatus.model_validate_json(data)

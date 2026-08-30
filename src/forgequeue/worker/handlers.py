from collections.abc import Callable, Mapping
from types import MappingProxyType

from forgequeue.jobs.schemas import SumNumbersPayload

type JobResult = dict[str, object]
type JobHandler = Callable[[dict[str, object]], JobResult]


def sum_numbers(payload: dict[str, object]) -> JobResult:
    validated_payload = SumNumbersPayload.model_validate(payload)
    return {"sum": sum(validated_payload.numbers)}


HANDLERS: Mapping[str, JobHandler] = MappingProxyType(
    {
        "sum_numbers": sum_numbers,
    }
)


class UnsupportedJobTypeError(ValueError):
    def __init__(self, job_type: str) -> None:
        self.job_type = job_type
        super().__init__(f"Unsupported job type: {job_type!r}")


def get_handler(job_type: str) -> JobHandler:
    if job_type not in HANDLERS:
        raise UnsupportedJobTypeError(job_type=job_type)

    return HANDLERS[job_type]

from fastapi import Request, status
from fastapi.responses import JSONResponse

from forgequeue.jobs.schemas import ErrorDetail, ErrorResponse
from forgequeue.jobs.service import JobNotFoundError


async def handle_job_not_found(
    _request: Request,
    exception: Exception,
) -> JSONResponse:
    if not isinstance(exception, JobNotFoundError):
        raise TypeError("handle_job_not_found received an unexpected exception")

    error = ErrorResponse(
        error=ErrorDetail(
            code="job_not_found",
            message="The requested job does not exist",
        )
    )
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=error.model_dump(mode="json"),
    )

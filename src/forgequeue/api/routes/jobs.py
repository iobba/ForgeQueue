from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, Response, status

from forgequeue.api.dependencies import (
    JobServiceDependency,
    JobSubmissionServiceDependency,
)
from forgequeue.jobs.schemas import (
    ErrorResponse,
    JobCreateRequest,
    JobListResponse,
    JobResponse,
)
from forgequeue.jobs.status import JobStatus

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_job(
    request: JobCreateRequest,
    submission_service: JobSubmissionServiceDependency,
    response: Response,
) -> JobResponse:
    job = await submission_service.submit(
        job_type=request.type,
        payload=request.payload.model_dump(),
        max_attempts=request.max_attempts,
    )
    response.headers["Location"] = f"/jobs/{job.id}"
    return JobResponse.model_validate(job)


@router.get(
    "",
    response_model=JobListResponse,
)
async def list_jobs(
    service: JobServiceDependency,
    status_filter: Annotated[
        JobStatus | None,
        Query(alias="status"),
    ] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
) -> JobListResponse:
    jobs, total = await service.list_jobs(
        status=status_filter,
        page=page,
        limit=limit,
    )
    pages = (total + limit - 1) // limit
    return JobListResponse(
        page=page,
        pages=pages,
        total=total,
        items=[JobResponse.model_validate(job) for job in jobs],
    )


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": ErrorResponse,
            "description": "Job not found",
        }
    },
)
async def get_job(
    job_id: UUID,
    service: JobServiceDependency,
) -> JobResponse:
    job = await service.get_job(job_id)
    return JobResponse.model_validate(job)

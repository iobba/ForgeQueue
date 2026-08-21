from fastapi import FastAPI

from forgequeue.api.errors import handle_job_not_found
from forgequeue.api.lifespan import lifespan
from forgequeue.api.routes.health import router as health_router
from forgequeue.api.routes.jobs import router as jobs_router
from forgequeue.core.config import get_settings
from forgequeue.core.logging import configure_logging
from forgequeue.jobs.service import JobNotFoundError


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)

    application = FastAPI(
        title=settings.app_name,
        lifespan=lifespan,
    )
    application.add_exception_handler(JobNotFoundError, handle_job_not_found)
    application.include_router(health_router)
    application.include_router(jobs_router)

    return application

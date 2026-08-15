from fastapi import FastAPI

from forgequeue.api.lifespan import lifespan
from forgequeue.api.routes.health import router as health_router
from forgequeue.core.config import get_settings
from forgequeue.core.logging import configure_logging


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)

    application = FastAPI(
        title=settings.app_name,
        lifespan=lifespan,
    )
    application.include_router(health_router)

    return application

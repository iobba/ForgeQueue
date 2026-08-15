from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from forgequeue.broker.redis import create_redis_client
from forgequeue.core.config import get_settings
from forgequeue.db.session import (
    create_database_engine,
    create_session_factory,
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    settings = get_settings()
    logger.info(
        "application_starting",
        app_name=settings.app_name,
        environment=settings.app_env,
    )
    engine = create_database_engine(settings)
    session_factory = create_session_factory(engine)
    redis_client = create_redis_client(settings)

    app.state.redis_client = redis_client
    app.state.db_engine = engine
    app.state.db_session_factory = session_factory
    logger.info("application_started")
    try:
        yield
    finally:
        logger.info("application_stopping")
        try:
            await redis_client.aclose()
        finally:
            await engine.dispose()
        logger.info("application_stopped")

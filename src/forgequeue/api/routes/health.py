from fastapi import APIRouter, Request, Response, status
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter(tags=["system"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def check_health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def check_readiness(
    request: Request,
    response: Response,
) -> dict[str, object]:
    dependencies = {
        "postgresql": "ready",
        "redis": "ready",
    }

    try:
        async with request.app.state.db_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except SQLAlchemyError:
        dependencies["postgresql"] = "unavailable"

    try:
        redis_ready = await request.app.state.redis_client.ping()
        if not redis_ready:
            dependencies["redis"] = "unavailable"
    except RedisError:
        dependencies["redis"] = "unavailable"

    is_ready = all(value == "ready" for value in dependencies.values())

    if not is_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "ready" if is_ready else "not_ready",
        "dependencies": dependencies,
    }

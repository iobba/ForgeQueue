import asyncio
from collections.abc import AsyncIterator, Iterator
from uuid import uuid7

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.api.app import create_app
from forgequeue.broker.redis import create_redis_client
from forgequeue.core.config import Settings, get_settings
from forgequeue.db.session import (
    create_database_engine,
    create_session_factory,
)


@pytest.fixture(autouse=True)
def integration_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    monkeypatch.setenv("POSTGRES_DB", "forgequeue_test")
    monkeypatch.setenv("APP_ENV", "test")
    get_settings.cache_clear()

    try:
        yield
    finally:
        get_settings.cache_clear()


@pytest.fixture
def integration_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    stream_name = f"forgequeue:test:api:{uuid7()}"
    group_name = f"forgequeue-test-api-{uuid7()}"
    monkeypatch.setenv("REDIS_JOBS_STREAM", stream_name)
    monkeypatch.setenv("REDIS_WORKER_GROUP", group_name)
    get_settings.cache_clear()
    settings = get_settings()

    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        asyncio.run(delete_redis_stream(settings, stream_name))
        get_settings.cache_clear()


async def delete_redis_stream(settings: Settings, stream_name: str) -> None:
    client = create_redis_client(settings)
    try:
        await client.delete(stream_name)
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def database_session() -> AsyncIterator[AsyncSession]:
    get_settings.cache_clear()
    settings = get_settings()

    if settings.postgres_db != "forgequeue_test":
        raise RuntimeError("Integration tests must use forgequeue_test")

    engine = create_database_engine(settings)

    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            session = AsyncSession(
                bind=connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )

            try:
                yield session
            finally:
                await session.close()

                if transaction.is_active:
                    await transaction.rollback()
    finally:
        await engine.dispose()
        get_settings.cache_clear()


@pytest_asyncio.fixture
async def database_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    get_settings.cache_clear()
    settings = get_settings()

    if settings.postgres_db != "forgequeue_test":
        raise RuntimeError("Integration tests must use forgequeue_test")

    engine = create_database_engine(settings)
    session_factory = create_session_factory(engine)

    try:
        yield session_factory
    finally:
        await engine.dispose()
        get_settings.cache_clear()


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client = create_redis_client(get_settings())

    try:
        yield client
    finally:
        await client.aclose()

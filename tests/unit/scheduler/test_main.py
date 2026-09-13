import asyncio
from typing import cast

import pytest
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

import forgequeue.scheduler.main as main_module
from forgequeue.core.config import Settings
from forgequeue.scheduler.retry_dispatcher import RetryDispatcher

pytestmark = pytest.mark.unit


class FakeEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    async def dispose(self) -> None:
        self.dispose_calls += 1


class FakeRedisClient:
    def __init__(self) -> None:
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1


class FakeRetryDispatcher:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.stop_events: list[asyncio.Event] = []

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        self.stop_events.append(stop_event)
        if self.error is not None:
            raise self.error


def build_settings() -> Settings:
    return Settings(
        postgres_user="forgequeue",
        postgres_password=SecretStr("not-a-real-secret"),
        postgres_db="forgequeue",
        redis_worker_block_ms=1_000,
        redis_socket_timeout_seconds=5.0,
    )


def install_runtime_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dispatcher_error: Exception | None = None,
) -> tuple[FakeEngine, FakeRedisClient, FakeRetryDispatcher, list[str]]:
    settings = build_settings()
    engine = FakeEngine()
    redis_client = FakeRedisClient()
    dispatcher = FakeRetryDispatcher(dispatcher_error)
    configured_levels: list[str] = []

    def ignore_shutdown_signals(_stop_event: asyncio.Event) -> None:
        return None

    def fake_create_database_engine(_settings: Settings) -> AsyncEngine:
        return cast(AsyncEngine, engine)

    def fake_create_redis_client(_settings: Settings) -> Redis:
        return cast(Redis, redis_client)

    def fake_create_session_factory(
        _engine: AsyncEngine,
    ) -> async_sessionmaker[AsyncSession]:
        return cast(async_sessionmaker[AsyncSession], object())

    def fake_create_retry_dispatcher(
        _settings: Settings,
        _redis_client: Redis,
        _session_factory: async_sessionmaker[AsyncSession],
    ) -> RetryDispatcher:
        return cast(RetryDispatcher, dispatcher)

    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    monkeypatch.setattr(main_module, "configure_logging", configured_levels.append)
    monkeypatch.setattr(
        main_module,
        "register_shutdown_signals",
        ignore_shutdown_signals,
    )
    monkeypatch.setattr(
        main_module,
        "create_database_engine",
        fake_create_database_engine,
    )
    monkeypatch.setattr(
        main_module,
        "create_redis_client",
        fake_create_redis_client,
    )
    monkeypatch.setattr(
        main_module,
        "create_session_factory",
        fake_create_session_factory,
    )
    monkeypatch.setattr(
        main_module,
        "create_retry_dispatcher",
        fake_create_retry_dispatcher,
    )

    return engine, redis_client, dispatcher, configured_levels


@pytest.mark.asyncio
async def test_run_retry_dispatcher_builds_runtime_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, redis_client, dispatcher, configured_levels = install_runtime_fakes(
        monkeypatch
    )

    await main_module.run_retry_dispatcher()

    assert configured_levels == ["INFO"]
    assert len(dispatcher.stop_events) == 1
    assert redis_client.close_calls == 1
    assert engine.dispose_calls == 1


@pytest.mark.asyncio
async def test_run_retry_dispatcher_closes_resources_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, redis_client, dispatcher, _ = install_runtime_fakes(
        monkeypatch,
        dispatcher_error=RuntimeError("dispatcher crashed"),
    )

    with pytest.raises(RuntimeError, match="dispatcher crashed"):
        await main_module.run_retry_dispatcher()

    assert len(dispatcher.stop_events) == 1
    assert redis_client.close_calls == 1
    assert engine.dispose_calls == 1

import asyncio
import signal
from collections.abc import Callable
from typing import cast

import pytest
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

import forgequeue.worker.main as main_module
from forgequeue.core.config import Settings
from forgequeue.worker.runner import Worker

pytestmark = pytest.mark.unit


class FakeSignalRegistrar:
    def __init__(self) -> None:
        self.handlers: dict[int, Callable[[], None]] = {}

    def add_signal_handler(
        self,
        signal_number: int,
        callback: Callable[[], None],
    ) -> None:
        self.handlers[signal_number] = callback


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


class FakeWorker:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.stop_events: list[asyncio.Event] = []
        self.block_values: list[int | None] = []

    async def run_forever(
        self,
        stop_event: asyncio.Event,
        *,
        block_ms: int | None = 1_000,
    ) -> None:
        self.stop_events.append(stop_event)
        self.block_values.append(block_ms)
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
    worker_error: Exception | None = None,
) -> tuple[FakeEngine, FakeRedisClient, FakeWorker, list[str]]:
    settings = build_settings()
    engine = FakeEngine()
    redis_client = FakeRedisClient()
    worker = FakeWorker(worker_error)
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

    def fake_create_worker(
        _settings: Settings,
        _redis_client: Redis,
        _session_factory: async_sessionmaker[AsyncSession],
    ) -> Worker:
        return cast(Worker, worker)

    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        main_module,
        "configure_logging",
        configured_levels.append,
    )
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
        "create_worker",
        fake_create_worker,
    )

    return engine, redis_client, worker, configured_levels


def test_register_shutdown_signals_sets_shared_stop_event() -> None:
    stop_event = asyncio.Event()
    registrar = FakeSignalRegistrar()

    main_module.register_shutdown_signals(stop_event, loop=registrar)

    assert set(registrar.handlers) == {signal.SIGINT, signal.SIGTERM}
    assert not stop_event.is_set()

    registrar.handlers[signal.SIGTERM]()

    assert stop_event.is_set()


@pytest.mark.asyncio
async def test_run_worker_builds_runtime_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, redis_client, worker, configured_levels = install_runtime_fakes(monkeypatch)

    await main_module.run_worker()

    assert configured_levels == ["INFO"]
    assert len(worker.stop_events) == 1
    assert worker.block_values == [1_000]
    assert redis_client.close_calls == 1
    assert engine.dispose_calls == 1


@pytest.mark.asyncio
async def test_run_worker_closes_resources_when_worker_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, redis_client, _, _ = install_runtime_fakes(
        monkeypatch,
        worker_error=RuntimeError("worker crashed"),
    )

    with pytest.raises(RuntimeError, match="worker crashed"):
        await main_module.run_worker()

    assert redis_client.close_calls == 1
    assert engine.dispose_calls == 1

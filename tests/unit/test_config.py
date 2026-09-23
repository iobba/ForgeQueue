from math import inf, nan

import pytest
from pydantic import SecretStr, ValidationError

from forgequeue.core.config import Settings

pytestmark = pytest.mark.unit


def build_settings() -> Settings:
    return Settings(
        postgres_user="forgequeue",
        postgres_password=SecretStr("not-a-real-secret"),
        postgres_db="forgequeue",
        redis_worker_block_ms=1_000,
        redis_socket_timeout_seconds=5.0,
    )


def test_accepts_redis_block_time_shorter_than_socket_timeout() -> None:
    settings = build_settings()

    assert settings.redis_worker_block_ms == 1_000
    assert settings.redis_socket_timeout_seconds == 5.0
    assert settings.redis_dead_letter_stream == "forgequeue:dead"
    assert settings.worker_recovery_min_idle_ms == 60_000
    assert settings.worker_recovery_batch_size == 10
    assert settings.worker_recovery_poll_seconds == 30.0
    assert settings.worker_lease_duration_seconds == 60.0
    assert settings.worker_heartbeat_interval_seconds == 15.0
    assert settings.retry_dispatcher_batch_size == 100
    assert settings.retry_dispatcher_poll_seconds == 1.0


def test_rejects_redis_block_time_that_can_collide_with_socket_timeout() -> None:
    with pytest.raises(
        ValidationError,
        match="redis_worker_block_ms must be shorter",
    ):
        Settings(
            postgres_user="forgequeue",
            postgres_password=SecretStr("not-a-real-secret"),
            postgres_db="forgequeue",
            redis_worker_block_ms=5_000,
            redis_socket_timeout_seconds=5.0,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("retry_dispatcher_batch_size", 0),
        ("retry_dispatcher_batch_size", 1_001),
        ("retry_dispatcher_poll_seconds", 0),
        ("retry_dispatcher_poll_seconds", inf),
    ],
)
def test_rejects_invalid_retry_dispatcher_settings(
    field: str,
    value: int | float,
) -> None:
    values: dict[str, object] = {
        "postgres_user": "forgequeue",
        "postgres_password": SecretStr("not-a-real-secret"),
        "postgres_db": "forgequeue",
        "redis_worker_block_ms": 1_000,
        "redis_socket_timeout_seconds": 5.0,
        field: value,
    }

    with pytest.raises(ValidationError):
        Settings.model_validate(values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("worker_recovery_min_idle_ms", 0),
        ("worker_recovery_batch_size", 0),
        ("worker_recovery_batch_size", 1_001),
        ("worker_recovery_poll_seconds", 0),
        ("worker_recovery_poll_seconds", inf),
        ("worker_recovery_poll_seconds", nan),
    ],
)
def test_rejects_invalid_worker_recovery_settings(
    field: str,
    value: int | float,
) -> None:
    values: dict[str, object] = {
        "postgres_user": "forgequeue",
        "postgres_password": SecretStr("not-a-real-secret"),
        "postgres_db": "forgequeue",
        "redis_worker_block_ms": 1_000,
        "redis_socket_timeout_seconds": 5.0,
        field: value,
    }

    with pytest.raises(ValidationError):
        Settings.model_validate(values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("worker_lease_duration_seconds", 0),
        ("worker_lease_duration_seconds", inf),
        ("worker_lease_duration_seconds", nan),
        ("worker_heartbeat_interval_seconds", 0),
        ("worker_heartbeat_interval_seconds", inf),
        ("worker_heartbeat_interval_seconds", nan),
    ],
)
def test_rejects_invalid_worker_lease_settings(
    field: str,
    value: int | float,
) -> None:
    values: dict[str, object] = {
        "postgres_user": "forgequeue",
        "postgres_password": SecretStr("not-a-real-secret"),
        "postgres_db": "forgequeue",
        field: value,
    }

    with pytest.raises(ValidationError):
        Settings.model_validate(values)


def test_rejects_heartbeat_interval_that_can_outlive_lease() -> None:
    with pytest.raises(
        ValidationError,
        match="worker_heartbeat_interval_seconds must be shorter",
    ):
        Settings(
            postgres_user="forgequeue",
            postgres_password=SecretStr("not-a-real-secret"),
            postgres_db="forgequeue",
            worker_lease_duration_seconds=30,
            worker_heartbeat_interval_seconds=30,
        )

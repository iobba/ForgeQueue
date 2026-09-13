from math import inf

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

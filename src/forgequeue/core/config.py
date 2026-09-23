from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "ForgeQueue"
    app_env: Literal["development", "test", "production"] = "development"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    log_level: Literal[
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ] = "INFO"

    postgres_user: str
    postgres_password: SecretStr
    postgres_db: str
    postgres_host: str = "127.0.0.1"
    postgres_port: int = Field(default=5432, ge=1, le=65535)

    redis_host: str = "127.0.0.1"
    redis_port: int = Field(default=6379, ge=1, le=65535)
    redis_db: int = Field(default=0, ge=0)
    redis_jobs_stream: str = "forgequeue:jobs"
    redis_dead_letter_stream: str = "forgequeue:dead"
    redis_worker_group: str = "forgequeue-workers"
    redis_socket_timeout_seconds: float = Field(default=5.0, gt=0)
    redis_worker_block_ms: int = Field(default=1_000, ge=1)

    worker_recovery_min_idle_ms: int = Field(default=60_000, ge=1)
    worker_recovery_batch_size: int = Field(default=10, ge=1, le=1_000)
    worker_recovery_poll_seconds: float = Field(
        default=30.0,
        gt=0,
        allow_inf_nan=False,
    )
    worker_lease_duration_seconds: float = Field(
        default=60.0,
        gt=0,
        allow_inf_nan=False,
    )
    worker_heartbeat_interval_seconds: float = Field(
        default=15.0,
        gt=0,
        allow_inf_nan=False,
    )

    retry_dispatcher_batch_size: int = Field(default=100, ge=1, le=1_000)
    retry_dispatcher_poll_seconds: float = Field(
        default=1.0,
        gt=0,
        allow_inf_nan=False,
    )

    database_pool_size: int = Field(default=5, ge=1)
    database_max_overflow: int = Field(default=10, ge=0)
    database_pool_timeout: int = Field(default=30, ge=1)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @model_validator(mode="after")
    def validate_redis_timeouts(self) -> Self:
        socket_timeout_ms = self.redis_socket_timeout_seconds * 1_000
        if self.redis_worker_block_ms >= socket_timeout_ms:
            raise ValueError(
                "redis_worker_block_ms must be shorter than "
                "redis_socket_timeout_seconds"
            )

        if self.worker_heartbeat_interval_seconds >= self.worker_lease_duration_seconds:
            raise ValueError(
                "worker_heartbeat_interval_seconds must be shorter than "
                "worker_lease_duration_seconds"
            )

        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Required values are loaded by BaseSettings from environment sources.
    return Settings()  # pyright: ignore[reportCallIssue]

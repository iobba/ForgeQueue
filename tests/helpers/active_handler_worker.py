"""Run a real worker with a controlled child to observe parent-death behavior."""

import asyncio
import os
import sys
import time
from pathlib import Path

from forgequeue.broker.redis import (
    RedisDeadLetterBroker,
    RedisJobBroker,
    create_redis_client,
)
from forgequeue.core.config import get_settings
from forgequeue.db.session import create_database_engine, create_session_factory
from forgequeue.worker.child_protocol import MAX_REQUEST_BYTES, decode_request
from forgequeue.worker.executor import HandlerSubprocessExecutor
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.runner import Worker

HELPER_PATH = Path(__file__).resolve()


def write_checkpoint(path: Path, content: str) -> None:
    # Publish complete contents so the test never reads a partially written PID.
    temporary_path = path.with_suffix(".tmp")
    temporary_path.write_text(content)
    temporary_path.replace(path)


def run_controlled_child(ready_file: Path, probe_file: Path, proof_file: Path) -> None:
    decode_request(sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1))
    write_checkpoint(ready_file, f"{os.getpid()} {os.getppid()}")
    # A bounded lifetime is a fallback if the test runner itself disappears.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if probe_file.exists() and not proof_file.exists():
            # The test creates the probe only after confirming the worker's death.
            # Writing this file proves execution continued after that point.
            write_checkpoint(proof_file, str(os.getpid()))
        time.sleep(0.01)


async def run_worker(ready_file: Path, probe_file: Path, proof_file: Path) -> None:
    settings = get_settings()
    if settings.app_env != "test" or settings.postgres_db != "forgequeue_test":
        raise RuntimeError("The active-handler worker must use the test database")
    if not all(
        name.startswith("forgequeue:test:")
        for name in (settings.redis_jobs_stream, settings.redis_dead_letter_stream)
    ):
        raise RuntimeError("The active-handler worker must use isolated test streams")

    engine = create_database_engine(settings)
    try:
        redis_client = create_redis_client(settings)
        try:
            broker = RedisJobBroker(
                redis_client,
                stream_name=settings.redis_jobs_stream,
                group_name=settings.redis_worker_group,
            )
            processor = JobProcessor(
                broker,
                RedisDeadLetterBroker(
                    redis_client, stream_name=settings.redis_dead_letter_stream
                ),
                create_session_factory(engine),
                executor=HandlerSubprocessExecutor(
                    command=(
                        sys.executable,
                        str(HELPER_PATH),
                        "child",
                        str(ready_file),
                        str(probe_file),
                        str(proof_file),
                    ),
                ),
                handler_timeout_seconds=20,
            )
            worker = Worker(broker, processor, worker_id="worker-with-active-child")
            await worker.run_once(block_ms=None)
        finally:
            await redis_client.aclose()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    ready_file, probe_file, proof_file = (Path(argument) for argument in sys.argv[2:5])
    if sys.argv[1] == "child":
        run_controlled_child(ready_file, probe_file, proof_file)
    elif sys.argv[1] == "worker":
        asyncio.run(run_worker(ready_file, probe_file, proof_file))
    else:
        raise ValueError("Expected worker or child mode")

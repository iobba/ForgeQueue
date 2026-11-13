"""Run a real worker with a controlled child to observe parent-death behavior."""

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Literal
from unittest.mock import patch

from forgequeue.broker.redis import (
    RedisDeadLetterBroker,
    RedisJobBroker,
    create_redis_client,
)
from forgequeue.core.config import get_settings
from forgequeue.db.session import create_database_engine, create_session_factory
from forgequeue.worker import supervisor_main, supervisor_pipes
from forgequeue.worker.child_protocol import MAX_REQUEST_BYTES, decode_request
from forgequeue.worker.executor import HandlerSubprocessExecutor
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.runner import Worker
from forgequeue.worker.supervisor_protocol import SupervisorStatus, encode_status

HELPER_PATH = Path(__file__).resolve()


def write_checkpoint(path: Path, content: str) -> None:
    # Publish complete contents so the test never reads a partially written PID.
    temporary_path = path.with_suffix(".tmp")
    temporary_path.write_text(content)
    temporary_path.replace(path)


def run_controlled_child(ready_file: Path) -> None:
    decode_request(sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1))
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    write_checkpoint(ready_file, f"{os.getpid()} {os.getppid()}")
    time.sleep(30)  # Longer than the test deadline; bounds a broken test run.


def run_input_waiting_child(ready_file: Path) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if sys.stdin.buffer.read(1) != b"{":
        raise RuntimeError("Expected the request prefix before waiting for input")
    write_checkpoint(ready_file, f"{os.getpid()} {os.getppid()}")
    # The test worker keeps stdin open without sending the remaining request.
    remaining = sys.stdin.buffer.read(MAX_REQUEST_BYTES)
    write_checkpoint(ready_file.with_suffix(".input-ended"), str(len(remaining)))
    # Worker death also closes stdin. Do not let that EOF alone make the test
    # pass: remain alive, ignore SIGTERM, and require supervisor SIGKILL/reaping.
    time.sleep(30)


def pause_supervisor(ready_file: Path) -> None:
    write_checkpoint(
        ready_file.with_suffix(".supervisor-ready"), f"{os.getpid()} {os.getppid()}"
    )
    deadline = time.monotonic() + 15
    while not ready_file.with_suffix(".release-supervisor").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Test did not release the paused supervisor")
        time.sleep(0.01)


def run_test_supervisor(
    ready_file: Path,
    *,
    launch_gate: Literal["before-launch", "during-launch"] | None = None,
) -> None:
    """Record real supervisor outcomes, optionally gating the handler launch."""

    def record_status(status: SupervisorStatus) -> bytes:
        encoded = encode_status(status)
        # The dead worker cannot read its status pipe. Record the real outcome
        # separately without changing it or performing any test-side cleanup.
        write_checkpoint(
            ready_file.with_suffix(".supervisor-status"), encoded.decode("utf-8")
        )
        return encoded

    supervisor_main.encode_status = record_status
    if launch_gate == "during-launch":

        class LaunchGatedProcess(subprocess.Popen[bytes]):
            def __init__(self, args: tuple[str, ...], *, close_fds: bool) -> None:
                # Reaching Popen proves the real initial lifeline check passed.
                # Keep that decision, let the test kill the worker, then really
                # launch the child. All monitoring/cleanup remains unchanged.
                pause_supervisor(ready_file)
                super().__init__(args, close_fds=close_fds)
                write_checkpoint(
                    ready_file.with_suffix(".launched-child"),
                    f"{self.pid} {os.getpid()}",
                )

        with patch.object(subprocess, "Popen", LaunchGatedProcess):
            supervisor_main.main()
    else:
        if launch_gate == "before-launch":
            pause_supervisor(ready_file)
        supervisor_main.main()


async def run_worker(
    ready_file: Path,
    *,
    launch_gate: Literal["before-launch", "during-launch"] | None = None,
    wait_for_input: bool = False,
) -> None:
    settings = get_settings()
    if settings.app_env != "test" or settings.postgres_db != "forgequeue_test":
        raise RuntimeError("The active-handler worker must use the test database")
    if not all(
        name.startswith("forgequeue:test:")
        for name in (settings.redis_jobs_stream, settings.redis_dead_letter_stream)
    ):
        raise RuntimeError("The active-handler worker must use isolated test streams")

    if launch_gate is not None or wait_for_input:
        # Only this test worker uses the helper; production launch is unchanged.
        supervisor_pipes.SUPERVISOR_COMMAND = (
            sys.executable,
            str(HELPER_PATH),
            f"supervisor-{launch_gate or 'input-wait'}",
            str(ready_file),
        )

    command = (sys.executable, str(HELPER_PATH), "child", str(ready_file))
    if launch_gate == "during-launch":
        # This child must stay alive even if the dead worker sent no payload.
        # Its PID is recorded by Popen, so cleanup can happen before Python starts.
        command = (sys.executable, "-c", "import time; time.sleep(30)")
    elif wait_for_input:
        command = (
            sys.executable,
            str(HELPER_PATH),
            "child-input-wait",
            str(ready_file),
        )

        async def send_prefix_only(
            self: supervisor_pipes.SupervisorPipes, request: bytes
        ) -> None:
            try:
                if os.write(self.request.writer.fileno(), request[:1]) != 1:
                    raise RuntimeError("Could not send the request prefix")
                # Hold the writer open until this real worker is killed. Neither
                # a complete request nor normal payload EOF reaches the child.
                await asyncio.Event().wait()
            finally:
                self.request.writer.close()

        supervisor_pipes.SupervisorPipes.send = send_prefix_only

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
                executor=HandlerSubprocessExecutor(command=command),
                handler_timeout_seconds=20,
            )
            worker = Worker(broker, processor, worker_id="worker-with-active-child")
            await worker.run_once(block_ms=None)
        finally:
            await redis_client.aclose()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    ready_file = Path(sys.argv[2])
    if sys.argv[1] == "child":
        run_controlled_child(ready_file)
    elif sys.argv[1] == "child-input-wait":
        run_input_waiting_child(ready_file)
    elif sys.argv[1] == "worker":
        asyncio.run(run_worker(ready_file))
    elif sys.argv[1] == "worker-before-launch":
        asyncio.run(run_worker(ready_file, launch_gate="before-launch"))
    elif sys.argv[1] == "worker-during-launch":
        asyncio.run(run_worker(ready_file, launch_gate="during-launch"))
    elif sys.argv[1] == "worker-input-wait":
        asyncio.run(run_worker(ready_file, wait_for_input=True))
    elif sys.argv[1] in (
        "supervisor-before-launch",
        "supervisor-during-launch",
        "supervisor-input-wait",
    ):
        launch_gate = (
            "before-launch"
            if sys.argv[1] == "supervisor-before-launch"
            else "during-launch"
            if sys.argv[1] == "supervisor-during-launch"
            else None
        )
        # Forward only the executor's original supervisor arguments to argparse.
        sys.argv = [sys.argv[0], *sys.argv[3:]]
        run_test_supervisor(ready_file, launch_gate=launch_gate)
    else:
        raise ValueError("Unknown parent-death helper mode")

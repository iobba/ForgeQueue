import asyncio
import os
import sys
from math import inf, nan
from pathlib import Path

import pytest

from forgequeue.jobs.attempts import JobFailureKind
from forgequeue.worker.child_protocol import MAX_RESPONSE_BYTES, HandlerRequest
from forgequeue.worker.executor import (
    HandlerChildExited,
    HandlerProtocolError,
    HandlerSubprocessExecutor,
    HandlerTimedOut,
)

pytestmark = pytest.mark.unit


def request() -> HandlerRequest:
    return HandlerRequest(
        job_type="sum_numbers",
        payload={"numbers": [10, 20, 30]},
    )


@pytest.mark.asyncio
async def test_executor_runs_real_child_and_decodes_success() -> None:
    response = await HandlerSubprocessExecutor().execute(
        request(),
        timeout_seconds=5,
    )

    assert response.status == "succeeded"
    assert response.result == {"sum": 60}


@pytest.mark.asyncio
async def test_executor_returns_classified_child_failure() -> None:
    response = await HandlerSubprocessExecutor().execute(
        HandlerRequest(job_type="sum_numbers", payload={"numbers": []}),
        timeout_seconds=5,
    )

    assert response.status == "failed"
    assert response.failure_kind is JobFailureKind.PERMANENT
    assert response.error_code == "invalid_job_payload"


@pytest.mark.asyncio
async def test_executor_rejects_nonzero_child_exit() -> None:
    executor = HandlerSubprocessExecutor(
        command=(sys.executable, "-c", "import sys; sys.exit(7)")
    )

    with pytest.raises(HandlerChildExited) as exc_info:
        await executor.execute(request(), timeout_seconds=5)

    assert exc_info.value.returncode == 7


@pytest.mark.asyncio
async def test_executor_rejects_invalid_child_output() -> None:
    executor = HandlerSubprocessExecutor(
        command=(sys.executable, "-c", "print('not-json')"),
    )

    with pytest.raises(HandlerProtocolError, match="invalid JSON"):
        await executor.execute(request(), timeout_seconds=5)


@pytest.mark.asyncio
async def test_executor_stops_child_after_oversized_response() -> None:
    executor = HandlerSubprocessExecutor(
        command=(
            sys.executable,
            "-c",
            f"import sys; sys.stdout.write('x' * {MAX_RESPONSE_BYTES * 16})",
        ),
    )

    with pytest.raises(HandlerProtocolError, match="size limit"):
        await executor.execute(request(), timeout_seconds=5)


def ignoring_termination_command(pid_file: Path) -> tuple[str, ...]:
    script = (
        "import os, signal, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "with open(sys.argv[1], 'w') as file: file.write(str(os.getpid()))\n"
        "sys.stdin.buffer.read()\n"
        "time.sleep(10)\n"
    )
    return sys.executable, "-c", script, str(pid_file)


def assert_child_reaped(pid_file: Path) -> None:
    pid = int(pid_file.read_text())
    with pytest.raises(ProcessLookupError):  # no process with this PID exists
        os.kill(pid, 0)


@pytest.mark.asyncio
async def test_timeout_kills_and_reaps_child_that_ignores_termination(
    tmp_path: Path,
) -> None:
    pid_file = tmp_path / "timeout-child.pid"
    executor = HandlerSubprocessExecutor(
        command=ignoring_termination_command(pid_file),
        termination_grace_seconds=0.1,
    )

    with pytest.raises(HandlerTimedOut):
        await executor.execute(request(), timeout_seconds=1)

    assert_child_reaped(pid_file)


@pytest.mark.asyncio
async def test_cancellation_stops_and_reaps_child(tmp_path: Path) -> None:
    pid_file = tmp_path / "cancelled-child.pid"
    executor = HandlerSubprocessExecutor(
        command=ignoring_termination_command(pid_file),
        termination_grace_seconds=0.1,
    )
    task = asyncio.create_task(executor.execute(request(), timeout_seconds=5))

    async with asyncio.timeout(2):
        while not pid_file.exists():  # noqa: ASYNC110 - another process writes this file
            await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert_child_reaped(pid_file)


@pytest.mark.parametrize("seconds", [0.0, -1.0, inf, nan])
def test_executor_rejects_invalid_grace(seconds: float) -> None:
    with pytest.raises(ValueError, match="termination_grace_seconds"):
        HandlerSubprocessExecutor(termination_grace_seconds=seconds)


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [0.0, -1.0, inf, nan])
async def test_executor_rejects_invalid_deadline(seconds: float) -> None:
    with pytest.raises(ValueError, match="timeout_seconds"):
        await HandlerSubprocessExecutor().execute(request(), timeout_seconds=seconds)

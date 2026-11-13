"""Exercise the worker side of supervision using isolated, real processes."""

import asyncio
import os
import signal
import sys
from contextlib import suppress
from pathlib import Path

import pytest

from forgequeue.worker import executor, supervisor_pipes
from forgequeue.worker.child_protocol import HandlerRequest
from forgequeue.worker.executor import (
    HandlerChildExited,
    HandlerSubprocessExecutor,
    HandlerSupervisionError,
    HandlerTimedOut,
)
from forgequeue.worker.supervisor_pipes import SupervisorPipes

pytestmark = [
    pytest.mark.unit,
    pytest.mark.asyncio,
    pytest.mark.skipif(os.name != "posix", reason="Requires POSIX process groups"),
]

REQUEST = HandlerRequest(job_type="sum_numbers", payload={"numbers": [1, 2]})


def fake_supervisor(monkeypatch: pytest.MonkeyPatch, body: str) -> None:
    script = (
        "import os, sys, time\n"
        "life = int(sys.argv[sys.argv.index('--lifeline-fd') + 1])\n"
        "status = int(sys.argv[sys.argv.index('--status-fd') + 1])\n" + body
    )
    monkeypatch.setattr(
        supervisor_pipes, "SUPERVISOR_COMMAND", (sys.executable, "-c", script)
    )


async def wait_checkpoint(path: Path) -> tuple[int, int]:
    async with asyncio.timeout(5):
        while not await asyncio.to_thread(path.exists):  # noqa: ASYNC110 - another process publishes this file
            await asyncio.sleep(0.01)
    pid, group = map(int, (await asyncio.to_thread(path.read_text)).split())
    assert group not in (0, 1, os.getpgrp())
    return pid, group


CHECKPOINT = (
    "import os, sys, time\n"
    "from pathlib import Path\n"
    "ready = Path(sys.argv[1])\n"
    "tmp = ready.with_suffix('.tmp')\n"
    "tmp.write_text(f'{os.getpid()} {os.getpgrp()}')\n"
    "tmp.replace(ready)\n"
)


async def test_large_request_and_response_make_progress_in_both_directions() -> None:
    # This handler writes before reading. A send-then-read implementation can
    # deadlock when the two messages exceed the pipes' available capacity.
    script = (
        "import json, sys\n"
        "print(json.dumps({'status':'succeeded','result':{'text':'b'*60000}}), flush=True)\n"
        "request = json.load(sys.stdin)\n"
        "assert request['payload']['text'] == 'a'*60000\n"
    )
    runner = HandlerSubprocessExecutor(command=(sys.executable, "-c", script))
    request = HandlerRequest(job_type="large", payload={"text": "a" * 60000})
    response = await runner.execute(request, timeout_seconds=5)
    assert response.result == {"text": "b" * 60000}


async def test_cancellation_is_not_reclassified_when_confirmation_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ready = tmp_path / "started"
    fake_supervisor(
        monkeypatch,
        "from pathlib import Path\n"
        f"path = Path({str(ready)!r})\n"
        "tmp = path.with_suffix('.tmp')\n"
        "tmp.write_text(f'{os.getpid()} {os.getpgrp()}')\ntmp.replace(path)\n"
        "assert os.read(life, 1) == b''\n",  # Exit with no confirmation.
    )
    task = asyncio.create_task(
        HandlerSubprocessExecutor().execute(REQUEST, timeout_seconds=20)
    )
    try:
        pid, _ = await wait_checkpoint(ready)
    finally:
        task.cancel()
        async with asyncio.timeout(5):
            await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.parametrize("code", [125, -signal.SIGTERM])
async def test_preserves_actual_handler_exit_not_supervisor_exit(code: int) -> None:
    script = (
        f"import os; os.kill(os.getpid(), {-code})"
        if code < 0
        else f"raise SystemExit({code})"
    )
    runner = HandlerSubprocessExecutor(command=(sys.executable, "-c", script))
    with pytest.raises(HandlerChildExited) as error:
        await runner.execute(REQUEST, timeout_seconds=5)
    assert error.value.returncode == code


@pytest.mark.parametrize(
    ("record", "exit_code"),
    [
        (b"", 0),
        (b"not JSON", 0),
        (b"x" * 257, 0),
        (b'{"version":1,"kind":"exited","returncode":0}', 7),
        (b'{"version":1,"kind":"stopped","returncode":0}', 0),
        (b'{"version":1,"kind":"not_started","returncode":null}', 0),
    ],
)
async def test_handler_result_alone_cannot_confirm_success(
    monkeypatch: pytest.MonkeyPatch, record: bytes, exit_code: int
) -> None:
    fake_supervisor(
        monkeypatch,
        "sys.stdin.buffer.read()\n"
        'print(\'{"protocol_version":1,"status":"succeeded","result":{}}\')\n'
        f"os.write(status, {record!r})\nraise SystemExit({exit_code})\n",
    )
    with pytest.raises(HandlerSupervisionError):
        await HandlerSubprocessExecutor().execute(REQUEST, timeout_seconds=5)


@pytest.mark.parametrize("confirmed", [True, False])
async def test_deadline_requires_confirmation_but_not_a_specific_signal(
    monkeypatch: pytest.MonkeyPatch, confirmed: bool
) -> None:
    record = b'{"version":1,"kind":"exited","returncode":0}' if confirmed else b""
    # No response before the deadline. A handler that finished at the boundary
    # may already be reaped without the supervisor sending it a signal.
    fake_supervisor(
        monkeypatch, f"assert os.read(life, 1) == b''\nos.write(status, {record!r})\n"
    )
    with pytest.raises(HandlerTimedOut if confirmed else HandlerSupervisionError):
        await HandlerSubprocessExecutor().execute(REQUEST, timeout_seconds=0.15)


async def test_unresponsive_supervisor_cannot_turn_into_normal_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pid_file = tmp_path / "supervisor"
    fake_supervisor(
        monkeypatch,
        f"from pathlib import Path\nPath({str(pid_file)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(30)\n",
    )
    monkeypatch.setattr(executor, "CLEANUP_MARGIN_SECONDS", 0.5)
    runner = HandlerSubprocessExecutor(termination_grace_seconds=0.1)
    async with asyncio.timeout(5):
        with pytest.raises(HandlerSupervisionError):
            await runner.execute(REQUEST, timeout_seconds=0.5)
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


async def test_supervisor_crash_does_not_wait_for_handler_stdout_or_kill_other_attempt(
    tmp_path: Path,
) -> None:
    healthy_file = tmp_path / "healthy"
    crashed_file = tmp_path / "crashed"
    release = tmp_path / "release"
    healthy_script = CHECKPOINT + (
        "while not Path(sys.argv[2]).exists(): time.sleep(0.01)\n"
        'print(\'{"protocol_version":1,"status":"succeeded","result":{}}\')\n'
    )
    crash_script = CHECKPOINT + (
        "import signal\nos.kill(os.getppid(), signal.SIGKILL)\n"
        "time.sleep(30)\n"  # Orphan deliberately keeps stdout open.
    )
    healthy = HandlerSubprocessExecutor(
        command=(sys.executable, "-c", healthy_script, str(healthy_file), str(release))
    )
    broken = HandlerSubprocessExecutor(
        command=(sys.executable, "-c", crash_script, str(crashed_file))
    )
    other = asyncio.create_task(healthy.execute(REQUEST, timeout_seconds=20))
    group: int | None = None
    try:
        healthy_pid, healthy_group = await wait_checkpoint(healthy_file)
        async with asyncio.timeout(5):
            with pytest.raises(HandlerSupervisionError):
                await broken.execute(REQUEST, timeout_seconds=20)
            orphan_pid, group = await wait_checkpoint(crashed_file)
            assert group != healthy_group
            while True:
                try:
                    os.kill(orphan_pid, 0)
                except ProcessLookupError:
                    break
                await asyncio.sleep(0.01)
        group = None
        os.kill(healthy_pid, 0)
        release.touch()
        assert (await asyncio.wait_for(other, 5)).status == "succeeded"
    finally:
        other.cancel()
        await asyncio.gather(other, return_exceptions=True)
        if group is not None:
            with suppress(ProcessLookupError):
                os.killpg(group, signal.SIGKILL)


@pytest.mark.parametrize("delay_before_spawn", [True, False])
async def test_repeated_cancellation_during_launch_collects_process_and_closes_fds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, delay_before_spawn: bool
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    captured: list[SupervisorPipes] = []
    processes: list[asyncio.subprocess.Process] = []
    original = SupervisorPipes.launch
    marker = tmp_path / "child"

    async def delayed(
        pipes: SupervisorPipes, command: tuple[str, ...], grace: float
    ) -> asyncio.subprocess.Process:
        captured.append(pipes)
        if delay_before_spawn:
            entered.set()
            await release.wait()
        process = await original(pipes, command, grace)
        processes.append(process)
        if not delay_before_spawn:
            await wait_checkpoint(marker)
            entered.set()
            await release.wait()
        return process

    monkeypatch.setattr(SupervisorPipes, "launch", delayed)
    runner = HandlerSubprocessExecutor(
        command=(sys.executable, "-c", CHECKPOINT + "time.sleep(30)", str(marker))
    )
    task = asyncio.create_task(runner.execute(REQUEST, timeout_seconds=30))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)  # Deliver cancellation while launch is paused.
        assert captured[0].lifeline.writer.closed
        task.cancel()
    finally:
        release.set()
        task.cancel()
        async with asyncio.timeout(5):
            await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
    assert len(processes) == 1 and processes[0].returncode == 0
    if delay_before_spawn:
        assert not marker.exists()
    else:
        pid, _ = await wait_checkpoint(marker)
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    pipes = captured[0]
    assert all(
        p.reader.closed and p.writer.closed
        for p in (pipes.request, pipes.response, pipes.status, pipes.lifeline)
    )


async def test_spawn_error_closes_all_worker_fds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original = SupervisorPipes.launch
    captured: list[SupervisorPipes] = []

    async def launch(
        pipes: SupervisorPipes, command: tuple[str, ...], grace: float
    ) -> asyncio.subprocess.Process:
        captured.append(pipes)
        return await original(pipes, command, grace)

    monkeypatch.setattr(SupervisorPipes, "launch", launch)
    monkeypatch.setattr(
        supervisor_pipes, "SUPERVISOR_COMMAND", (str(tmp_path / "absent"),)
    )
    with pytest.raises(HandlerSupervisionError, match="launch"):
        await HandlerSubprocessExecutor().execute(REQUEST, timeout_seconds=5)
    pipes = captured[0]
    assert all(
        p.reader.closed and p.writer.closed
        for p in (pipes.request, pipes.response, pipes.status, pipes.lifeline)
    )

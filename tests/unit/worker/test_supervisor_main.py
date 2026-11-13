"""Real subprocess checks, isolated from PostgreSQL, Redis, and the executor."""

import os
import selectors
import signal
import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import BinaryIO

import pytest

from forgequeue.worker import supervisor_main
from forgequeue.worker.child_protocol import (
    HandlerRequest,
    decode_response,
    encode_request,
)
from forgequeue.worker.supervisor_protocol import (
    MAX_STATUS_BYTES,
    SupervisorStatus,
    decode_status,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(os.name != "posix", reason="Requires POSIX pipes and signals"),
]


@contextmanager
def pipe() -> Generator[tuple[BinaryIO, BinaryIO]]:
    read_fd, write_fd = os.pipe()
    with os.fdopen(read_fd, "rb", buffering=0) as reader:
        with os.fdopen(write_fd, "wb", buffering=0) as writer:
            yield reader, writer


def read_bounded(fd: int, *, until_newline: bool = False) -> bytes:
    """Wait for a tiny checkpoint/status with an outer deadline, never a blind read."""
    data = bytearray()
    deadline = monotonic() + 5
    with selectors.DefaultSelector() as selector:
        selector.register(fd, selectors.EVENT_READ)
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0 or not selector.select(remaining):
                pytest.fail("Timed out reading supervisor output")
            chunk = os.read(fd, 1 if until_newline else MAX_STATUS_BYTES + 1)
            if not chunk:
                return bytes(data)
            data.extend(chunk)
            assert len(data) <= MAX_STATUS_BYTES
            if until_newline and chunk == b"\n":
                return bytes(data)


@dataclass
class RunningSupervisor:
    process: subprocess.Popen[bytes]
    lifeline: BinaryIO
    status: BinaryIO

    def checkpoint(self) -> bytes:
        assert self.process.stdout is not None
        return read_bounded(self.process.stdout.fileno(), until_newline=True).strip()

    def completion(self) -> SupervisorStatus:
        assert self.process.wait(timeout=5) == 0
        return decode_status(read_bounded(self.status.fileno()))


@contextmanager
def start_supervisor(
    script: str | None = None,
    *,
    closed_lifeline: bool = False,
    extra_arguments: tuple[str, ...] = (),
    command: tuple[str, ...] | None = None,
) -> Generator[RunningSupervisor]:
    with (
        pipe() as (lifeline_read, lifeline_write),
        pipe() as (status_read, status_write),
    ):
        if closed_lifeline:
            lifeline_write.close()
        child_command = command or ((sys.executable, "-c", script) if script else ())
        arguments = (
            sys.executable,
            "-m",
            "forgequeue.worker.supervisor_main",
            "--lifeline-fd",
            str(lifeline_read.fileno()),
            "--status-fd",
            str(status_write.fileno()),
            "--termination-grace-seconds",
            "0.1",
            *extra_arguments,
            *(("--", *child_command) if child_command else ()),
        )
        process = subprocess.Popen[bytes](
            arguments,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(lifeline_read.fileno(), status_write.fileno()),
            # Test fallback cleanup must never reach the test runner or other jobs.
            start_new_session=True,
            env={
                **os.environ,
                "FORGEQUEUE_TEST_CONTROL_FDS": (
                    f"{lifeline_read.fileno()},{status_write.fileno()}"
                ),
            },
        )
        lifeline_read.close()
        status_write.close()
        try:
            yield RunningSupervisor(process, lifeline_write, status_read)
        finally:
            lifeline_write.close()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            finally:
                # Last resort only: assertions above must prove cleanup first.
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()


def assert_reaped(pid: int) -> None:
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_supervisor_passes_real_handler_request_and_result_unchanged() -> None:
    with start_supervisor() as running:
        output, errors = running.process.communicate(
            encode_request(
                HandlerRequest(job_type="sum_numbers", payload={"numbers": [10, 20]})
            ),
            timeout=5,
        )

        assert errors == b""
        assert decode_response(output).result == {"sum": 30}
        assert running.completion() == SupervisorStatus(
            version=1, kind="exited", returncode=0
        )


@pytest.mark.parametrize("exit_code", [0, 7, 125, -signal.SIGTERM])
def test_handler_exit_code_is_separate_from_supervisor_exit(exit_code: int) -> None:
    script = (
        "import os, sys\n"
        "print(os.getpid(), flush=True)\n"
        f"if {exit_code} < 0: os.kill(os.getpid(), {-exit_code})\n"
        f"sys.exit({exit_code})\n"
    )
    with start_supervisor(script) as running:
        pid = int(running.checkpoint())

        assert running.completion() == SupervisorStatus(
            version=1, kind="exited", returncode=exit_code
        )
        assert_reaped(pid)


@pytest.mark.parametrize("ignore_termination", [False, True])
def test_lifeline_closure_stops_and_reaps_handler_blocked_on_input(
    ignore_termination: bool,
) -> None:
    script = (
        "import os, signal, sys\n"
        f"if {ignore_termination}: signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print(os.getpid(), flush=True)\n"
        "sys.stdin.buffer.read()\n"
    )
    with start_supervisor(script) as running:
        pid = int(running.checkpoint())
        # Deliberately leave stdin open: cleanup must not wait for a payload.
        running.lifeline.close()

        assert running.completion() == SupervisorStatus(
            version=1,
            kind="stopped",
            returncode=-signal.SIGKILL if ignore_termination else -signal.SIGTERM,
        )
        assert_reaped(pid)


def test_handler_gets_grace_to_exit_after_termination() -> None:
    script = (
        "import os, signal, sys\n"
        "signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))\n"
        "print(os.getpid(), flush=True)\n"
        "sys.stdin.buffer.read()\n"
    )
    with start_supervisor(script) as running:
        pid = int(running.checkpoint())
        running.lifeline.close()

        assert running.completion() == SupervisorStatus(
            version=1, kind="stopped", returncode=0
        )
        assert_reaped(pid)


def test_normal_payload_eof_is_not_a_cleanup_signal() -> None:
    script = (
        "import os, signal, sys\n"
        "print(os.getpid(), flush=True)\n"
        "sys.stdin.buffer.read()\n"
        "print('payload-ended', flush=True)\n"
        "signal.pause()\n"
    )
    with start_supervisor(script) as running:
        pid = int(running.checkpoint())
        assert running.process.stdin is not None
        running.process.stdin.close()
        assert running.checkpoint() == b"payload-ended"
        # A bounded negative assertion: wait longer than the supervisor poll.
        with pytest.raises(subprocess.TimeoutExpired):
            running.process.wait(timeout=0.2)
        os.kill(pid, 0)

        running.lifeline.close()
        assert running.completion().kind == "stopped"
        assert_reaped(pid)


def test_already_closed_lifeline_prevents_handler_launch(tmp_path: Path) -> None:
    marker = tmp_path / "must-not-start"
    with start_supervisor(
        f"from pathlib import Path; Path({str(marker)!r}).touch()",
        closed_lifeline=True,
    ) as running:
        assert running.completion() == SupervisorStatus(
            version=1, kind="not_started", returncode=None
        )
        assert not marker.exists()
        assert running.process.stdout is not None
        assert read_bounded(running.process.stdout.fileno()) == b""


def test_missing_status_reader_does_not_prevent_cleanup() -> None:
    script = (
        "import os, signal, sys\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print(os.getpid(), flush=True)\n"
        "sys.stdin.buffer.read()\n"
    )
    with start_supervisor(script) as running:
        pid = int(running.checkpoint())
        running.status.close()
        running.lifeline.close()

        assert running.process.wait(timeout=5) == 0
        assert_reaped(pid)


def test_control_fds_are_not_inherited_and_handler_stays_in_attempt_group() -> None:
    script = (
        "import os\n"
        "for fd in map(int, os.environ['FORGEQUEUE_TEST_CONTROL_FDS'].split(',')):\n"
        "    try: os.fstat(fd)\n"
        "    except OSError: pass\n"
        "    else: raise AssertionError('control descriptor leaked')\n"
        "print(os.getpid(), os.getppid(), os.getpgrp(), flush=True)\n"
    )
    with start_supervisor(script) as running:
        pid, parent, group = map(int, running.checkpoint().split())

        assert parent == group == running.process.pid
        assert group != os.getpgrp()
        assert running.completion().returncode == 0
        assert_reaped(pid)


def test_supervisor_does_not_retain_handler_stdout() -> None:
    script = (
        "import os, sys\n"
        "print(os.getpid(), flush=True)\n"
        "os.close(1)\n"
        "sys.stdin.buffer.read()\n"
    )
    with start_supervisor(script) as running:
        pid = int(running.checkpoint())
        assert running.process.stdout is not None
        # EOF is visible while the handler and supervisor are both still alive.
        assert read_bounded(running.process.stdout.fileno()) == b""
        assert running.process.poll() is None
        os.kill(pid, 0)

        running.lifeline.close()
        assert running.completion().kind == "stopped"
        assert_reaped(pid)


def test_lifeline_protocol_error_still_stops_existing_handler() -> None:
    script = "import os, sys\nprint(os.getpid(), flush=True)\nsys.stdin.buffer.read()\n"
    with start_supervisor(script) as running:
        pid = int(running.checkpoint())
        running.lifeline.write(b"unexpected")

        assert (
            running.process.wait(timeout=5) == supervisor_main.SUPERVISOR_FAILURE_EXIT
        )
        assert read_bounded(running.status.fileno()) == b""
        assert_reaped(pid)


def test_handler_launch_failure_is_not_reported_as_completed(tmp_path: Path) -> None:
    with start_supervisor(command=(str(tmp_path / "missing-executable"),)) as running:
        assert (
            running.process.wait(timeout=5) == supervisor_main.SUPERVISOR_FAILURE_EXIT
        )
        assert read_bounded(running.status.fileno()) == b""


@pytest.mark.parametrize("grace", ["0", "-1", "nan", "inf"])
def test_invalid_grace_is_rejected_before_handler_launch(
    grace: str, tmp_path: Path
) -> None:
    marker = tmp_path / "must-not-start"
    with start_supervisor(
        f"from pathlib import Path; Path({str(marker)!r}).touch()",
        extra_arguments=("--termination-grace-seconds", grace),
    ) as running:
        assert (
            running.process.wait(timeout=5) == supervisor_main.SUPERVISOR_FAILURE_EXIT
        )
        assert read_bounded(running.status.fileno()) == b""
        assert not marker.exists()


def test_unsupported_platform_is_rejected_before_using_fds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as patch:
        patch.setattr(supervisor_main.os, "name", "nt")
        with pytest.raises(RuntimeError, match="requires POSIX"):
            supervisor_main.run_supervisor(
                lifeline_fd=-1, status_fd=-1, termination_grace_seconds=0.1
            )

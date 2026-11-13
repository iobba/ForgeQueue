"""Internal, single-use POSIX supervisor for one handler attempt."""

import argparse
import os
import selectors
import stat
import subprocess
import sys
from contextlib import suppress
from math import isfinite

from forgequeue.worker.supervisor_protocol import SupervisorStatus, encode_status

POLL_INTERVAL_SECONDS = 0.05
SUPERVISOR_FAILURE_EXIT = 1


def _validate_pipe(fd: int, *, write: bool) -> None:
    # Import only after the platform check: fcntl is POSIX-only.
    import fcntl

    if not stat.S_ISFIFO(os.fstat(fd).st_mode):
        raise ValueError("supervisor control descriptors must be pipes")
    mode = fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE
    if mode != (os.O_WRONLY if write else os.O_RDONLY):
        raise ValueError("supervisor control pipe has the wrong direction")
    os.set_inheritable(fd, False)
    os.set_blocking(fd, False)


def _lifeline_closed(selector: selectors.BaseSelector, fd: int, timeout: float) -> bool:
    if not selector.select(timeout):
        return False
    try:
        data = os.read(fd, 1)
    except BlockingIOError:
        return False
    if data:
        raise ValueError("lifeline must not carry payload data")
    return True


def _stop_and_reap(child: subprocess.Popen[bytes], grace: float) -> int:
    if child.poll() is None:
        with suppress(ProcessLookupError):
            child.terminate()
        try:
            return child.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            with suppress(ProcessLookupError):
                child.kill()
    return child.wait()


def _watch_handler(
    selector: selectors.BaseSelector,
    *,
    lifeline_fd: int,
    command: tuple[str, ...],
    grace: float,
) -> SupervisorStatus:
    if _lifeline_closed(selector, lifeline_fd, 0):
        return SupervisorStatus(version=1, kind="not_started", returncode=None)

    # Only stdin/stdout/stderr reach the handler, never either control pipe.
    child = subprocess.Popen[bytes](command, close_fds=True)
    try:
        # These are pass-through channels. Retaining our copies could hide EOF
        # or a broken pipe from the worker after the handler has exited.
        os.close(0)
        os.close(1)
        while True:
            if child.poll() is not None:
                return SupervisorStatus(
                    version=1, kind="exited", returncode=child.wait()
                )
            if _lifeline_closed(selector, lifeline_fd, POLL_INTERVAL_SECONDS):
                return SupervisorStatus(
                    version=1,
                    kind="stopped",
                    returncode=_stop_and_reap(child, grace),
                )
    finally:
        # Internal errors must not abandon a child that was already launched.
        _stop_and_reap(child, grace)


def run_supervisor(
    *,
    lifeline_fd: int,
    status_fd: int,
    termination_grace_seconds: float,
    command: tuple[str, ...] | None = None,
) -> None:
    """Own the control FDs and stdio in a dedicated supervisor process only."""
    if os.name != "posix":
        raise RuntimeError("handler supervision requires POSIX")
    if lifeline_fd < 3 or status_fd < 3 or lifeline_fd == status_fd:
        raise ValueError("control descriptors must be distinct and above stderr")

    try:
        if not isfinite(termination_grace_seconds) or termination_grace_seconds <= 0:
            raise ValueError("termination_grace_seconds must be finite and positive")
        if command is not None and not command:
            raise ValueError("command must not be empty")
        _validate_pipe(lifeline_fd, write=False)
        _validate_pipe(status_fd, write=True)
        with selectors.DefaultSelector() as selector:
            selector.register(lifeline_fd, selectors.EVENT_READ)
            status = _watch_handler(
                selector,
                lifeline_fd=lifeline_fd,
                command=command
                or (sys.executable, "-m", "forgequeue.worker.child_main"),
                grace=termination_grace_seconds,
            )

        # Cleanup is already complete. A dead worker cannot read confirmation,
        # and a full/misconfigured pipe must never block this process forever.
        encoded = encode_status(status)
        try:
            written = os.write(status_fd, encoded)
        except BrokenPipeError:
            return
        if written != len(encoded):
            raise OSError("incomplete supervisor status write")
    finally:
        for fd in (lifeline_fd, status_fd):
            with suppress(OSError):
                os.close(fd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Internal handler supervisor")
    parser.add_argument("--lifeline-fd", type=int, required=True)
    parser.add_argument("--status-fd", type=int, required=True)
    parser.add_argument("--termination-grace-seconds", type=float, default=1.0)
    # Explicit commands let subprocess tests exercise crashes and stuck handlers.
    parser.add_argument("command", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    command = tuple(arguments.command)
    if command[:1] == ("--",):
        command = command[1:]
        if not command:
            parser.error("handler command after -- must not be empty")
    try:
        run_supervisor(
            lifeline_fd=arguments.lifeline_fd,
            status_fd=arguments.status_fd,
            termination_grace_seconds=arguments.termination_grace_seconds,
            command=command or None,
        )
    except OSError, ValueError, RuntimeError:
        # Diagnostics never share stdout with the handler result protocol.
        print("Handler supervisor failed", file=sys.stderr)
        raise SystemExit(SUPERVISOR_FAILURE_EXIT) from None


if __name__ == "__main__":
    main()

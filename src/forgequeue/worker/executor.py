import asyncio
import os
import signal
import sys
from contextlib import ExitStack, suppress
from math import isfinite
from typing import Protocol

from forgequeue.worker.child_protocol import (
    MAX_RESPONSE_BYTES,
    HandlerRequest,
    HandlerResponse,
    decode_response,
    encode_request,
)
from forgequeue.worker.supervisor_pipes import SupervisorPipes
from forgequeue.worker.supervisor_protocol import SupervisorStatus

CLEANUP_MARGIN_SECONDS = 5.0


class HandlerSubprocessError(RuntimeError):
    """The child did not produce a trusted handler outcome."""


class HandlerSupervisionError(HandlerSubprocessError):
    """Supervisor failure: do not classify or retry as a handler timeout."""


class HandlerTimedOut(HandlerSubprocessError):
    """The execution deadline expired and cleanup was confirmed."""


class HandlerChildExited(HandlerSubprocessError):
    def __init__(self, returncode: int) -> None:
        self.returncode = returncode
        super().__init__(f"Handler child exited with code {returncode}")


class HandlerProtocolError(HandlerSubprocessError):
    """The child produced an invalid or oversized response."""


class HandlerExecutor(Protocol):
    async def execute(
        self,
        request: HandlerRequest,
        *,
        timeout_seconds: float,
    ) -> HandlerResponse: ...


class HandlerSubprocessExecutor:
    def __init__(
        self,
        *,
        # Override the handler command in tests, not the supervisor.
        command: tuple[str, ...] | None = None,
        termination_grace_seconds: float = 1.0,
    ) -> None:
        if command is not None and not command:
            raise ValueError("command must not be empty")
        if not isfinite(termination_grace_seconds) or termination_grace_seconds <= 0:
            raise ValueError("termination_grace_seconds must be finite and positive")
        self._command = command or (
            sys.executable,
            "-m",
            "forgequeue.worker.child_main",
        )
        self._termination_grace_seconds = termination_grace_seconds

    async def execute(
        self,
        request: HandlerRequest,
        *,
        timeout_seconds: float,
    ) -> HandlerResponse:
        if not isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive")
        if os.name != "posix":
            raise HandlerSupervisionError("Handler supervision requires POSIX")
        encoded = encode_request(request)
        stop = asyncio.get_running_loop().create_future()
        with ExitStack() as resources:
            pipes = SupervisorPipes(resources)
            # Cancellation must not discard the process handle mid-launch or
            # close inherited descriptors before the spawn has finished.
            attempt = asyncio.create_task(
                self._run(pipes, encoded, timeout_seconds, stop)
            )
            try:
                return await asyncio.shield(attempt)
            except asyncio.CancelledError:
                pipes.lifeline.writer.close()  # tell the supervisor process to stop the handler.
                stop.set_result(
                    None
                )  # tell the Python _run() task to take its stop path.
                joined = asyncio.gather(attempt, return_exceptions=True)
                while not joined.done():
                    with suppress(asyncio.CancelledError):
                        await asyncio.shield(joined)
                raise

    async def _run(
        self,
        pipes: SupervisorPipes,
        request: bytes,
        timeout_seconds: float,
        stop: asyncio.Future[None],
    ) -> HandlerResponse:
        try:
            process = await pipes.launch(self._command, self._termination_grace_seconds)
        except OSError as exc:
            raise HandlerSupervisionError(
                "Could not launch handler supervisor"
            ) from exc

        confirmation = asyncio.create_task(self._confirm(process, pipes))
        exchange = asyncio.create_task(self._exchange(pipes, request))
        normal = asyncio.gather(exchange, confirmation)
        try:
            done, _ = await asyncio.wait(
                (normal, stop),
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop in done:
                raise asyncio.CancelledError
            if normal not in done:
                # The finally block must confirm cleanup before this escapes.
                raise HandlerTimedOut("Handler child exceeded its deadline")
            output, status = normal.result()
            if status.kind != "exited" or status.returncode is None:
                raise HandlerSupervisionError(
                    "Unexpected supervisor stop on normal path"
                )
            if status.returncode != 0:
                raise HandlerChildExited(status.returncode)
            try:
                return decode_response(output)
            except ValueError as exc:
                raise HandlerProtocolError(
                    "Handler child returned invalid JSON"
                ) from exc
        finally:
            pipes.lifeline.writer.close()
            exchange.cancel()
            try:
                async with asyncio.timeout(
                    self._termination_grace_seconds + CLEANUP_MARGIN_SECONDS
                ):
                    await asyncio.shield(confirmation)
            except (TimeoutError, HandlerSupervisionError) as exc:
                # A missing/invalid confirmation is NEVER a normal timeout.
                await self._kill_attempt_group(process)
                raise HandlerSupervisionError(
                    "Handler cleanup was not confirmed"
                ) from exc
            finally:
                confirmation.cancel()
                await asyncio.gather(
                    normal, exchange, confirmation, return_exceptions=True
                )

    @staticmethod
    async def _confirm(
        process: asyncio.subprocess.Process, pipes: SupervisorPipes
    ) -> SupervisorStatus:
        code = await process.wait()
        if code != 0:
            raise HandlerSupervisionError(f"Supervisor exited with code {code}")
        try:
            status = pipes.read_final_status()
        except (OSError, ValueError) as exc:
            raise HandlerSupervisionError(
                "Invalid or missing supervisor status"
            ) from exc
        if status.kind != "exited" and not pipes.lifeline.writer.closed:
            raise HandlerSupervisionError(
                "Unexpected supervisor stop with an open lifeline"
            )
        return status

    @staticmethod
    async def _exchange(pipes: SupervisorPipes, request: bytes) -> bytes:
        sender = asyncio.create_task(pipes.send(request))
        try:
            try:
                output = await pipes.read_response(MAX_RESPONSE_BYTES)
            except ValueError as exc:
                raise HandlerProtocolError(str(exc)) from exc
            await sender
            return output
        finally:
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    @staticmethod
    async def _kill_attempt_group(process: asyncio.subprocess.Process) -> None:
        # start_new_session=True made this PID the leader of only this attempt.
        try:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            async with asyncio.timeout(CLEANUP_MARGIN_SECONDS):
                await process.wait()
        except (OSError, TimeoutError) as exc:
            raise HandlerSupervisionError(
                "Could not finish attempt-group cleanup"
            ) from exc

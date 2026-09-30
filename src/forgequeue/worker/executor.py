import asyncio
import sys
from math import isfinite
from typing import Protocol

from forgequeue.worker.child_protocol import (
    MAX_RESPONSE_BYTES,
    HandlerRequest,
    HandlerResponse,
    decode_response,
    encode_request,
)


class HandlerSubprocessError(RuntimeError):
    """The child did not produce a trusted handler outcome."""


class HandlerTimedOut(HandlerSubprocessError):
    """The child exceeded its execution deadline and was stopped."""


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
        # override the child command in tests to exercise failure and cleanup paths.
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
        encoded_request = encode_request(request)

        process = await asyncio.create_subprocess_exec(
            *self._command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            try:
                async with asyncio.timeout(timeout_seconds):
                    output = await self._exchange(process, encoded_request)
                    returncode = await process.wait()
            except TimeoutError as exc:
                raise HandlerTimedOut("Handler child exceeded its deadline") from exc

            if returncode != 0:
                raise HandlerChildExited(returncode)
            try:
                return decode_response(output)
            except ValueError as exc:
                raise HandlerProtocolError(
                    "Handler child returned invalid JSON"
                ) from exc
        except BaseException:
            await self._stop(process)
            raise

    async def _exchange(
        self,
        process: asyncio.subprocess.Process,
        request: bytes,
    ) -> bytes:
        sender = asyncio.create_task(self._send_request(process, request))
        try:
            output = await self._read_bounded_response(process)
            await sender
            return output
        finally:
            if not sender.done():
                sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    @staticmethod
    async def _send_request(
        process: asyncio.subprocess.Process,
        request: bytes,
    ) -> None:
        stdin = process.stdin
        if stdin is None:
            raise HandlerProtocolError("Handler child has no input pipe")
        try:
            stdin.write(request)
            await stdin.drain()
        except BrokenPipeError, ConnectionResetError:
            pass
        finally:
            stdin.close()
            try:
                await stdin.wait_closed()
            except BrokenPipeError, ConnectionResetError:
                pass

    @staticmethod
    async def _read_bounded_response(process: asyncio.subprocess.Process) -> bytes:
        stdout = process.stdout
        if stdout is None:
            raise HandlerProtocolError("Handler child has no output pipe")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = await stdout.read(min(8192, MAX_RESPONSE_BYTES + 1 - size))
            if not chunk:
                return b"".join(chunks)
            size += len(chunk)
            if size > MAX_RESPONSE_BYTES:
                raise HandlerProtocolError("Handler child response exceeds size limit")
            chunks.append(chunk)

    async def _stop(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            await process.wait()
            return
        try:
            process.terminate()
        except ProcessLookupError:
            await process.wait()
            return
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=self._termination_grace_seconds,
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()

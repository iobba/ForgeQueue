"""Worker-owned pipe ends for one supervised attempt (POSIX only)."""

import asyncio
import os
import sys
from contextlib import ExitStack
from dataclasses import dataclass
from io import FileIO

from forgequeue.worker.supervisor_protocol import (
    MAX_STATUS_BYTES,
    SupervisorStatus,
    decode_status,
)

SUPERVISOR_COMMAND = (sys.executable, "-m", "forgequeue.worker.supervisor_main")


@dataclass
class _Pipe:
    reader: FileIO
    writer: FileIO

    @classmethod
    def open(cls, resources: ExitStack) -> _Pipe:
        read_fd, write_fd = os.pipe()
        return cls(
            resources.enter_context(FileIO(read_fd, "rb")),
            resources.enter_context(FileIO(write_fd, "wb")),
        )


async def _wait_ready(fd: int, *, writing: bool) -> None:
    loop = asyncio.get_running_loop()
    ready = loop.create_future()

    def wake() -> None:
        if not ready.done():
            ready.set_result(None)

    register = loop.add_writer if writing else loop.add_reader
    unregister = loop.remove_writer if writing else loop.remove_reader
    register(fd, wake)
    try:
        await ready
    finally:
        unregister(fd)


class SupervisorPipes:
    def __init__(self, resources: ExitStack) -> None:
        self.request = _Pipe.open(resources)
        self.response = _Pipe.open(resources)
        self.lifeline = _Pipe.open(resources)
        self.status = _Pipe.open(resources)
        # Leave the handler's opposite ends blocking for its ordinary stdio.
        for end in (self.request.writer, self.response.reader, self.status.reader):
            os.set_blocking(end.fileno(), False)

    async def launch(
        self, command: tuple[str, ...], grace: float
    ) -> asyncio.subprocess.Process:
        lifeline_fd = self.lifeline.reader.fileno()
        status_fd = self.status.writer.fileno()
        try:
            # Supply actual stdio descriptors rather than asyncio.PIPE. The
            # supervisor's wait() must not depend on an orphan closing stdout.
            return await asyncio.create_subprocess_exec(
                *SUPERVISOR_COMMAND,
                "--lifeline-fd",
                str(lifeline_fd),
                "--status-fd",
                str(status_fd),
                "--termination-grace-seconds",
                str(grace),
                "--",
                *command,
                stdin=self.request.reader,
                stdout=self.response.writer,
                stderr=asyncio.subprocess.DEVNULL,
                pass_fds=(lifeline_fd, status_fd),
                start_new_session=True,
            )
        finally:
            for end in (
                self.request.reader,
                self.response.writer,
                self.lifeline.reader,
                self.status.writer,
            ):
                end.close()

    async def send(self, request: bytes) -> None:
        fd = self.request.writer.fileno()
        remaining = memoryview(request)
        try:
            while remaining:
                try:
                    written = os.write(fd, remaining)
                except BlockingIOError:
                    await _wait_ready(fd, writing=True)
                else:
                    remaining = remaining[written:]
        except BrokenPipeError, ConnectionResetError:
            pass  # The handler may exit before consuming its request.
        finally:
            self.request.writer.close()  # Normal payload EOF, not a stop request.

    async def read_response(self, limit: int) -> bytes:
        fd = self.response.reader.fileno()
        data = bytearray()
        while True:
            try:
                chunk = os.read(fd, min(8192, limit + 1 - len(data)))
            except BlockingIOError:
                await _wait_ready(fd, writing=False)
                continue
            if not chunk:
                return bytes(data)
            data.extend(chunk)
            if len(data) > limit:
                raise ValueError("Handler child response exceeds size limit")

    def read_final_status(self) -> SupervisorStatus:
        # Called only after supervisor exit. Its one small record must now be
        # followed by EOF, not by a live writer or another record.
        fd = self.status.reader.fileno()
        data = os.read(fd, MAX_STATUS_BYTES + 1)
        if os.read(fd, 1):
            raise ValueError("Excess supervisor status data")
        return decode_status(data)

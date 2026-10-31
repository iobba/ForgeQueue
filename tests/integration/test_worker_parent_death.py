"""Characterize the current orphan-child limitation, not the desired contract."""

import asyncio
import os
import signal
import sys
from contextlib import suppress
from pathlib import Path
from uuid import UUID, uuid7

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.broker.redis import RedisJobBroker
from forgequeue.db.models import Job
from forgequeue.jobs.attempt_repository import JobAttemptRepository
from forgequeue.jobs.attempts import JobAttemptStatus
from forgequeue.jobs.repository import JobRepository
from forgequeue.jobs.status import JobStatus
from forgequeue.jobs.submission import JobSubmissionService

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
    pytest.mark.skipif(os.name != "posix", reason="Requires POSIX process groups"),
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]


async def test_sigkilled_worker_currently_leaves_active_handler_running(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    stream_name = f"forgequeue:test:parent-death:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:parent-death:dead:{uuid7()}"
    group_name = "forgequeue-test-workers"
    broker = RedisJobBroker(
        redis_client, stream_name=stream_name, group_name=group_name
    )
    ready_file = tmp_path / "handler-ready"
    probe_file = tmp_path / "continue-after-parent-death"
    proof_file = tmp_path / "handler-continued"
    worker_process: asyncio.subprocess.Process | None = None
    child_pid: int | None = None
    job_id: UUID | None = None

    try:
        await broker.ensure_consumer_group()
        job = await JobSubmissionService(database_session_factory, broker).submit(
            job_type="sum_numbers", payload={"numbers": [10, 20, 30]}, max_attempts=2
        )
        job_id = job.id
        worker_process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(PROJECT_ROOT / "tests" / "helpers" / "active_handler_worker.py"),
            "worker",
            str(ready_file),
            str(probe_file),
            str(proof_file),
            cwd=PROJECT_ROOT,
            env={
                **os.environ,
                "APP_ENV": "test",
                "POSTGRES_DB": "forgequeue_test",
                "REDIS_JOBS_STREAM": stream_name,
                "REDIS_DEAD_LETTER_STREAM": dead_letter_stream_name,
                "REDIS_WORKER_GROUP": group_name,
            },
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            # A dedicated group lets finally stop descendants even if a failure
            # happens before their PID checkpoint is read. This is test-only.
            start_new_session=True,
        )
        async with asyncio.timeout(10):
            while not ready_file.exists():
                if worker_process.returncode is not None:
                    pytest.fail("Worker exited before its handler became ready")
                await asyncio.sleep(0.01)
        child_pid, parent_pid = map(int, ready_file.read_text().split())
        assert parent_pid == worker_process.pid
        assert os.getpgid(child_pid) == worker_process.pid
        assert not proof_file.exists()

        async with database_session_factory() as session:
            running_job = await JobRepository(session).get(job_id)
            attempts = await JobAttemptRepository(session).list_for_job(job_id)
        assert running_job is not None
        assert running_job.status is JobStatus.RUNNING
        assert len(attempts) == 1
        running_attempt = attempts[0]
        assert running_attempt.status is JobAttemptStatus.RUNNING
        assert running_attempt.worker_id == "worker-with-active-child"
        pending = await broker.list_pending()
        assert len(pending) == 1
        original_entry_id = pending[0].entry_id

        # Kill only the worker here, not its process group: the test must observe
        # what the child does without any cleanup assistance from the test runner.
        worker_process.kill()
        assert (
            await asyncio.wait_for(worker_process.wait(), timeout=5) == -signal.SIGKILL
        )
        with pytest.raises(ProcessLookupError):
            os.kill(worker_process.pid, 0)

        probe_file.touch()
        async with asyncio.timeout(5):
            while not proof_file.exists():
                # PID existence alone could describe a zombie. The child's new
                # checkpoint demonstrates it actually ran after its parent died.
                os.kill(child_pid, 0)
                await asyncio.sleep(0.01)
        assert int(proof_file.read_text()) == child_pid

        async with database_session_factory() as session:
            abandoned_job = await JobRepository(session).get(job_id)
            abandoned_attempt = await JobAttemptRepository(session).get(
                running_attempt.id
            )
        assert abandoned_job is not None
        assert abandoned_job.status is JobStatus.RUNNING
        assert abandoned_job.attempts == 1
        assert abandoned_job.result is None
        assert abandoned_job.completed_at is None
        assert abandoned_attempt is not None
        assert abandoned_attempt.status is JobAttemptStatus.RUNNING
        assert abandoned_attempt.completed_at is None
        assert [entry.entry_id for entry in await broker.list_pending()] == [
            original_entry_id
        ]
        assert await redis_client.xlen(stream_name) == 1
        assert await redis_client.xlen(dead_letter_stream_name) == 0
    finally:
        try:
            if worker_process is not None:
                # Stop the isolated test group, including the orphaned handler.
                with suppress(ProcessLookupError):
                    os.killpg(worker_process.pid, signal.SIGKILL)
                await asyncio.wait_for(worker_process.wait(), timeout=5)
                if child_pid is not None:
                    async with asyncio.timeout(5):
                        while True:
                            try:
                                os.kill(child_pid, 0)
                            except ProcessLookupError:
                                break
                            await asyncio.sleep(0.01)
        finally:
            try:
                await redis_client.delete(stream_name, dead_letter_stream_name)
            finally:
                if job_id is not None:
                    async with database_session_factory.begin() as session:
                        await session.execute(delete(Job).where(Job.id == job_id))

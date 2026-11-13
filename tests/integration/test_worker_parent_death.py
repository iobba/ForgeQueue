"""Kill only the worker: prevent handler launch or stop and reap its handler."""

import asyncio
import os
import signal
import sys
from contextlib import suppress
from pathlib import Path
from typing import Literal
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
from forgequeue.worker.supervisor_protocol import decode_status

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
    pytest.mark.skipif(os.name != "posix", reason="Requires POSIX process groups"),
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "worker_stage", ["active", "before-launch", "during-launch", "input-wait"]
)
async def test_sigkilled_worker_leaves_no_active_handler_but_delivery_pending(
    redis_client: Redis,
    database_session_factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    worker_stage: Literal["active", "before-launch", "during-launch", "input-wait"],
) -> None:
    stream_name = f"forgequeue:test:parent-death:{uuid7()}"
    dead_letter_stream_name = f"forgequeue:test:parent-death:dead:{uuid7()}"
    group_name = "forgequeue-test-workers"
    broker = RedisJobBroker(
        redis_client, stream_name=stream_name, group_name=group_name
    )
    ready_file = tmp_path / "handler-ready"
    gated_launch = worker_stage in ("before-launch", "during-launch")
    checkpoint = (
        ready_file.with_suffix(".supervisor-ready") if gated_launch else ready_file
    )
    worker_process: asyncio.subprocess.Process | None = None
    attempt_group: int | None = None
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
            f"worker-{worker_stage}" if worker_stage != "active" else "worker",
            str(ready_file),
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
            # Isolate the worker; the executor creates a separate attempt group.
            start_new_session=True,
        )
        async with asyncio.timeout(10):
            while not checkpoint.exists():
                if worker_process.returncode is not None:
                    pytest.fail("Worker exited before the parent-death checkpoint")
                await asyncio.sleep(0.01)
        checkpoint_pid, parent_pid = map(int, checkpoint.read_text().split())
        supervisor_pid = checkpoint_pid if gated_launch else parent_pid
        attempt_group = supervisor_pid
        assert supervisor_pid not in (worker_process.pid, os.getpgrp())
        assert os.getpgid(checkpoint_pid) == os.getsid(supervisor_pid) == supervisor_pid
        if gated_launch:
            assert parent_pid == worker_process.pid
            assert not ready_file.exists()
            assert not ready_file.with_suffix(".launched-child").exists()

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

        if worker_stage == "input-wait":
            # The child consumed only the first byte and has not finished its
            # remaining-input read. Killing the worker will close its writer.
            assert not ready_file.with_suffix(".input-ended").exists()

        # Kill only the worker here, not its process group: the test must observe
        # what the child does without any cleanup assistance from the test runner.
        worker_process.kill()
        assert (
            await asyncio.wait_for(worker_process.wait(), timeout=5) == -signal.SIGKILL
        )
        with pytest.raises(ProcessLookupError):
            os.kill(worker_process.pid, 0)

        if gated_launch:
            # Resume only after actual worker death has closed its lifeline.
            ready_file.with_suffix(".release-supervisor").touch()

        # Every observed attempt process must disappear before fallback cleanup.
        async with asyncio.timeout(5):
            for pid in {checkpoint_pid, supervisor_pid}:
                while True:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    await asyncio.sleep(0.01)
        if worker_stage != "active":
            status = decode_status(
                ready_file.with_suffix(".supervisor-status").read_bytes()
            )
            if worker_stage == "before-launch":
                # Absence of a ready file alone would miss a briefly launched child.
                assert status.kind == "not_started"
                assert status.returncode is None
                assert not ready_file.exists()
            elif worker_stage == "during-launch":
                child_pid, child_parent_pid = map(
                    int, ready_file.with_suffix(".launched-child").read_text().split()
                )
                assert child_parent_pid == supervisor_pid
                assert child_pid not in (
                    supervisor_pid,
                    worker_process.pid,
                    os.getpid(),
                )
                assert status.kind == "stopped"
                assert status.returncode == -signal.SIGTERM
                # Check the actual spawned PID: a dead supervisor alone would
                # not prove that it stopped and reaped its newly launched child.
                with pytest.raises(ProcessLookupError):
                    os.kill(child_pid, 0)
            else:
                assert ready_file.with_suffix(".input-ended").read_text() == "0"
                assert status.kind == "stopped"
                assert status.returncode == -signal.SIGKILL
        attempt_group = None

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
                if worker_process.returncode is None:
                    with suppress(ProcessLookupError):
                        worker_process.kill()
                await asyncio.wait_for(worker_process.wait(), timeout=5)
                if attempt_group is not None:
                    with suppress(ProcessLookupError):
                        os.killpg(attempt_group, signal.SIGKILL)
        finally:
            try:
                await redis_client.delete(stream_name, dead_letter_stream_name)
            finally:
                if job_id is not None:
                    async with database_session_factory.begin() as session:
                        await session.execute(delete(Job).where(Job.id == job_id))

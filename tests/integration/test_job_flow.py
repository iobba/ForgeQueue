import asyncio
from typing import cast
from uuid import UUID

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from forgequeue.broker.redis import RedisJobBroker, create_redis_client
from forgequeue.core.config import get_settings
from forgequeue.db.models import Job
from forgequeue.db.session import create_database_engine, create_session_factory
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.runner import Worker

pytestmark = pytest.mark.integration


async def process_one_delivery() -> tuple[bool, int]:
    settings = get_settings()
    engine = create_database_engine(settings)
    session_factory = create_session_factory(engine)
    redis_client = create_redis_client(settings)
    broker = RedisJobBroker(
        redis_client,
        stream_name=settings.redis_jobs_stream,
        group_name=settings.redis_worker_group,
    )
    worker = Worker(
        broker,
        JobProcessor(broker, session_factory),
        worker_id="worker-end-to-end",
    )

    try:
        await broker.ensure_consumer_group()
        handled = await worker.run_once(block_ms=1_000)
        pending_count = len(await broker.list_pending())
        return handled, pending_count
    finally:
        try:
            await redis_client.aclose()
        finally:
            await engine.dispose()


async def delete_job(job_id: UUID) -> None:
    engine = create_database_engine(get_settings())
    session_factory = create_session_factory(engine)

    try:
        async with session_factory.begin() as session:
            job = await session.get(Job, job_id)
            if job is not None:
                await session.delete(job)
    finally:
        await engine.dispose()


def test_submitted_job_is_processed_and_retrieved(
    integration_client: TestClient,
) -> None:
    job_id: UUID | None = None

    try:
        submission_response = integration_client.post(
            "/jobs",
            json={
                "type": "sum_numbers",
                "payload": {"numbers": [10, 20, 30]},
            },
        )

        assert submission_response.status_code == status.HTTP_202_ACCEPTED
        submitted_job = cast(dict[str, object], submission_response.json())
        job_id = UUID(cast(str, submitted_job["id"]))
        assert submitted_job["status"] == "queued"
        assert submitted_job["result"] is None

        handled, pending_count = asyncio.run(process_one_delivery())

        assert handled is True
        assert pending_count == 0

        retrieval_response = integration_client.get(f"/jobs/{job_id}")

        assert retrieval_response.status_code == status.HTTP_200_OK
        completed_job = cast(dict[str, object], retrieval_response.json())
        assert completed_job["id"] == str(job_id)
        assert completed_job["status"] == "completed"
        assert completed_job["result"] == {"sum": 60}
        assert completed_job["started_at"] is not None
        assert completed_job["completed_at"] is not None
        assert completed_job["error_code"] is None
        assert completed_job["error_message"] is None
    finally:
        if job_id is not None:
            asyncio.run(delete_job(job_id))

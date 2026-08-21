import asyncio
from datetime import datetime
from typing import cast
from uuid import UUID

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from forgequeue.core.config import get_settings
from forgequeue.db.models import Job
from forgequeue.db.session import create_database_engine, create_session_factory
from forgequeue.jobs.status import JobStatus

pytestmark = pytest.mark.integration

type PersistedJob = tuple[str, JobStatus, dict[str, object], int]


async def read_and_delete_job(job_id: UUID) -> PersistedJob | None:
    engine = create_database_engine(get_settings())
    session_factory = create_session_factory(engine)

    try:
        async with session_factory.begin() as session:
            job = await session.get(Job, job_id)
            if job is None:
                return None

            persisted_job = (
                job.job_type,
                job.status,
                job.payload,
                job.max_attempts,
            )
            await session.delete(job)
            return persisted_job
    finally:
        await engine.dispose()


def test_submit_job_returns_accepted_job_and_persists_it(
    integration_client: TestClient,
) -> None:
    payload: dict[str, object] = {"numbers": [10, 20, 30]}

    response = integration_client.post(
        "/jobs",
        json={
            "type": "sum_numbers",
            "payload": payload,
        },
    )

    assert response.status_code == status.HTTP_202_ACCEPTED

    body = cast(dict[str, object], response.json())
    job_id = UUID(cast(str, body["id"]))

    assert job_id.version == 7
    assert response.headers["location"] == f"/jobs/{job_id}"
    assert body["type"] == "sum_numbers"
    assert body["status"] == "queued"
    assert body["payload"] == payload
    assert body["result"] is None
    assert body["error_code"] is None
    assert body["error_message"] is None
    assert body["attempts"] == 0
    assert body["max_attempts"] == 1
    assert body["started_at"] is None
    assert body["completed_at"] is None
    assert datetime.fromisoformat(cast(str, body["created_at"])).tzinfo is not None
    assert datetime.fromisoformat(cast(str, body["updated_at"])).tzinfo is not None

    get_response = integration_client.get(response.headers["location"])

    assert get_response.status_code == status.HTTP_200_OK
    assert get_response.json() == body

    persisted_job = asyncio.run(read_and_delete_job(job_id))

    assert persisted_job == (
        "sum_numbers",
        JobStatus.QUEUED,
        payload,
        1,
    )


def test_submit_job_respects_custom_max_attempts(
    integration_client: TestClient,
) -> None:
    response = integration_client.post(
        "/jobs",
        json={
            "type": "sum_numbers",
            "payload": {"numbers": [1, 2]},
            "max_attempts": 5,
        },
    )

    assert response.status_code == status.HTTP_202_ACCEPTED

    body = cast(dict[str, object], response.json())
    job_id = UUID(cast(str, body["id"]))

    assert body["max_attempts"] == 5
    assert asyncio.run(read_and_delete_job(job_id)) == (
        "sum_numbers",
        JobStatus.QUEUED,
        {"numbers": [1, 2]},
        5,
    )


@pytest.mark.parametrize(
    "request_body",
    [
        {"type": "generate_report", "payload": {"numbers": [1]}},
        {"type": "sum_numbers", "payload": {}},
        {"type": "sum_numbers", "payload": {"numbers": []}},
        {"type": "sum_numbers", "payload": {"numbers": ["1"]}},
        {
            "type": "sum_numbers",
            "payload": {"numbers": [1], "unexpected": True},
        },
        {
            "type": "sum_numbers",
            "payload": {"numbers": [1]},
            "max_attempts": 0,
        },
        {"type": "sum_numbers", "payload": [1, 2]},
        {
            "type": "sum_numbers",
            "payload": {"numbers": [1]},
            "priority": 5,
        },
    ],
)
def test_submit_job_rejects_invalid_requests(
    integration_client: TestClient,
    request_body: dict[str, object],
) -> None:
    response = integration_client.post("/jobs", json=request_body)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


def test_get_job_returns_stable_not_found_error(
    integration_client: TestClient,
) -> None:
    response = integration_client.get(f"/jobs/{UUID(int=0)}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json() == {
        "error": {
            "code": "job_not_found",
            "message": "The requested job does not exist",
        }
    }


def test_get_job_rejects_malformed_id(
    integration_client: TestClient,
) -> None:
    response = integration_client.get("/jobs/not-a-uuid")

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


def test_list_jobs_returns_newest_first_and_applies_pagination(
    integration_client: TestClient,
) -> None:
    job_ids: list[UUID] = []

    try:
        initial_response = integration_client.get(
            "/jobs",
            params={"status": "queued", "page": 1, "limit": 1},
        )
        assert initial_response.status_code == status.HTTP_200_OK
        initial_body = cast(dict[str, object], initial_response.json())
        initial_total = cast(int, initial_body["total"])

        for index in range(3):
            response = integration_client.post(
                "/jobs",
                json={
                    "type": "sum_numbers",
                    "payload": {"numbers": [index]},
                },
            )
            assert response.status_code == status.HTTP_202_ACCEPTED
            body = cast(dict[str, object], response.json())
            job_ids.append(UUID(cast(str, body["id"])))

        first_page = integration_client.get(
            "/jobs",
            params={
                "status": "queued",
                "limit": 2,
                "page": 1,
            },
        )
        second_page = integration_client.get(
            "/jobs",
            params={
                "status": "queued",
                "limit": 2,
                "page": 2,
            },
        )

        assert first_page.status_code == status.HTTP_200_OK
        assert second_page.status_code == status.HTTP_200_OK

        first_body = cast(dict[str, object], first_page.json())
        second_body = cast(dict[str, object], second_page.json())
        first_items = cast(list[dict[str, object]], first_body["items"])
        second_items = cast(list[dict[str, object]], second_body["items"])

        assert [UUID(cast(str, item["id"])) for item in first_items] == [
            job_ids[2],
            job_ids[1],
        ]
        assert UUID(cast(str, second_items[0]["id"])) == job_ids[0]
        expected_total = initial_total + 3
        expected_pages = (expected_total + 1) // 2
        assert first_body["page"] == 1
        assert first_body["pages"] == expected_pages
        assert first_body["total"] == expected_total
        assert second_body["page"] == 2
        assert second_body["pages"] == expected_pages
        assert second_body["total"] == expected_total
        assert "limit" not in first_body

        completed_jobs = integration_client.get(
            "/jobs",
            params={"status": "completed"},
        )
        assert completed_jobs.status_code == status.HTTP_200_OK
        completed_body = cast(dict[str, object], completed_jobs.json())
        completed_items = cast(list[dict[str, object]], completed_body["items"])
        completed_ids = {UUID(cast(str, item["id"])) for item in completed_items}
        assert completed_ids.isdisjoint(job_ids)
    finally:
        for job_id in job_ids:
            asyncio.run(read_and_delete_job(job_id))


@pytest.mark.parametrize(
    "query_params",
    [
        {"limit": 0},
        {"limit": 101},
        {"page": 0},
        {"status": "unknown"},
    ],
)
def test_list_jobs_rejects_invalid_query_parameters(
    integration_client: TestClient,
    query_params: dict[str, str | int],
) -> None:
    response = integration_client.get("/jobs", params=query_params)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

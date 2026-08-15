from typing import cast

import pytest
from fastapi import status
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


def test_ready_when_dependencies_are_available(
    integration_client: TestClient,
) -> None:
    response = integration_client.get("/ready")

    assert response.status_code == status.HTTP_200_OK
    assert cast(dict[str, object], response.json()) == {
        "status": "ready",
        "dependencies": {
            "postgresql": "ready",
            "redis": "ready",
        },
    }

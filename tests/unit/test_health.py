from typing import cast

import pytest
from fastapi import status
from fastapi.testclient import TestClient

pytestmark = pytest.mark.unit


def test_health_returns_ok(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert cast(dict[str, str], response.json()) == {"status": "ok"}

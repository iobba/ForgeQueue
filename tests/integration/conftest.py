from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from forgequeue.api.app import create_app
from forgequeue.core.config import get_settings


@pytest.fixture
def integration_client() -> Iterator[TestClient]:
    get_settings.cache_clear()

    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        get_settings.cache_clear()

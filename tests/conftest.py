from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from forgequeue.api.app import create_app
from forgequeue.core.config import get_settings


@pytest.fixture
def application(monkeypatch: pytest.MonkeyPatch) -> Iterator[FastAPI]:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("POSTGRES_USER", "forgequeue_test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-password")
    monkeypatch.setenv("POSTGRES_DB", "forgequeue_test")

    get_settings.cache_clear()

    try:
        yield create_app()
    finally:
        get_settings.cache_clear()


@pytest.fixture
def client(application: FastAPI) -> Iterator[TestClient]:
    with TestClient(application) as test_client:
        yield test_client

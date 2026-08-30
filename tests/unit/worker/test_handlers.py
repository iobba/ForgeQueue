from types import MappingProxyType

import pytest
from pydantic import ValidationError

from forgequeue.worker.handlers import (
    HANDLERS,
    UnsupportedJobTypeError,
    get_handler,
    sum_numbers,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("numbers", "expected_sum"),
    [
        ([10, 20, 30], 60),
        ([-10, 5, 3], -2),
        ([0], 0),
    ],
)
def test_sum_numbers_returns_sum(
    numbers: list[int],
    expected_sum: int,
) -> None:
    result = sum_numbers({"numbers": numbers})

    assert result == {"sum": expected_sum}


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"numbers": []},
        {"numbers": ["10"]},
        {"numbers": [1, 2], "operation": "sum"},
    ],
)
def test_sum_numbers_validates_persisted_payload(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        sum_numbers(payload)


def test_get_handler_returns_registered_handler() -> None:
    handler = get_handler("sum_numbers")

    assert handler is sum_numbers
    assert handler({"numbers": [4, 5]}) == {"sum": 9}


def test_get_handler_rejects_unknown_job_type_without_fallback() -> None:
    with pytest.raises(
        UnsupportedJobTypeError,
        match="Unsupported job type",
    ) as exc_info:
        get_handler("generate_report")

    assert exc_info.value.job_type == "generate_report"
    assert str(exc_info.value) == "Unsupported job type: 'generate_report'"


def test_handler_registry_is_immutable() -> None:
    assert isinstance(HANDLERS, MappingProxyType)

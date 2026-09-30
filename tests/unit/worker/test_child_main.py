import io
import subprocess
import sys
from typing import cast

import pytest

from forgequeue.jobs.attempts import JobFailureKind
from forgequeue.jobs.execution_errors import RetryableJobError
from forgequeue.worker import child_main
from forgequeue.worker.child_protocol import (
    MAX_REQUEST_BYTES,
    HandlerRequest,
    decode_response,
    encode_request,
)
from forgequeue.worker.handlers import JobHandler

pytestmark = pytest.mark.unit


def test_child_module_executes_registered_handler() -> None:
    request = HandlerRequest(job_type="sum_numbers", payload={"numbers": [10, 20, 30]})

    completed = subprocess.run(
        [sys.executable, "-m", "forgequeue.worker.child_main"],
        input=encode_request(request),
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert completed.returncode == 0
    assert decode_response(completed.stdout).result == {"sum": 60}


def test_child_returns_sanitized_known_failure() -> None:
    request = HandlerRequest(job_type="sum_numbers", payload={"numbers": []})
    stdout = io.BytesIO()

    exit_code = child_main.run_child(io.BytesIO(encode_request(request)), stdout)

    response = decode_response(stdout.getvalue())
    assert exit_code == 0
    assert response.status == "failed"
    assert response.failure_kind is JobFailureKind.PERMANENT
    assert response.error_code == "invalid_job_payload"
    assert response.safe_message == "Stored job payload failed validation"


def test_child_preserves_classified_handler_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_payload: dict[str, object]) -> dict[str, object]:
        raise RetryableJobError(
            error_code="dependency_unavailable",
            safe_message="Please retry",
        )

    def select_handler(_name: str) -> JobHandler:
        return fail

    monkeypatch.setattr(child_main, "get_handler", select_handler)
    stdout = io.BytesIO()
    request = HandlerRequest(job_type="sum_numbers", payload={})

    exit_code = child_main.run_child(io.BytesIO(encode_request(request)), stdout)

    response = decode_response(stdout.getvalue())
    assert exit_code == 0
    assert response.status == "failed"
    assert response.failure_kind is JobFailureKind.RETRYABLE
    assert response.error_code == "dependency_unavailable"
    assert response.safe_message == "Please retry"


def test_handler_print_does_not_corrupt_protocol(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def noisy(_payload: dict[str, object]) -> dict[str, object]:
        print("handler diagnostic")
        return {"sum": 60}

    def select_handler(_name: str) -> JobHandler:
        return noisy

    monkeypatch.setattr(child_main, "get_handler", select_handler)
    stdout = io.BytesIO()
    request = HandlerRequest(job_type="sum_numbers", payload={})

    assert child_main.run_child(io.BytesIO(encode_request(request)), stdout) == 0
    assert decode_response(stdout.getvalue()).result == {"sum": 60}
    assert "handler diagnostic" in capsys.readouterr().err


def test_unknown_handler_exception_has_no_success_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def crash(_payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("sensitive internal failure")

    def select_handler(_name: str) -> JobHandler:
        return crash

    monkeypatch.setattr(child_main, "get_handler", select_handler)
    stdout = io.BytesIO()
    request = HandlerRequest(job_type="sum_numbers", payload={})

    exit_code = child_main.run_child(io.BytesIO(encode_request(request)), stdout)

    assert exit_code == child_main.UNEXPECTED_FAILURE_EXIT
    assert stdout.getvalue() == b""


def test_invalid_handler_result_has_no_success_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def invalid(_payload: dict[str, object]) -> dict[str, object]:
        return cast(dict[str, object], None)

    def select_handler(_name: str) -> JobHandler:
        return invalid

    monkeypatch.setattr(child_main, "get_handler", select_handler)
    stdout = io.BytesIO()
    request = HandlerRequest(job_type="sum_numbers", payload={})

    exit_code = child_main.run_child(io.BytesIO(encode_request(request)), stdout)

    assert exit_code == child_main.UNEXPECTED_FAILURE_EXIT
    assert stdout.getvalue() == b""


def test_oversized_child_input_is_rejected_without_running_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unreachable(_name: str) -> JobHandler:
        raise AssertionError("handler should not be selected")

    monkeypatch.setattr(child_main, "get_handler", unreachable)
    stdout = io.BytesIO()

    exit_code = child_main.run_child(io.BytesIO(b"x" * (MAX_REQUEST_BYTES + 1)), stdout)

    assert exit_code == child_main.INVALID_REQUEST_EXIT
    assert stdout.getvalue() == b""

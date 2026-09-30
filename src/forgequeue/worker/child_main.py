import sys
from contextlib import redirect_stdout
from typing import BinaryIO

from pydantic import ValidationError

from forgequeue.jobs.attempts import JobFailureKind
from forgequeue.jobs.execution_errors import JobExecutionError
from forgequeue.worker.child_protocol import (
    MAX_REQUEST_BYTES,
    HandlerResponse,
    decode_request,
    encode_response,
)
from forgequeue.worker.handlers import UnsupportedJobTypeError, get_handler

INVALID_REQUEST_EXIT = 2
UNEXPECTED_FAILURE_EXIT = 3


def run_child(stdin: BinaryIO, stdout: BinaryIO) -> int:
    try:
        request = decode_request(stdin.read(MAX_REQUEST_BYTES + 1))
    except ValueError:
        return INVALID_REQUEST_EXIT

    try:
        with redirect_stdout(sys.stderr):
            result = get_handler(request.job_type)(request.payload)
    except UnsupportedJobTypeError:
        response = HandlerResponse(
            status="failed",
            failure_kind=JobFailureKind.PERMANENT,
            error_code="unsupported_job_type",
            safe_message="No handler is registered for this job type",
        )
    except ValidationError:
        response = HandlerResponse(
            status="failed",
            failure_kind=JobFailureKind.PERMANENT,
            error_code="invalid_job_payload",
            safe_message="Stored job payload failed validation",
        )
    except JobExecutionError as exc:
        response = HandlerResponse(
            status="failed",
            failure_kind=exc.failure_kind,
            error_code=exc.error_code,
            safe_message=exc.safe_message,
        )
    except Exception:
        return UNEXPECTED_FAILURE_EXIT
    else:
        try:
            response = HandlerResponse(status="succeeded", result=result)
        except ValidationError:
            return UNEXPECTED_FAILURE_EXIT

    try:
        encoded = encode_response(response)
    except TypeError, ValueError:
        return UNEXPECTED_FAILURE_EXIT
    stdout.write(encoded)
    stdout.flush()
    return 0


def main() -> None:
    raise SystemExit(run_child(sys.stdin.buffer, sys.stdout.buffer))


if __name__ == "__main__":
    main()

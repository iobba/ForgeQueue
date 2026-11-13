import pytest
from pydantic import ValidationError

from forgequeue.worker.supervisor_protocol import (
    MAX_STATUS_BYTES,
    SupervisorStatus,
    decode_status,
    encode_status,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "status",
    [
        SupervisorStatus(version=1, kind="exited", returncode=0),
        SupervisorStatus(version=1, kind="exited", returncode=125),
        SupervisorStatus(version=1, kind="exited", returncode=-9),
        SupervisorStatus(version=1, kind="stopped", returncode=-15),
        SupervisorStatus(version=1, kind="stopped", returncode=0),
        SupervisorStatus(version=1, kind="not_started", returncode=None),
    ],
)
def test_status_round_trips_as_bounded_json(status: SupervisorStatus) -> None:
    encoded = encode_status(status)

    assert len(encoded) <= MAX_STATUS_BYTES
    assert decode_status(encoded) == status


@pytest.mark.parametrize(
    "data",
    [
        b'{"version":2,"kind":"exited","returncode":0}',
        b'{"version":true,"kind":"exited","returncode":0}',
        b'{"version":1.0,"kind":"exited","returncode":0}',
        b'{"version":"1","kind":"exited","returncode":0}',
        b'{"kind":"exited","returncode":0}',
        b'{"version":1,"returncode":0}',
        b'{"version":1,"kind":"not_started"}',
        b'{"version":1,"kind":"unknown","returncode":0}',
        b'{"version":1,"kind":"exited","returncode":true}',
        b'{"version":1,"kind":"exited","returncode":"0"}',
        b'{"version":1,"kind":"exited","returncode":0.0}',
        b'{"version":1,"kind":"exited","returncode":null}',
        b'{"version":1,"kind":"stopped","returncode":null}',
        b'{"version":1,"kind":"not_started","returncode":0}',
        b'{"version":1,"kind":"exited","returncode":0,"extra":1}',
        b'{"version":1,"kind":"exited","returncode":0}{}',
        b'{"version":1,',
        b"[]",
        b"null",
        b"",
        b"\xff",
    ],
)
def test_status_rejects_invalid_or_incomplete_records(data: bytes) -> None:
    with pytest.raises(ValueError):
        decode_status(data)


def test_status_enforces_size_limit_in_both_directions() -> None:
    with pytest.raises(ValueError, match="size limit"):
        decode_status(b" " * (MAX_STATUS_BYTES + 1))
    with pytest.raises(ValueError, match="size limit"):
        encode_status(SupervisorStatus(version=1, kind="exited", returncode=10**256))


def test_status_is_immutable() -> None:
    status = SupervisorStatus(version=1, kind="exited", returncode=0)

    with pytest.raises(ValidationError, match="frozen"):
        status.returncode = 125  # pyright: ignore[reportAttributeAccessIssue]

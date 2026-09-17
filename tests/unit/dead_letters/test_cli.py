import argparse
import json
from uuid import uuid7

import pytest

from forgequeue.broker.messages import (
    DeadLetterMessage,
    DeadLetterReason,
    ReceivedDeadLetterMessage,
)
from forgequeue.dead_letters.cli import (
    dead_letter_limit,
    execute_command,
    serialize_entry,
)

pytestmark = pytest.mark.unit


class FakeDeadLetterReader:
    def __init__(
        self,
        entries: list[ReceivedDeadLetterMessage] | None = None,
    ) -> None:
        self.entries = entries or []
        self.list_counts: list[int] = []
        self.get_entry_ids: list[str] = []

    async def list_recent(
        self,
        *,
        count: int = 20,
    ) -> list[ReceivedDeadLetterMessage]:
        self.list_counts.append(count)
        return self.entries[:count]

    async def get(self, entry_id: str) -> ReceivedDeadLetterMessage | None:
        self.get_entry_ids.append(entry_id)
        return next(
            (entry for entry in self.entries if entry.entry_id == entry_id),
            None,
        )


def build_entry(entry_id: str = "1730000000000-0") -> ReceivedDeadLetterMessage:
    return ReceivedDeadLetterMessage(
        entry_id=entry_id,
        message=DeadLetterMessage(
            source_entry_id="1729999999999-0",
            job_id=uuid7(),
            job_type="sum_numbers",
            attempt_number=3,
            reason=DeadLetterReason.RETRIES_EXHAUSTED,
            failure_kind="retryable",
            error_code="dependency_unavailable",
        ),
    )


def test_dead_letter_limit_accepts_supported_range() -> None:
    assert dead_letter_limit("1") == 1
    assert dead_letter_limit("1000") == 1_000


@pytest.mark.parametrize("value", ["zero", "0", "1001"])
def test_dead_letter_limit_rejects_invalid_value(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        dead_letter_limit(value)


def test_serialize_entry_emits_flat_sanitized_json() -> None:
    entry = build_entry()

    serialized = json.loads(serialize_entry(entry))

    assert serialized == {
        "entry_id": entry.entry_id,
        "schema_version": "1",
        "source_entry_id": "1729999999999-0",
        "job_id": str(entry.message.job_id),
        "job_type": "sum_numbers",
        "attempt_number": 3,
        "reason": "retries_exhausted",
        "failure_kind": "retryable",
        "error_code": "dependency_unavailable",
    }
    assert "payload" not in serialized
    assert "error_message" not in serialized


@pytest.mark.asyncio
async def test_list_command_outputs_one_json_object_per_entry() -> None:
    entries = [build_entry("2-0"), build_entry("1-0")]
    reader = FakeDeadLetterReader(entries)
    output: list[str] = []

    exit_code = await execute_command(
        argparse.Namespace(command="list", limit=2),
        reader,
        stdout=output.append,
    )

    assert exit_code == 0
    assert reader.list_counts == [2]
    assert [json.loads(line)["entry_id"] for line in output] == ["2-0", "1-0"]


@pytest.mark.asyncio
async def test_show_command_outputs_exact_entry() -> None:
    entry = build_entry()
    reader = FakeDeadLetterReader([entry])
    output: list[str] = []

    exit_code = await execute_command(
        argparse.Namespace(command="show", entry_id=entry.entry_id),
        reader,
        stdout=output.append,
    )

    assert exit_code == 0
    assert reader.get_entry_ids == [entry.entry_id]
    assert len(output) == 1
    assert json.loads(output[0])["entry_id"] == entry.entry_id


@pytest.mark.asyncio
async def test_show_command_reports_missing_entry() -> None:
    reader = FakeDeadLetterReader()
    errors: list[str] = []

    exit_code = await execute_command(
        argparse.Namespace(command="show", entry_id="missing-id"),
        reader,
        stderr=errors.append,
    )

    assert exit_code == 1
    assert reader.get_entry_ids == ["missing-id"]
    assert errors == ["Dead-letter entry 'missing-id' was not found"]

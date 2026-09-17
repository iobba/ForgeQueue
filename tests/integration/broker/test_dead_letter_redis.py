from typing import cast
from uuid import uuid7

import pytest
from redis.asyncio import Redis

from forgequeue.broker.messages import DeadLetterMessage, DeadLetterReason
from forgequeue.broker.redis import RedisDeadLetterBroker

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]

type StreamEntry = tuple[str, dict[str, str]]


async def test_publish_stores_safe_dead_letter_fields(
    redis_client: Redis,
) -> None:
    stream_name = f"forgequeue:test:dead:{uuid7()}"
    broker = RedisDeadLetterBroker(
        redis_client,
        stream_name=stream_name,
    )
    message = DeadLetterMessage(
        source_entry_id="1730000000000-0",
        job_id=uuid7(),
        job_type="sum_numbers",
        attempt_number=3,
        reason=DeadLetterReason.RETRIES_EXHAUSTED,
        failure_kind="retryable",
        error_code="dependency_unavailable",
    )

    try:
        message_id = await broker.publish(message)
        entries = cast(
            list[StreamEntry],
            await redis_client.xrange(stream_name),
        )

        assert entries == [
            (
                message_id,
                {
                    "schema_version": "1",
                    "source_entry_id": "1730000000000-0",
                    "job_id": str(message.job_id),
                    "job_type": "sum_numbers",
                    "attempt_number": "3",
                    "reason": "retries_exhausted",
                    "failure_kind": "retryable",
                    "error_code": "dependency_unavailable",
                },
            )
        ]
    finally:
        await redis_client.delete(stream_name)


async def test_publish_creates_dead_letter_stream_on_first_entry(
    redis_client: Redis,
) -> None:
    stream_name = f"forgequeue:test:dead:{uuid7()}"
    broker = RedisDeadLetterBroker(
        redis_client,
        stream_name=stream_name,
    )
    message = DeadLetterMessage(
        source_entry_id="1730000000000-0",
        job_id=uuid7(),
        job_type="generate_report",
        attempt_number=1,
        reason=DeadLetterReason.PERMANENT_FAILURE,
        failure_kind="permanent",
        error_code="invalid_job_payload",
    )

    try:
        assert await redis_client.exists(stream_name) == 0

        await broker.publish(message)

        assert await redis_client.type(stream_name) == "stream"
    finally:
        await redis_client.delete(stream_name)


async def test_list_recent_returns_newest_entries_first_and_respects_count(
    redis_client: Redis,
) -> None:
    stream_name = f"forgequeue:test:dead:{uuid7()}"
    broker = RedisDeadLetterBroker(redis_client, stream_name=stream_name)
    first_message = DeadLetterMessage(
        source_entry_id="1730000000000-0",
        reason=DeadLetterReason.MALFORMED_MESSAGE,
        error_code="malformed_job_message",
    )
    second_message = DeadLetterMessage(
        source_entry_id="1730000000001-0",
        reason=DeadLetterReason.MALFORMED_MESSAGE,
        error_code="malformed_job_message",
    )

    try:
        await broker.publish(first_message)
        second_entry_id = await broker.publish(second_message)

        entries = await broker.list_recent(count=1)

        assert len(entries) == 1
        assert entries[0].entry_id == second_entry_id
        assert entries[0].message == second_message
    finally:
        await redis_client.delete(stream_name)


async def test_get_returns_exact_entry_or_none(
    redis_client: Redis,
) -> None:
    stream_name = f"forgequeue:test:dead:{uuid7()}"
    broker = RedisDeadLetterBroker(redis_client, stream_name=stream_name)
    message = DeadLetterMessage(
        source_entry_id="1730000000000-0",
        reason=DeadLetterReason.MALFORMED_MESSAGE,
        error_code="malformed_job_message",
    )

    try:
        entry_id = await broker.publish(message)

        stored_entry = await broker.get(entry_id)
        missing_entry = await broker.get("1-0")

        assert stored_entry is not None
        assert stored_entry.entry_id == entry_id
        assert stored_entry.message == message
        assert missing_entry is None
    finally:
        await redis_client.delete(stream_name)


@pytest.mark.parametrize("count", [0, 1_001])
async def test_list_recent_rejects_invalid_count(
    redis_client: Redis,
    count: int,
) -> None:
    broker = RedisDeadLetterBroker(
        redis_client,
        stream_name=f"forgequeue:test:dead:{uuid7()}",
    )

    with pytest.raises(ValueError):
        await broker.list_recent(count=count)


async def test_get_rejects_blank_entry_id(redis_client: Redis) -> None:
    broker = RedisDeadLetterBroker(
        redis_client,
        stream_name=f"forgequeue:test:dead:{uuid7()}",
    )

    with pytest.raises(ValueError, match="entry_id must not be blank"):
        await broker.get("   ")

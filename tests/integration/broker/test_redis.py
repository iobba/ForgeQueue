from collections.abc import AsyncIterator
from typing import cast
from uuid import uuid7

import pytest
import pytest_asyncio
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT

from forgequeue.broker.messages import (
    JobMessage,
    PendingJobDelivery,
    ReceivedJobMessage,
)
from forgequeue.broker.redis import RedisJobBroker

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
]

type RedisBrokerFixture = tuple[Redis, RedisJobBroker, str, str]
type StreamEntry = tuple[str, dict[str, str]]


@pytest_asyncio.fixture
async def redis_broker(redis_client: Redis) -> AsyncIterator[RedisBrokerFixture]:
    stream_name = f"forgequeue:test:jobs:{uuid7()}"
    group_name = "forgequeue-test-workers"
    broker = RedisJobBroker(
        redis_client,
        stream_name=stream_name,
        group_name=group_name,
    )

    try:
        yield redis_client, broker, stream_name, group_name
    finally:
        await redis_client.delete(stream_name)


async def read_stream_entries(
    redis_client: Redis,
    stream_name: str,
) -> list[StreamEntry]:
    return cast(
        list[StreamEntry],
        await redis_client.xrange(stream_name),
    )


async def test_ensure_consumer_group_creates_stream_and_group(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, group_name = redis_broker

    await broker.ensure_consumer_group()

    groups = cast(
        list[dict[str, object]],
        await redis_client.xinfo_groups(stream_name),
    )
    assert len(groups) == 1
    assert groups[0]["name"] == group_name
    assert groups[0]["last-delivered-id"] == "0-0"
    assert groups[0]["consumers"] == 0
    assert groups[0]["pending"] == 0


async def test_ensure_consumer_group_is_idempotent(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, _ = redis_broker

    await broker.ensure_consumer_group()
    await broker.ensure_consumer_group()

    groups = await redis_client.xinfo_groups(stream_name)
    assert len(groups) == 1


async def test_ensure_consumer_group_does_not_hide_other_redis_errors(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, _ = redis_broker
    await redis_client.set(stream_name, "not-a-stream")

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await broker.ensure_consumer_group()


async def test_publish_creates_stream_when_it_does_not_exist(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, _ = redis_broker
    message = JobMessage(job_id=uuid7(), job_type="sum_numbers")

    assert await redis_client.exists(stream_name) == 0

    await broker.publish(message)

    assert await redis_client.type(stream_name) == "stream"


async def test_publish_returns_the_stored_entry_id(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, _ = redis_broker
    message = JobMessage(job_id=uuid7(), job_type="sum_numbers")

    message_id = await broker.publish(message)
    entries = await read_stream_entries(redis_client, stream_name)

    assert len(entries) == 1
    assert entries[0][0] == message_id


async def test_publish_stores_the_exact_message_fields(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, _ = redis_broker
    message = JobMessage(job_id=uuid7(), job_type="sum_numbers")

    message_id = await broker.publish(message)
    entries = await read_stream_entries(redis_client, stream_name)

    assert entries == [
        (
            message_id,
            {
                "schema_version": "1",
                "job_id": str(message.job_id),
                "job_type": "sum_numbers",
            },
        )
    ]


async def test_publish_appends_messages_without_replacing_existing_entries(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, _ = redis_broker
    first_message = JobMessage(job_id=uuid7(), job_type="sum_numbers")
    second_message = JobMessage(job_id=uuid7(), job_type="sum_numbers")

    first_message_id = await broker.publish(first_message)
    second_message_id = await broker.publish(second_message)
    entries = await read_stream_entries(redis_client, stream_name)

    assert [entry_id for entry_id, _ in entries] == [
        first_message_id,
        second_message_id,
    ]
    assert [fields["job_id"] for _, fields in entries] == [
        str(first_message.job_id),
        str(second_message.job_id),
    ]


async def test_read_returns_published_message_and_entry_id(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    message = JobMessage(job_id=uuid7(), job_type="sum_numbers")
    published_entry_id = await broker.publish(message)
    await broker.ensure_consumer_group()

    received_messages = await broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    assert received_messages == [
        ReceivedJobMessage(
            entry_id=published_entry_id,
            message=message,
        )
    ]


async def test_read_respects_count_and_leaves_newer_messages_available(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    messages = [JobMessage(job_id=uuid7(), job_type="sum_numbers") for _ in range(3)]
    for message in messages:
        await broker.publish(message)
    await broker.ensure_consumer_group()

    first_batch = await broker.read(
        consumer_name="worker-one",
        count=2,
        block_ms=None,
    )
    second_batch = await broker.read(
        consumer_name="worker-one",
        count=2,
        block_ms=None,
    )

    assert [received.message for received in first_batch] == messages[:2]
    assert [received.message for received in second_batch] == messages[2:]


async def test_read_returns_empty_list_when_no_new_messages_exist(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    await broker.ensure_consumer_group()

    received_messages = await broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    assert received_messages == []


async def test_read_assigns_message_to_consumer_and_pending_list(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, group_name = redis_broker
    await broker.publish(JobMessage(job_id=uuid7(), job_type="sum_numbers"))
    await broker.ensure_consumer_group()

    await broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    consumers = cast(
        list[dict[str, object]],
        await redis_client.xinfo_consumers(stream_name, group_name),
    )
    assert len(consumers) == 1
    assert consumers[0]["name"] == "worker-one"
    assert consumers[0]["pending"] == 1


async def test_read_rejects_invalid_stored_message(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, _ = redis_broker
    invalid_fields: dict[FieldT, EncodableT] = {
        "schema_version": "2",
        "job_id": str(uuid7()),
        "job_type": "sum_numbers",
    }
    await redis_client.xadd(stream_name, invalid_fields)
    await broker.ensure_consumer_group()

    with pytest.raises(ValidationError):
        await broker.read(
            consumer_name="worker-one",
            block_ms=None,
        )


async def test_acknowledge_removes_message_from_pending_list(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, group_name = redis_broker
    await broker.publish(JobMessage(job_id=uuid7(), job_type="sum_numbers"))
    await broker.ensure_consumer_group()
    received_messages = await broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    acknowledged_count = await broker.acknowledge(received_messages[0].entry_id)

    groups = cast(
        list[dict[str, object]],
        await redis_client.xinfo_groups(stream_name),
    )
    assert acknowledged_count == 1
    assert groups[0]["name"] == group_name
    assert groups[0]["pending"] == 0


async def test_acknowledge_returns_zero_when_message_is_already_acknowledged(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    await broker.publish(JobMessage(job_id=uuid7(), job_type="sum_numbers"))
    await broker.ensure_consumer_group()
    received_messages = await broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )
    entry_id = received_messages[0].entry_id

    first_count = await broker.acknowledge(entry_id)
    second_count = await broker.acknowledge(entry_id)

    assert first_count == 1
    assert second_count == 0


async def test_acknowledge_keeps_entry_in_stream(
    redis_broker: RedisBrokerFixture,
) -> None:
    redis_client, broker, stream_name, _ = redis_broker
    message = JobMessage(job_id=uuid7(), job_type="sum_numbers")
    published_entry_id = await broker.publish(message)
    await broker.ensure_consumer_group()
    received_messages = await broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )

    await broker.acknowledge(received_messages[0].entry_id)
    entries = await read_stream_entries(redis_client, stream_name)

    assert entries == [
        (
            published_entry_id,
            {
                "schema_version": "1",
                "job_id": str(message.job_id),
                "job_type": "sum_numbers",
            },
        )
    ]


async def test_list_pending_returns_empty_when_no_messages_are_pending(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    await broker.ensure_consumer_group()

    pending_deliveries = await broker.list_pending()

    assert pending_deliveries == []


async def test_list_pending_returns_delivery_metadata(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    message = JobMessage(job_id=uuid7(), job_type="sum_numbers")
    entry_id = await broker.publish(message)
    await broker.ensure_consumer_group()
    await broker.read(consumer_name="worker-one", block_ms=None)

    pending_deliveries = await broker.list_pending()

    assert len(pending_deliveries) == 1
    assert pending_deliveries[0] == PendingJobDelivery(
        entry_id=entry_id,
        consumer_name="worker-one",
        idle_ms=pending_deliveries[0].idle_ms,
        delivery_count=1,
    )
    assert pending_deliveries[0].idle_ms >= 0


async def test_list_pending_filters_by_consumer(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    messages = [JobMessage(job_id=uuid7(), job_type="sum_numbers") for _ in range(2)]
    for message in messages:
        await broker.publish(message)
    await broker.ensure_consumer_group()
    first_delivery = await broker.read(
        consumer_name="worker-one",
        count=1,
        block_ms=None,
    )
    await broker.read(
        consumer_name="worker-two",
        count=1,
        block_ms=None,
    )

    pending_deliveries = await broker.list_pending(consumer_name="worker-one")

    assert [delivery.entry_id for delivery in pending_deliveries] == [
        first_delivery[0].entry_id
    ]
    assert all(
        delivery.consumer_name == "worker-one" for delivery in pending_deliveries
    )


async def test_list_pending_respects_count(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    messages = [JobMessage(job_id=uuid7(), job_type="sum_numbers") for _ in range(3)]
    for message in messages:
        await broker.publish(message)
    await broker.ensure_consumer_group()
    received_messages = await broker.read(
        consumer_name="worker-one",
        count=3,
        block_ms=None,
    )

    pending_deliveries = await broker.list_pending(count=2)

    assert [delivery.entry_id for delivery in pending_deliveries] == [
        received.entry_id for received in received_messages[:2]
    ]


async def test_list_pending_excludes_acknowledged_delivery(
    redis_broker: RedisBrokerFixture,
) -> None:
    _, broker, _, _ = redis_broker
    await broker.publish(JobMessage(job_id=uuid7(), job_type="sum_numbers"))
    await broker.ensure_consumer_group()
    received_messages = await broker.read(
        consumer_name="worker-one",
        block_ms=None,
    )
    assert len(await broker.list_pending()) == 1

    await broker.acknowledge(received_messages[0].entry_id)

    assert await broker.list_pending() == []

import asyncio
from dataclasses import dataclass
from typing import cast
from uuid import UUID, uuid7

import pytest

from forgequeue.broker.messages import JobMessage, ReceivedJobMessage
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.runner import Worker, generate_worker_id

pytestmark = pytest.mark.unit


@dataclass(frozen=True, slots=True)
class ReadCall:
    consumer_name: str
    count: int
    block_ms: int | None


class FakeBroker:
    def __init__(
        self,
        deliveries: list[ReceivedJobMessage] | None = None,
        *,
        read_error: Exception | None = None,
        stop_event: asyncio.Event | None = None,
        stop_after_reads: int | None = None,
    ) -> None:
        self.deliveries = deliveries or []
        self.read_error = read_error
        self.stop_event = stop_event
        self.stop_after_reads = stop_after_reads
        self.ensure_consumer_group_calls = 0
        self.read_calls: list[ReadCall] = []

    async def ensure_consumer_group(self) -> None:
        self.ensure_consumer_group_calls += 1

    async def read(
        self,
        *,
        consumer_name: str,
        count: int,
        block_ms: int | None,
    ) -> list[ReceivedJobMessage]:
        self.read_calls.append(
            ReadCall(
                consumer_name=consumer_name,
                count=count,
                block_ms=block_ms,
            )
        )
        if self.read_error is not None:
            raise self.read_error
        if (
            self.stop_event is not None
            and self.stop_after_reads is not None
            and len(self.read_calls) >= self.stop_after_reads
        ):
            self.stop_event.set()
        return self.deliveries


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.processed_deliveries: list[ReceivedJobMessage] = []

    async def process(self, delivery: ReceivedJobMessage) -> None:
        self.processed_deliveries.append(delivery)
        if self.error is not None:
            raise self.error


def build_worker(
    *,
    deliveries: list[ReceivedJobMessage] | None = None,
    processor_error: Exception | None = None,
    read_error: Exception | None = None,
    stop_event: asyncio.Event | None = None,
    stop_after_reads: int | None = None,
    worker_id: str | None = "worker-test",
) -> tuple[Worker, FakeBroker, FakeProcessor]:
    broker = FakeBroker(
        deliveries,
        read_error=read_error,
        stop_event=stop_event,
        stop_after_reads=stop_after_reads,
    )
    processor = FakeProcessor(processor_error)
    worker = Worker(
        cast(RedisJobBroker, broker),
        cast(JobProcessor, processor),
        worker_id=worker_id,
    )
    return worker, broker, processor


def build_delivery() -> ReceivedJobMessage:
    return ReceivedJobMessage(
        entry_id="1730000000000-0",
        message=JobMessage(
            job_id=uuid7(),
            job_type="sum_numbers",
        ),
    )


def test_generate_worker_id_returns_unique_uuid7_identifiers() -> None:
    first_worker_id = generate_worker_id()
    second_worker_id = generate_worker_id()

    assert first_worker_id.startswith("worker-")
    assert second_worker_id.startswith("worker-")
    assert UUID(first_worker_id.removeprefix("worker-")).version == 7
    assert UUID(second_worker_id.removeprefix("worker-")).version == 7
    assert first_worker_id != second_worker_id


def test_worker_uses_explicit_identity() -> None:
    worker, _, _ = build_worker(worker_id="worker-explicit")

    assert worker.worker_id == "worker-explicit"


@pytest.mark.parametrize("worker_id", ["", "   "])
def test_worker_rejects_blank_identity(worker_id: str) -> None:
    with pytest.raises(ValueError, match="worker_id must not be blank"):
        build_worker(worker_id=worker_id)


@pytest.mark.asyncio
async def test_run_once_returns_false_without_processing_when_queue_is_empty() -> None:
    worker, broker, processor = build_worker()

    processed = await worker.run_once(block_ms=None)

    assert processed is False
    assert broker.read_calls == [
        ReadCall(
            consumer_name="worker-test",
            count=1,
            block_ms=None,
        )
    ]
    assert processor.processed_deliveries == []


@pytest.mark.asyncio
async def test_run_once_delegates_one_delivery_and_returns_true() -> None:
    delivery = build_delivery()
    worker, broker, processor = build_worker(deliveries=[delivery])

    processed = await worker.run_once(block_ms=250)

    assert processed is True
    assert broker.read_calls == [
        ReadCall(
            consumer_name="worker-test",
            count=1,
            block_ms=250,
        )
    ]
    assert processor.processed_deliveries == [delivery]


@pytest.mark.asyncio
async def test_run_once_propagates_processor_exception() -> None:
    delivery = build_delivery()
    worker, _, processor = build_worker(
        deliveries=[delivery],
        processor_error=RuntimeError("processor failed"),
    )

    with pytest.raises(RuntimeError, match="processor failed"):
        await worker.run_once(block_ms=None)

    assert processor.processed_deliveries == [delivery]


@pytest.mark.asyncio
async def test_run_forever_initializes_group_once_and_skips_reads_when_stopped() -> (
    None
):
    stop_event = asyncio.Event()
    stop_event.set()
    worker, broker, processor = build_worker()

    await worker.run_forever(stop_event, block_ms=None)

    assert broker.ensure_consumer_group_calls == 1
    assert broker.read_calls == []
    assert processor.processed_deliveries == []


@pytest.mark.asyncio
async def test_run_forever_reuses_identity_until_stop_is_requested() -> None:
    stop_event = asyncio.Event()
    worker, broker, _ = build_worker(
        stop_event=stop_event,
        stop_after_reads=3,
        worker_id="worker-loop",
    )

    await worker.run_forever(stop_event, block_ms=25)

    assert broker.ensure_consumer_group_calls == 1
    assert broker.read_calls == [
        ReadCall(consumer_name="worker-loop", count=1, block_ms=25),
        ReadCall(consumer_name="worker-loop", count=1, block_ms=25),
        ReadCall(consumer_name="worker-loop", count=1, block_ms=25),
    ]


@pytest.mark.asyncio
async def test_run_forever_propagates_processor_exception() -> None:
    delivery = build_delivery()
    worker, broker, processor = build_worker(
        deliveries=[delivery],
        processor_error=RuntimeError("processor failed"),
    )

    with pytest.raises(RuntimeError, match="processor failed"):
        await worker.run_forever(asyncio.Event(), block_ms=None)

    assert broker.ensure_consumer_group_calls == 1
    assert processor.processed_deliveries == [delivery]


@pytest.mark.asyncio
async def test_run_forever_propagates_broker_exception() -> None:
    worker, broker, processor = build_worker(
        read_error=ConnectionError("redis unavailable"),
    )

    with pytest.raises(ConnectionError, match="redis unavailable"):
        await worker.run_forever(asyncio.Event(), block_ms=None)

    assert broker.ensure_consumer_group_calls == 1
    assert processor.processed_deliveries == []

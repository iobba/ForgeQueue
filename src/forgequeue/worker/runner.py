import asyncio
from uuid import uuid7

import structlog
from structlog.contextvars import bound_contextvars

from forgequeue.broker.redis import RedisJobBroker
from forgequeue.worker.processor import JobProcessor

logger = structlog.get_logger(__name__)


def generate_worker_id() -> str:
    return f"worker-{uuid7()}"


class Worker:
    def __init__(
        self,
        broker: RedisJobBroker,
        processor: JobProcessor,
        *,
        worker_id: str | None = None,
    ) -> None:
        resolved_worker_id = (
            worker_id if worker_id is not None else generate_worker_id()
        )
        if not resolved_worker_id.strip():
            raise ValueError("worker_id must not be blank")

        self._broker = broker
        self._processor = processor
        self.worker_id = resolved_worker_id

    async def run_once(
        self,
        *,
        block_ms: int | None = 1_000,
    ) -> bool:
        deliveries = await self._broker.read(
            consumer_name=self.worker_id,
            count=1,
            block_ms=block_ms,
        )

        if not deliveries:
            return False

        delivery = deliveries[0]
        with bound_contextvars(
            worker_id=self.worker_id,
            job_id=str(delivery.message.job_id),
            job_type=delivery.message.job_type,
            entry_id=delivery.entry_id,
        ):
            logger.info("job_delivery_received")
            try:
                await self._processor.process(delivery)
            except Exception as exc:
                logger.error(
                    "job_delivery_interrupted",
                    error_type=type(exc).__name__,
                )
                raise

        return True

    async def run_forever(
        self,
        stop_event: asyncio.Event,
        *,
        block_ms: int | None = 1_000,
    ) -> None:
        await self._broker.ensure_consumer_group()

        logger.info(
            "worker_started",
            worker_id=self.worker_id,
        )

        try:
            while not stop_event.is_set():
                await self.run_once(block_ms=block_ms)
        finally:
            logger.info(
                "worker_stopped",
                worker_id=self.worker_id,
            )

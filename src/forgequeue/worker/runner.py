import asyncio
from typing import Protocol
from uuid import uuid7

import structlog
from structlog.contextvars import bound_contextvars

from forgequeue.broker.messages import MalformedJobDelivery
from forgequeue.broker.redis import RedisJobBroker
from forgequeue.worker.processor import JobProcessor
from forgequeue.worker.recovery import RecoveryBatchResult

logger = structlog.get_logger(__name__)


def generate_worker_id() -> str:
    return f"worker-{uuid7()}"


class WorkerRecovery(Protocol):
    async def run_if_due(
        self,
        *,
        worker_id: str,
    ) -> RecoveryBatchResult | None: ...


class Worker:
    def __init__(
        self,
        broker: RedisJobBroker,
        processor: JobProcessor,
        *,
        worker_id: str | None = None,
        recovery: WorkerRecovery | None = None,
    ) -> None:
        resolved_worker_id = (
            worker_id if worker_id is not None else generate_worker_id()
        )
        if not resolved_worker_id.strip():
            raise ValueError("worker_id must not be blank")

        self._broker = broker
        self._processor = processor
        self._recovery = recovery
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
        if isinstance(delivery, MalformedJobDelivery):
            with bound_contextvars(entry_id=delivery.entry_id):
                logger.warning(
                    "malformed_job_delivery_received",
                    error_code=delivery.error_code,
                )
                await self._processor.quarantine_malformed(delivery)
            return True

        with bound_contextvars(
            worker_id=self.worker_id,
            job_id=str(delivery.message.job_id),
            job_type=delivery.message.job_type,
            entry_id=delivery.entry_id,
        ):
            logger.info("job_delivery_received")
            try:
                await self._processor.process(
                    delivery,
                    worker_id=self.worker_id,
                )
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
                if self._recovery is not None:
                    recovery_result = await self._recovery.run_if_due(
                        worker_id=self.worker_id,
                    )
                    if recovery_result is not None:
                        logger.info(
                            "worker_recovery_batch_completed",
                            recovered_count=len(recovery_result.outcomes),
                            deleted_entry_count=len(recovery_result.deleted_entry_ids),
                            next_start_id=recovery_result.next_start_id,
                        )
                if stop_event.is_set():
                    break
                await self.run_once(block_ms=block_ms)
        finally:
            logger.info(
                "worker_stopped",
                worker_id=self.worker_id,
            )

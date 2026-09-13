import asyncio
from math import inf, nan
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from forgequeue.scheduler.retry_dispatcher import JobPublisher, RetryDispatcher

pytestmark = pytest.mark.unit


class ControlledRetryDispatcher(RetryDispatcher):
    def __init__(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        stop_after_runs: int | None = None,
        error: Exception | None = None,
        poll_interval_seconds: float = 0.001,
    ) -> None:
        super().__init__(
            cast(JobPublisher, object()),
            cast(async_sessionmaker[AsyncSession], object()),
            poll_interval_seconds=poll_interval_seconds,
        )
        self.stop_event = stop_event
        self.stop_after_runs = stop_after_runs
        self.error = error
        self.run_count = 0

    async def run_once(self) -> int:
        self.run_count += 1
        if self.error is not None:
            raise self.error
        if (
            self.stop_event is not None
            and self.stop_after_runs is not None
            and self.run_count >= self.stop_after_runs
        ):
            self.stop_event.set()
        return 0


@pytest.mark.parametrize("batch_size", [0, -1])
def test_rejects_non_positive_batch_size(batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        RetryDispatcher(
            cast(JobPublisher, object()),
            cast(async_sessionmaker[AsyncSession], object()),
            batch_size=batch_size,
        )


@pytest.mark.parametrize("poll_interval_seconds", [0.0, -1.0, inf, nan])
def test_rejects_non_positive_poll_interval(
    poll_interval_seconds: float,
) -> None:
    with pytest.raises(
        ValueError,
        match="poll_interval_seconds must be finite and positive",
    ):
        RetryDispatcher(
            cast(JobPublisher, object()),
            cast(async_sessionmaker[AsyncSession], object()),
            poll_interval_seconds=poll_interval_seconds,
        )


@pytest.mark.asyncio
async def test_run_forever_skips_work_when_already_stopped() -> None:
    stop_event = asyncio.Event()
    stop_event.set()
    dispatcher = ControlledRetryDispatcher()

    await dispatcher.run_forever(stop_event)

    assert dispatcher.run_count == 0


@pytest.mark.asyncio
async def test_run_forever_polls_until_stop_is_requested() -> None:
    stop_event = asyncio.Event()
    dispatcher = ControlledRetryDispatcher(
        stop_event=stop_event,
        stop_after_runs=3,
    )

    await dispatcher.run_forever(stop_event)

    assert dispatcher.run_count == 3


@pytest.mark.asyncio
async def test_run_forever_propagates_dispatch_error() -> None:
    dispatcher = ControlledRetryDispatcher(
        error=ConnectionError("redis unavailable"),
    )

    with pytest.raises(ConnectionError, match="redis unavailable"):
        await dispatcher.run_forever(asyncio.Event())

    assert dispatcher.run_count == 1

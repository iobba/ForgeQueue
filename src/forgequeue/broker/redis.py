from typing import cast

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT, KeyT, StreamIdT

from forgequeue.broker.messages import (
    JobMessage,
    PendingJobDelivery,
    ReceivedJobMessage,
)
from forgequeue.core.config import Settings

type RawStreamEntry = tuple[bytes | str, dict[bytes | str, bytes | str]]
type RawStreamResponse = tuple[bytes | str, list[RawStreamEntry]]


def decode_redis_value(value: bytes | str) -> str:
    if isinstance(value, bytes):
        return value.decode()

    return value


def create_redis_client(settings: Settings) -> Redis:
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )


class RedisJobBroker:
    def __init__(
        self,
        client: Redis,
        *,
        stream_name: str,
        group_name: str,
    ) -> None:
        self._client = client
        self._stream_name = stream_name
        self._group_name = group_name

    async def ensure_consumer_group(self) -> None:
        try:
            await self._client.xgroup_create(
                name=self._stream_name,
                groupname=self._group_name,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if not str(exc).startswith("BUSYGROUP"):
                raise

    async def publish(self, message: JobMessage) -> str:
        fields: dict[FieldT, EncodableT] = {
            "schema_version": message.schema_version,
            "job_id": str(message.job_id),
            "job_type": message.job_type,
        }

        message_id = await self._client.xadd(
            name=self._stream_name,
            fields=fields,
        )

        if isinstance(message_id, bytes):
            return message_id.decode()

        return message_id

    async def read(
        self,
        *,
        consumer_name: str,
        count: int = 1,
        block_ms: int | None = 5_000,
    ) -> list[ReceivedJobMessage]:
        if not consumer_name.strip():
            raise ValueError("consumer_name must not be blank")
        if count < 1:
            raise ValueError("count must be at least 1")
        if block_ms is not None and block_ms < 1:
            raise ValueError("block_ms must be at least 1 or None")

        streams: dict[KeyT, StreamIdT] = {self._stream_name: ">"}
        response = await self._client.xreadgroup(
            groupname=self._group_name,
            consumername=consumer_name,
            streams=streams,
            count=count,
            block=block_ms,
            noack=False,
        )
        stream_responses = cast(list[RawStreamResponse], response)
        received_messages: list[ReceivedJobMessage] = []

        for _, entries in stream_responses:
            for entry_id, fields in entries:
                decoded_fields = {
                    decode_redis_value(key): decode_redis_value(value)
                    for key, value in fields.items()
                }
                received_messages.append(
                    ReceivedJobMessage(
                        entry_id=decode_redis_value(entry_id),
                        message=JobMessage.model_validate(decoded_fields),
                    )
                )

        return received_messages

    async def acknowledge(self, entry_id: str) -> int:
        if not entry_id.strip():
            raise ValueError("entry_id must not be blank")

        return await self._client.xack(
            self._stream_name,
            self._group_name,
            entry_id,
        )

    async def list_pending(
        self,
        *,
        count: int = 10,
        consumer_name: str | None = None,
        min_idle_ms: int | None = None,
    ) -> list[PendingJobDelivery]:
        if count < 1:
            raise ValueError("count must be at least 1")
        if consumer_name is not None and not consumer_name.strip():
            raise ValueError("consumer_name must not be blank")
        if min_idle_ms is not None and min_idle_ms < 0:
            raise ValueError("min_idle_ms must not be negative")

        entries = await self._client.xpending_range(
            name=self._stream_name,
            groupname=self._group_name,
            min="-",
            max="+",
            count=count,
            consumername=consumer_name,
            idle=min_idle_ms,
        )

        return [
            PendingJobDelivery(
                entry_id=decode_redis_value(cast(bytes | str, entry["message_id"])),
                consumer_name=decode_redis_value(cast(bytes | str, entry["consumer"])),
                idle_ms=int(entry["time_since_delivered"]),
                delivery_count=int(entry["times_delivered"]),
            )
            for entry in entries
        ]

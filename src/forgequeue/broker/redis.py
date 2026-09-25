import re
from typing import cast

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT, KeyT, StreamIdT

from forgequeue.broker.messages import (
    ClaimedDeliveryBatch,
    DeadLetterMessage,
    JobDelivery,
    JobMessage,
    MalformedJobDelivery,
    PendingJobDelivery,
    ReceivedDeadLetterMessage,
    ReceivedJobMessage,
)
from forgequeue.core.config import Settings

type RawStreamEntry = tuple[bytes | str, dict[bytes | str, bytes | str]]
type RawStreamResponse = tuple[bytes | str, list[RawStreamEntry]]
type RawAutoClaimResponse = tuple[
    bytes | str,
    list[RawStreamEntry],
    list[bytes | str],
]

STREAM_ID_PATTERN = re.compile(r"\d+-\d+")


def decode_redis_value(value: bytes | str) -> str:
    if isinstance(value, bytes):
        return value.decode()

    return value


def create_redis_client(settings: Settings) -> Redis:
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        socket_timeout=settings.redis_socket_timeout_seconds,
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
        if message.attempt_number is not None:
            fields["attempt_number"] = message.attempt_number

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
        block_ms: int | None = 1_000,
    ) -> list[JobDelivery]:
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
        received_messages: list[JobDelivery] = []

        for _, entries in stream_responses:
            for entry_id, fields in entries:
                received_messages.append(self._decode_delivery(entry_id, fields))

        return received_messages

    async def claim_stale(
        self,
        *,
        consumer_name: str,
        min_idle_ms: int,
        start_id: str = "0-0",  # start from the beginning of the PEL
        count: int = 10,
    ) -> ClaimedDeliveryBatch:
        if not consumer_name.strip():
            raise ValueError("consumer_name must not be blank")
        if min_idle_ms < 0:
            raise ValueError("min_idle_ms must not be negative")
        if not STREAM_ID_PATTERN.fullmatch(start_id):
            raise ValueError("start_id must be a Redis stream ID")
        if count < 1:
            raise ValueError("count must be at least 1")

        response = cast(
            RawAutoClaimResponse,
            await self._client.xautoclaim(
                name=self._stream_name,
                groupname=self._group_name,
                consumername=consumer_name,
                min_idle_time=min_idle_ms,
                start_id=start_id,
                count=count,
                justid=False,
            ),
        )
        next_start_id, entries, deleted_entry_ids = response

        return ClaimedDeliveryBatch(
            next_start_id=decode_redis_value(next_start_id),
            deliveries=[
                self._decode_delivery(entry_id, fields) for entry_id, fields in entries
            ],
            deleted_entry_ids=[
                decode_redis_value(entry_id) for entry_id in deleted_entry_ids
            ],
        )

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

    @staticmethod
    def _decode_delivery(
        entry_id: bytes | str,
        fields: dict[bytes | str, bytes | str],
    ) -> JobDelivery:
        decoded_entry_id = decode_redis_value(entry_id)
        try:
            decoded_fields = {
                decode_redis_value(key): decode_redis_value(value)
                for key, value in fields.items()
            }
            return ReceivedJobMessage(
                entry_id=decoded_entry_id,
                message=JobMessage.model_validate(decoded_fields),
            )
        except UnicodeDecodeError, ValidationError:
            return MalformedJobDelivery(entry_id=decoded_entry_id)


class RedisDeadLetterBroker:
    def __init__(
        self,
        client: Redis,
        *,
        stream_name: str,
    ) -> None:
        self._client = client
        self._stream_name = stream_name

    async def publish(self, message: DeadLetterMessage) -> str:
        fields: dict[FieldT, EncodableT] = {
            "schema_version": message.schema_version,
            "source_entry_id": message.source_entry_id,
            "reason": message.reason.value,
            "error_code": message.error_code,
        }
        if message.job_id is not None:
            fields["job_id"] = str(message.job_id)
        if message.job_type is not None:
            fields["job_type"] = message.job_type
        if message.attempt_number is not None:
            fields["attempt_number"] = str(message.attempt_number)
        if message.failure_kind is not None:
            fields["failure_kind"] = message.failure_kind
        message_id = await self._client.xadd(
            name=self._stream_name,
            fields=fields,
        )

        if isinstance(message_id, bytes):
            return message_id.decode()

        return message_id

    async def list_recent(self, *, count: int = 20) -> list[ReceivedDeadLetterMessage]:
        if count < 1:
            raise ValueError("count must be at least 1")
        if count > 1_000:
            raise ValueError("count must not exceed 1000")

        entries = cast(
            list[RawStreamEntry],
            await self._client.xrevrange(
                name=self._stream_name,
                max="+",
                min="-",
                count=count,
            ),
        )
        return [self._decode_entry(entry_id, fields) for entry_id, fields in entries]

    async def get(self, entry_id: str) -> ReceivedDeadLetterMessage | None:
        normalized_entry_id = entry_id.strip()
        if not normalized_entry_id:
            raise ValueError("entry_id must not be blank")

        entries = cast(
            list[RawStreamEntry],
            await self._client.xrange(
                name=self._stream_name,
                min=normalized_entry_id,
                max=normalized_entry_id,
                count=1,
            ),
        )
        if not entries:
            return None

        stored_entry_id, fields = entries[0]
        return self._decode_entry(stored_entry_id, fields)

    @staticmethod
    def _decode_entry(
        entry_id: bytes | str,
        fields: dict[bytes | str, bytes | str],
    ) -> ReceivedDeadLetterMessage:
        decoded_fields = {
            decode_redis_value(key): decode_redis_value(value)
            for key, value in fields.items()
        }
        return ReceivedDeadLetterMessage(
            entry_id=decode_redis_value(entry_id),
            message=DeadLetterMessage.model_validate(decoded_fields),
        )

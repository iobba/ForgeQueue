import argparse
import asyncio
import json
import sys
from collections.abc import Callable, Sequence
from typing import Protocol

from forgequeue.broker.messages import ReceivedDeadLetterMessage
from forgequeue.broker.redis import (
    RedisDeadLetterBroker,
    create_redis_client,
)
from forgequeue.core.config import get_settings

type OutputWriter = Callable[[str], object]


class DeadLetterReader(Protocol):
    async def list_recent(
        self,
        *,
        count: int = 20,
    ) -> list[ReceivedDeadLetterMessage]: ...

    async def get(self, entry_id: str) -> ReceivedDeadLetterMessage | None: ...


def dead_letter_limit(value: str) -> int:
    try:
        limit = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("limit must be an integer") from exc

    if not 1 <= limit <= 1_000:
        raise argparse.ArgumentTypeError("limit must be between 1 and 1000")
    return limit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="forgequeue-dead-letters",
        description="Inspect ForgeQueue's dead-letter stream.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    list_parser = commands.add_parser(
        "list",
        help="list the most recent dead-letter entries",
    )
    list_parser.add_argument(
        "--limit",
        type=dead_letter_limit,
        default=20,
        help="maximum entries to return (default: 20, maximum: 1000)",
    )

    show_parser = commands.add_parser(
        "show",
        help="show one dead-letter entry",
    )
    show_parser.add_argument("entry_id", help="Redis stream entry ID")
    return parser


def serialize_entry(entry: ReceivedDeadLetterMessage) -> str:
    output = {
        "entry_id": entry.entry_id,
        **entry.message.model_dump(mode="json", exclude_none=True),
    }
    return json.dumps(output, sort_keys=True)


async def execute_command(
    arguments: argparse.Namespace,
    reader: DeadLetterReader,
    *,
    stdout: OutputWriter = print,
    stderr: OutputWriter = lambda value: print(value, file=sys.stderr),
) -> int:
    if arguments.command == "list":
        entries = await reader.list_recent(count=arguments.limit)
        for entry in entries:
            stdout(serialize_entry(entry))
        return 0

    entry = await reader.get(arguments.entry_id)
    if entry is None:
        stderr(f"Dead-letter entry {arguments.entry_id!r} was not found")
        return 1

    stdout(serialize_entry(entry))
    return 0


async def run(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    settings = get_settings()
    redis_client = create_redis_client(settings)
    try:
        reader = RedisDeadLetterBroker(
            redis_client,
            stream_name=settings.redis_dead_letter_stream,
        )
        return await execute_command(arguments, reader)
    finally:
        await redis_client.aclose()


def main() -> None:
    exit_code = asyncio.run(run())
    if exit_code != 0:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()

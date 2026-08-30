# ForgeQueue

ForgeQueue is a distributed background-job processing platform built from first
principles with Python. It explores the mechanics behind systems such as Celery
and Sidekiq: durable job state, asynchronous delivery, worker coordination,
failure recovery, and observable execution.

## Why this project exists

ForgeQueue deliberately avoids using an existing task framework internally.
The goal is to implement and understand the engineering problems those
frameworks solve, including:

- producer-consumer architecture and backpressure;
- at-least-once delivery and idempotent execution;
- retries, timeouts, dead-letter handling, and abandoned-work recovery;
- transactional consistency between PostgreSQL and a message broker;
- concurrency control, graceful shutdown, and horizontal worker scaling;
- logs, metrics, traces, and operational failure testing.

## Architecture

PostgreSQL is the authoritative store for job state. Redis Streams will provide
the initial delivery mechanism; workers and broker delivery are planned for the
remaining V1 milestones.

```text
Client
  |
  v
FastAPI --------> PostgreSQL
  |
  | planned
  v
Redis Streams --> Workers --> PostgreSQL
```


## Technology stack

- Python 3.14
- FastAPI and Uvicorn
- PostgreSQL, SQLAlchemy 2.x, Psycopg, and Alembic
- Redis and Redis Streams
- Docker Compose
- Pytest and pytest-asyncio
- Ruff and Pyright
- structlog

NATS JetStream is planned as a later broker implementation after the Redis
Streams version is mature.

## Local setup

Prerequisites:

- Docker with Compose support;
- `uv`;
- Python is installed automatically by `uv` from `.python-version` when needed.

```bash
uv sync
cp .env.example .env
make infra-up
uv run alembic upgrade head
make test-db-setup
make check-all
```

## Development commands

```bash
make help              # list all commands
make infra-up          # start PostgreSQL and Redis
make infra-check       # verify dependency readiness
make test-db-setup     # create and migrate the isolated test database
make check             # static checks and unit tests
make check-all         # static checks and the complete test suite
make infra-down        # stop containers while preserving data
```

`make infra-reset CONFIRM=yes` deletes the local PostgreSQL and Redis volumes.

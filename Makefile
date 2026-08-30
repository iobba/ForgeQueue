.DEFAULT_GOAL := help

COMPOSE := docker compose
UV := uv
TEST_DATABASE := forgequeue_test

.PHONY: help \
	compose-config \
	infra-up \
	infra-down \
	infra-reset \
	infra-restart \
	infra-status \
	infra-logs \
	postgres-logs \
	redis-logs \
	infra-check \
	test-db-create \
	test-db-upgrade \
	test-db-setup \
	format \
	format-check \
	lint \
	lint-fix \
	typecheck \
	test-unit \
	test-integration \
	test \
	check \
	check-all \
	api-dev \
	worker

help:
	@echo "ForgeQueue development commands:"
	@echo "  make compose-config           Validate the Compose configuration"
	@echo "  make infra-up                 Start PostgreSQL and Redis"
	@echo "  make infra-down               Stop containers and preserve data"
	@echo "  make infra-reset CONFIRM=yes  Stop containers and delete all local data"
	@echo "  make infra-restart            Restart PostgreSQL and Redis"
	@echo "  make infra-status             Show container status"
	@echo "  make infra-logs               Follow infrastructure logs"
	@echo "  make postgres-logs            Follow PostgreSQL logs"
	@echo "  make redis-logs               Follow Redis logs"
	@echo "  make infra-check              Verify PostgreSQL and Redis readiness"
	@echo "  make test-db-create           Create the test database when missing"
	@echo "  make test-db-upgrade          Apply migrations to the test database"
	@echo "  make test-db-setup            Start infrastructure and prepare the test database"
	@echo "  make format                   Format Python files"
	@echo "  make format-check             Verify Python formatting"
	@echo "  make lint                     Run Ruff lint checks"
	@echo "  make lint-fix                 Apply Ruff's safe lint fixes"
	@echo "  make typecheck                Run strict Pyright checks"
	@echo "  make test-unit                Run tests isolated from infrastructure"
	@echo "  make test-integration         Run tests requiring local infrastructure"
	@echo "  make test                     Run the complete test suite"
	@echo "  make check                    Run quality checks and unit tests"
	@echo "  make check-all                Run all quality checks and tests"
	@echo "  make api-dev                  Start the FastAPI development server"
	@echo "  make worker                   Start one ForgeQueue worker"

compose-config:
	$(COMPOSE) config

infra-up:
	$(COMPOSE) up -d --wait

infra-down:
	$(COMPOSE) down

infra-reset:
	@if [ "$(CONFIRM)" != "yes" ]; then \
		echo "This deletes the PostgreSQL and Redis volumes."; \
		echo "Run 'make infra-reset CONFIRM=yes' to continue."; \
		exit 1; \
	fi
	$(COMPOSE) down -v

infra-restart:
	$(COMPOSE) restart

infra-status:
	$(COMPOSE) ps

infra-logs:
	$(COMPOSE) logs -f postgres redis

postgres-logs:
	$(COMPOSE) logs -f postgres

redis-logs:
	$(COMPOSE) logs -f redis

infra-check:
	$(COMPOSE) exec postgres sh -c 'pg_isready -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'
	$(COMPOSE) exec redis redis-cli ping

test-db-create:
	@database_names="$$( $(COMPOSE) exec -T postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d postgres -tAc "SELECT datname FROM pg_database"' )" || exit $$?; \
	if printf '%s\n' "$$database_names" | grep -Fxq "$(TEST_DATABASE)"; then \
		echo "Database $(TEST_DATABASE) already exists"; \
	else \
		$(COMPOSE) exec -T postgres sh -c \
			'createdb -U "$$POSTGRES_USER" "$(TEST_DATABASE)"'; \
		echo "Created database $(TEST_DATABASE)"; \
	fi

test-db-upgrade:
	POSTGRES_DB=$(TEST_DATABASE) $(UV) run alembic upgrade head

test-db-setup: infra-up
	$(MAKE) test-db-create
	$(MAKE) test-db-upgrade

format:
	$(UV) run ruff format .

format-check:
	$(UV) run ruff format --check .

lint:
	$(UV) run ruff check .

lint-fix:
	$(UV) run ruff check --fix .

typecheck:
	$(UV) run pyright

test-unit:
	$(UV) run pytest -m unit

test-integration:
	$(UV) run pytest -m integration

test:
	$(UV) run pytest

check: lint format-check typecheck test-unit

check-all: lint format-check typecheck test

api-dev:
	$(UV) run uvicorn forgequeue.api.app:create_app \
		--factory \
		--reload \
		--host 127.0.0.1 \
		--port 8000

worker:
	$(UV) run forgequeue-worker

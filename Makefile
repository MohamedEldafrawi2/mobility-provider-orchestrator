.PHONY: sync lint format typecheck lint-imports test test-unit test-integration check up up-dev up-observability down migrate run demo bench

sync:
	uv sync --all-groups

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

typecheck:
	uv run mypy

lint-imports:
	uv run lint-imports

test: test-unit test-integration

test-unit:
	uv run pytest -m "not integration" -q

test-integration:
	uv run pytest -m integration -q

check: lint typecheck lint-imports test

up:
	docker compose up --build -d

up-dev:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build -d

up-observability:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.observability.yml up --build -d

down:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.observability.yml down -v

demo:
	uv run python scripts/demo.py

bench:
	uv run python -m benchmarks.envelope
	uv run python -m benchmarks.confirmation_path

migrate:
	uv run alembic upgrade head

run:
	uv run mpo-api

.PHONY: install test lint format sample up up-live crash-test down

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check .
	ruff format --check .

format:
	ruff format .
	ruff check --fix .

sample:
	hsl-sample

# Synthetic replay (works offline)
up:
	docker compose --profile replay up --build

# Real HSL feed
up-live:
	docker compose --profile live up --build

# Failure test: kill the streaming job, wait, restart it from its checkpoint
crash-test:
	docker compose kill spark && sleep 60 && docker compose --profile replay up -d spark

down:
	docker compose --profile replay --profile live down -v

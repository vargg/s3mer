.PHONY: lint format check type-check test test-unit clean help

# Default target: show help
help:
	@echo "S3M Development Makefile"
	@echo "Usage:"
	@echo "  make lint         Run all linting and type checking (format, check, type-check)"
	@echo "  make format       Format code with ruff"
	@echo "  make check        Lint code with ruff"
	@echo "  make type-check   Strict type checking with ty"
	@echo "  make test         Run full E2E suite (Docker Compose)"
	@echo "  make test-unit    Run local unit tests"
	@echo "  make clean        Cleanup containers and caches"

lint: format check type-check

format:
	uv run ruff format src tests

check:
	uv run ruff check src tests

type-check:
	uv run ty check src tests

test:
	docker compose -f docker-compose-test.yaml up --build --attach pytest-runner --exit-code-from pytest-runner

test-unit:
	uv run pytest tests/unit

clean:
	docker compose -f docker-compose-test.yaml down -v
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +

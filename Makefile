.PHONY: up down logs check test smoke
up:
	docker compose up --build -d --wait
down:
	docker compose down
logs:
	docker compose logs -f --tail=100
check:
	uv sync --extra dev --frozen
	uv run ruff format --check .
	uv run ruff check .
test:
	docker compose --profile test build test
	docker compose --profile test run --rm test
smoke:
	docker compose exec -T api python scripts/smoke.py

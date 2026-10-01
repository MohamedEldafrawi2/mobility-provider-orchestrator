FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS base

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
WORKDIR /app

# Install dependencies first so the layer is cached independently of source changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# The project itself: hatchling reads README.md and LICENSE as package metadata.
COPY README.md LICENSE alembic.ini ./
COPY src ./src
COPY migrations ./migrations
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

# Public listener; the admin listener (8001) is intentionally not exposed here.
EXPOSE 8000

CMD ["sh", "-c", "alembic upgrade head && mpo-api"]

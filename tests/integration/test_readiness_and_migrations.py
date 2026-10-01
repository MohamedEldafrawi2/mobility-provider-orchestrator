"""Real PostgreSQL and Redis via testcontainers. Nothing internal is mocked."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from orchestrator.api import create_public_app
from orchestrator.persistence.seed import seed_api_clients
from tests.conftest import _client_for, make_settings

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:18", driver="asyncpg") as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="module")
def redis_url() -> Iterator[str]:
    with RedisContainer("redis:8") as r:
        yield f"redis://{r.get_container_host_ip()}:{r.get_exposed_port(6379)}/0"


def _alembic(url: str) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


async def test_migrations_upgrade_and_downgrade_cleanly(postgres_url: str) -> None:
    command.upgrade(_alembic(postgres_url), "head")
    engine = create_async_engine(postgres_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            )
            tables = result.scalars().all()
        assert "api_clients" in tables
        assert "alembic_version" in tables

        settings = make_settings(database_url=postgres_url)
        assert await seed_api_clients(engine, settings) == 1
        assert await seed_api_clients(engine, settings) == 0, "seeding is idempotent"
    finally:
        await engine.dispose()

    command.downgrade(_alembic(postgres_url), "base")
    command.upgrade(_alembic(postgres_url), "head")


async def test_readyz_reports_each_dependency(postgres_url: str, redis_url: str) -> None:
    command.upgrade(_alembic(postgres_url), "head")

    healthy = create_public_app(make_settings(database_url=postgres_url, redis_url=redis_url))
    async for client in _client_for(healthy):
        response = await client.get("/readyz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "postgres": "ok", "redis": "ok"}

    redis_down = create_public_app(make_settings(database_url=postgres_url))
    async for client in _client_for(redis_down):
        response = await client.get("/readyz")
        assert response.status_code == 200, "Redis loss degrades, it does not fail readiness"
        assert response.json()["redis"] == "degraded"

    postgres_down = create_public_app(make_settings(redis_url=redis_url))
    async for client in _client_for(postgres_down):
        response = await client.get("/readyz")
        assert response.status_code == 503
        assert response.json()["postgres"] == "down"

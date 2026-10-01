from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from orchestrator.api import create_admin_app, create_public_app
from orchestrator.config import Settings

DEV_CLIENT_KEY = "test-client-key"
DEV_ADMIN_KEY = "test-admin-key"


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def make_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "environment": "test",
        "api_keys": {sha256(DEV_CLIENT_KEY): "test-client"},
        "admin_key_hash": sha256(DEV_ADMIN_KEY),
        "log_json": False,
        # Closed ports by default: unit tests never touch real infrastructure.
        "database_url": "postgresql+asyncpg://x:x@127.0.0.1:1/x",
        "redis_url": "redis://127.0.0.1:1/0",
        "readiness_timeout_seconds": 0.5,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)  # type: ignore[call-arg]


async def _client_for(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with LifespanManager(app):
        # raise_app_exceptions=False so the 500 problem-details response is observable, as a
        # real client would see it, instead of the exception propagating into the test.
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
async def public_client() -> AsyncIterator[AsyncClient]:
    async for client in _client_for(create_public_app(make_settings())):
        yield client


@pytest.fixture
async def admin_client() -> AsyncIterator[AsyncClient]:
    async for client in _client_for(create_admin_app(make_settings())):
        yield client

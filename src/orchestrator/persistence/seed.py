"""Seed rows derived from configuration."""

from __future__ import annotations

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from orchestrator.config import Settings
from orchestrator.persistence.models import ApiClient


async def seed_api_clients(engine: AsyncEngine, settings: Settings) -> int:
    """Ensure every configured client has an ``api_clients`` row. Idempotent."""
    client_ids = sorted(set(settings.api_keys.values()))
    if not client_ids:
        return 0
    statement = (
        insert(ApiClient)
        .values([{"client_id": cid, "name": cid} for cid in client_ids])
        .on_conflict_do_nothing(index_elements=["client_id"])
    )
    async with engine.begin() as conn:
        result = await conn.execute(statement)
    return int(result.rowcount or 0)

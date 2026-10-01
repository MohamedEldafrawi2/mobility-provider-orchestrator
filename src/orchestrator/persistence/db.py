"""Async engine and session factory.

One session per task, never shared across concurrent tasks. Pools are bounded; the confirmation
loop gets its own reserved slice (its own engine).
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from orchestrator.config import Settings


def create_engine(settings: Settings, *, pool_size: int = 10, max_overflow: int = 5) -> AsyncEngine:
    """A bounded pool. The confirmation loop gets its own engine, hence its own pool: a slice
    of connections no other loop can borrow (docs/resilience-strategy.md)."""
    return create_async_engine(
        settings.database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_timeout=5,
        # Bound parameters (names, contact details) never appear in error messages, and so
        # never in logs or in exceptions recorded on spans.
        hide_parameters=True,
    )


def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def dispose_engine(engine: AsyncEngine) -> None:
    await engine.dispose()

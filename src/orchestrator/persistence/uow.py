"""A unit of work is one session and one transaction. Nothing else.

Callbacks registered with ``store.after_commit`` run once the transaction has committed, and
never otherwise: a metric that says "settled" must not be emitted for a write a lease fence
rolled back.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from orchestrator.persistence.bookings import BookingStore


class UnitOfWorkFactory:
    def __init__(self, engine: AsyncEngine) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[BookingStore]:
        session: AsyncSession = self._sessions()
        store = BookingStore(session)
        try:
            async with session.begin():
                yield store
        finally:
            await session.close()
        for callback in store.after_commit_callbacks:
            callback()

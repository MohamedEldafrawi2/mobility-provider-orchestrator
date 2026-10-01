"""A bulkhead is a bounded number of in-flight calls plus a bounded wait for a slot.

The slot is held for the dispatch mark and the provider call only. The attempt loop sleeps
*outside* the slot, so a purpose that is backing off never blocks a purpose that is not.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from time import monotonic

from orchestrator.resilience.errors import REASON_BULKHEAD, NotDispatchedError


class Bulkhead:
    def __init__(
        self, limit: int, *, max_wait: float, clock: Callable[[], float] = monotonic
    ) -> None:
        if limit < 1:
            raise ValueError("a bulkhead admits at least one call")
        if max_wait < 0:
            raise ValueError("max_wait cannot be negative")
        self.limit = limit
        self.max_wait = max_wait
        self._clock = clock
        self._semaphore = asyncio.Semaphore(limit)
        self._in_use = 0
        self._waiting = 0

    @property
    def in_use(self) -> int:
        return self._in_use

    @property
    def waiting(self) -> int:
        return self._waiting

    @asynccontextmanager
    async def slot(self, *, deadline: float | None = None) -> AsyncIterator[float]:
        """Hold one slot; yields how long the caller waited for it.

        The wait is bounded by ``max_wait`` and, when given, by the caller's ``deadline``
        (a monotonic instant): waiting for a slot past the moment the call could no longer
        finish in time is pointless load.
        """
        started = self._clock()
        budget = self.max_wait
        if deadline is not None:
            budget = min(budget, max(deadline - started, 0.0))
        self._waiting += 1
        try:
            try:
                async with asyncio.timeout(budget):
                    await self._semaphore.acquire()
            except TimeoutError:
                raise NotDispatchedError(REASON_BULKHEAD) from None
        finally:
            self._waiting -= 1
        self._in_use += 1
        try:
            yield self._clock() - started
        finally:
            self._in_use -= 1
            self._semaphore.release()

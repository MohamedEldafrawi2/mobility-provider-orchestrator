"""The worker's loops in isolation: draining, per-row failure containment, lease release."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from orchestrator.application.policy import RecoveryPolicy
from orchestrator.domain import BookingId, BookingState
from orchestrator.domain.cutoffs import ExpiryPolicy
from orchestrator.persistence.bookings import Booking, Lease, StaleLeaseError
from orchestrator.worker.loops import Loop, LoopSpec


class FakeStore:
    """An in-memory work queue with the store's claim/renew/release contract."""

    def __init__(self, queue: list[Booking], *, stale: set[str] | None = None) -> None:
        self.queue = queue
        self.stale = stale or set()
        self.released: list[str] = []
        self.deferred: list[tuple[str, datetime]] = []
        self.claims = 0

    async def claim(
        self, states: Any, *, limit: int, ttl: timedelta
    ) -> list[tuple[Booking, Lease]]:
        self.claims += 1
        batch, self.queue = self.queue[:limit], self.queue[limit:]
        now = datetime.now(UTC)
        return [(b, Lease(b.id, f"tok-{b.id}", now + ttl)) for b in batch]

    async def renew(self, lease: Lease, *, ttl: timedelta) -> Lease:
        if lease.booking_id in self.stale:
            raise StaleLeaseError(lease.booking_id)
        return Lease(lease.booking_id, lease.token, datetime.now(UTC) + ttl)

    async def release(self, lease: Lease) -> None:
        self.released.append(lease.booking_id)

    async def defer(self, lease: Lease, *, until: datetime) -> None:
        self.deferred.append((lease.booking_id, until))


class FakeUow:
    def __init__(self, store: FakeStore) -> None:
        self.store = store

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[FakeStore]:
        yield self.store


def _booking(i: int) -> Booking:
    now = datetime.now(UTC)
    return Booking(  # type: ignore[call-arg]
        id=BookingId(f"bk_{i}"),
        client_id="c",  # type: ignore[arg-type]
        provider="p",  # type: ignore[arg-type]
        state=BookingState.HELD,
        offer=None,  # type: ignore[arg-type]
        passenger_names=(),
        contact_email="x@y.z",
        provider_booking_ref=None,
        unresolved_reason=None,
        failure_code=None,
        version=0,
        next_action_at=now,
        created_at=now,
        updated_at=now,
        lease_expires_at=None,
    )


def _policy() -> RecoveryPolicy:
    return RecoveryPolicy(
        lookup_budget=3,
        max_attempts=8,
        reconcile_backoff=timedelta(0),
        reschedule_backoff=timedelta(0),
        abandon_after=timedelta(0),
        submitting_stale_after=timedelta(0),
        mutation_timeout=timedelta(seconds=1),
        lease_ttl=timedelta(seconds=30),
        expiry=ExpiryPolicy(),
        confirm_budget=3,
        pending_poll=timedelta(seconds=1),
        pending_max_age=timedelta(minutes=15),
    )


async def test_a_tick_drains_the_queue_without_sleeping() -> None:
    """6.6 #49 and the envelope: 200 due holds are handled in one tick, batch after batch."""
    handled: list[str] = []

    async def handle(booking: Booking, lease: Lease) -> str:
        await asyncio.sleep(0)
        handled.append(booking.id)
        return "confirmed"

    store = FakeStore([_booking(i) for i in range(200)])
    loop = Loop(
        LoopSpec("confirm", (BookingState.HELD,), handle, FakeUow(store), 8, 10),  # type: ignore[arg-type]
        _policy(),
    )
    counters = await loop.tick()
    assert counters == {"confirmed": 200}
    assert len(handled) == 200 and store.claims == 21, "20 full batches, then an empty claim"
    assert sorted(store.released) == sorted(b for b in handled)


async def test_one_failing_row_neither_stops_its_siblings_nor_leaks_its_lease() -> None:
    async def handle(booking: Booking, lease: Lease) -> str:
        if booking.id == "bk_1":
            raise RuntimeError("boom")
        await asyncio.sleep(0.01)
        return "submitted"

    store = FakeStore([_booking(i) for i in range(4)], stale={"bk_2"})
    loop = Loop(
        LoopSpec("submit", (BookingState.CREATED,), handle, FakeUow(store), 4, 10),  # type: ignore[arg-type]
        _policy(),
    )
    counters = await loop.tick()
    assert counters == {"submitted": 2, "failed": 1}
    assert sorted(store.released) == ["bk_0", "bk_1", "bk_2", "bk_3"], "every lease released"
    assert [b for b, _ in store.deferred] == ["bk_1"], "the failing row is pushed back"
    assert loop.progressed(counters)


async def test_a_persistently_failing_row_does_not_keep_the_loop_busy() -> None:
    """Closure item 16: failures are deferred and are not progress; the loop sleeps."""

    async def handle(booking: Booking, lease: Lease) -> str:
        raise RuntimeError("always")

    class Sticky(FakeStore):
        async def claim(
            self, states: Any, *, limit: int, ttl: timedelta
        ) -> list[tuple[Booking, Lease]]:
            self.claims += 1
            now = datetime.now(UTC)
            return [(b, Lease(b.id, "tok", now + ttl)) for b in self.queue[:limit]]

    store = Sticky([_booking(0)])
    loop = Loop(
        LoopSpec("submit", (BookingState.CREATED,), handle, FakeUow(store), 4, 10),  # type: ignore[arg-type]
        _policy(),
    )
    counters = await loop.tick()
    assert counters == {"failed": 1} and store.claims == 1, "one round, no spinning"
    assert not loop.progressed(counters)
    assert len(store.deferred) == 1

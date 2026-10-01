"""The worker: leased claims over the bookings table, one loop per owner
(docs/booking-state-machine.md; ADR 006).

Loops, each with its own concurrency and its own claim:

- **confirm**: holds due for confirmation (``HELD``, ``CONFIRMING``), imminent first. Runs on
  its own unit-of-work factory over a *reserved* PostgreSQL pool slice and its own
  concurrency, so neither a reconciliation storm nor a submission backlog can starve it on
  the platform side. The
  isolation and the draining scheduler are in place and tested now.
- **submit**: ``CREATED`` bookings that are due are submitted through the attempt loop, or
  abandoned when overdue and never dispatched.
- **reconcile**: ``UNKNOWN`` bookings that are due get one lookup each.
- **recovery**: ``SUBMITTING`` bookings whose submitter died become ``UNKNOWN``.

A tick **drains**: it claims batch after batch until a claim comes back short, so a queue of
two hundred holds is dispatched in one pass, not one batch per second. ``run_forever`` sleeps
only after an idle tick. Every write a loop makes is fenced by its lease; the lease is renewed
right before each row is processed, and a lease lost mid-way (``StaleLeaseError``) means another
worker owns the row now, so the loop moves on. One row's failure never stops its siblings, and
a tick returns only when every row it claimed has been processed and released.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog
from opentelemetry import trace
from opentelemetry.metrics import Observation

from orchestrator.application.booking_create import BookingCreator
from orchestrator.application.policy import RecoveryPolicy
from orchestrator.application.recovery import Recovery
from orchestrator.domain import BookingState
from orchestrator.persistence.bookings import Booking, Lease, StaleLeaseError
from orchestrator.persistence.uow import UnitOfWorkFactory
from orchestrator.telemetry import bind_context, get_logger, metrics

log = get_logger(__name__)
_tracer = trace.get_tracer("mobility-provider-orchestrator.worker")

CONFIRM_STATES: tuple[BookingState, ...] = (BookingState.HELD,)
MAX_ROUNDS_PER_TICK = 50  # a bound on draining, so one tick cannot starve the scheduler


@dataclass(slots=True)
class LoopSpec:
    """One loop: what it claims, what it does with a claim, and how many at once."""

    name: str
    states: tuple[BookingState, ...]
    handle: Callable[[Booking, Lease], Awaitable[str]]  # returns the counter it advanced
    uow: UnitOfWorkFactory
    concurrency: int
    batch: int


@dataclass(slots=True)
class Loop:
    spec: LoopSpec
    policy: RecoveryPolicy
    ticks: int = 0
    _semaphore: asyncio.Semaphore = field(init=False)

    def __post_init__(self) -> None:
        self._semaphore = asyncio.Semaphore(self.spec.concurrency)

    async def tick(self) -> dict[str, int]:
        """Drain the due work: claim and process batches until a claim comes back short, or a
        batch made no progress. Failed rows are deferred with backoff and never count as
        progress, so a persistently failing row cannot keep a loop spinning."""
        self.ticks += 1
        counters: dict[str, int] = {}
        for _ in range(MAX_ROUNDS_PER_TICK):
            async with self.spec.uow() as store:
                claimed = await store.claim(
                    self.spec.states, limit=self.spec.batch, ttl=self.policy.lease_ttl
                )
            if not claimed:
                break
            results = await asyncio.gather(
                *(self._one(booking, lease) for booking, lease in claimed),
                return_exceptions=True,
            )
            progressed = False
            for result in results:
                if isinstance(result, BaseException):
                    log.error("worker_row_failed", loop=self.spec.name, error=repr(result))
                    counters["failed"] = counters.get("failed", 0) + 1
                elif result:
                    counters[result] = counters.get(result, 0) + 1
                    progressed = True
            if len(claimed) < self.spec.batch or not progressed:
                break
        return counters

    def progressed(self, counters: dict[str, int]) -> bool:
        """Whether a tick did useful work (failures are not work)."""
        return any(n for name, n in counters.items() if name != "failed")

    async def _one(self, booking: Booking, lease: Lease) -> str:
        async with self._semaphore:
            structlog.contextvars.clear_contextvars()
            bind_context(booking_id=booking.id, provider=booking.provider, loop=self.spec.name)
            try:
                with _tracer.start_as_current_span(
                    f"worker.{self.spec.name}",
                    attributes={"booking.id": booking.id, "provider": booking.provider},
                ):
                    async with self.spec.uow() as store:
                        lease = await store.renew(lease, ttl=self.policy.lease_ttl)
                    return await self.spec.handle(booking, lease)
            except StaleLeaseError:
                metrics.worker_leases_expired.add(1, {"loop": self.spec.name})
                log.info("lease_lost", booking_id=booking.id, loop=self.spec.name)
                return ""
            except Exception:
                # Deferred with the reschedule backoff so the next pass does not spin on it.
                try:
                    async with self.spec.uow() as store:
                        await store.defer(
                            lease, until=datetime.now(UTC) + self.policy.reschedule_backoff
                        )
                except Exception:
                    log.exception("defer_failed", booking_id=booking.id)
                raise
            finally:
                try:
                    async with self.spec.uow() as store:
                        await store.release(lease)
                except Exception:
                    log.exception("lease_release_failed", booking_id=booking.id)


class Worker:
    """One process, separate loops, one scheduler (``run_forever``)."""

    def __init__(
        self,
        uow: UnitOfWorkFactory,
        creator: BookingCreator,
        recovery: Recovery,
        policy: RecoveryPolicy,
        *,
        confirm_uow: UnitOfWorkFactory | None = None,
        batch: int = 10,
        concurrency: int = 4,
        confirm_concurrency: int = 8,
    ) -> None:
        self.uow = uow
        self.creator = creator
        self.recovery = recovery
        self.policy = policy
        self.batch = batch
        self.confirm = Loop(
            LoopSpec(
                "confirm",
                CONFIRM_STATES,
                self._confirm,
                confirm_uow or uow,
                confirm_concurrency,
                batch,
            ),
            policy,
        )
        self.submit = Loop(
            LoopSpec("submit", (BookingState.CREATED,), self._submit, uow, concurrency, batch),
            policy,
        )
        self.reconcile = Loop(
            LoopSpec(
                "reconcile", (BookingState.UNKNOWN,), self._reconcile, uow, concurrency, batch
            ),
            policy,
        )
        self.cancel = Loop(
            LoopSpec("cancel", (BookingState.CANCELLING,), self._cancel, uow, concurrency, batch),
            policy,
        )
        self.poll = Loop(
            LoopSpec("poll", (BookingState.PENDING_PROVIDER,), self._poll, uow, concurrency, batch),
            policy,
        )
        self._state_counts: dict[tuple[str, str], int] = {}
        self._open_cases: dict[tuple[str, str], int] = {}
        self._oldest_unresolved: dict[str, float] = {}
        metrics.register_gauge_source(
            "bookings_in_state", self._observe_states, description="Bookings per state"
        )
        metrics.register_gauge_source(
            "review_cases_open", self._observe_cases, description="Open review cases"
        )
        metrics.register_gauge_source(
            "booking_unresolved_oldest_age",
            self._observe_oldest,
            description="Age of the oldest booking still in flight (not settled, not in review)",
            unit="s",
        )

    def close(self) -> None:
        metrics.unregister_gauge_source("bookings_in_state", self._observe_states)
        metrics.unregister_gauge_source("review_cases_open", self._observe_cases)
        metrics.unregister_gauge_source("booking_unresolved_oldest_age", self._observe_oldest)

    # Handlers ------------------------------------------------------------------------------

    async def _submit(self, booking: Booking, lease: Lease) -> str:
        if await self.recovery.abandon_if_due(booking.id, lease=lease):
            return "abandoned"
        await self.creator.submit(booking, lease=lease, correlation_id=None)
        return "submitted"

    async def _reconcile(self, booking: Booking, lease: Lease) -> str:
        await self.recovery.reconcile_once(booking.id, lease=lease)
        return "reconciled"

    async def _confirm(self, booking: Booking, lease: Lease) -> str:
        confirmer = self.recovery.confirmer
        if confirmer is None:
            raise RuntimeError("no confirmer wired")
        with confirmer.on_reserved_pool():  # this loop's slice, and only this loop's
            if booking.confirmation_deadline is not None and datetime.now(UTC) >= (
                booking.confirmation_deadline
            ):
                await confirmer.recover(booking.id, lease=lease)  # past the deadline: observe
                return "confirmed"
            await confirmer.confirm(booking, lease=lease, correlation_id=None)
        return "confirmed"

    async def _poll(self, booking: Booking, lease: Lease) -> str:
        await self.recovery.poll_pending(booking.id, lease=lease)
        return "polled"

    async def _cancel(self, booking: Booking, lease: Lease) -> str:
        canceller = self.recovery.canceller
        if canceller is None:
            raise RuntimeError("no canceller wired")
        await canceller.run(booking.id, lease=lease, correlation_id=None)
        return "cancelled"

    # Scheduling ----------------------------------------------------------------------------

    async def tick(self) -> dict[str, int]:
        """One pass of every loop, in order. Returns how much each did (tests, metrics)."""
        done = {
            "submitted": 0,
            "abandoned": 0,
            "reconciled": 0,
            "recovered": 0,
            "confirmed": 0,
            "cancelled": 0,
            "polled": 0,
            "failed": 0,
        }
        done["recovered"] = len(await self.recovery.recover_stale_submitting(now=datetime.now(UTC)))
        # Confirmation last: a hold produced by a submission or a resubmission earlier in the
        # same pass is confirmed in this pass (in production each loop is its own task).
        for loop in (self.submit, self.reconcile, self.poll, self.cancel, self.confirm):
            for counter, n in (await loop.tick()).items():
                done[counter] = done.get(counter, 0) + n
        await self.refresh_gauges()
        return done

    async def run_forever(self, *, interval: float, confirm_interval: float | None = None) -> None:
        """Each loop runs as its own task and sleeps only after an idle pass: the confirmation
        loop's cadence never waits for the others. The recovery scan rides with submission."""

        async def forever(name: str, step: Callable[[], Awaitable[bool]], idle: float) -> None:
            while True:
                busy = False
                try:
                    busy = await step()
                except Exception:
                    log.exception("worker_loop_failed", loop=name)
                if not busy:
                    await asyncio.sleep(idle)

        async def confirm_step() -> bool:
            return self.confirm.progressed(await self.confirm.tick())

        async def submit_step() -> bool:
            recovered = await self.recovery.recover_stale_submitting(now=datetime.now(UTC))
            return self.submit.progressed(await self.submit.tick()) or bool(recovered)

        async def reconcile_step() -> bool:
            return self.reconcile.progressed(await self.reconcile.tick())

        async def cancel_step() -> bool:
            return self.cancel.progressed(await self.cancel.tick())

        async def poll_step() -> bool:
            return self.poll.progressed(await self.poll.tick())

        async def gauges() -> bool:
            await self.refresh_gauges()
            return False

        await asyncio.gather(
            forever("confirm", confirm_step, confirm_interval or interval),
            forever("submit", submit_step, interval),
            forever("reconcile", reconcile_step, interval),
            forever("cancel", cancel_step, interval),
            forever("poll", poll_step, interval),
            forever("gauges", gauges, max(interval * 10, 5.0)),
        )

    # Gauges (worker-owned; ADR 014) --------------------------------------------

    async def refresh_gauges(self) -> None:
        async with self.uow() as store:
            self._state_counts = await store.count_by_state()
            self._open_cases = await store.count_open_cases()
            self._oldest_unresolved = await store.oldest_unresolved_age(now=datetime.now(UTC))

    def _observe_states(self) -> Iterable[Observation]:
        for (state, provider), n in list(self._state_counts.items()):
            yield Observation(n, {"state": state, "provider": provider})

    def _observe_cases(self) -> Iterable[Observation]:
        for (provider, remediable), n in list(self._open_cases.items()):
            yield Observation(n, {"provider": provider, "remediable": remediable})

    def _observe_oldest(self) -> Iterable[Observation]:
        for state, age in list(self._oldest_unresolved.items()):
            yield Observation(age, {"state": state})

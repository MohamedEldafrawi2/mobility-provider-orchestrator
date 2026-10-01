"""Recovery use cases the worker runs (docs/booking-state-machine.md).

- ``reconcile_once``: for an UNKNOWN booking, look the command up by our reference and apply
  the domain's decision: bind if found, keep looking within budget, escalate otherwise.
- ``recover_stale_submitting``: a SUBMITTING booking whose request-path submission died has a
  journaled attempt with no outcome; that is a possible effect, so it becomes UNKNOWN.
- ``abandon_if_due``: a CREATED booking nothing ever dispatched is abandoned after a bounded
  age, and only then.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from time import monotonic
from typing import Any

from orchestrator.application.booking_create import BookingCreator
from orchestrator.application.policy import RecoveryPolicy
from orchestrator.domain import (
    Abandon,
    BookingId,
    BookingState,
    CommandKind,
    Disposition,
    DispositionBasis,
    Escalate,
    Reservation,
    ReservationState,
    Trigger,
    bind,
    decide_abandon,
    decide_create_after_lookup,
    note_lookup,
    settle,
)
from orchestrator.domain.observations import ObservationOutcome, order_observation
from orchestrator.domain.settlement import Reschedule
from orchestrator.domain.settlement_fenced import (
    FencedLookupDue,
    Resubmit,
    WaitForExclusion,
    decide_create_after_fenced_lookup,
    decide_create_recovery,
)
from orchestrator.persistence.bookings import Booking, BookingStore, Lease
from orchestrator.persistence.uow import UnitOfWorkFactory
from orchestrator.providers import ProviderAdapter, ProviderError
from orchestrator.providers.registry import ProviderRegistry
from orchestrator.resilience import (
    AdmissionController,
    AttemptContext,
    NotDispatchedError,
    Purpose,
    RetryPolicy,
    run_attempts,
)
from orchestrator.telemetry import get_logger, metrics

log = get_logger(__name__)


class Recovery:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        registry: ProviderRegistry,
        creator: BookingCreator,
        policy: RecoveryPolicy,
        *,
        admission: AdmissionController,
        retry: RetryPolicy | None = None,
        confirmer: Any = None,
        canceller: Any = None,
    ) -> None:
        self.uow = uow
        self.registry = registry
        self.creator = creator
        self.policy = policy
        self.admission = admission
        self.retry = retry or RetryPolicy()
        self.confirmer = confirmer
        self.canceller = canceller

    async def reconcile_once(self, booking_id: BookingId, *, lease: Lease | None) -> None:
        """Settle an UNKNOWN booking: by the outstanding command's kind and the provider's
        capabilities. Without finality: one lookup by client reference and one decision, never
        a mutating dispatch. With key binding and finality: resubmit inside the cutoff, then a
        fenced lookup."""
        async with self.uow() as store:
            booking = await store.get(booking_id)
            confirm = await store.open_command(booking_id, CommandKind.CONFIRM)
            cancel = await store.open_command(booking_id, CommandKind.CANCEL)
        if booking.state is not BookingState.UNKNOWN:
            return
        adapter = self.registry.get(booking.provider)
        if confirm is not None and self.confirmer is not None:
            # An outstanding CONFIRM in UNKNOWN: either an attempt may have executed, or the
            # provider rejected one for a reason that asks for a read. Both read the hold.
            await self.confirmer.recover(booking_id, lease=lease)
            return
        if cancel is not None and cancel.possibly_executed and self.canceller is not None:
            await self.canceller.recover(booking_id, lease=lease)
            return
        if adapter.capabilities.finality_lookup and adapter.capabilities.key_bound_before_execution:
            await self._recover_fenced(booking_id, adapter, lease=lease)
            return
        try:
            found = await self._lookup(adapter, booking_id, lease=lease)
        except (ProviderError, NotDispatchedError) as exc:
            log.info("reconcile_lookup_failed", booking_id=booking_id, error=str(exc))
            if adapter.capabilities.finality_lookup:
                # Fenced settlement bounds this by elapsed time; until then a
                # provider with finality is simply asked again later.
                async with self.uow() as store:
                    booking = await store.get(booking_id, for_update=True)
                    await store.save_booking(
                        booking,
                        next_action_at=datetime.now(UTC) + self.policy.reconcile_backoff,
                        lease=lease,
                    )
                return
            # Without finality, a lookup that could not be made and one that found nothing
            # carry the same information: none. Both spend the budget, so escalation stays
            # bounded (docs/booking-state-machine.md) instead of polling an unreachable provider
            # forever.
            found = ()
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = note_lookup(await store.command_for(booking_id, CommandKind.CREATE))
            decision = decide_create_after_lookup(
                adapter.capabilities, command, found, lookup_budget=self.policy.lookup_budget
            )
            await self.creator.apply(
                store, booking, command, decision, lease=lease, correlation_id=None, source="POLL"
            )

    async def _recover_fenced(
        self, booking_id: BookingId, adapter: ProviderAdapter, *, lease: Lease | None
    ) -> None:
        now = datetime.now(UTC)
        async with self.uow() as store:
            booking = await store.get(booking_id)
            command = await store.command_for(booking_id, CommandKind.CREATE)
        if command.disposition is not Disposition.OPEN:
            return
        plan = decide_create_recovery(
            adapter.capabilities, command, now=now, policy=self.policy.expiry
        )
        match plan:
            case Resubmit():
                # The same key, a fresh expiry: the provider replays or creates exactly once.
                async with self.uow() as store:
                    current = await store.get(booking_id, for_update=True)
                    await store.save_booking(
                        current,
                        state=BookingState.CREATED,
                        trigger=None,
                        source="RECONCILER",
                        next_action_at=now,
                        lease=lease,
                    )
                await self.creator.submit(booking, lease=lease, correlation_id=None)
            case WaitForExclusion(until=until):
                async with self.uow() as store:
                    current = await store.get(booking_id, for_update=True)
                    await store.save_booking(current, next_action_at=until, lease=lease)
            case FencedLookupDue():
                try:
                    async with self.admission.admit(
                        adapter.code, Purpose.LOOKUP, operation="fenced_lookup"
                    ) as ticket:
                        try:
                            fenced = await adapter.fenced_lookup(command.provider_key)
                        except ProviderError as exc:
                            ticket.record_provider_error(exc)
                            raise
                        ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
                except (ProviderError, NotDispatchedError) as exc:
                    log.info("fenced_lookup_failed", booking_id=booking_id, error=str(exc))
                    async with self.uow() as store:
                        current = await store.get(booking_id, for_update=True)
                        await store.save_booking(
                            current, next_action_at=now + self.policy.reconcile_backoff, lease=lease
                        )
                    return
                async with self.uow() as store:
                    current = await store.get(booking_id, for_update=True)
                    command = await store.command_for(booking_id, CommandKind.CREATE)
                    decision = decide_create_after_fenced_lookup(
                        adapter.capabilities, command, fenced.reservations
                    )
                    await self.creator.apply(
                        store,
                        current,
                        command,
                        decision,
                        lease=lease,
                        correlation_id=None,
                        source="POLL",
                    )
            case Reschedule():
                async with self.uow() as store:
                    current = await store.get(booking_id, for_update=True)
                    await store.save_booking(
                        current,
                        state=BookingState.CREATED,
                        trigger=None,
                        source="RECONCILER",
                        next_action_at=now,
                        lease=lease,
                    )
            case _:
                raise AssertionError(f"unexpected recovery plan {plan!r}")

    async def _lookup(
        self, adapter: ProviderAdapter, booking_id: BookingId, *, lease: Lease | None
    ) -> tuple[Reservation, ...]:
        """A read under the lookup purpose: admitted, and retried while nothing was sent."""
        deadline = monotonic() + (
            max((lease.expires_at - datetime.now(UTC)).total_seconds(), 0.0)
            if lease is not None
            else self.policy.mutation_timeout.total_seconds()
        )
        labels = {"provider": adapter.code, "purpose": Purpose.LOOKUP.value}

        async def attempt(context: AttemptContext) -> tuple[Reservation, ...]:
            async with self.admission.admit(
                adapter.code,
                Purpose.LOOKUP,
                operation="find_bookings_by_client_ref",
                attempt_n=context.n,
                after_timeout=context.after_timeout,
                charged=context.charged,
                deadline=deadline,
            ) as ticket:
                try:
                    found = tuple(await adapter.find_bookings_by_client_ref(booking_id))
                except ProviderError as exc:
                    ticket.record_provider_error(exc)
                    raise
                ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
                return found

        return await run_attempts(attempt, policy=self.retry, deadline=deadline, labels=labels)

    async def poll_pending(self, booking_id: BookingId, *, lease: Lease | None) -> None:
        """An authoritative read of a pending reservation, ordered by generation and revision
        (6.4); overdue pending bookings escalate to review (6.6 row: PENDING_PROVIDER)."""
        async with self.uow() as store:
            booking = await store.get(booking_id)
        if (
            booking.state is not BookingState.PENDING_PROVIDER
            or booking.provider_booking_ref is None
        ):
            return
        adapter = self.registry.get(booking.provider)
        now = datetime.now(UTC)
        try:
            async with self.admission.admit(
                adapter.code, Purpose.LOOKUP, operation="get_booking"
            ) as ticket:
                try:
                    observation = await adapter.get_booking(booking.provider_booking_ref)
                except ProviderError as exc:
                    ticket.record_provider_error(exc)
                    raise
                ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
        except (ProviderError, NotDispatchedError) as exc:
            log.info("poll_failed", booking_id=booking_id, error=str(exc))
            observation = None
        async with self.uow() as store:
            current = await store.get(booking_id, for_update=True)
            if current.state is not BookingState.PENDING_PROVIDER:
                return
            if observation is not None:
                ordering = order_observation(
                    observation,
                    bound_ref=current.provider_booking_ref,
                    state=current.state,
                    generation=current.provider_generation,
                    last_revision=current.last_revision,
                    authoritative=True,
                )
                if ordering.outcome is ObservationOutcome.APPLIED and ordering.trigger is not None:
                    assert ordering.next_state is not None
                    await self._apply_observation(
                        store, current, observation, ordering.trigger, ordering.next_state, lease
                    )
                    return
                if ordering.outcome in (
                    ObservationOutcome.CONTRADICTORY,
                    ObservationOutcome.REGRESSED,
                ):
                    command = await store.command_for(booking_id, CommandKind.CREATE)
                    reason = (
                        "observation-contradicts-state"
                        if ordering.outcome is ObservationOutcome.CONTRADICTORY
                        else "observation-regressed"
                    )
                    await self.creator.apply(
                        store,
                        current,
                        command,
                        Escalate(reason, implicated=(observation.ref,), remediable=True),
                        lease=lease,
                        correlation_id=None,
                        source="POLL",
                    )
                    return
                if ordering.advances_watermark and (
                    observation.generation != current.provider_generation
                    or observation.revision != current.last_revision
                ):
                    # The same state, but a newer fact: the watermarks move so that an older
                    # event arriving later cannot pass as new (6.4).
                    await store.save_booking(
                        current,
                        provider_generation=observation.generation,
                        last_revision=observation.revision,
                        next_action_at=now + self.policy.pending_poll,
                        lease=lease,
                    )
                    return
            if now - current.created_at > self.policy.pending_max_age:
                command = await store.command_for(booking_id, CommandKind.CREATE)
                await self.creator.apply(
                    store,
                    current,
                    command,
                    Escalate(
                        "pending-overdue",
                        implicated=tuple(
                            r for r in (current.provider_booking_ref,) if r is not None
                        ),
                        remediable=True,
                    ),
                    lease=lease,
                    correlation_id=None,
                    source="POLL",
                )
                return
            await store.save_booking(
                current, next_action_at=now + self.policy.pending_poll, lease=lease
            )

    async def _apply_observation(
        self,
        store: BookingStore,
        booking: Booking,
        observation: Reservation,
        trigger: Trigger,
        next_state: BookingState,
        lease: Lease | None,
    ) -> None:
        caps = self.registry.get(booking.provider).capabilities
        create = await store.command_for(booking.id, CommandKind.CREATE)
        if (
            create.disposition is Disposition.OPEN
            and observation.state is ReservationState.CONFIRMED
        ):
            create = settle(
                bind(create, observation.ref),
                Disposition.SUCCEEDED,
                DispositionBasis.LOOKUP,
                caps=caps,
            )
            await store.save_command(create)
            self.creator._count(store, booking, create)
        elif (
            create.disposition is Disposition.OPEN and observation.state is ReservationState.FAILED
        ):
            create = settle(create, Disposition.REJECTED, DispositionBasis.FENCED_LOOKUP, caps=caps)
            await store.save_command(create)
            self.creator._count(store, booking, create)
        await store.save_booking(
            booking,
            state=next_state,
            trigger=trigger,
            source="POLL",
            provider_generation=observation.generation,
            last_revision=observation.revision,
            failure_code="booking-failed" if next_state is BookingState.FAILED else None,
            clear_next_action=next_state is not BookingState.PENDING_PROVIDER,
            clear_unresolved=True,
            lease=lease,
        )

    async def recover_stale_submitting(self, *, now: datetime) -> list[BookingId]:
        """SUBMITTING bookings whose submitter died own a journaled, unfinished attempt."""
        recovered: list[BookingId] = []
        async with self.uow() as store:
            stale = await store.expired_submitting(
                stale_before=now - self.policy.submitting_stale_after
            )
        stale_before = now - self.policy.submitting_stale_after
        for booking in stale:
            async with self.uow() as store:
                current = await store.get(booking.id, for_update=True)
                if (
                    current.state not in (BookingState.SUBMITTING, BookingState.CONFIRMING)
                    or current.version != booking.version
                    or current.lease_live(now)
                    or current.updated_at >= stale_before
                ):
                    continue  # it moved, or a live worker owns it now: not ours to touch
                confirming = current.state is BookingState.CONFIRMING
                command = (
                    await store.open_command(booking.id, CommandKind.CONFIRM)
                    if confirming
                    else await store.command_for(booking.id, CommandKind.CREATE)
                )
                if command is None or not command.possibly_executed:
                    # Every attempt is finished without effect (a refusal after the mark, a
                    # lost lease): nothing could have happened; back to where it was (6.2).
                    await store.save_booking(
                        current,
                        state=BookingState.HELD if confirming else BookingState.CREATED,
                        trigger=Trigger.NOT_DISPATCHED,
                        source="RECONCILER",
                        next_action_at=now,
                    )
                    recovered.append(booking.id)
                    continue
                await store.save_booking(
                    current,
                    state=BookingState.UNKNOWN,
                    trigger=Trigger.OUTCOME_UNCERTAIN,
                    source="RECONCILER",
                    unresolved_reason="submitter-died-after-dispatch",
                    next_action_at=now,
                )
                recovered.append(booking.id)
        return recovered

    async def abandon_if_due(self, booking_id: BookingId, *, lease: Lease | None) -> bool:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            if booking.state is not BookingState.CREATED:
                return False
            command = await store.command_for(booking_id, CommandKind.CREATE)
            decision = decide_abandon(
                command, now=datetime.now(UTC), max_age=self.policy.abandon_after
            )
            if not isinstance(decision, Abandon):
                return False
            caps = self.registry.get(booking.provider).capabilities
            command = settle(command, Disposition.ABANDONED, DispositionBasis.LOCAL, caps=caps)
            await store.save_command(replace(command))
            labels = {
                "provider": booking.provider,
                "kind": command.kind.value,
                "disposition": command.disposition.value,
                "basis": "LOCAL",
            }
            store.after_commit(lambda: metrics.booking_commands.add(1, labels))
            await store.save_booking(
                booking,
                state=BookingState.FAILED,
                trigger=Trigger.ABANDONED,
                source="RECONCILER",
                failure_code="booking-not-submitted",
                clear_next_action=True,
                lease=lease,
            )
            return True

"""Confirming holds (docs/booking-state-machine.md; edge cases 12 to 14).

A hold-then-confirm provider answers a create with a hold that must be confirmed before its
deadline. The platform confirms **immediately** on the request path and, if that dies or is
refused, from the worker's dedicated confirmation loop. Both paths own the row through a
lease. Every attempt is journaled before the network IO, carries an expiry inside the command
cutoff and inside the hold's deadline, is re-checked against both right before the IO, and
settles by the provider's answer or, when the answer was lost, by an authoritative read of the
bound reservation once the attempt's expiry plus the provider's clock skew has passed.

Replacement CONFIRM commands (a new key, a new cutoff) share the booking's confirmation budget
and the original hold deadline. A replacement is created only under the booking's row lock,
only when no attempt of any earlier CONFIRM command may still execute, and the predecessor is
closed as ``UNRESOLVED`` on a ``LOCAL`` basis (nothing about the provider is claimed). When the
deadline can no longer be met, the loop waits for the provider to report the expiry.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterator
from contextvars import ContextVar
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from time import monotonic

from orchestrator.application.booking_create import (
    AttemptReport,
    BookingCreator,
    _context_from_history,
    _RetryableAttemptError,
    _verdict,
)
from orchestrator.application.ids import new_attempt_id, new_command_id
from orchestrator.application.policy import RecoveryPolicy
from orchestrator.domain import (
    Attempt,
    AttemptOutcome,
    BookingId,
    BookingState,
    Command,
    CommandId,
    CommandKind,
    ConfirmIntent,
    Disposition,
    DispositionBasis,
    Escalate,
    ProviderRequest,
    ProviderResult,
    Reschedule,
    Reservation,
    SideEffect,
    Trigger,
    Uncertain,
    anchor,
    bind,
    dispatch_allowed,
    exclude_all_possible,
    record_attempt,
    settle,
)
from orchestrator.domain.confirmation import (
    AttemptExcluded,
    Confirmed,
    DeadlineTooClose,
    HoldExpired,
    Reconcile,
    confirm_dispatch_window,
    decide_confirm_after_attempt,
    decide_confirm_after_lookup,
)
from orchestrator.persistence.bookings import Booking, BookingStore, Lease, StaleLeaseError
from orchestrator.persistence.uow import UnitOfWorkFactory
from orchestrator.providers import ProviderAdapter, ProviderError
from orchestrator.providers.errors import ErrorKind
from orchestrator.providers.registry import ProviderRegistry
from orchestrator.resilience import (
    REASON_DEADLINE,
    AdmissionController,
    AttemptContext,
    NotDispatchedError,
    Purpose,
    RetryPolicy,
    Ticket,
    retry_verdict,
    run_attempts,
)
from orchestrator.telemetry import get_logger, metrics

log = get_logger(__name__)

_active_uow: ContextVar[UnitOfWorkFactory | None] = ContextVar("confirmer_uow", default=None)

REASON_HOLD_DEADLINE = "hold-deadline-too-close"
REASON_BUDGET = "confirmation-budget-exhausted"


class Confirmer:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        registry: ProviderRegistry,
        creator: BookingCreator,
        policy: RecoveryPolicy,
        *,
        admission: AdmissionController,
        retry: RetryPolicy | None = None,
        request_uow: UnitOfWorkFactory | None = None,
    ) -> None:
        # ``uow`` is the reserved pool slice the confirmation loop runs on: confirmation never
        # waits behind other worker loops. The request path (no lease) uses the ordinary pool,
        # ``request_uow``: many concurrent requests must not exhaust the loop's slice.
        self._loop_uow = uow
        self._request_uow = request_uow or uow
        self.registry = registry
        self.creator = creator
        self.policy = policy
        self.admission = admission
        self.retry = retry or RetryPolicy()

    @property
    def uow(self) -> UnitOfWorkFactory:
        """The unit of work for the current call: the reserved slice inside
        ``on_reserved_pool`` (the confirmation loop), the ordinary pool everywhere else."""
        chosen = _active_uow.get(None)
        return chosen if chosen is not None else self._request_uow

    @contextlib.contextmanager
    def on_reserved_pool(self) -> Iterator[None]:
        """Run the enclosed calls on the reserved pool slice. The confirmation loop wraps each
        leased operation in it; nested calls inherit the choice, and nothing else ever selects
        the slice, whatever lease it happens to hold."""
        token = _active_uow.set(self._loop_uow)
        try:
            yield
        finally:
            _active_uow.reset(token)

    # Commands --------------------------------------------------------------------------------

    @staticmethod
    def new_confirm_command(booking: Booking, ref: str, *, now: datetime, n: int) -> Command:
        return Command(
            id=new_command_id(),
            booking_id=booking.id,
            kind=CommandKind.CONFIRM,
            intent=ConfirmIntent(ref),  # type: ignore[arg-type]
            provider_key=f"{booking.id}:confirm:{n}",
            created_at=now,
            submission_ref=ref,  # type: ignore[arg-type]
        )

    # Entry points ----------------------------------------------------------------------------

    async def confirm(
        self, booking: Booking, *, lease: Lease | None, correlation_id: str | None
    ) -> None:
        """The attempt loop around ``confirm_once``. Without a lease (the request path), the
        row is claimed first: the request path and the confirmation loop never both own it."""
        await self._confirm(booking, lease=lease, correlation_id=correlation_id)

    async def _confirm(
        self, booking: Booking, *, lease: Lease | None, correlation_id: str | None
    ) -> None:
        owned = lease is None
        if owned:
            async with self.uow() as store:
                lease = await store.claim_one(booking.id, ttl=self.policy.lease_ttl)
            if lease is None:
                return  # a worker owns the hold: it confirms
        assert lease is not None
        try:
            await self._loop(booking, lease=lease, correlation_id=correlation_id)
        finally:
            if owned:
                with contextlib.suppress(Exception):
                    async with self.uow() as store:
                        await store.release(lease)

    async def _loop(self, booking: Booking, *, lease: Lease, correlation_id: str | None) -> None:
        deadline = self.creator._deadline(lease)
        labels = {"provider": booking.provider, "purpose": Purpose.CONFIRM.value}
        async with self.uow() as store:
            command = await store.open_command(booking.id, CommandKind.CONFIRM)
        first = _context_from_history(command) if command is not None else AttemptContext()

        async def attempt(context: AttemptContext) -> None:
            if context.n < first.n:
                context = first
            report = await self.confirm_once(
                booking.id, lease=lease, correlation_id=correlation_id, context=context
            )
            if report.retry is not None:
                raise _RetryableAttemptError(report.retry)

        with contextlib.suppress(_RetryableAttemptError, NotDispatchedError, StaleLeaseError):
            await run_attempts(
                attempt,
                policy=self.retry,
                deadline=deadline,
                labels=labels,
                verdict=_verdict,
                first=first,
            )

    async def confirm_once(
        self,
        booking_id: BookingId,
        *,
        lease: Lease | None,
        correlation_id: str | None,
        context: AttemptContext | None = None,
    ) -> AttemptReport:
        """One journaled CONFIRM attempt under admission, inside the hold's deadline."""
        context = context or AttemptContext()
        async with self.uow() as store:
            booking = await store.get(booking_id)
        if booking.state is not BookingState.HELD or booking.provider_booking_ref is None:
            return AttemptReport(dispatched=False, retry=None)
        adapter = self.registry.get(booking.provider)
        caps = adapter.capabilities
        now = datetime.now(UTC)
        if booking.confirmation_deadline is None:
            raise RuntimeError("a held booking carries its confirmation deadline")
        latest = confirm_dispatch_window(
            now=now,
            hold_deadline=booking.confirmation_deadline,
            caps=caps,
            policy=self.policy.expiry,
        )
        if latest is None:
            await self._deadline_too_close(booking_id, lease=lease, correlation_id=correlation_id)
            return AttemptReport(dispatched=False, retry=None)
        command, uncertain = await self._active_command(booking_id, lease=lease)
        if uncertain:
            # Some attempt of some CONFIRM command may still execute: nothing new is sent
            # until an authoritative read has settled every outstanding attempt.
            await self.recover(booking_id, lease=lease)
            return AttemptReport(dispatched=False, retry=None)
        if command is None:
            return AttemptReport(dispatched=False, retry=None)
        window = monotonic() + (latest - now).total_seconds()
        try:
            async with self.admission.admit(
                booking.provider,
                Purpose.CONFIRM,
                operation="confirm_booking",
                attempt_n=context.n,
                after_timeout=context.after_timeout,
                charged=context.charged,
                deadline=min(self.creator._deadline(lease), window),
            ) as ticket:
                return await self._dispatch(
                    booking_id,
                    command.id,
                    adapter,
                    latest,
                    ticket,
                    lease=lease,
                    correlation_id=correlation_id,
                )
        except NotDispatchedError as refusal:
            if not refusal.journaled:
                await self._journal_refusal(
                    booking_id, refusal, lease=lease, correlation_id=correlation_id
                )
            raise

    async def _active_command(
        self, booking_id: BookingId, *, lease: Lease | None
    ) -> tuple[Command | None, bool]:
        """The one CONFIRM command that may dispatch now, creating a replacement from the
        booking's budget only when every earlier command is settled and none may still
        execute. All under the booking's row lock. The flag says that some attempt of *any*
        CONFIRM command of the booking (open or not) may still execute: recovery first."""
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            if booking.state is not BookingState.HELD:
                return None, False
            caps = self.registry.get(booking.provider).capabilities
            commands = await store.commands_for(booking_id, CommandKind.CONFIRM)
            now = datetime.now(UTC)
            if any(c.possibly_executed for c in commands):
                return None, True  # something may still execute: recovery settles it first
            open_ones = [c for c in commands if c.disposition is Disposition.OPEN]
            latest = open_ones[-1] if open_ones else None
            if latest is not None and dispatch_allowed(caps, latest, now=now):
                return latest, False
            budget = booking.confirm_budget_remaining or 0
            if budget <= 0:
                await store.save_booking(
                    booking,
                    unresolved_reason=REASON_BUDGET,
                    next_action_at=booking.confirmation_deadline,
                    lease=lease,
                )
                return None, False
            if latest is not None:
                # The predecessor's cutoff passed with nothing outstanding: closed without any
                # claim about the provider (nothing was confirmed or excluded by it).
                closed = replace(
                    latest, disposition=Disposition.UNRESOLVED, basis=DispositionBasis.LOCAL
                )
                await store.save_command(closed)
            replacement = self.new_confirm_command(
                booking, str(booking.provider_booking_ref), now=now, n=len(commands) + 1
            )
            await store.add_command(replacement)
            await store.save_booking(booking, confirm_budget_remaining=budget - 1, lease=lease)
            return replacement, False

    async def _dispatch(
        self,
        booking_id: BookingId,
        command_id: str,
        adapter: ProviderAdapter,
        latest: datetime,
        ticket: Ticket,
        *,
        lease: Lease | None,
        correlation_id: str | None,
    ) -> AttemptReport:
        caps = adapter.capabilities
        ticket.require_time()
        # Transaction 1: journal before the network IO; the first mark anchors the cutoff.
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_by_id(CommandId(command_id))
            # The hold began when its first CONFIRM command was created, at the bind.
            held_since = (await store.commands_for(booking_id, CommandKind.CONFIRM))[0].created_at
            now = datetime.now(UTC)
            if lease is not None:
                lease = await store.renew(lease, ttl=self.policy.lease_ttl)  # ours, or stale
            if booking.state is not BookingState.HELD or not dispatch_allowed(
                caps, command, now=now
            ):
                ticket.record(outcome="NOT_DISPATCHED", side_effect="NOT_DISPATCHED", healthy=True)
                return AttemptReport(dispatched=False, retry=None)
            if command.first_dispatch_at is None:
                command = anchor(
                    command, first_dispatch_at=now, cutoff=self.policy.expiry.cutoff(caps, now)
                )
            expiry = self.policy.expiry.attempt_expiry(now, command.execution_cutoff)
            expiry = min(expiry, latest) if expiry is not None else latest
            attempt = Attempt(
                id=new_attempt_id(),
                command_id=command.id,
                n=len(command.attempts) + 1,
                request=ProviderRequest(
                    payload=(("reservation", str(booking.provider_booking_ref)),), expiry=expiry
                ),
                dispatch_marked_at=now,
            )
            command = record_attempt(command, attempt)
            await store.save_command(command)
            booking = await store.save_booking(
                booking,
                state=BookingState.CONFIRMING,
                trigger=Trigger.ATTEMPT_DISPATCHED,
                source="PLATFORM",
                correlation_id=correlation_id,
                clear_next_action=True,
                lease=lease,
            )
        # The last check before the IO: the transaction may have used the time that was left,
        # and the hold window is a second deadline.
        remaining = ticket.remaining()
        if (remaining is not None and remaining <= 0) or datetime.now(UTC) >= expiry:
            await self._finish_unsent(
                booking_id, command.id, attempt, lease=lease, correlation_id=correlation_id
            )
            raise NotDispatchedError(REASON_DEADLINE, journaled=True)
        assert booking.provider_booking_ref is not None
        metrics.confirm_dispatch_latency.record(
            max((datetime.now(UTC) - held_since).total_seconds(), 0.0),
            {"provider": booking.provider},
        )
        result, error = await self._call(adapter, booking.provider_booking_ref, expiry)
        if error is not None:
            ticket.record_provider_error(error)
        else:
            ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
        # Transaction 2: record and decide.
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_by_id(CommandId(command_id))
            finished = replace(
                attempt,
                finished_at=datetime.now(UTC),
                outcome=self._outcome(result),
                side_effect=result.side_effect,
                error=error.kind.value if error is not None else None,
            )
            command = record_attempt(command, finished)
            if command.is_settled:
                await store.save_command(command)
                return AttemptReport(dispatched=True, retry=None)
            decision = decide_confirm_after_attempt(
                command, attempt.id, result, bound=str(booking.provider_booking_ref)
            )
            retry = retry_verdict(error) if error is not None else None
            if isinstance(decision, Reschedule) and retry is not None:
                decision = Reschedule(reason=None, retry_after=retry.retry_after)
            await self.apply(
                store, booking, command, decision, lease=lease, correlation_id=correlation_id
            )
        return AttemptReport(dispatched=True, retry=retry)

    async def _call(
        self, adapter: ProviderAdapter, ref: str, expiry: datetime
    ) -> tuple[ProviderResult, ProviderError | None]:
        try:
            async with asyncio.timeout(self.policy.mutation_timeout.total_seconds()):
                reservation: Reservation = await adapter.confirm_booking(ref, expiry)  # type: ignore[arg-type]
        except ProviderError as exc:
            return (
                ProviderResult(
                    exc.side_effect,
                    definitive_rejection=exc.definitive,
                    hold_expired=exc.kind is ErrorKind.HOLD_EXPIRED,
                    request_expired=exc.kind is ErrorKind.EXPIRED_REQUEST,
                ),
                exc,
            )
        except TimeoutError:
            return ProviderResult(SideEffect.POSSIBLE), ProviderError(
                ErrorKind.TIMEOUT, SideEffect.POSSIBLE, "mutation timeout"
            )
        return ProviderResult(SideEffect.NONE, reservation=reservation), None

    @staticmethod
    def _outcome(result: ProviderResult) -> AttemptOutcome:
        if result.reservation is not None:
            return AttemptOutcome.SUCCESS
        if result.definitive_rejection:
            return AttemptOutcome.REJECTED
        return AttemptOutcome.UNKNOWN

    # Recovery --------------------------------------------------------------------------------

    async def recover(self, booking_id: BookingId, *, lease: Lease | None) -> None:
        """An uncertain CONFIRM (state UNKNOWN) or a hold past its deadline: an authoritative
        read of the bound reservation decides, for every CONFIRM command still outstanding."""
        await self._recover(booking_id, lease=lease)

    async def _recover(self, booking_id: BookingId, *, lease: Lease | None) -> None:
        async with self.uow() as store:
            booking = await store.get(booking_id)
            commands = await store.commands_for(booking_id, CommandKind.CONFIRM)
        if booking.provider_booking_ref is None or not commands:
            return
        adapter = self.registry.get(booking.provider)
        try:
            async with self.admission.admit(
                booking.provider, Purpose.LOOKUP, operation="get_booking"
            ) as ticket:
                try:
                    reservation = await adapter.get_booking(booking.provider_booking_ref)
                except ProviderError as exc:
                    ticket.record_provider_error(exc)
                    raise
                ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
        except (ProviderError, NotDispatchedError) as exc:
            log.info("confirm_lookup_failed", booking_id=booking_id, error=str(exc))
            async with self.uow() as store:
                booking = await store.get(booking_id, for_update=True)
                await store.save_booking(
                    booking,
                    next_action_at=datetime.now(UTC) + self.policy.reconcile_backoff,
                    lease=lease,
                )
            return
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            commands = await store.commands_for(booking_id, CommandKind.CONFIRM)
            outstanding = [c for c in commands if c.disposition is Disposition.OPEN]
            command = outstanding[-1] if outstanding else commands[-1]
            now = datetime.now(UTC)
            caps = adapter.capabilities
            decision = decide_confirm_after_lookup(
                caps,
                command,
                reservation,
                bound=str(booking.provider_booking_ref),
                now=now,
                policy=self.policy.expiry,
            )
            others = [c for c in commands if c.id != command.id]
            if isinstance(decision, AttemptExcluded) and any(
                a.effective_side_effect is SideEffect.POSSIBLE
                and (
                    a.request.expiry is None
                    or now < self.policy.expiry.excluded_after(caps, a.request.expiry)
                )
                for c in others
                for a in c.attempts
            ):
                # Still a hold, but an attempt of an earlier command may yet execute.
                decision = Uncertain()
            if isinstance(decision, Confirmed | HoldExpired | AttemptExcluded):
                # The one authoritative read accounts for every attempt of every CONFIRM
                # command of this booking, whatever its disposition: a terminal reservation
                # settles the open ones, a live hold excludes what can no longer execute.
                disposition = (
                    Disposition.SUCCEEDED
                    if isinstance(decision, Confirmed)
                    else Disposition.REJECTED
                )
                for other in others:
                    settled = exclude_all_possible(other, at=now)
                    if other.disposition is Disposition.OPEN and not isinstance(
                        decision, AttemptExcluded
                    ):
                        settled = settle(
                            settled, disposition, DispositionBasis.FENCED_LOOKUP, caps=caps
                        )
                    if settled != other:
                        await store.save_command(settled)
            await self.apply(
                store, booking, command, decision, lease=lease, correlation_id=None, source="POLL"
            )

    # Applying decisions ----------------------------------------------------------------------

    async def apply(
        self,
        store: BookingStore,
        booking: Booking,
        command: Command,
        decision: object,
        *,
        lease: Lease | None,
        correlation_id: str | None,
        source: str = "PROVIDER_RESPONSE",
    ) -> None:
        caps = self.registry.get(booking.provider).capabilities
        now = datetime.now(UTC)
        create = await store.command_for(booking.id, CommandKind.CREATE)
        if booking.state is BookingState.NEEDS_REVIEW and not isinstance(decision, Escalate):
            # Under review the booking moves only through the case's evidence (6.1). The
            # commands still record what is known: an authoritative answer settles them.
            if isinstance(decision, Confirmed | HoldExpired):
                final = (
                    DispositionBasis.FENCED_LOOKUP
                    if decision.basis is DispositionBasis.LOOKUP
                    else decision.basis
                )
                disposition = (
                    Disposition.SUCCEEDED
                    if isinstance(decision, Confirmed)
                    else Disposition.REJECTED
                )
                command = settle(
                    exclude_all_possible(command, at=now), disposition, final, caps=caps
                )
                self.creator._count(store, booking, command)
            await store.save_command(command)
            return
        match decision:
            case Confirmed(reservation=res, basis=basis):
                final = (
                    DispositionBasis.FENCED_LOOKUP if basis is DispositionBasis.LOOKUP else basis
                )
                command = settle(
                    exclude_all_possible(command, at=now), Disposition.SUCCEEDED, final, caps=caps
                )
                await store.save_command(command)
                if create.disposition is Disposition.OPEN:
                    create = settle(
                        exclude_all_possible(bind(create, res.ref), at=now),
                        Disposition.SUCCEEDED,
                        final,
                        caps=caps,
                    )
                    await store.save_command(create)
                self.creator._count(store, booking, command)
                trigger = (
                    None if booking.state is BookingState.CONFIRMED else Trigger.PROVIDER_CONFIRMED
                )
                await store.save_booking(
                    booking,
                    state=BookingState.CONFIRMED,
                    trigger=trigger,
                    source=source,
                    clear_next_action=True,
                    clear_unresolved=True,
                    provider_generation=res.generation,
                    last_revision=res.revision,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case HoldExpired(basis=basis):
                # An authoritative read from a provider with finality: a hold cannot un-expire.
                final = (
                    DispositionBasis.FENCED_LOOKUP if basis is DispositionBasis.LOOKUP else basis
                )
                command = settle(
                    exclude_all_possible(command, at=now), Disposition.REJECTED, final, caps=caps
                )
                await store.save_command(command)
                if create.disposition is Disposition.OPEN:
                    create = settle(
                        exclude_all_possible(create, at=now), Disposition.REJECTED, final, caps=caps
                    )
                    await store.save_command(create)
                self.creator._count(store, booking, command)
                await store.save_booking(
                    booking,
                    state=BookingState.FAILED,
                    trigger=Trigger.HOLD_EXPIRED,
                    source=source,
                    failure_code="hold-expired",
                    clear_next_action=True,
                    clear_unresolved=True,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case AttemptExcluded():
                # The attempt's possible effect is settled durably.
                await store.save_command(exclude_all_possible(command, at=now))
                trigger = (
                    Trigger.ATTEMPT_EXCLUDED if booking.state is BookingState.CONFIRMING else None
                )
                if booking.state is BookingState.UNKNOWN:
                    trigger = Trigger.PROVIDER_HELD
                await store.save_booking(
                    booking,
                    state=BookingState.HELD,
                    trigger=trigger,
                    source=source,
                    next_action_at=now,
                    clear_unresolved=True,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Uncertain():
                await store.save_command(command)
                last = command.attempts[-1] if command.attempts else None
                when = now + self.policy.reconcile_backoff
                if last is not None and last.request.expiry is not None:
                    when = max(when, self.policy.expiry.excluded_after(caps, last.request.expiry))
                trigger = (
                    None if booking.state is BookingState.UNKNOWN else Trigger.OUTCOME_UNCERTAIN
                )
                await store.save_booking(
                    booking,
                    state=BookingState.UNKNOWN,
                    trigger=trigger,
                    source=source,
                    unresolved_reason="confirmation-outcome-unknown",
                    next_action_at=when,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Reconcile(reason=reason):
                # The attempt certainly did nothing, and the provider's reason says the hold is
                # not simply waiting: its state is read now, not presumed.
                await store.save_command(command)
                trigger = (
                    None if booking.state is BookingState.UNKNOWN else Trigger.OUTCOME_UNCERTAIN
                )
                await store.save_booking(
                    booking,
                    state=BookingState.UNKNOWN,
                    trigger=trigger,
                    source=source,
                    unresolved_reason=reason,
                    next_action_at=now,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Reschedule(reason=reason, retry_after=retry_after):
                await store.save_command(command)
                wait = max(self.policy.reschedule_backoff.total_seconds(), retry_after or 0.0)
                when = now + timedelta(seconds=wait)
                if booking.confirmation_deadline is not None:
                    when = min(when, booking.confirmation_deadline)  # backoff capped by the hold
                trigger = (
                    Trigger.NOT_DISPATCHED if booking.state is BookingState.CONFIRMING else None
                )
                await store.save_booking(
                    booking,
                    state=BookingState.HELD,
                    trigger=trigger,
                    source=source,
                    unresolved_reason=reason,
                    next_action_at=when,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case DeadlineTooClose(deadline=deadline):
                await store.save_booking(
                    booking,
                    unresolved_reason=REASON_HOLD_DEADLINE,
                    next_action_at=deadline + (caps.max_clock_skew or timedelta(0)),
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Escalate(reason=reason, implicated=implicated, remediable=remediable):
                if command.disposition is Disposition.OPEN:
                    command = replace(
                        command, disposition=Disposition.UNRESOLVED, basis=DispositionBasis.LOOKUP
                    )
                await store.save_command(command)
                await store.open_review_case(
                    booking.id,
                    case_id=new_command_id().replace("cmd_", "rev_"),
                    reason=reason,
                    remediable=remediable,
                    outstanding_command_id=command.id,
                    implicated=implicated,
                )
                await store.save_booking(
                    booking,
                    state=BookingState.NEEDS_REVIEW,
                    trigger=Trigger.ESCALATE_REVIEW,
                    source=source,
                    unresolved_reason=reason,
                    clear_next_action=True,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case _:
                raise AssertionError(f"unexpected confirm decision {decision!r}")

    # Helpers ---------------------------------------------------------------------------------

    async def _deadline_too_close(
        self, booking_id: BookingId, *, lease: Lease | None, correlation_id: str | None
    ) -> None:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            if booking.state is not BookingState.HELD or booking.confirmation_deadline is None:
                return
            command = await store.open_command(booking_id, CommandKind.CONFIRM)
            if command is None:
                return
            await self.apply(
                store,
                booking,
                command,
                DeadlineTooClose(booking.confirmation_deadline),
                lease=lease,
                correlation_id=correlation_id,
                source="PLATFORM",
            )

    async def _finish_unsent(
        self,
        booking_id: BookingId,
        command_id: str,
        attempt: Attempt,
        *,
        lease: Lease | None,
        correlation_id: str | None,
    ) -> None:
        async with self.uow() as store:
            command = await store.command_by_id(CommandId(command_id))
            unsent = replace(
                attempt,
                finished_at=datetime.now(UTC),
                outcome=AttemptOutcome.NOT_DISPATCHED,
                side_effect=SideEffect.NOT_DISPATCHED,
                error=REASON_DEADLINE,
            )
            await store.save_command(record_attempt(command, unsent))
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_by_id(CommandId(command_id))
            if booking.state is not BookingState.CONFIRMING:
                return
            try:
                await self.apply(
                    store,
                    booking,
                    command,
                    Reschedule(reason=REASON_DEADLINE, retry_after=None),
                    lease=lease,
                    correlation_id=correlation_id,
                    source="ADMISSION",
                )
            except StaleLeaseError:
                log.info("unsent_confirm_lease_lost", booking_id=booking_id)

    async def _journal_refusal(
        self,
        booking_id: BookingId,
        refusal: NotDispatchedError,
        *,
        lease: Lease | None,
        correlation_id: str | None,
    ) -> None:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.open_command(booking_id, CommandKind.CONFIRM)
            if booking.state is not BookingState.HELD or command is None:
                return
            now = datetime.now(UTC)
            denied = Attempt(
                id=new_attempt_id(),
                command_id=command.id,
                n=len(command.attempts) + 1,
                request=ProviderRequest(payload=(), expiry=None),
                dispatch_marked_at=None,
                finished_at=now,
                outcome=AttemptOutcome.NOT_DISPATCHED,
                side_effect=SideEffect.NOT_DISPATCHED,
                error=refusal.reason if refusal.reason == REASON_DEADLINE else None,
            )
            command = record_attempt(command, denied)
            decision: object = (
                Uncertain()
                if command.possibly_executed
                else Reschedule(reason=refusal.reason, retry_after=refusal.retry_after)
            )
            await self.apply(
                store,
                booking,
                command,
                decision,
                lease=lease,
                correlation_id=correlation_id,
                source="ADMISSION",
            )

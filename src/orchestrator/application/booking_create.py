"""Create a booking against a provider and submit it once, safely.

This is the use case the request path and the worker share. The sequence is the one the design
fixes in sections 6.2 to 6.5:

1. ``accept``: one transaction inserts the booking (CREATED), its CREATE command, and the
   idempotency record. A concurrent duplicate loses on the unique key and replays the winner.
2. ``submit_once``: journal the attempt (dispatch-marked, booking SUBMITTING) and commit;
   call the provider with no row locked; then record the outcome and apply the domain's
   decision in a second transaction. If that second transaction never happens (crash, database
   down), the journaled attempt is exactly what the reconciler expects to find.
3. ``outcome_for``: the response, initial or replayed, comes from the command's disposition.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any

from sqlalchemy.exc import IntegrityError

from orchestrator.application.ids import new_attempt_id, new_booking_id, new_command_id
from orchestrator.application.policy import RecoveryPolicy
from orchestrator.domain import (
    Attempt,
    AttemptOutcome,
    Bind,
    BookingId,
    BookingState,
    ClientId,
    Command,
    CommandKind,
    CreateIntent,
    Disposition,
    DispositionBasis,
    Escalate,
    Evidence,
    EvidenceKind,
    KeepLooking,
    NotClosable,
    ProviderRequest,
    ProviderResult,
    Rejected,
    Replay,
    Reschedule,
    Reservation,
    SideEffect,
    Trigger,
    Uncertain,
    anchor,
    bind,
    closable_into,
    decide_create_after_attempt,
    decide_exhausted,
    decide_late_result,
    dispatch_allowed,
    exclude_all_possible,
    record_attempt,
    replay_for,
    settle,
    transition,
    trigger_for,
    trigger_for_escalation,
    trigger_for_reschedule,
)
from orchestrator.domain.offers import Offer
from orchestrator.domain.settlement import REASON_EXHAUSTED
from orchestrator.persistence.bookings import (
    Booking,
    BookingStore,
    IdempotencyRecord,
    Lease,
    StaleLeaseError,
)
from orchestrator.persistence.uow import UnitOfWorkFactory
from orchestrator.providers import CreateBookingRequest, Passenger, ProviderAdapter, ProviderError
from orchestrator.providers.errors import ErrorKind
from orchestrator.providers.registry import ProviderRegistry
from orchestrator.resilience import (
    REASON_DEADLINE,
    AdmissionController,
    AttemptContext,
    NotDispatchedError,
    Purpose,
    RetryPolicy,
    RetryVerdict,
    Ticket,
    retry_verdict,
    run_attempts,
)
from orchestrator.telemetry import get_logger, metrics

log = get_logger(__name__)


class IdempotencyConflictError(Exception):
    """Same key, different request."""


@dataclass(frozen=True, slots=True)
class AttemptReport:
    """What one journaled attempt tells its caller: whether it reached the network, and
    whether the loop may try again (a failure that certainly had no effect)."""

    dispatched: bool
    retry: RetryVerdict | None


class _RetryableAttemptError(Exception):
    """Carries a journaled attempt's retry verdict through the attempt loop."""

    def __init__(self, verdict: RetryVerdict) -> None:
        super().__init__(verdict)
        self.verdict = verdict


def _verdict(exc: BaseException) -> RetryVerdict | None:
    if isinstance(exc, _RetryableAttemptError):
        return exc.verdict
    return retry_verdict(exc)


def _context_from_history(command: Command) -> AttemptContext:
    """Where the journal left off: the next attempt number, whether the last attempt was a
    dispatched failure (this retry pays), and whether it was a timeout (it pays more)."""
    if not command.attempts:
        return AttemptContext()
    # A local refusal between two dispatches does not erase the classification the last
    # *dispatched* attempt earned: scan back to it.
    sent = [a for a in command.attempts if a.effective_side_effect is not SideEffect.NOT_DISPATCHED]
    if not sent:
        return AttemptContext(n=len(command.attempts) + 1)
    last = sent[-1]
    dispatched_failure = last.outcome not in (AttemptOutcome.SUCCESS, None)
    return AttemptContext(
        n=len(command.attempts) + 1,
        after_timeout=last.error == ErrorKind.TIMEOUT.value,
        charged=dispatched_failure,
    )


# Kinds that say the provider is unhealthy, as opposed to answering "no" quickly.
_UNHEALTHY_KINDS = frozenset({ErrorKind.TIMEOUT, ErrorKind.TRANSIENT, ErrorKind.RATE_LIMITED})


@dataclass(frozen=True, slots=True)
class CreateRequest:
    client_id: ClientId
    idempotency_key: str
    offer: Offer
    passenger_names: tuple[str, ...]
    contact_email: str
    correlation_id: str | None = None

    @property
    def fingerprint(self) -> str:
        return fingerprint_for(self.offer.id, self.passenger_names, self.contact_email)


def fingerprint_for(offer_id: str, passenger_names: tuple[str, ...], contact_email: str) -> str:
    """The request identity an idempotency key is bound to. Needs no resolved offer, so a
    replay can be recognised even after the offer left the store (docs/api.md, idempotency)."""
    canonical = json.dumps(
        {
            "method": "POST",
            "path": "/v1/bookings",
            "offer_id": offer_id,
            "passengers": list(passenger_names),
            "contact_email": contact_email,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Outcome:
    booking: Booking
    command: Command
    replay: Replay
    replayed: bool


# A test hook: called between the provider call and the outcome transaction. Tests use it to
# crash the process at the one boundary the design promises to survive (edge case 10).
FailpointHook = Callable[[str], None]


class BookingCreator:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        registry: ProviderRegistry,
        policy: RecoveryPolicy,
        *,
        admission: AdmissionController,
        retry: RetryPolicy | None = None,
        failpoint: FailpointHook | None = None,
    ) -> None:
        self.uow = uow
        self.registry = registry
        self.policy = policy
        self.admission = admission
        self.retry = retry or RetryPolicy()
        self.failpoint = failpoint
        self.confirmer: Any = None  # set by wiring: confirms holds on the request path

    # Public entry points -----------------------------------------------------------------------

    async def create(self, request: CreateRequest) -> Outcome:
        accepted, replayed = await self.accept(request)
        if replayed:
            return await self.outcome_for(accepted, replayed=True)
        # If the worker took over mid-way, the state we report is whatever is committed. A
        # refusal by admission is journaled and the booking waits for the worker (202).
        with contextlib.suppress(StaleLeaseError, NotDispatchedError):
            await self.submit_once(accepted, lease=None, correlation_id=request.correlation_id)
        # A hold is confirmed immediately (6.3); if that fails, the confirmation loop owns it.
        async with self.uow() as store:
            booking = await store.get(accepted)
        if booking.state is BookingState.HELD and self.confirmer is not None:
            try:
                await self.confirmer.confirm(
                    booking, lease=None, correlation_id=request.correlation_id
                )
            except Exception:
                log.exception("request_path_confirm_failed", booking_id=accepted)
        return await self.outcome_for(accepted, replayed=False)

    async def find_replay(
        self, client_id: ClientId, idempotency_key: str, fingerprint: str
    ) -> Outcome | None:
        """The durable record decides a replay before anything else is consulted."""
        async with self.uow() as store:
            existing = await store.find_idempotency(client_id, idempotency_key)
            if existing is None:
                return None
            if existing.fingerprint != fingerprint:
                raise IdempotencyConflictError(idempotency_key)
            command = await store.command_by_id(existing.command_id)
        return await self.outcome_for(command.booking_id, replayed=True)

    async def accept(self, request: CreateRequest) -> tuple[BookingId, bool]:
        """Persist the intent once. Returns (booking id, replayed)."""
        async with self.uow() as store:
            existing = await store.find_idempotency(request.client_id, request.idempotency_key)
            if existing is not None:
                return await self._replayed(store, existing, request)
        booking_id = new_booking_id()
        command_id = new_command_id()
        now = datetime.now(UTC)
        booking = Booking(
            id=booking_id,
            client_id=request.client_id,
            provider=request.offer.provider,
            state=BookingState.CREATED,
            offer=request.offer,
            passenger_names=request.passenger_names,
            contact_email=request.contact_email,
            provider_booking_ref=None,
            unresolved_reason=None,
            failure_code=None,
            version=0,
            next_action_at=now,
            created_at=now,
            updated_at=now,
            lease_expires_at=None,
        )
        command = Command(
            id=command_id,
            booking_id=booking_id,
            kind=CommandKind.CREATE,
            intent=CreateIntent(
                request.offer.id,
                request.passenger_names,
                request.contact_email,
                product_ref=request.offer.provider_offer_ref,
                service_date=request.offer.trip.departure.date(),
            ),
            provider_key=str(booking_id),
            created_at=now,
        )
        record = IdempotencyRecord(
            request.client_id, request.idempotency_key, request.fingerprint, command_id
        )
        try:
            async with self.uow() as store:
                await store.insert_new(booking, command, record, next_action_at=now)
        except IntegrityError:
            async with self.uow() as store:
                winner = await store.find_idempotency(request.client_id, request.idempotency_key)
                if winner is None:
                    raise
                return await self._replayed(store, winner, request)
        return booking_id, False

    async def submit(
        self, booking: Booking, *, lease: Lease | None, correlation_id: str | None
    ) -> None:
        """The worker's attempt loop: journaled attempts until one is final, the lease runs
        out, or the retry budget is spent. The loop starts where the journal left off: attempt
        numbers, the retry charge and the timeout classification come from the persisted
        history, so a retry after a request-path failure or an earlier tick still pays."""
        deadline = self._deadline(lease)
        labels = {"provider": booking.provider, "purpose": Purpose.CREATE.value}
        async with self.uow() as store:
            command = await store.command_for(booking.id, CommandKind.CREATE)
        first = _context_from_history(command)

        async def attempt(context: AttemptContext) -> None:
            if context.n < first.n:
                context = first  # the loop's first call carries the persisted context
            report = await self.submit_once(
                booking.id, lease=lease, correlation_id=correlation_id, context=context
            )
            if report.retry is not None:
                raise _RetryableAttemptError(report.retry)

        try:
            await run_attempts(
                attempt,
                policy=self.retry,
                deadline=deadline,
                labels=labels,
                verdict=_verdict,
                first=first,
            )
        except (_RetryableAttemptError, NotDispatchedError):
            return  # journaled and rescheduled; the schedule decides when to try again

    async def submit_once(
        self,
        booking_id: BookingId,
        *,
        lease: Lease | None,
        correlation_id: str | None,
        context: AttemptContext | None = None,
    ) -> AttemptReport:
        """One journaled attempt under admission: admit, mark, call, record. Never retries.

        A refusal by admission is journaled as a ``NOT_DISPATCHED`` attempt (with its reason
        on the booking) and raised as ``NotDispatchedError`` so the caller's loop can decide.
        """
        context = context or AttemptContext()
        if self.failpoint is not None:
            self.failpoint("before_dispatch_mark")
        async with self.uow() as store:
            booking = await store.get(booking_id)
            command = await store.command_for(booking_id, CommandKind.CREATE)
        adapter = self.registry.get(booking.provider)
        now = datetime.now(UTC)
        if booking.state is not BookingState.CREATED:
            return AttemptReport(dispatched=False, retry=None)
        if not dispatch_allowed(adapter.capabilities, command, now=now):
            if (
                command.disposition is Disposition.OPEN
                and command.execution_cutoff is not None
                and now >= command.execution_cutoff
                and not command.possibly_executed
            ):
                # Dispatched, never with effect, and the provider may have forgotten the key:
                # nothing can be sent any more, and nothing proves the provider's state.
                await self._escalate_cutoff(booking_id, lease=lease, correlation_id=correlation_id)
            return AttemptReport(dispatched=False, retry=None)
        try:
            async with self.admission.admit(
                booking.provider,
                Purpose.CREATE,
                operation="create_booking",
                attempt_n=context.n,
                after_timeout=context.after_timeout,
                charged=context.charged,
                deadline=self._deadline(lease),
            ) as ticket:
                return await self._dispatch(
                    booking_id, ticket, lease=lease, correlation_id=correlation_id
                )
        except NotDispatchedError as refusal:
            if not refusal.journaled:
                await self._journal_refusal(
                    booking_id, refusal, lease=lease, correlation_id=correlation_id
                )
            raise

    async def _dispatch(
        self,
        booking_id: BookingId,
        ticket: Ticket,
        *,
        lease: Lease | None,
        correlation_id: str | None,
    ) -> AttemptReport:
        ticket.require_time()  # admission may have waited: never mark past the deadline
        # Transaction 1: journal the attempt before any network IO.
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_for(booking_id, CommandKind.CREATE)
            adapter = self.registry.get(booking.provider)
            if booking.state is not BookingState.CREATED or not dispatch_allowed(
                adapter.capabilities, command
            ):
                ticket.record(outcome="NOT_DISPATCHED", side_effect="NOT_DISPATCHED", healthy=True)
                return AttemptReport(dispatched=False, retry=None)
            now = datetime.now(UTC)
            if command.first_dispatch_at is None:
                # The first mark anchors the command: the cutoff is absolute from here on.
                command = anchor(
                    command,
                    first_dispatch_at=now,
                    cutoff=self.policy.expiry.cutoff(adapter.capabilities, now),
                )
            expiry = self.policy.expiry.attempt_expiry(now, command.execution_cutoff)
            attempt = Attempt(
                id=new_attempt_id(),
                command_id=command.id,
                n=len(command.attempts) + 1,
                request=ProviderRequest(payload=self._payload(booking), expiry=expiry),
                dispatch_marked_at=now,
            )
            command = record_attempt(command, attempt)
            await store.save_command(command)
            booking = await store.save_booking(
                booking,
                state=BookingState.SUBMITTING,
                trigger=Trigger.ATTEMPT_DISPATCHED,
                source="PLATFORM",
                correlation_id=correlation_id,
                clear_next_action=True,
                lease=lease,
            )

        if self.failpoint is not None:
            self.failpoint("after_dispatch_mark")
        # No row is locked while the provider is called. The deadline is checked once more,
        # right here: the transaction above may have taken the time that was left.
        if ticket.remaining() is not None and ticket.remaining() <= 0:  # type: ignore[operator]
            await self._finish_unsent(
                booking_id, attempt, lease=lease, correlation_id=correlation_id
            )
            raise NotDispatchedError(REASON_DEADLINE, journaled=True)
        result, error = await self._call_provider(adapter, booking, expiry=expiry)
        ticket.record(
            outcome=self._outcome(result).value,
            side_effect=result.side_effect.value,
            healthy=error is None or error.kind not in _UNHEALTHY_KINDS,
            timeout=error is not None and error.kind is ErrorKind.TIMEOUT,
        )
        if self.failpoint is not None:
            self.failpoint("after_provider_response")

        # Transaction 2: record the outcome and apply the decision.
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_for(booking_id, CommandKind.CREATE)
            finished = replace(
                attempt,
                finished_at=datetime.now(UTC),
                outcome=self._outcome(result),
                side_effect=result.side_effect,
                error=error.kind.value if error is not None else None,
            )
            command = record_attempt(command, finished)
            if command.is_settled:
                # A lookup settled the command while this call was in flight (ADR 007):
                # the attempt still closes with its outcome, and only a contradiction acts.
                await store.save_command(command)
                late = decide_late_result(command, result)
                if late is not None:
                    await self.apply(
                        store, booking, command, late, lease=lease, correlation_id=correlation_id
                    )
                return AttemptReport(dispatched=True, retry=None)
            decision = decide_create_after_attempt(
                adapter.capabilities, command, attempt.id, result
            )
            retry = retry_verdict(error) if error is not None else None
            if isinstance(decision, Reschedule) and retry is not None:
                # The provider's Retry-After is part of the schedule, durably: a later tick
                # honours it even if this loop runs out of lease or attempts first.
                decision = Reschedule(reason=None, retry_after=retry.retry_after)
            await self.apply(
                store, booking, command, decision, lease=lease, correlation_id=correlation_id
            )
        return AttemptReport(dispatched=True, retry=retry)

    async def _escalate_cutoff(
        self, booking_id: BookingId, *, lease: Lease | None, correlation_id: str | None
    ) -> None:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_for(booking_id, CommandKind.CREATE)
            if (
                booking.state is not BookingState.CREATED
                or command.disposition is not Disposition.OPEN
            ):
                return
            await self.apply(
                store,
                booking,
                command,
                Escalate("command-cutoff-reached", implicated=(), remediable=True),
                lease=lease,
                correlation_id=correlation_id,
                source="PLATFORM",
            )

    async def _finish_unsent(
        self,
        booking_id: BookingId,
        attempt: Attempt,
        *,
        lease: Lease | None,
        correlation_id: str | None,
    ) -> None:
        """A dispatch-marked attempt that was never sent: finished as NOT_DISPATCHED (the mark
        stays, the journal is honest about what happened after it) and rescheduled."""
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_for(booking_id, CommandKind.CREATE)
            unsent = replace(
                attempt,
                finished_at=datetime.now(UTC),
                outcome=AttemptOutcome.NOT_DISPATCHED,
                side_effect=SideEffect.NOT_DISPATCHED,
                error=REASON_DEADLINE,
            )
            command = record_attempt(command, unsent)
            # The journal entry is not lease-fenced: what happened to the attempt is a fact
            # whoever owns the row now. The booking's move back is fenced, and a lost lease
            # leaves it to recovery (a SUBMITTING booking without a possible effect returns
            # to CREATED there).
            await store.save_command(command)
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_for(booking_id, CommandKind.CREATE)
            adapter = self.registry.get(booking.provider)
            decision = decide_create_after_attempt(
                adapter.capabilities, command, unsent.id, ProviderResult(SideEffect.NOT_DISPATCHED)
            )
            if isinstance(decision, Reschedule):
                decision = Reschedule(reason=REASON_DEADLINE, retry_after=None)
            try:
                await self.apply(
                    store,
                    booking,
                    command,
                    decision,
                    lease=lease,
                    correlation_id=correlation_id,
                    source="ADMISSION",
                )
            except StaleLeaseError:
                log.info("unsent_attempt_lease_lost", booking_id=booking_id)

    async def _journal_refusal(
        self,
        booking_id: BookingId,
        refusal: NotDispatchedError,
        *,
        lease: Lease | None,
        correlation_id: str | None,
    ) -> None:
        """A local refusal is an attempt that certainly did nothing; the journal says so."""
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_for(booking_id, CommandKind.CREATE)
            if booking.state is not BookingState.CREATED or command.is_settled:
                return
            now = datetime.now(UTC)
            denied = Attempt(
                id=new_attempt_id(),
                command_id=command.id,
                n=len(command.attempts) + 1,
                request=ProviderRequest(payload=self._payload(booking), expiry=None),
                dispatch_marked_at=None,
                finished_at=now,
                outcome=AttemptOutcome.NOT_DISPATCHED,
                side_effect=SideEffect.NOT_DISPATCHED,
            )
            command = record_attempt(command, denied)
            adapter = self.registry.get(booking.provider)
            decision = decide_create_after_attempt(
                adapter.capabilities, command, denied.id, ProviderResult(SideEffect.NOT_DISPATCHED)
            )
            if isinstance(decision, Reschedule):
                decision = Reschedule(reason=refusal.reason, retry_after=refusal.retry_after)
            await self.apply(
                store,
                booking,
                command,
                decision,
                lease=lease,
                correlation_id=correlation_id,
                source="ADMISSION",
            )

    def _deadline(self, lease: Lease | None) -> float:
        """The monotonic instant after which this attempt must not start: the lease's end for
        a worker, one mutation timeout for the request path."""
        if lease is None:
            return monotonic() + self.policy.mutation_timeout.total_seconds()
        remaining = (lease.expires_at - datetime.now(UTC)).total_seconds()
        return monotonic() + max(remaining, 0.0)

    # Decision application (shared with the reconciler and the review service) ----------------

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
        actor: str | None = None,
    ) -> None:
        caps = self.registry.get(booking.provider).capabilities
        now = datetime.now(UTC)
        if booking.state is BookingState.NEEDS_REVIEW and not isinstance(decision, Bind | Escalate):
            # Under review, an attempt's outcome never moves the booking by itself (6.1): the
            # command records it (a definitive rejection settles the command, an uncertain
            # outcome stays on it as a possible effect), and the case's evidence decides.
            if isinstance(decision, Rejected):
                command = settle(command, Disposition.REJECTED, decision.basis, caps=caps)
                self._count(store, booking, command)
            await store.save_command(command)
            return
        match decision:
            case Bind(reservation=res, disposition=disp, basis=basis):
                trigger = trigger_for(booking.state, decision)
                closing_case: str | None = None
                if booking.state is BookingState.NEEDS_REVIEW:
                    # A booking under review leaves it only when the case's complete evidence
                    # set closes it (6.1): the new observation joins the evidence, and every
                    # implicated reservation must be affirmatively accounted for.
                    closing_case = await self._case_closable_by(
                        store,
                        booking,
                        command,
                        res,
                        expected=self._next(booking.state, trigger),
                        now=now,
                    )
                    if closing_case is None:
                        # The case stays open, but the attempt that produced this observation
                        # is finished and must be persisted as such (ADR 007).
                        await store.save_command(command)
                        return
                command = bind(command, res.ref)
                if caps.key_bound_before_execution:
                    # The reservation the key produced accounts for every attempt of it.
                    command = exclude_all_possible(command, at=now)
                if disp is not Disposition.OPEN:
                    command = settle(command, disp, basis, caps=caps)
                    self._count(store, booking, command)
                await store.save_command(command)
                next_state = self._next(booking.state, trigger)
                held = next_state is BookingState.HELD
                pending = next_state is BookingState.PENDING_PROVIDER
                if held and await store.open_command(booking.id, CommandKind.CONFIRM) is None:
                    from orchestrator.application.confirmation import Confirmer

                    await store.add_command(
                        Confirmer.new_confirm_command(booking, str(res.ref), now=now, n=1)
                    )
                await store.save_booking(
                    booking,
                    state=next_state,
                    trigger=trigger,
                    source=source,
                    actor=actor,
                    provider_booking_ref=res.ref,
                    next_action_at=(
                        now if held else now + self.policy.pending_poll if pending else None
                    ),
                    clear_next_action=not (held or pending),
                    clear_unresolved=True,
                    confirmation_deadline=res.valid_until if held else None,
                    confirm_budget_remaining=(
                        self.policy.confirm_budget
                        if held and booking.confirm_budget_remaining is None
                        else None
                    ),
                    provider_generation=res.generation,
                    last_revision=res.revision,
                    correlation_id=correlation_id,
                    lease=lease,
                )
                if closing_case not in (None, "no-case") and trigger is not None:
                    assert closing_case is not None
                    await store.close_review_case(
                        closing_case, resolution=trigger.value, actor=actor or source
                    )
            case Rejected(basis=basis):
                command = settle(command, Disposition.REJECTED, basis, caps=caps)
                self._count(store, booking, command)
                await store.save_command(command)
                await store.save_booking(
                    booking,
                    state=BookingState.FAILED,
                    trigger=Trigger.PROVIDER_REJECTED,
                    source=source,
                    failure_code="booking-rejected",
                    clear_next_action=True,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Uncertain():
                await store.save_command(command)
                trigger = (
                    None if booking.state is BookingState.UNKNOWN else Trigger.OUTCOME_UNCERTAIN
                )
                when = now + self.policy.reconcile_backoff
                if caps.key_bound_before_execution and command.execution_cutoff is not None:
                    when = now  # a resubmission is safe right away, inside the cutoff
                await store.save_booking(
                    booking,
                    state=BookingState.UNKNOWN,
                    trigger=trigger,
                    source=source,
                    unresolved_reason="provider-outcome-unknown",
                    next_action_at=when,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Reschedule(reason=reason, retry_after=retry_after):
                exhausted = decide_exhausted(command, max_attempts=self.policy.max_attempts)
                if exhausted is not None:
                    # The budget of attempts that certainly had no effect is spent: an
                    # operator decides when to stop waiting for this provider (6.2).
                    await self.apply(
                        store,
                        booking,
                        command,
                        exhausted,
                        lease=lease,
                        correlation_id=correlation_id,
                        source=source,
                        actor=actor,
                    )
                    return
                await store.save_command(command)
                trigger = trigger_for_reschedule(booking.state)
                backoff = self.policy.reschedule_backoff.total_seconds()
                wait = max(backoff, retry_after or 0.0)
                await store.save_booking(
                    booking,
                    state=BookingState.CREATED,
                    trigger=trigger,
                    source=source,
                    next_action_at=now + timedelta(seconds=wait),
                    unresolved_reason=reason,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case KeepLooking():
                await store.save_command(command)
                await store.save_booking(
                    booking,
                    next_action_at=now + self.policy.reconcile_backoff,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Escalate(reason=reason, implicated=implicated, remediable=remediable):
                if command.disposition is Disposition.OPEN:
                    basis = (
                        DispositionBasis.LOCAL
                        if reason == REASON_EXHAUSTED
                        else DispositionBasis.LOOKUP
                    )
                    command = replace(command, disposition=Disposition.UNRESOLVED, basis=basis)
                    self._count(store, booking, command)
                await store.save_command(command)
                await store.open_review_case(
                    booking.id,
                    case_id=new_command_id().replace("cmd_", "rev_"),
                    reason=reason,
                    remediable=remediable,
                    outstanding_command_id=command.id,
                    implicated=implicated,
                )
                trigger = trigger_for_escalation(booking.state)
                await store.save_booking(
                    booking,
                    state=BookingState.NEEDS_REVIEW,
                    trigger=trigger,
                    source=source,
                    unresolved_reason=reason,
                    clear_next_action=True,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case _:
                raise AssertionError(f"unexpected decision {decision!r}")

    # Helpers -------------------------------------------------------------------------------------

    async def _case_closable_by(
        self,
        store: BookingStore,
        booking: Booking,
        command: Command,
        reservation: Reservation,
        *,
        expected: BookingState,
        now: datetime,
    ) -> str | None:
        """Record the observation on the open case; return the case id if it now closes."""
        found = await store.review_case(booking.id)
        if found is None:
            return "no-case"  # under review without a case: nothing to keep open
        case, case_id = found
        observation = Evidence(EvidenceKind.OBSERVATION, now, reservation.ref, reservation)
        await store.add_evidence(case_id, new_command_id().replace("cmd_", "ev_"), observation)
        await store.extend_review_case(case_id, implicated=(reservation.ref,))
        case = replace(
            case,
            implicated=tuple(dict.fromkeys([*case.implicated, reservation.ref])),
            evidence=(*case.evidence, observation),
        )
        target = closable_into(case, reservation.ref, now=now, command=command)
        if isinstance(target, NotClosable):
            log.info("review_case_stays_open", booking_id=booking.id, reason=target.reason)
            return None
        if target is not expected:
            log.info(
                "review_case_stays_open",
                booking_id=booking.id,
                reason=f"evidence supports {target}, observation implies {expected}",
            )
            return None
        return case_id

    async def outcome_for(self, booking_id: BookingId, *, replayed: bool) -> Outcome:
        async with self.uow() as store:
            # A share lock: the command read next belongs to the same committed state.
            booking = await store.get(booking_id, for_share=True)
            command = await store.command_for(booking_id, CommandKind.CREATE)
        unresolved = command.disposition in (Disposition.OPEN, Disposition.UNRESOLVED)
        replay = replay_for(
            CommandKind.CREATE,
            command.disposition,
            booking_state=booking.state,
            unresolved_reason=booking.unresolved_reason if unresolved else None,
        )
        return Outcome(booking, command, replay, replayed)

    async def _replayed(
        self, store: BookingStore, existing: IdempotencyRecord, request: CreateRequest
    ) -> tuple[BookingId, bool]:
        if existing.fingerprint != request.fingerprint:
            raise IdempotencyConflictError(request.idempotency_key)
        command = await store.command_by_id(existing.command_id)
        return command.booking_id, True

    async def _call_provider(
        self, adapter: ProviderAdapter, booking: Booking, *, expiry: datetime | None = None
    ) -> tuple[ProviderResult, ProviderError | None]:
        """The call itself, under the mutation timeout. Returns the result and, for a
        failure, the provider error it came from (its kind decides retries and health)."""
        request = CreateBookingRequest(
            booking_id=booking.id,
            offer=booking.offer,
            passengers=tuple(Passenger(n) for n in booking.passenger_names),
            contact_email=booking.contact_email,
        )
        try:
            async with asyncio.timeout(self.policy.mutation_timeout.total_seconds()):
                reservation: Reservation = await adapter.create_booking(
                    request, key=str(booking.id), expiry=expiry
                )
        except ProviderError as exc:
            log.info(
                "provider_error",
                booking_id=booking.id,
                kind=exc.kind,
                side_effect=exc.side_effect,
            )
            return (
                ProviderResult(
                    exc.side_effect,
                    definitive_rejection=exc.definitive,
                    request_expired=exc.kind is ErrorKind.EXPIRED_REQUEST,
                ),
                exc,
            )
        except TimeoutError:
            log.info("provider_timeout", booking_id=booking.id)
            timeout = ProviderError(ErrorKind.TIMEOUT, SideEffect.POSSIBLE, "mutation timeout")
            return ProviderResult(SideEffect.POSSIBLE), timeout
        return ProviderResult(SideEffect.NONE, reservation=reservation), None

    @staticmethod
    def _outcome(result: ProviderResult) -> AttemptOutcome:
        if result.reservation is not None:
            return AttemptOutcome.SUCCESS
        if result.definitive_rejection:
            return AttemptOutcome.REJECTED
        if result.side_effect is SideEffect.NOT_DISPATCHED:
            return AttemptOutcome.NOT_DISPATCHED
        return AttemptOutcome.UNKNOWN

    @staticmethod
    def _count(store: BookingStore, booking: Booking, command: Command) -> None:
        """Counted once the transaction commits (ADR 014): a write a lease fence rolls
        back must not be reported as a settlement."""
        labels = {
            "provider": booking.provider,
            "kind": command.kind.value,
            "disposition": command.disposition.value,
            "basis": command.basis.value if command.basis else "none",
        }
        store.after_commit(lambda: metrics.booking_commands.add(1, labels))

    @staticmethod
    def _payload(booking: Booking) -> tuple[tuple[str, str], ...]:
        return (
            ("offer_ref", booking.offer.provider_offer_ref),
            ("client_ref", str(booking.id)),
            ("pax", str(booking.offer.passengers.total)),
        )

    @staticmethod
    def _next(state: BookingState, trigger: Trigger | None) -> BookingState:
        if trigger is None:
            return state
        nxt = transition(state, trigger)
        if isinstance(nxt, BookingState):
            return nxt
        raise AssertionError(f"{trigger} illegal from {state}")

"""Cancelling under authorised terms (docs/booking-state-machine.md; edge cases 27 to 36).

The client asks to cancel and states the terms it accepts (a maximum fee; none means only a
free cancellation). The platform obtains the provider's refund offer where one exists, checks
it against those terms, and accepts *that exact offer* under an attempt expiry; a provider
without refunds is cancelled directly, for free. Phases are durable (``NONE``, ``QUOTED``,
``ACCEPTING``) so a crash at any point resumes from the right one. Whoever runs a phase owns
the row through a lease (the request path claims one too), every write is fenced by it, and
the acceptance is revalidated under the booking's row lock right before dispatch: the phase,
the exact quote, no attempt that may still commit.

An acceptance whose answer was lost settles only by the exact offer's status as the provider
reports it (or, for free cancellation, by the reservation's state once the attempt is
excluded); an excluded attempt is settled durably so recovery does not repeat forever.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from sqlalchemy.exc import IntegrityError

from orchestrator.application.booking_create import BookingCreator, Outcome
from orchestrator.application.ids import new_attempt_id, new_command_id
from orchestrator.application.policy import RecoveryPolicy
from orchestrator.domain import (
    Attempt,
    AttemptOutcome,
    BookingId,
    BookingState,
    CancelPhase,
    ClientId,
    Command,
    CommandId,
    CommandKind,
    Disposition,
    DispositionBasis,
    Escalate,
    ProviderRequest,
    ProviderResult,
    RefundQuote,
    Reschedule,
    Reservation,
    SideEffect,
    Trigger,
    Uncertain,
    anchor,
    dispatch_allowed,
    exclude_all_possible,
    record_attempt,
    replay_for,
    settle,
)
from orchestrator.domain.cancellation import (
    REASON_REFUSED_EXPIRED,
    AcceptanceExcluded,
    CancelIntent,
    Cancelled,
    Refused,
    Requote,
    TermsChanged,
    check_terms,
    decide_cancel_after_acceptance,
    decide_cancel_after_lookup,
)
from orchestrator.persistence.bookings import (
    Booking,
    BookingStore,
    IdempotencyRecord,
    Lease,
    StaleLeaseError,
)
from orchestrator.persistence.codecs import quote_to_json
from orchestrator.persistence.uow import UnitOfWorkFactory
from orchestrator.providers import CapabilityNotSupportedError, ProviderAdapter, ProviderError
from orchestrator.providers.errors import ErrorKind
from orchestrator.providers.registry import ProviderRegistry
from orchestrator.resilience import (
    REASON_DEADLINE,
    AdmissionController,
    NotDispatchedError,
    Purpose,
)
from orchestrator.telemetry import get_logger

log = get_logger(__name__)


class NotCancellableError(Exception):
    """The booking is not in a state that can be cancelled, or the provider cannot cancel."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class CancelConflictError(Exception):
    """Same key, different request (or the key belongs to another booking)."""


@dataclass(frozen=True, slots=True)
class CancelRequest:
    booking_id: BookingId
    client_id: ClientId
    idempotency_key: str
    intent: CancelIntent
    correlation_id: str | None = None

    @property
    def fingerprint(self) -> str:
        canonical = json.dumps(
            {
                "method": "POST",
                "path": f"/v1/bookings/{self.booking_id}/cancel",
                "max_fee": (
                    [self.intent.max_fee.amount_minor, self.intent.max_fee.currency]
                    if self.intent.max_fee
                    else None
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


class Canceller:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        registry: ProviderRegistry,
        creator: BookingCreator,
        policy: RecoveryPolicy,
        *,
        admission: AdmissionController,
    ) -> None:
        self.uow = uow
        self.registry = registry
        self.creator = creator
        self.policy = policy
        self.admission = admission

    # Request path ------------------------------------------------------------------------------

    async def request(self, request: CancelRequest) -> Outcome:
        """Accept the cancellation once (idempotent), run it as far as it goes, and report."""
        command_id: str | None = None
        replayed = False
        async with self.uow() as store:
            existing = await store.find_idempotency(request.client_id, request.idempotency_key)
            if existing is not None:
                command_id = await self._replay_of(store, request)
                replayed = True
            else:
                booking = await store.get(request.booking_id, for_update=True)
                # Two identical requests raced for the key: the loser sees the winner's record
                # once it holds the row lock, and replays it instead of competing.
                existing = await store.find_idempotency(request.client_id, request.idempotency_key)
            if existing is not None:
                command_id = await self._replay_of(store, request)
                replayed = True
            else:
                caps = self.registry.get(booking.provider).capabilities
                if caps.cancellation.value == "NONE":
                    raise NotCancellableError("this provider does not support cancellation")
                if (
                    booking.state is not BookingState.CONFIRMED
                    or booking.provider_booking_ref is None
                ):
                    raise NotCancellableError(
                        f"a {booking.state.value} booking cannot be cancelled"
                    )
                now = datetime.now(UTC)
                command = Command(
                    id=new_command_id(),
                    booking_id=booking.id,
                    kind=CommandKind.CANCEL,
                    intent=request.intent,
                    provider_key=f"{booking.id}:cancel:{request.idempotency_key[:16]}",
                    created_at=now,
                    submission_ref=booking.provider_booking_ref,
                    phase=CancelPhase.NONE,
                )
                try:
                    async with store.savepoint():  # a violation leaves the transaction usable
                        await store.add_command(
                            command,
                            idempotency=IdempotencyRecord(
                                request.client_id,
                                request.idempotency_key,
                                request.fingerprint,
                                command.id,
                            ),
                        )
                except IntegrityError:
                    # A concurrent identical request won the key: this one replays it.
                    command_id = await self._replay_of(store, request)
                    replayed = True
                else:
                    command_id = command.id
                    await store.save_booking(
                        booking,
                        state=BookingState.CANCELLING,
                        trigger=Trigger.CANCEL_ACCEPTED,
                        source="CLIENT",
                        next_action_at=now,
                        correlation_id=request.correlation_id,
                    )
        assert command_id is not None
        if not replayed:
            with contextlib.suppress(StaleLeaseError, NotDispatchedError):
                await self.run(
                    request.booking_id, lease=None, correlation_id=request.correlation_id
                )
        return await self.outcome_for(request.booking_id, command_id, replayed=replayed)

    async def _replay_of(self, store: BookingStore, request: CancelRequest) -> str:
        record = await store.find_idempotency(request.client_id, request.idempotency_key)
        if record is None or record.fingerprint != request.fingerprint:
            raise CancelConflictError(request.idempotency_key)
        command = await store.command_by_id(record.command_id)
        if command.booking_id != request.booking_id:
            raise CancelConflictError(request.idempotency_key)
        return command.id

    async def outcome_for(
        self, booking_id: BookingId, command_id: str, *, replayed: bool
    ) -> Outcome:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_share=True)
            command = await store.command_by_id(CommandId(command_id))
        unresolved = command.disposition in (Disposition.OPEN, Disposition.UNRESOLVED)
        replay = replay_for(
            CommandKind.CANCEL,
            command.disposition,
            booking_state=booking.state,
            unresolved_reason=booking.unresolved_reason if unresolved else None,
        )
        return Outcome(booking, command, replay, replayed)

    # Phases --------------------------------------------------------------------------------------

    async def run(
        self, booking_id: BookingId, *, lease: Lease | None, correlation_id: str | None
    ) -> None:
        """Advance the cancellation through its phases as far as one pass can. Without a
        lease (the request path) the row is claimed first; a worker that owns it wins."""
        owned = lease is None
        if owned:
            async with self.uow() as store:
                lease = await store.claim_one(booking_id, ttl=self.policy.lease_ttl)
            if lease is None:
                return
        assert lease is not None
        try:
            await self._phases(booking_id, lease=lease, correlation_id=correlation_id)
        finally:
            if owned:
                with contextlib.suppress(Exception):
                    async with self.uow() as store:
                        await store.release(lease)

    async def _phases(
        self, booking_id: BookingId, *, lease: Lease, correlation_id: str | None
    ) -> None:
        for _ in range(3):  # quote, accept, and one re-quote at most per pass
            async with self.uow() as store:
                booking = await store.get(booking_id)
                command = await store.open_command(booking_id, CommandKind.CANCEL)
            if booking.state is not BookingState.CANCELLING or command is None:
                return
            if command.possibly_executed:
                await self.recover(booking_id, lease=lease)
                return
            caps = self.registry.get(booking.provider).capabilities
            if command.phase in (None, CancelPhase.NONE):
                if not caps.supports_refund:
                    # Free cancellation: nothing to quote. The phase still advances under the
                    # row lock, on a fresh read, fenced by the lease like every other write.
                    async with self.uow() as store:
                        locked = await store.get(booking_id, for_update=True)
                        lease = await store.renew(lease, ttl=self.policy.lease_ttl)
                        current = await store.command_by_id(command.id)
                        if (
                            locked.state is BookingState.CANCELLING
                            and current.disposition is Disposition.OPEN
                            and current.phase in (None, CancelPhase.NONE)
                        ):
                            await store.save_command(replace(current, phase=CancelPhase.QUOTED))
                            await store.save_booking(locked, lease=lease)
                    continue
                if not await self._quote(
                    booking, command, lease=lease, correlation_id=correlation_id
                ):
                    return
                continue
            if command.phase is CancelPhase.QUOTED:
                quote = command.quote
                if isinstance(quote, RefundQuote) and datetime.now(UTC) >= quote.valid_until:
                    await self._requote(booking_id, lease=lease)
                    continue
                await self._accept(booking, command, lease=lease, correlation_id=correlation_id)
                return
            if command.phase is CancelPhase.ACCEPTING:
                await self.recover(booking_id, lease=lease)
                return

    async def _quote(
        self, booking: Booking, command: Command, *, lease: Lease, correlation_id: str | None
    ) -> bool:
        adapter = self.registry.get(booking.provider)
        assert booking.provider_booking_ref is not None
        try:
            async with self.admission.admit(
                booking.provider, Purpose.CANCEL, operation="quote_cancellation"
            ) as ticket:
                try:
                    quote = await adapter.quote_cancellation(booking.provider_booking_ref)
                except ProviderError as exc:
                    ticket.record_provider_error(exc)
                    raise
                ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
        except CapabilityNotSupportedError:
            await self._settle(
                booking.id,
                Refused("cancellation-unavailable"),
                lease=lease,
                correlation_id=correlation_id,
            )
            return False
        except ProviderError as exc:
            if exc.definitive:
                await self._settle(
                    booking.id, Refused(exc.kind.value), lease=lease, correlation_id=correlation_id
                )
            else:
                await self._backoff(booking.id, lease=lease, reason=exc.kind.value)
            return False
        except NotDispatchedError as exc:
            await self._backoff(
                booking.id, lease=lease, reason=exc.reason, retry_after=exc.retry_after
            )
            return False
        intent = command.intent
        assert isinstance(intent, CancelIntent)
        worse = check_terms(intent, quote)
        async with self.uow() as store:
            booking = await store.get(booking.id, for_update=True)
            command = await store.command_by_id(command.id)
            if (
                command.phase not in (None, CancelPhase.NONE)
                or command.disposition is not Disposition.OPEN
            ):
                return False  # someone else advanced it: their quote stands
            if worse is not None:
                command = replace(command, phase=CancelPhase.QUOTED, quote=quote)
                command = settle(
                    command,
                    Disposition.TERMS_CHANGED,
                    DispositionBasis.PROVIDER_RESULT,
                    caps=adapter.capabilities,
                )
                await store.save_command(command)
                self.creator._count(store, booking, command)
                await store.save_booking(
                    booking,
                    state=BookingState.CONFIRMED,
                    trigger=Trigger.CANCEL_REFUSED,
                    source="PROVIDER_RESPONSE",
                    refund={"quote": quote_to_json(quote), "status": "TERMS_CHANGED"},
                    clear_next_action=True,
                    correlation_id=correlation_id,
                    lease=lease,
                )
                return False
            await store.save_command(replace(command, phase=CancelPhase.QUOTED, quote=quote))
            await store.save_booking(
                booking,
                refund={"quote": quote_to_json(quote), "status": "QUOTED"},
                next_action_at=datetime.now(UTC),
                correlation_id=correlation_id,
                lease=lease,
            )
        return True

    async def _requote(self, booking_id: BookingId, *, lease: Lease) -> None:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.open_command(booking_id, CommandKind.CANCEL)
            if command is None or command.possibly_executed:
                return
            await store.save_command(replace(command, phase=CancelPhase.NONE))
            await store.save_booking(booking, next_action_at=datetime.now(UTC), lease=lease)

    async def _accept(
        self, booking: Booking, command: Command, *, lease: Lease, correlation_id: str | None
    ) -> None:
        adapter = self.registry.get(booking.provider)
        caps = adapter.capabilities
        quote = command.quote if isinstance(command.quote, RefundQuote) else None
        assert booking.provider_booking_ref is not None
        attempt: Attempt | None = None
        try:
            async with self.admission.admit(
                booking.provider,
                Purpose.CANCEL,
                operation="cancel_booking",
                deadline=self.creator._deadline(lease),
            ) as ticket:
                ticket.require_time()
                # Transaction 1: under the row lock, revalidate ownership and the exact phase and
                # quote; journal the acceptance with its expiry before any IO.
                async with self.uow() as store:
                    current = await store.get(booking.id, for_update=True)
                    command = await store.command_by_id(command.id)
                    lease = await store.renew(lease, ttl=self.policy.lease_ttl)
                    now = datetime.now(UTC)
                    same_quote = (
                        (command.quote == quote) if quote is not None else command.quote is None
                    )
                    if (
                        current.state is not BookingState.CANCELLING
                        or command.phase is not CancelPhase.QUOTED
                        or not same_quote
                        or command.possibly_executed
                        or not dispatch_allowed(caps, command, now=now)
                    ):
                        ticket.record(
                            outcome="NOT_DISPATCHED", side_effect="NOT_DISPATCHED", healthy=True
                        )
                        if (
                            command.execution_cutoff is not None
                            and now >= command.execution_cutoff
                            and command.disposition is Disposition.OPEN
                        ):
                            await self.apply(
                                store,
                                current,
                                command,
                                Refused(REASON_REFUSED_EXPIRED),
                                lease=lease,
                                correlation_id=correlation_id,
                            )
                        return
                    if command.first_dispatch_at is None:
                        command = anchor(
                            command,
                            first_dispatch_at=now,
                            cutoff=self.policy.expiry.cutoff(caps, now),
                        )
                    expiry = self.policy.expiry.attempt_expiry(
                        now, command.execution_cutoff, acceptance=True
                    )
                    if expiry is None:
                        expiry = now + self.policy.expiry.acceptance_ttl
                    if quote is not None:
                        expiry = min(expiry, quote.valid_until)
                    attempt = Attempt(
                        id=new_attempt_id(),
                        command_id=command.id,
                        n=len(command.attempts) + 1,
                        request=ProviderRequest(
                            payload=(("refund_offer", quote.offer_id if quote else "free"),),
                            expiry=expiry,
                        ),
                        dispatch_marked_at=now,
                    )
                    command = replace(record_attempt(command, attempt), phase=CancelPhase.ACCEPTING)
                    await store.save_command(command)
                remaining = ticket.remaining()
                if (remaining is not None and remaining <= 0) or datetime.now(UTC) >= expiry:
                    await self._finish_unsent(booking.id, command.id, attempt, lease=lease)
                    raise NotDispatchedError(REASON_DEADLINE, journaled=True)
                result, error = await self._call(
                    adapter, booking.provider_booking_ref, quote, expiry
                )
                if error is not None:
                    ticket.record_provider_error(error)
                else:
                    ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
        except NotDispatchedError as exc:
            if not exc.journaled:
                await self._backoff(
                    booking.id, lease=lease, reason=exc.reason, retry_after=exc.retry_after
                )
            return
        assert attempt is not None
        async with self.uow() as store:
            current = await store.get(booking.id, for_update=True)
            command = await store.command_by_id(command.id)
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
                return
            decision = decide_cancel_after_acceptance(
                command,
                attempt.id,
                result,
                bound=str(current.provider_booking_ref),
                quote=quote,
            )
            await self.apply(
                store, current, command, decision, lease=lease, correlation_id=correlation_id
            )

    async def _finish_unsent(
        self, booking_id: BookingId, command_id: str, attempt: Attempt, *, lease: Lease
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
            await store.save_command(
                replace(record_attempt(command, unsent), phase=CancelPhase.QUOTED)
            )
        await self._backoff(booking_id, lease=lease, reason=REASON_DEADLINE)

    async def _call(
        self, adapter: ProviderAdapter, ref: str, quote: RefundQuote | None, expiry: datetime
    ) -> tuple[ProviderResult, ProviderError | None]:
        try:
            async with asyncio.timeout(self.policy.mutation_timeout.total_seconds()):
                reservation: Reservation = await adapter.cancel_booking(ref, quote, expiry)  # type: ignore[arg-type]
        except ProviderError as exc:
            return ProviderResult(exc.side_effect, definitive_rejection=exc.definitive), exc
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

    # Recovery -----------------------------------------------------------------------------------

    async def recover(self, booking_id: BookingId, *, lease: Lease | None) -> None:
        """Settle an acceptance whose answer was lost by the exact offer's status."""
        async with self.uow() as store:
            booking = await store.get(booking_id)
            command = await store.open_command(booking_id, CommandKind.CANCEL)
        if command is None or booking.provider_booking_ref is None:
            return
        quote = command.quote if isinstance(command.quote, RefundQuote) else None
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
            await self._backoff(booking_id, lease=lease, reason=str(exc))
            return
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.command_by_id(command.id)
            decision = decide_cancel_after_lookup(
                adapter.capabilities,
                command,
                reservation,
                bound=str(booking.provider_booking_ref),
                quote=quote,
                now=datetime.now(UTC),
                policy=self.policy.expiry,
            )
            await self.apply(
                store, booking, command, decision, lease=lease, correlation_id=None, source="POLL"
            )

    # Applying decisions ---------------------------------------------------------------------------

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
        if booking.state is BookingState.NEEDS_REVIEW and not isinstance(decision, Escalate):
            # Under review the booking moves only through the case's evidence (6.1); a
            # settled cancellation is recorded on its command, the case closes into CANCELLED
            # through the review service.
            if isinstance(decision, Cancelled):
                final = (
                    DispositionBasis.FENCED_LOOKUP
                    if decision.basis is DispositionBasis.LOOKUP
                    else decision.basis
                )
                command = settle(
                    exclude_all_possible(command, at=now), Disposition.SUCCEEDED, final, caps=caps
                )
                self.creator._count(store, booking, command)
            elif isinstance(decision, Refused | TermsChanged):
                disposition = (
                    Disposition.REFUSED
                    if isinstance(decision, Refused)
                    else Disposition.TERMS_CHANGED
                )
                command = settle(
                    exclude_all_possible(command, at=now),
                    disposition,
                    DispositionBasis.PROVIDER_RESULT,
                    caps=caps,
                )
                self.creator._count(store, booking, command)
            await store.save_command(command)
            return
        match decision:
            case Cancelled(reservation=res, basis=basis):
                final = (
                    DispositionBasis.FENCED_LOOKUP if basis is DispositionBasis.LOOKUP else basis
                )
                command = settle(
                    exclude_all_possible(command, at=now), Disposition.SUCCEEDED, final, caps=caps
                )
                await store.save_command(command)
                self.creator._count(store, booking, command)
                await store.save_booking(
                    booking,
                    state=BookingState.CANCELLED,
                    trigger=Trigger.PROVIDER_CANCELLED,
                    source=source,
                    refund={"quote": quote_to_json(command.quote), "status": "CONFIRMED"},
                    provider_generation=res.generation,
                    last_revision=res.revision,
                    clear_next_action=True,
                    clear_unresolved=True,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case AcceptanceExcluded():
                # Durable: the attempt's possible effect is settled as excluded.
                await store.save_command(
                    replace(exclude_all_possible(command, at=now), phase=CancelPhase.QUOTED)
                )
                await store.save_booking(
                    booking,
                    next_action_at=now,
                    clear_unresolved=True,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Requote():
                await store.save_command(
                    replace(exclude_all_possible(command, at=now), phase=CancelPhase.NONE)
                )
                await store.save_booking(
                    booking,
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
                await store.save_booking(
                    booking,
                    unresolved_reason="cancellation-outcome-unknown",
                    next_action_at=when,
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Reschedule(reason=reason, retry_after=retry_after):
                await store.save_command(replace(command, phase=CancelPhase.QUOTED))
                wait = max(self.policy.reschedule_backoff.total_seconds(), retry_after or 0.0)
                await store.save_booking(
                    booking,
                    unresolved_reason=reason,
                    next_action_at=now + timedelta(seconds=wait),
                    correlation_id=correlation_id,
                    lease=lease,
                )
            case Refused() | TermsChanged():
                await self._settle_in(
                    store,
                    booking,
                    command,
                    decision,
                    lease=lease,
                    correlation_id=correlation_id,
                    source=source,
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
                raise AssertionError(f"unexpected cancel decision {decision!r}")

    async def _settle(
        self,
        booking_id: BookingId,
        decision: Refused | TermsChanged,
        *,
        lease: Lease | None,
        correlation_id: str | None,
    ) -> None:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            command = await store.open_command(booking_id, CommandKind.CANCEL)
            if command is None:
                return
            await self._settle_in(
                store,
                booking,
                command,
                decision,
                lease=lease,
                correlation_id=correlation_id,
                source="PROVIDER_RESPONSE",
            )

    async def _settle_in(
        self,
        store: BookingStore,
        booking: Booking,
        command: Command,
        decision: Refused | TermsChanged,
        *,
        lease: Lease | None,
        correlation_id: str | None,
        source: str,
    ) -> None:
        caps = self.registry.get(booking.provider).capabilities
        disposition = (
            Disposition.REFUSED if isinstance(decision, Refused) else Disposition.TERMS_CHANGED
        )
        command = settle(
            exclude_all_possible(command, at=datetime.now(UTC)),
            disposition,
            DispositionBasis.PROVIDER_RESULT,
            caps=caps,
        )
        await store.save_command(command)
        self.creator._count(store, booking, command)
        refund: dict[str, object] = {"status": disposition.value}
        if isinstance(decision, TermsChanged):
            refund["quote"] = quote_to_json(decision.quote)
        await store.save_booking(
            booking,
            state=BookingState.CONFIRMED,
            trigger=Trigger.CANCEL_REFUSED,
            source=source,
            failure_code=decision.reason if isinstance(decision, Refused) else None,
            refund=refund,
            clear_next_action=True,
            clear_unresolved=True,
            correlation_id=correlation_id,
            lease=lease,
        )

    async def _backoff(
        self,
        booking_id: BookingId,
        *,
        lease: Lease | None,
        reason: str,
        retry_after: float | None = None,
    ) -> None:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            wait = max(self.policy.reschedule_backoff.total_seconds(), retry_after or 0.0)
            await store.save_booking(
                booking,
                unresolved_reason=reason[:64],
                next_action_at=datetime.now(UTC) + timedelta(seconds=wait),
                lease=lease,
            )

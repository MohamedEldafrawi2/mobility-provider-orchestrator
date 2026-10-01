"""CREATE settlement for a provider that cannot supply finality (docs/booking-state-machine.md, the
Provider B slice).

Pure decision functions. The application layer applies a decision (bind, transition,
disposition, scheduling); this module only says what the evidence permits.

Precedence:

1. A reservation returned or discovered for this booking binds the reference and adopts its
   state. The CREATE command settles ``SUCCEEDED`` only when the reservation is confirmed (or
   already cancelled: the purchase happened); a held or pending one keeps it ``OPEN``; a
   definitively failed one settles ``REJECTED``.
2. A definitive rejection settles ``REJECTED`` immediately, but only if no *other* attempt could
   have executed. The rejected attempt itself is, by that answer, known to have had no effect.
3. Any remaining possible effect leaves the command unsettled. Without a fenced lookup it can
   only be settled positively or escalated to review. Never ``FAILED``, and never another
   mutating dispatch.
4. A failure that certainly had no effect (local denial, connection refused, rate limited, an
   edge rejection) with no possible effect anywhere is not uncertainty: reschedule, or abandon
   after a bounded age if nothing was ever dispatched.
5. Evidence that contradicts identity (a reservation for another booking, a second reference,
   a reservation for a booking already settled) always escalates.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from enum import StrEnum

from orchestrator.domain.capabilities import ProviderCapabilities
from orchestrator.domain.commands import (
    AttemptOutcome,
    Command,
    CommandKind,
    CreateIntent,
    Disposition,
    DispositionBasis,
    InvariantError,
    SideEffect,
)
from orchestrator.domain.ids import AttemptId, BookingId, ProviderBookingRef
from orchestrator.domain.refunds import RefundOfferStatus
from orchestrator.domain.states import TERMINAL_STATES, BookingState, Trigger
from orchestrator.domain.time import require_aware


class ReservationState(StrEnum):
    CONFIRMED = "CONFIRMED"
    HELD = "HELD"
    PENDING = "PENDING"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


LIVE_RESERVATION_STATES: frozenset[ReservationState] = frozenset(
    {ReservationState.CONFIRMED, ReservationState.HELD, ReservationState.PENDING}
)


@dataclass(frozen=True, slots=True)
class Reservation:
    """A provider-side reservation as observed by the platform."""

    ref: ProviderBookingRef
    client_ref: BookingId
    state: ReservationState
    observed_at: datetime
    product_ref: str | None = None  # the provider's product (journey) the reservation is for
    service_date: date | None = None
    generation: int | None = None  # reported by the provider, never inferred (section 4)
    revision: int | None = None  # the provider's version within the generation
    valid_until: datetime | None = None  # a hold's deadline: time-limited evidence
    refund_offers: tuple[RefundOfferStatus, ...] = ()

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "Reservation.observed_at")
        require_aware(self.valid_until, "Reservation.valid_until")


@dataclass(frozen=True, slots=True)
class ProviderResult:
    """What one attempt's provider call produced, after adapter translation."""

    side_effect: SideEffect
    reservation: Reservation | None = None
    definitive_rejection: bool = False
    hold_expired: bool = False  # the definitive rejection was "the hold has expired"
    request_expired: bool = False  # the definitive rejection was "executeBefore passed"

    def __post_init__(self) -> None:
        if self.reservation is not None and self.side_effect is not SideEffect.NONE:
            raise ValueError("a returned reservation is a certain effect: side_effect must be NONE")
        if self.definitive_rejection and self.side_effect is not SideEffect.NONE:
            raise ValueError("a definitive rejection has no side effect")


# Decisions ---------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Bind:
    """Bind the reservation, apply ``trigger`` to the booking, set the command disposition."""

    reservation: Reservation
    trigger: Trigger
    disposition: Disposition
    basis: DispositionBasis


@dataclass(frozen=True, slots=True)
class Rejected:
    basis: DispositionBasis


@dataclass(frozen=True, slots=True)
class Uncertain:
    """The command may have executed; the booking goes to UNKNOWN and the reconciler owns it."""


@dataclass(frozen=True, slots=True)
class Reschedule:
    """Nothing reached the provider and nothing could have; try again later.

    ``reason`` names a local refusal (bulkhead, breaker, quota, quota outage) for the booking's
    ``unresolved_reason``; ``retry_after`` is the refusing resource's advice in seconds.
    """

    reason: str | None = None
    retry_after: float | None = None


@dataclass(frozen=True, slots=True)
class KeepLooking:
    lookups_so_far: int


@dataclass(frozen=True, slots=True)
class Escalate:
    reason: str
    implicated: tuple[ProviderBookingRef, ...] = ()
    remediable: bool = False


@dataclass(frozen=True, slots=True)
class Abandon:
    """Never dispatched and past the age limit: ABANDONED with LOCAL basis."""


Decision = Bind | Rejected | Uncertain | Reschedule | KeepLooking | Escalate | Abandon

REASON_CANNOT_SETTLE = "provider-cannot-settle"
REASON_DUPLICATE = "duplicate-reservations"
REASON_IDENTITY = "reservation-identity-mismatch"
REASON_SETTLED_CONTRADICTED = "settled-command-contradicted"
REASON_UNEXPECTED = "reservation-without-dispatch"
REASON_EXHAUSTED = "provider-unavailable"


def identity_mismatch(command: Command, reservation: Reservation) -> str | None:
    """Why ``reservation`` is not verifiably the one ``command`` asked for, or None.

    The provider must echo our reference, and, when the command expects them, the product and
    the service date. A reservation for another product or day that carries our reference is
    not ours; one that does not say which product or day it is for cannot be verified.
    """
    if reservation.client_ref != command.booking_id:
        return "client reference differs"
    intent = command.intent
    if not isinstance(intent, CreateIntent):
        return None  # only a create names a product and a day to verify
    if intent.product_ref is not None and reservation.product_ref != intent.product_ref:
        return f"product {reservation.product_ref!r} is not {intent.product_ref!r}"
    if intent.service_date is not None and reservation.service_date != intent.service_date:
        return f"service date {reservation.service_date} is not {intent.service_date}"
    return None


def _bind_for(command: Command, reservation: Reservation, basis: DispositionBasis) -> Decision:
    """Validate identity, then map the reservation's state to a decision."""
    if reservation.client_ref != command.booking_id:
        return Escalate(REASON_IDENTITY, implicated=_refs(command, reservation.ref))
    if command.submission_ref is not None and command.submission_ref != reservation.ref:
        return Escalate(REASON_DUPLICATE, implicated=_refs(command, reservation.ref))
    if command.is_settled and command.disposition is not Disposition.SUCCEEDED:
        # A reservation for a command we had settled negatively: our record was wrong.
        return Escalate(REASON_SETTLED_CONTRADICTED, implicated=_refs(command, reservation.ref))
    if identity_mismatch(command, reservation) is not None:
        return Escalate(REASON_IDENTITY, implicated=_refs(command, reservation.ref))
    match reservation.state:
        case ReservationState.CONFIRMED:
            return Bind(reservation, Trigger.PROVIDER_CONFIRMED, Disposition.SUCCEEDED, basis)
        case ReservationState.HELD:
            return Bind(reservation, Trigger.PROVIDER_HELD, Disposition.OPEN, basis)
        case ReservationState.PENDING:
            return Bind(reservation, Trigger.PROVIDER_PENDING, Disposition.OPEN, basis)
        case ReservationState.FAILED:
            return Bind(reservation, Trigger.PROVIDER_REJECTED, Disposition.REJECTED, basis)
        case ReservationState.CANCELLED:
            return Bind(reservation, Trigger.PROVIDER_CANCELLED, Disposition.SUCCEEDED, basis)


def _could_have_created(command: Command) -> bool:
    """Did any attempt possibly or certainly create a reservation?"""
    return command.possibly_executed or any(
        a.outcome is AttemptOutcome.SUCCESS for a in command.attempts
    )


def _refs(command: Command, *found: ProviderBookingRef) -> tuple[ProviderBookingRef, ...]:
    refs: list[ProviderBookingRef] = []
    if command.submission_ref is not None:
        refs.append(command.submission_ref)
    refs.extend(r for r in found if r not in refs)
    return tuple(refs)


def decide_create_after_attempt(
    caps: ProviderCapabilities, command: Command, attempt_id: AttemptId, result: ProviderResult
) -> Decision:
    """Decide from one attempt's result. ``command`` must already include the finished attempt."""
    _require_create(command)
    if result.reservation is not None:
        return _bind_for(command, result.reservation, DispositionBasis.PROVIDER_RESULT)
    if result.definitive_rejection:
        if command.other_attempts_possibly_executed(attempt_id):
            # Another attempt may have executed; this rejection settles only itself.
            return Uncertain()
        if result.request_expired:
            # Our request arrived too late to execute; nothing was created by it. With time
            # left before the cutoff, another attempt may go (RESUBMIT providers).
            return Reschedule()
        return Rejected(DispositionBasis.PROVIDER_RESULT)
    if result.side_effect in (SideEffect.NOT_DISPATCHED, SideEffect.NONE):
        # Nothing happened in this attempt (local denial, connection refused, rate limited,
        # edge rejection). A fresh try is safe only if nothing anywhere could have executed.
        return Uncertain() if command.possibly_executed else Reschedule()
    # A possible effect is uncertainty for every provider. How it is resolved differs: review
    # providers look up by client reference; RESUBMIT providers resubmit inside the cutoff and
    # settle by a fenced lookup (settlement_fenced).
    return Uncertain()


def decide_create_after_lookup(
    caps: ProviderCapabilities,
    command: Command,
    reservations: tuple[Reservation, ...],
    *,
    lookup_budget: int,
) -> Decision:
    """Decide from a lookup by client reference. ``command.lookups_performed`` counts this one."""
    _require_create(command)
    # A plain lookup by client reference settles positively only, for every provider; negative
    # settlement needs the fenced lookup (settlement_fenced), never this one.
    matching = tuple(r for r in reservations if r.client_ref == command.booking_id)
    if len(matching) > 1:
        return Escalate(REASON_DUPLICATE, implicated=_refs(command, *(r.ref for r in matching)))
    if len(matching) == 1:
        if command.is_settled:
            return _bind_for(command, matching[0], DispositionBasis.LOOKUP)  # contradiction
        if not _could_have_created(command):
            # A reservation carrying our reference although no attempt could have made it: not
            # ours to adopt on trust (a collision, a replay, an operator action). Review decides.
            return Escalate(REASON_UNEXPECTED, implicated=_refs(command, matching[0].ref))
        decision = _bind_for(command, matching[0], DispositionBasis.LOOKUP)
        if (
            isinstance(decision, Bind)
            and decision.disposition is Disposition.REJECTED
            and caps.key_bound_before_execution
            and caps.finality_lookup
        ):
            # The key is bound before execution and the provider offers finality: the one
            # reservation the key produced is terminal, and no other can ever carry the key.
            # The read is as final as a fence; a plain lookup alone would settle nothing.
            decision = replace(decision, basis=DispositionBasis.FENCED_LOOKUP)
        return decision
    if not command.possibly_executed:
        return Reschedule()
    if command.lookups_performed < lookup_budget:
        return KeepLooking(command.lookups_performed)
    return Escalate(REASON_CANNOT_SETTLE, implicated=_refs(command), remediable=False)


def decide_exhausted(command: Command, *, max_attempts: int) -> Decision | None:
    """Stop retrying a create once ``max_attempts`` were made and none could have executed.

    Every attempt of such a command is journaled as ``NOT_DISPATCHED`` or ``NONE``, so the
    platform is not guessing; but attempts *were* dispatched, so the command is not
    ``ABANDONED`` (that disposition means proven zero dispatch, section 4). It escalates to a
    remediable review case instead: the provider is unavailable, an operator decides when to
    stop waiting. A command with one ``POSSIBLE`` attempt never reaches this path.
    """
    _require_create(command)
    if command.disposition is not Disposition.OPEN or command.possibly_executed:
        return None
    # Only attempts that reached the network count: a local refusal says nothing about the
    # provider, and a command nothing ever dispatched is abandoned by age instead (6.6 #15).
    dispatched = sum(1 for a in command.attempts if a.dispatch_marked_at is not None)
    if dispatched < max_attempts:
        return None
    return Escalate(REASON_EXHAUSTED, implicated=_refs(command), remediable=True)


def decide_late_result(command: Command, result: ProviderResult) -> Decision | None:
    """A response that arrived after other evidence settled the command (ADR 007).

    The attempt is closed with its outcome by the caller; this decides whether the outcome
    contradicts the settlement. Agreement with the bound reservation is a no-op.
    """
    _require_create(command)
    if not command.is_settled:
        raise InvariantError("decide_late_result is for settled commands")
    if result.reservation is not None:
        if (
            result.reservation.client_ref == command.booking_id
            and result.reservation.ref == command.submission_ref
        ):
            return None
        return _bind_for(command, result.reservation, DispositionBasis.PROVIDER_RESULT)
    if result.definitive_rejection and command.disposition is Disposition.SUCCEEDED:
        return Escalate(REASON_SETTLED_CONTRADICTED, implicated=_refs(command))
    return None


def decide_abandon(command: Command, *, now: datetime, max_age: timedelta) -> Decision | None:
    """Abandon a create that never reached a provider once it is older than ``max_age``."""
    _require_create(command)
    require_aware(now, "now")
    if command.disposition is not Disposition.OPEN or command.ever_dispatched:
        return None
    if now - command.created_at < max_age:
        return None
    return Abandon()


_TARGET_STATE: dict[Trigger, BookingState] = {
    Trigger.PROVIDER_CONFIRMED: BookingState.CONFIRMED,
    Trigger.PROVIDER_HELD: BookingState.HELD,
    Trigger.PROVIDER_PENDING: BookingState.PENDING_PROVIDER,
    Trigger.PROVIDER_REJECTED: BookingState.FAILED,
    Trigger.PROVIDER_CANCELLED: BookingState.CANCELLED,
}


def trigger_for(state: BookingState, decision: Bind) -> Trigger | None:
    """The booking trigger a Bind implies from the current state, or None for a no-op.

    Re-observing the bound reservation in the state the booking already holds changes
    nothing. A reservation discovered for a booking in a terminal state that says otherwise is
    contradictory evidence: it reopens the booking to review rather than applying the ordinary
    trigger.
    """
    if _TARGET_STATE[decision.trigger] is state:
        return None
    if state in TERMINAL_STATES:
        return Trigger.VERIFIED_RESERVATION_ON_TERMINAL
    return decision.trigger


def trigger_for_reschedule(state: BookingState) -> Trigger | None:
    """A local admission rejection only moves a booking back if it had been dispatch-marked.

    Denied before the mark, the booking never left CREATED (or HELD); there is nothing to undo.
    """
    if state in (BookingState.SUBMITTING, BookingState.CONFIRMING):
        return Trigger.NOT_DISPATCHED
    return None


def trigger_for_escalation(state: BookingState) -> Trigger | None:
    """How an Escalate reaches review from ``state``; None if the booking is already there."""
    if state is BookingState.NEEDS_REVIEW:
        return None
    if state in TERMINAL_STATES:
        return Trigger.VERIFIED_RESERVATION_ON_TERMINAL
    return Trigger.ESCALATE_REVIEW


def _require_create(command: Command) -> None:
    if command.kind is not CommandKind.CREATE:
        raise NotImplementedError(f"{command.kind} settlement is not implemented")

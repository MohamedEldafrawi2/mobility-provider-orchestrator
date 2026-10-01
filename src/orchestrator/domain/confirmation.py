"""CONFIRM settlement for hold-then-confirm providers (edge cases 12 to 14).

A hold is a reservation the provider keeps for a bounded time. Confirming it is one immutable
operation addressing one reservation (it never creates another), idempotent at the provider,
and bounded by two clocks: the attempt's own expiry (the provider rejects a late request) and
the hold's deadline (after which the provider reports expiry).

Predicates:

- **after an attempt**: a confirmed reservation settles the CONFIRM ``SUCCEEDED`` (and, with
  it, the CREATE); a definitive rejection is the hold's expiry (``HoldExpired``: CONFIRM
  ``REJECTED``, CREATE ``REJECTED``, booking ``FAILED``), a request that expired before
  execution (``AttemptExcluded``: try again while the deadline and the budget allow), or any
  other rejection (``Reconcile``: the hold's state is read, never presumed); a failure that
  certainly had no effect reschedules; a possible effect is uncertain until a status lookup
  settles it.
- **after a status lookup** (authoritative: this provider reports the reservation by id): the
  reservation's state decides. Still a live hold after the attempt's expiry plus skew: the
  attempt is excluded and a new one may go; confirmed: succeeded; expired: failed.
- **deadline awareness**: no attempt is dispatched when the hold can no longer be confirmed
  in time; the worker then waits for the provider to report the expiry instead of guessing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from orchestrator.domain.capabilities import ProviderCapabilities
from orchestrator.domain.commands import (
    Command,
    CommandKind,
    Disposition,
    DispositionBasis,
    InvariantError,
    SideEffect,
)
from orchestrator.domain.cutoffs import ExpiryPolicy
from orchestrator.domain.ids import AttemptId
from orchestrator.domain.settlement import (
    REASON_IDENTITY,
    Escalate,
    ProviderResult,
    Reschedule,
    Reservation,
    ReservationState,
    Uncertain,
    _refs,
)
from orchestrator.domain.time import require_aware


@dataclass(frozen=True, slots=True)
class Confirmed:
    """The bound reservation is confirmed: CONFIRM and CREATE succeed, booking CONFIRMED."""

    reservation: Reservation
    basis: DispositionBasis


@dataclass(frozen=True, slots=True)
class HoldExpired:
    """The provider reports the hold expired: CONFIRM and CREATE rejected, booking FAILED."""

    basis: DispositionBasis
    reservation: Reservation | None = None


@dataclass(frozen=True, slots=True)
class AttemptExcluded:
    """This attempt certainly did not confirm; the hold is still live: another may go."""


@dataclass(frozen=True, slots=True)
class DeadlineTooClose:
    """Not enough usable time before the hold's deadline for a supported attempt."""

    deadline: datetime


@dataclass(frozen=True, slots=True)
class Reconcile:
    """The provider rejected the attempt for a reason other than an expiry (the hold may be
    cancelled, gone, or already confirmed by someone else): this attempt did nothing, but the
    hold's state is not presumed. An authoritative read decides."""

    reason: str


ConfirmDecision = (
    Confirmed
    | HoldExpired
    | AttemptExcluded
    | Reconcile
    | Uncertain
    | Reschedule
    | Escalate
    | DeadlineTooClose
)


def _require_confirm(command: Command) -> None:
    if command.kind is not CommandKind.CONFIRM:
        raise InvariantError("confirmation predicates are for CONFIRM commands")


def confirm_dispatch_window(
    *, now: datetime, hold_deadline: datetime, caps: ProviderCapabilities, policy: ExpiryPolicy
) -> datetime | None:
    """The latest expiry a CONFIRM attempt may carry now, or None when the deadline is too
    close: the attempt must be executable before the hold expires, with skew and margin."""
    require_aware(now, "now")
    require_aware(hold_deadline, "hold_deadline")
    skew = caps.max_clock_skew or timedelta(0)
    latest = hold_deadline - skew - policy.margin
    if now >= latest:
        return None
    return min(now + policy.attempt_ttl, latest)


def decide_confirm_after_attempt(
    command: Command, attempt_id: AttemptId, result: ProviderResult, *, bound: str
) -> ConfirmDecision:
    """``command`` must already include the finished attempt; ``bound`` is the booking's
    reservation reference, which the reservation returned must echo."""
    _require_confirm(command)
    if result.reservation is not None:
        r = result.reservation
        if r.ref != bound or r.client_ref != command.booking_id:
            return Escalate(REASON_IDENTITY, implicated=_refs(command, r.ref))
        match r.state:
            case ReservationState.CONFIRMED:
                return Confirmed(r, DispositionBasis.PROVIDER_RESULT)
            case ReservationState.HELD:
                return AttemptExcluded()  # answered, but not confirmed: try again
            case ReservationState.FAILED:
                return HoldExpired(DispositionBasis.PROVIDER_RESULT, r)
            case _:
                return Escalate(REASON_IDENTITY, implicated=_refs(command, r.ref))
    if result.definitive_rejection:
        if command.other_attempts_possibly_executed(attempt_id):
            return Uncertain()
        if result.hold_expired:
            return HoldExpired(DispositionBasis.PROVIDER_RESULT)
        if result.request_expired:
            return AttemptExcluded()  # our request expired before execution; the hold lives on
        return Reconcile("confirmation-rejected")  # not expiry: read before presuming a hold
    if result.side_effect in (SideEffect.NOT_DISPATCHED, SideEffect.NONE):
        return Uncertain() if command.possibly_executed else Reschedule()
    return Uncertain()


def decide_confirm_after_lookup(
    caps: ProviderCapabilities,
    command: Command,
    reservation: Reservation,
    *,
    bound: str,
    now: datetime,
    policy: ExpiryPolicy,
) -> ConfirmDecision:
    """An authoritative read of the bound reservation settles what an attempt could not."""
    _require_confirm(command)
    require_aware(now, "now")
    if reservation.ref != bound or reservation.client_ref != command.booking_id:
        return Escalate(REASON_IDENTITY, implicated=_refs(command, reservation.ref))
    match reservation.state:
        case ReservationState.CONFIRMED:
            return Confirmed(reservation, DispositionBasis.LOOKUP)
        case ReservationState.FAILED:
            return HoldExpired(DispositionBasis.LOOKUP, reservation)
        case ReservationState.CANCELLED:
            return Escalate(REASON_IDENTITY, implicated=_refs(command, reservation.ref))
        case _:
            pass
    # Still a hold. A possibly executed attempt is excluded once its expiry plus skew passed.
    for a in command.attempts:
        if a.effective_side_effect is SideEffect.POSSIBLE and (
            a.request.expiry is None or now < policy.excluded_after(caps, a.request.expiry)
        ):
            return Uncertain()
    return AttemptExcluded()


def confirm_disposition_after(
    decision: ConfirmDecision,
) -> tuple[Disposition, DispositionBasis] | None:
    """The disposition a decision settles the CONFIRM into, if any."""
    if isinstance(decision, Confirmed):
        return Disposition.SUCCEEDED, decision.basis
    if isinstance(decision, HoldExpired):
        return Disposition.REJECTED, decision.basis
    return None

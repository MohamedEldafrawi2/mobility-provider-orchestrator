"""CANCEL settlement with a refund quote (docs/booking-state-machine.md and ADR 013; edge cases
28 to 36).

A cancellation is a command with **phases**:

- ``NONE``: nothing asked of the provider yet. Recovery obtains a quote.
- ``QUOTED``: a refund offer is persisted with its identity, amounts and validity. The terms are
  validated against what the client authorised (a maximum fee); worse terms settle the command
  ``TERMS_CHANGED`` and the booking stays ``CONFIRMED``. An expired quote with no acceptance
  dispatched is simply re-quoted.
- ``ACCEPTING``: an acceptance attempt of *that exact offer* is dispatch-marked. The attempt
  settles only by the offer's status: ``CONFIRMED`` succeeds the command (booking
  ``CANCELLED``); ``EXPIRED`` or ``REJECTED`` excludes the attempt. While any acceptance may
  still commit, nothing is re-quoted; when no acceptance can commit any more and the offer is
  gone, the command is ``REFUSED`` and the booking stays ``CONFIRMED``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from orchestrator.domain.capabilities import ProviderCapabilities
from orchestrator.domain.commands import (
    CancelPhase,
    Command,
    CommandKind,
    Disposition,
    DispositionBasis,
    InvariantError,
    SideEffect,
)
from orchestrator.domain.cutoffs import ExpiryPolicy
from orchestrator.domain.ids import AttemptId
from orchestrator.domain.money import Money
from orchestrator.domain.refunds import RefundOfferState, RefundQuote
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
class CancelIntent:
    """What the client authorised: a cancellation under at most this fee (None: only free)."""

    max_fee: Money | None


@dataclass(frozen=True, slots=True)
class Cancelled:
    reservation: Reservation
    basis: DispositionBasis


@dataclass(frozen=True, slots=True)
class TermsChanged:
    quote: RefundQuote


@dataclass(frozen=True, slots=True)
class Refused:
    reason: str


@dataclass(frozen=True, slots=True)
class Requote:
    """The quote is unusable and no acceptance can still commit: obtain a fresh one."""


@dataclass(frozen=True, slots=True)
class AcceptanceExcluded:
    """This acceptance certainly did not commit; the quote may still be live."""


CancelDecision = (
    Cancelled
    | TermsChanged
    | Refused
    | Requote
    | AcceptanceExcluded
    | Uncertain
    | Reschedule
    | Escalate
)

__all__ = [
    "AcceptanceExcluded",
    "CancelDecision",
    "CancelIntent",
    "CancelPhase",
    "Cancelled",
    "Refused",
    "Requote",
    "TermsChanged",
    "cancel_disposition_after",
    "check_terms",
    "decide_cancel_after_acceptance",
    "decide_cancel_after_lookup",
]

REASON_REFUSED_EXPIRED = "refund-offer-expired"
REASON_REFUSED_UNAVAILABLE = "cancellation-unavailable"


def _require_cancel(command: Command) -> None:
    if command.kind not in (CommandKind.CANCEL, CommandKind.CANCEL_EXTRA):
        raise InvariantError("cancellation predicates are for CANCEL commands")


def check_terms(intent: CancelIntent, quote: RefundQuote) -> TermsChanged | None:
    """Worse terms than authorised are not accepted on the client's behalf (D2)."""
    if intent.max_fee is None:
        return None if quote.fee.amount_minor == 0 else TermsChanged(quote)
    if quote.fee.currency != intent.max_fee.currency:
        return TermsChanged(quote)
    if quote.fee.amount_minor > intent.max_fee.amount_minor:
        return TermsChanged(quote)
    return None


def decide_cancel_after_acceptance(
    command: Command,
    attempt_id: AttemptId,
    result: ProviderResult,
    *,
    bound: str,
    quote: RefundQuote | None,
) -> CancelDecision:
    """``command`` includes the finished acceptance attempt of ``quote``."""
    _require_cancel(command)
    if result.reservation is not None:
        r = result.reservation
        if r.ref != bound or r.client_ref != command.booking_id:
            return Escalate(REASON_IDENTITY, implicated=_refs(command, r.ref))
        accepted = (
            next((o for o in r.refund_offers if o.offer_id == quote.offer_id), None)
            if quote is not None
            else None
        )
        if quote is None:
            if r.state is ReservationState.CANCELLED:
                return Cancelled(r, DispositionBasis.PROVIDER_RESULT)
            return AcceptanceExcluded()
        if accepted is not None and accepted.state is RefundOfferState.CONFIRMED:
            if r.state is not ReservationState.CANCELLED:
                return Escalate(REASON_IDENTITY, implicated=_refs(command, r.ref))
            return Cancelled(r, DispositionBasis.PROVIDER_RESULT)
        if r.state is ReservationState.CANCELLED:
            # Cancelled, but not verifiably through our offer: review, never a guess.
            return Escalate(REASON_IDENTITY, implicated=_refs(command, r.ref))
        if accepted is None:
            return Uncertain()  # the answer names no such offer: the status is not known
        return AcceptanceExcluded()
    if result.definitive_rejection:
        if command.other_attempts_possibly_executed(attempt_id):
            return Uncertain()
        return AcceptanceExcluded()  # the offer expired or was rejected: this one did nothing
    if result.side_effect in (SideEffect.NOT_DISPATCHED, SideEffect.NONE):
        return Uncertain() if command.possibly_executed else Reschedule()
    return Uncertain()


def decide_cancel_after_lookup(
    caps: ProviderCapabilities,
    command: Command,
    reservation: Reservation,
    *,
    bound: str,
    quote: RefundQuote | None,
    now: datetime,
    policy: ExpiryPolicy,
) -> CancelDecision:
    """Settle an acceptance by the status of the exact offer it targeted; without a quote
    (free cancellation), by the reservation's state once the attempt is excluded."""
    _require_cancel(command)
    require_aware(now, "now")
    if reservation.ref != bound or reservation.client_ref != command.booking_id:
        return Escalate(REASON_IDENTITY, implicated=_refs(command, reservation.ref))
    if quote is None:
        if reservation.state is ReservationState.CANCELLED:
            return Cancelled(reservation, DispositionBasis.LOOKUP)
        for a in command.attempts:
            if a.effective_side_effect is SideEffect.POSSIBLE and (
                a.request.expiry is None or now < policy.excluded_after(caps, a.request.expiry)
            ):
                return Uncertain()
        if command.execution_cutoff is not None and now >= command.execution_cutoff:
            return Refused(REASON_REFUSED_EXPIRED)  # 6.6 #33: after the cutoff, a new command
        return AcceptanceExcluded()  # 6.6 #32: a fresh attempt inside the same command
    offer = next((o for o in reservation.refund_offers if o.offer_id == quote.offer_id), None)
    if offer is not None and offer.state is RefundOfferState.CONFIRMED:
        if reservation.state is not ReservationState.CANCELLED:
            return Escalate(REASON_IDENTITY, implicated=_refs(command, reservation.ref))
        return Cancelled(reservation, DispositionBasis.LOOKUP)
    if reservation.state is ReservationState.CANCELLED:
        return Escalate(REASON_IDENTITY, implicated=_refs(command, reservation.ref))
    # Not accepted. An acceptance attempt may still commit until its expiry plus skew.
    for a in command.attempts:
        if a.effective_side_effect is SideEffect.POSSIBLE and (
            a.request.expiry is None or now < policy.excluded_after(caps, a.request.expiry)
        ):
            return Uncertain()
    if offer is None or offer.state in (RefundOfferState.EXPIRED, RefundOfferState.REJECTED):
        return Requote()
    return AcceptanceExcluded()  # the offer is still proposed and live: accept it again


def cancel_disposition_after(
    decision: CancelDecision,
) -> tuple[Disposition, DispositionBasis] | None:
    if isinstance(decision, Cancelled):
        return Disposition.SUCCEEDED, decision.basis
    if isinstance(decision, TermsChanged):
        return Disposition.TERMS_CHANGED, DispositionBasis.PROVIDER_RESULT
    if isinstance(decision, Refused):
        return Disposition.REFUSED, DispositionBasis.PROVIDER_RESULT
    return None

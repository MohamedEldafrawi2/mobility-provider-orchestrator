"""Review cases and their closing conditions (docs/booking-state-machine.md).

A case closes only when every implicated reservation is *affirmatively* accounted for by the
newest valid evidence about it, at most one is live, and, to close into a live state, the sole
live reservation is the bound reference. A negative plain lookup never accounts for anything:
for a provider without finality, "not found" is not "gone". Cases for such a provider may stay
open indefinitely; that is a supported outcome, not a defect. An outstanding cancellation
resumes (``CANCELLING``) when the bound reservation is still live and verifiably ours, and a
cancellation case closes into ``CANCELLED`` only through the exact refund offer accepted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from orchestrator.domain.commands import Command, CommandKind
from orchestrator.domain.ids import ProviderBookingRef
from orchestrator.domain.refunds import RefundOfferState
from orchestrator.domain.settlement import (
    LIVE_RESERVATION_STATES,
    Reservation,
    ReservationState,
    identity_mismatch,
)
from orchestrator.domain.states import BookingState
from orchestrator.domain.time import require_aware


class EvidenceKind(StrEnum):
    LOOKUP_BY_REF = "LOOKUP_BY_REF"
    LOOKUP_BY_CLIENT_REF = "LOOKUP_BY_CLIENT_REF"
    FENCED_LOOKUP = "FENCED_LOOKUP"
    OBSERVATION = "OBSERVATION"  # the provider's own answer to a request of ours
    WEBHOOK = "WEBHOOK"  # a pushed event: informational until a read confirms it


AUTHORITATIVE_KINDS: frozenset[EvidenceKind] = frozenset(
    {
        EvidenceKind.LOOKUP_BY_REF,
        EvidenceKind.LOOKUP_BY_CLIENT_REF,
        EvidenceKind.FENCED_LOOKUP,
        EvidenceKind.OBSERVATION,
    }
)


@dataclass(frozen=True, slots=True)
class Evidence:
    kind: EvidenceKind
    initiated_at: datetime
    subject_ref: ProviderBookingRef
    reservation: Reservation | None  # None records a negative lookup for subject_ref
    valid_until: datetime | None = None
    superseded: bool = False

    def __post_init__(self) -> None:
        require_aware(self.initiated_at, "Evidence.initiated_at")
        require_aware(self.valid_until, "Evidence.valid_until")
        if self.reservation is not None and self.reservation.ref != self.subject_ref:
            raise ValueError("evidence reservation must be about its subject reference")


@dataclass(frozen=True, slots=True)
class ReviewCase:
    reason: str
    remediable: bool
    outstanding_command: CommandKind | None
    implicated: tuple[ProviderBookingRef, ...]
    evidence: tuple[Evidence, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class NotClosable:
    reason: str


def _ordering(evidence: Evidence) -> tuple[int, int]:
    reservation = evidence.reservation
    if reservation is None:
        return (-1, -1)
    return (reservation.generation or -1, reservation.revision or -1)


def _newest(case: ReviewCase, ref: ProviderBookingRef) -> Evidence | NotClosable | None:
    """The evidence that may close the case for ``ref``: the authoritative evidence with the
    highest provider ordering (generation, revision), then the most recent. A pushed event is
    never that evidence; if one claims a newer ordering than every authoritative fact, the case
    stays open until a read confirms or refutes it. Receipt time alone ranks nothing."""
    candidates = [e for e in case.evidence if e.subject_ref == ref and not e.superseded]
    if not candidates:
        return None
    authoritative = [e for e in candidates if e.kind in AUTHORITATIVE_KINDS]
    pushed = [e for e in candidates if e.kind is EvidenceKind.WEBHOOK]
    if not authoritative:
        return NotClosable(f"only pushed events about {ref}; an authoritative read is required")
    best = max(authoritative, key=lambda e: (_ordering(e), e.initiated_at))
    if pushed:
        loudest = max(pushed, key=_ordering)
        if _ordering(loudest) > _ordering(best):
            return NotClosable(
                f"a pushed event about {ref} claims a newer ordering than any read; reconcile"
            )
    return best


def closable_into(
    case: ReviewCase,
    bound_ref: ProviderBookingRef | None,
    *,
    now: datetime,
    command: Command | None = None,
) -> BookingState | NotClosable:
    """Return the state the evidence supports, or why the case must stay open.

    With ``command`` given, a live reservation may close the case only if it is verifiably
    the one the command asked for (our reference, the expected product and service date), and
    a live reservation that verifiably belongs to another client does not keep the case open.
    """
    require_aware(now, "now")
    if case.outstanding_command in (CommandKind.CANCEL, CommandKind.CANCEL_EXTRA):
        # An outstanding cancellation resumes (CANCELLING) only when the bound reservation is
        # still live and verifiably ours; the ordinary rules below decide that too, and a
        # CANCELLED bound reservation closes the case into CANCELLED.
        pass
    if not case.implicated:
        return NotClosable("no implicated reservation; discovery by client reference required")

    accounted: dict[ProviderBookingRef, Reservation] = {}
    for ref in case.implicated:
        newest = _newest(case, ref)
        if newest is None:
            return NotClosable(f"no evidence for {ref}")
        if isinstance(newest, NotClosable):
            return newest
        if newest.valid_until is not None and now >= newest.valid_until:
            return NotClosable(f"evidence for {ref} has expired; fresh evidence required")
        if newest.reservation is None:
            # A negative plain lookup proves nothing for a provider without finality.
            return NotClosable(f"no affirmative evidence for {ref}")
        if newest.reservation.state is ReservationState.HELD and newest.valid_until is None:
            return NotClosable(f"hold evidence for {ref} needs a validity bound")
        accounted[ref] = newest.reservation

    live = [
        r
        for r in accounted.values()
        if r.state in LIVE_RESERVATION_STATES
        # A reservation whose newest evidence names another client is affirmatively not ours
        # (the provider binds the client reference): it is accounted for, it competes for
        # nothing. Only when the command is known can "ours" be judged.
        and not (command is not None and r.client_ref != command.booking_id)
    ]
    if len(live) > 1:
        return NotClosable("more than one live reservation")

    if len(live) == 1:
        sole = live[0]
        if bound_ref is None:
            return NotClosable("no bound reference; bind through settlement before closing")
        if sole.ref != bound_ref:
            return NotClosable("the live reservation is not the bound reference")
        if command is not None and (why := identity_mismatch(command, sole)) is not None:
            return NotClosable(f"the live reservation is not verifiably ours: {why}")
        if (
            case.outstanding_command in (CommandKind.CANCEL, CommandKind.CANCEL_EXTRA)
            and sole.state is ReservationState.CONFIRMED
        ):
            return BookingState.CANCELLING  # the outstanding cancellation resumes (6.1)
        return {
            ReservationState.CONFIRMED: BookingState.CONFIRMED,
            ReservationState.HELD: BookingState.HELD,
            ReservationState.PENDING: BookingState.PENDING_PROVIDER,
        }[sole.state]

    # Nothing live, every implicated reservation affirmatively terminal at the provider.
    if bound_ref is not None and bound_ref in accounted:
        bound_state = accounted[bound_ref].state
        if bound_state is ReservationState.CANCELLED:
            quote = getattr(command, "quote", None) if command is not None else None
            if quote is not None and not any(
                o.offer_id == quote.offer_id and o.state is RefundOfferState.CONFIRMED
                for o in accounted[bound_ref].refund_offers
            ):
                return NotClosable("cancelled, but not verifiably through the accepted offer")
            return BookingState.CANCELLED
        if bound_state is ReservationState.FAILED:
            return BookingState.FAILED
    return NotClosable("no live reservation and no provider finality")

"""The booking state machine as a table (docs/booking-state-machine.md).

``transition`` is total: for every state and trigger it returns either the next state or an
``InvalidTransition`` value. It never raises, so callers must handle the invalid case
explicitly, and the exhaustive tests can enumerate every pair.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class BookingState(StrEnum):
    CREATED = "CREATED"
    SUBMITTING = "SUBMITTING"
    HELD = "HELD"
    CONFIRMING = "CONFIRMING"
    PENDING_PROVIDER = "PENDING_PROVIDER"
    UNKNOWN = "UNKNOWN"
    CONFIRMED = "CONFIRMED"
    CANCELLING = "CANCELLING"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATES: frozenset[BookingState] = frozenset({BookingState.FAILED, BookingState.CANCELLED})

# States where the platform is waiting on its own work or the provider; progress is bounded.
ACTIVE_STATES: frozenset[BookingState] = frozenset(
    {
        BookingState.CREATED,
        BookingState.SUBMITTING,
        BookingState.HELD,
        BookingState.CONFIRMING,
        BookingState.PENDING_PROVIDER,
        BookingState.UNKNOWN,
        BookingState.CANCELLING,
    }
)


class Trigger(StrEnum):
    ATTEMPT_DISPATCHED = "ATTEMPT_DISPATCHED"
    NOT_DISPATCHED = "NOT_DISPATCHED"
    ABANDONED = "ABANDONED"
    PROVIDER_CONFIRMED = "PROVIDER_CONFIRMED"
    PROVIDER_HELD = "PROVIDER_HELD"
    PROVIDER_PENDING = "PROVIDER_PENDING"
    PROVIDER_REJECTED = "PROVIDER_REJECTED"
    PROVIDER_CANCELLED = "PROVIDER_CANCELLED"
    HOLD_EXPIRED = "HOLD_EXPIRED"
    OUTCOME_UNCERTAIN = "OUTCOME_UNCERTAIN"
    ATTEMPT_EXCLUDED = "ATTEMPT_EXCLUDED"
    ESCALATE_REVIEW = "ESCALATE_REVIEW"
    CANCEL_ACCEPTED = "CANCEL_ACCEPTED"
    CANCEL_REFUSED = "CANCEL_REFUSED"
    CANCEL_RESUMED = "CANCEL_RESUMED"
    VERIFIED_RESERVATION_ON_TERMINAL = "VERIFIED_RESERVATION_ON_TERMINAL"


@dataclass(frozen=True, slots=True)
class InvalidTransition:
    state: BookingState
    trigger: Trigger


S = BookingState
T = Trigger

TRANSITIONS: dict[tuple[BookingState, Trigger], BookingState] = {
    # Creation
    (S.CREATED, T.ATTEMPT_DISPATCHED): S.SUBMITTING,
    (S.CREATED, T.ABANDONED): S.FAILED,
    (S.CREATED, T.ESCALATE_REVIEW): S.NEEDS_REVIEW,
    (S.SUBMITTING, T.NOT_DISPATCHED): S.CREATED,
    (S.SUBMITTING, T.PROVIDER_CONFIRMED): S.CONFIRMED,
    (S.SUBMITTING, T.PROVIDER_HELD): S.HELD,
    (S.SUBMITTING, T.PROVIDER_PENDING): S.PENDING_PROVIDER,
    (S.SUBMITTING, T.PROVIDER_REJECTED): S.FAILED,
    (S.SUBMITTING, T.PROVIDER_CANCELLED): S.CANCELLED,  # discovered already cancelled (section 4)
    (S.SUBMITTING, T.OUTCOME_UNCERTAIN): S.UNKNOWN,
    (S.SUBMITTING, T.ESCALATE_REVIEW): S.NEEDS_REVIEW,
    # Hold then confirm
    (S.HELD, T.ATTEMPT_DISPATCHED): S.CONFIRMING,
    (S.HELD, T.HOLD_EXPIRED): S.FAILED,
    (S.HELD, T.ESCALATE_REVIEW): S.NEEDS_REVIEW,
    (S.CONFIRMING, T.NOT_DISPATCHED): S.HELD,
    (S.CONFIRMING, T.ATTEMPT_EXCLUDED): S.HELD,
    (S.CONFIRMING, T.PROVIDER_CONFIRMED): S.CONFIRMED,
    (S.CONFIRMING, T.HOLD_EXPIRED): S.FAILED,
    (S.CONFIRMING, T.OUTCOME_UNCERTAIN): S.UNKNOWN,
    (S.CONFIRMING, T.ESCALATE_REVIEW): S.NEEDS_REVIEW,
    # Unknown outcome, settled by evidence only
    (S.UNKNOWN, T.PROVIDER_CONFIRMED): S.CONFIRMED,
    (S.UNKNOWN, T.PROVIDER_HELD): S.HELD,
    (S.UNKNOWN, T.PROVIDER_PENDING): S.PENDING_PROVIDER,
    (S.UNKNOWN, T.PROVIDER_REJECTED): S.FAILED,
    (S.UNKNOWN, T.PROVIDER_CANCELLED): S.CANCELLED,
    (S.UNKNOWN, T.ESCALATE_REVIEW): S.NEEDS_REVIEW,
    # Asynchronous confirmation
    (S.PENDING_PROVIDER, T.PROVIDER_CONFIRMED): S.CONFIRMED,
    (S.PENDING_PROVIDER, T.PROVIDER_REJECTED): S.FAILED,
    (S.PENDING_PROVIDER, T.ESCALATE_REVIEW): S.NEEDS_REVIEW,
    # Settled, but contradictory evidence can still arrive (section 6.4: review from any state)
    (S.CONFIRMED, T.CANCEL_ACCEPTED): S.CANCELLING,
    (S.CONFIRMED, T.ESCALATE_REVIEW): S.NEEDS_REVIEW,
    # Cancellation
    (S.CANCELLING, T.PROVIDER_CANCELLED): S.CANCELLED,
    (S.CANCELLING, T.CANCEL_REFUSED): S.CONFIRMED,
    (S.CANCELLING, T.ESCALATE_REVIEW): S.NEEDS_REVIEW,
    # Review closes only on evidence (section 6.1 closing conditions)
    (S.NEEDS_REVIEW, T.PROVIDER_CONFIRMED): S.CONFIRMED,
    (S.NEEDS_REVIEW, T.PROVIDER_HELD): S.HELD,
    (S.NEEDS_REVIEW, T.PROVIDER_PENDING): S.PENDING_PROVIDER,
    (S.NEEDS_REVIEW, T.CANCEL_RESUMED): S.CANCELLING,
    (S.NEEDS_REVIEW, T.PROVIDER_CANCELLED): S.CANCELLED,
    (S.NEEDS_REVIEW, T.PROVIDER_REJECTED): S.FAILED,
    # Terminal states reopen only on verified evidence of a reservation
    (S.FAILED, T.VERIFIED_RESERVATION_ON_TERMINAL): S.NEEDS_REVIEW,
    (S.CANCELLED, T.VERIFIED_RESERVATION_ON_TERMINAL): S.NEEDS_REVIEW,
}


def transition(state: BookingState, trigger: Trigger) -> BookingState | InvalidTransition:
    nxt = TRANSITIONS.get((state, trigger))
    return nxt if nxt is not None else InvalidTransition(state, trigger)

"""Ordering of pushed and pulled observations (docs/architecture.md, observations).

A provider that reports a **generation** and a **revision** lets the platform order what it
hears about a reservation, whether it arrives as a webhook or as a poll:

1. the reference must match the bound reference, else the observation is ``UNMATCHED``;
2. the generation must equal the booking's current generation: an older one is
   ``SUPERSEDED_GENERATION`` and discarded; a newer one is adopted only from an authoritative
   read, otherwise quarantined to review;
3. the revision must exceed the last applied one, else the observation is ``STALE`` when
   pushed; an *authoritative* read that reports a lower revision than one already applied, or
   the same revision with another state, is ``REGRESSED``: the provider contradicts its own
   history, and review decides.

An observation that passes is applied through the ordinary transition table; a legal
transition changes the booking, an illegal one is contradictory evidence for review. An
observation that confirms the current state (``NO_CHANGE``) still advances the generation and
revision watermarks, so that an older fact arriving later cannot pass as new. The attempt that
produced a mutation response is closed with its outcome even when its observation is stale.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from orchestrator.domain.ids import ProviderBookingRef
from orchestrator.domain.settlement import Reservation, ReservationState
from orchestrator.domain.states import BookingState, InvalidTransition, Trigger, transition


class ObservationOutcome(StrEnum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    SUPERSEDED_GENERATION = "SUPERSEDED_GENERATION"
    NEWER_GENERATION = "NEWER_GENERATION"  # adopted from an authoritative read only
    UNMATCHED = "UNMATCHED"
    CONTRADICTORY = "CONTRADICTORY"
    NO_CHANGE = "NO_CHANGE"
    REGRESSED = "REGRESSED"  # an authoritative read behind what was already applied
    QUARANTINED = "QUARANTINED"  # recorded as review evidence; the case decides


@dataclass(frozen=True, slots=True)
class Ordering:
    outcome: ObservationOutcome
    trigger: Trigger | None = None
    next_state: BookingState | None = None

    @property
    def advances_watermark(self) -> bool:
        """Whether the booking's generation and revision move to the observation's."""
        return self.outcome in (ObservationOutcome.APPLIED, ObservationOutcome.NO_CHANGE)


_TRIGGER_FOR_STATE: dict[ReservationState, Trigger] = {
    ReservationState.CONFIRMED: Trigger.PROVIDER_CONFIRMED,
    ReservationState.PENDING: Trigger.PROVIDER_PENDING,
    ReservationState.HELD: Trigger.PROVIDER_HELD,
    ReservationState.FAILED: Trigger.PROVIDER_REJECTED,
    ReservationState.CANCELLED: Trigger.PROVIDER_CANCELLED,
}

_STATE_OF: dict[ReservationState, BookingState] = {
    ReservationState.CONFIRMED: BookingState.CONFIRMED,
    ReservationState.PENDING: BookingState.PENDING_PROVIDER,
    ReservationState.HELD: BookingState.HELD,
    ReservationState.FAILED: BookingState.FAILED,
    ReservationState.CANCELLED: BookingState.CANCELLED,
}


def order_observation(
    observation: Reservation,
    *,
    bound_ref: ProviderBookingRef | None,
    state: BookingState,
    generation: int | None,
    last_revision: int | None,
    authoritative: bool,
) -> Ordering:
    """Decide whether ``observation`` may be applied to a booking in ``state``."""
    if bound_ref is None or observation.ref != bound_ref:
        return Ordering(ObservationOutcome.UNMATCHED)
    if observation.generation is not None and generation is not None:
        if observation.generation < generation:
            return Ordering(ObservationOutcome.SUPERSEDED_GENERATION)
        if observation.generation > generation and not authoritative:
            return Ordering(ObservationOutcome.NEWER_GENERATION)
    same_generation = (
        observation.generation is None or generation is None or observation.generation == generation
    )
    target = _STATE_OF[observation.state]
    if (
        same_generation
        and observation.revision is not None
        and last_revision is not None
        and observation.revision <= last_revision
    ):
        if observation.revision == last_revision and target is state:
            return Ordering(ObservationOutcome.NO_CHANGE)  # the same fact, read again
        if authoritative:
            return Ordering(ObservationOutcome.REGRESSED)  # the provider lost its own history
        return Ordering(ObservationOutcome.STALE)
    if target is state:
        return Ordering(ObservationOutcome.NO_CHANGE)
    trigger = _TRIGGER_FOR_STATE[observation.state]
    nxt = transition(state, trigger)
    if isinstance(nxt, InvalidTransition):
        return Ordering(ObservationOutcome.CONTRADICTORY, trigger)
    return Ordering(ObservationOutcome.APPLIED, trigger, nxt)

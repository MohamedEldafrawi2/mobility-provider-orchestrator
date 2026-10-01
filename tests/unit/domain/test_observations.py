"""Ordering of pushed and pulled observations (docs/architecture.md, observations)."""

from __future__ import annotations

from orchestrator.domain import (
    BookingId,
    BookingState,
    ProviderBookingRef,
    Reservation,
    ReservationState,
    Trigger,
)
from orchestrator.domain.observations import ObservationOutcome, order_observation
from tests.unit.domain.helpers import T0

BK = BookingId("bk_1")
R1 = ProviderBookingRef("MB1")
R2 = ProviderBookingRef("MB2")


def _obs(
    state: ReservationState, *, generation: int = 1, revision: int = 2, ref: ProviderBookingRef = R1
) -> Reservation:
    return Reservation(ref, BK, state, T0, generation=generation, revision=revision)


def test_reference_must_match_the_bound_one() -> None:
    result = order_observation(
        _obs(ReservationState.CONFIRMED, ref=R2),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=1,
        last_revision=1,
        authoritative=False,
    )
    assert result.outcome is ObservationOutcome.UNMATCHED
    unbound = order_observation(
        _obs(ReservationState.CONFIRMED),
        bound_ref=None,
        state=BookingState.SUBMITTING,
        generation=None,
        last_revision=None,
        authoritative=False,
    )
    assert unbound.outcome is ObservationOutcome.UNMATCHED


def test_generation_then_revision() -> None:
    older = order_observation(
        _obs(ReservationState.CONFIRMED, generation=1),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=2,
        last_revision=1,
        authoritative=False,
    )
    assert older.outcome is ObservationOutcome.SUPERSEDED_GENERATION
    newer_pushed = order_observation(
        _obs(ReservationState.CONFIRMED, generation=3),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=2,
        last_revision=1,
        authoritative=False,
    )
    assert newer_pushed.outcome is ObservationOutcome.NEWER_GENERATION, (
        "quarantined unless authoritative"
    )
    newer_read = order_observation(
        _obs(ReservationState.CONFIRMED, generation=3),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=2,
        last_revision=1,
        authoritative=True,
    )
    assert (
        newer_read.outcome is ObservationOutcome.APPLIED
        and newer_read.trigger is Trigger.PROVIDER_CONFIRMED
    )
    stale = order_observation(
        _obs(ReservationState.CONFIRMED, revision=1),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=1,
        last_revision=1,
        authoritative=False,
    )
    assert stale.outcome is ObservationOutcome.STALE


def test_legal_transitions_apply_and_illegal_ones_are_contradictory() -> None:
    applied = order_observation(
        _obs(ReservationState.CONFIRMED),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=1,
        last_revision=1,
        authoritative=False,
    )
    assert (applied.outcome, applied.next_state) == (
        ObservationOutcome.APPLIED,
        BookingState.CONFIRMED,
    )
    same = order_observation(
        _obs(ReservationState.PENDING),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=1,
        last_revision=1,
        authoritative=False,
    )
    assert same.outcome is ObservationOutcome.NO_CHANGE
    contradictory = order_observation(
        _obs(ReservationState.PENDING, revision=5),
        bound_ref=R1,
        state=BookingState.CANCELLED,
        generation=1,
        last_revision=4,
        authoritative=False,
    )
    assert contradictory.outcome is ObservationOutcome.CONTRADICTORY
    reopened = order_observation(
        _obs(ReservationState.CONFIRMED, revision=5),
        bound_ref=R1,
        state=BookingState.FAILED,
        generation=1,
        last_revision=4,
        authoritative=True,
    )
    assert reopened.outcome is ObservationOutcome.CONTRADICTORY, (
        "a terminal state reopens through review"
    )

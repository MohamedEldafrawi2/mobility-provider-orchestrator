"""CREATE settlement against Provider B: the edge cases of docs/edge-cases.md."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from orchestrator.domain import (
    Abandon,
    AttemptOutcome,
    Bind,
    BookingId,
    BookingState,
    Disposition,
    DispositionBasis,
    Escalate,
    KeepLooking,
    ProviderBookingRef,
    ProviderResult,
    Rejected,
    Reschedule,
    Reservation,
    ReservationState,
    SideEffect,
    Trigger,
    Uncertain,
    bind,
    decide_abandon,
    decide_create_after_attempt,
    decide_create_after_lookup,
    note_lookup,
    record_attempt,
    settle,
    trigger_for,
    trigger_for_escalation,
    trigger_for_reschedule,
)
from orchestrator.domain.settlement import (
    REASON_CANNOT_SETTLE,
    REASON_DUPLICATE,
    REASON_IDENTITY,
    REASON_SETTLED_CONTRADICTED,
    REASON_UNEXPECTED,
)
from tests.unit.domain.helpers import PROVIDER_B, T0, attempt, attempt_id, new_command, open_attempt

BK = BookingId("bk_1")
B1, B2 = ProviderBookingRef("B1"), ProviderBookingRef("B2")


def reservation(
    ref: ProviderBookingRef,
    state: ReservationState = ReservationState.CONFIRMED,
    client: BookingId = BK,
) -> Reservation:
    return Reservation(ref, client, state, T0)


def test_success_binds_and_succeeds() -> None:
    result = ProviderResult(SideEffect.NONE, reservation=reservation(B1))
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.SUCCESS, SideEffect.NONE))
    assert decide_create_after_attempt(PROVIDER_B, cmd, attempt_id(1), result) == Bind(
        reservation(B1),
        Trigger.PROVIDER_CONFIRMED,
        Disposition.SUCCEEDED,
        DispositionBasis.PROVIDER_RESULT,
    )


def test_journal_then_definitive_rejection_settles_immediately() -> None:
    """Section 6.6 #8 with the real sequence: dispatch mark, then the provider's answer."""
    cmd = record_attempt(new_command(), open_attempt(1))
    assert cmd.possibly_executed
    answered = replace(
        open_attempt(1),
        finished_at=T0 + timedelta(seconds=2),
        outcome=AttemptOutcome.REJECTED,
        side_effect=SideEffect.NONE,
    )
    cmd = record_attempt(cmd, answered)
    decision = decide_create_after_attempt(
        PROVIDER_B, cmd, attempt_id(1), ProviderResult(SideEffect.NONE, definitive_rejection=True)
    )
    assert decision == Rejected(DispositionBasis.PROVIDER_RESULT)
    assert settle(
        cmd, Disposition.REJECTED, DispositionBasis.PROVIDER_RESULT, caps=PROVIDER_B
    ).is_settled


def test_rejection_after_an_earlier_possible_effect_does_not_settle() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    cmd = record_attempt(cmd, attempt(2, AttemptOutcome.REJECTED, SideEffect.NONE))
    decision = decide_create_after_attempt(
        PROVIDER_B, cmd, attempt_id(2), ProviderResult(SideEffect.NONE, definitive_rejection=True)
    )
    assert decision == Uncertain()


def test_timeout_is_uncertain_not_failed() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    assert (
        decide_create_after_attempt(
            PROVIDER_B, cmd, attempt_id(1), ProviderResult(SideEffect.POSSIBLE)
        )
        == Uncertain()
    )


def test_local_rejection_after_a_possible_effect_never_authorises_a_retry() -> None:
    """Section 6.2: REVIEW forbids any mutating dispatch after POSSIBLE."""
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    cmd = record_attempt(
        cmd, attempt(2, AttemptOutcome.NOT_DISPATCHED, SideEffect.NOT_DISPATCHED, marked=False)
    )
    decision = decide_create_after_attempt(
        PROVIDER_B, cmd, attempt_id(2), ProviderResult(SideEffect.NOT_DISPATCHED)
    )
    assert decision == Uncertain()


def test_not_dispatched_reschedules_and_abandons_only_if_never_dispatched() -> None:
    cmd = record_attempt(
        new_command(),
        attempt(1, AttemptOutcome.NOT_DISPATCHED, SideEffect.NOT_DISPATCHED, marked=False),
    )
    assert (
        decide_create_after_attempt(
            PROVIDER_B, cmd, attempt_id(1), ProviderResult(SideEffect.NOT_DISPATCHED)
        )
        == Reschedule()
    )
    assert decide_abandon(cmd, now=T0 + timedelta(minutes=1), max_age=timedelta(minutes=5)) is None
    assert (
        decide_abandon(cmd, now=T0 + timedelta(minutes=6), max_age=timedelta(minutes=5))
        == Abandon()
    )
    dispatched = record_attempt(
        new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE)
    )
    assert (
        decide_abandon(dispatched, now=T0 + timedelta(days=1), max_age=timedelta(minutes=5)) is None
    )


def test_reservation_for_another_product_or_day_escalates() -> None:
    """Our reference on a reservation for a different journey or date is not our booking."""
    from dataclasses import replace as dc_replace
    from datetime import date

    from orchestrator.domain import CreateIntent

    intent = CreateIntent(
        "off_1", ("Ada",), "a@x.io", product_ref="J1", service_date=date(2026, 6, 15)
    )
    cmd = dc_replace(new_command(), intent=intent)
    cmd = note_lookup(record_attempt(cmd, attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE)))
    wrong_day = Reservation(
        B1, BK, ReservationState.CONFIRMED, T0, product_ref="J1", service_date=date(2026, 6, 16)
    )
    assert decide_create_after_lookup(PROVIDER_B, cmd, (wrong_day,), lookup_budget=3) == Escalate(
        REASON_IDENTITY, implicated=(B1,)
    )
    right = Reservation(
        B1, BK, ReservationState.CONFIRMED, T0, product_ref="J1", service_date=date(2026, 6, 15)
    )
    assert isinstance(decide_create_after_lookup(PROVIDER_B, cmd, (right,), lookup_budget=3), Bind)


def test_reservation_for_another_booking_escalates() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.SUCCESS, SideEffect.NONE))
    other = reservation(B1, client=BookingId("bk_other"))
    decision = decide_create_after_attempt(
        PROVIDER_B, cmd, attempt_id(1), ProviderResult(SideEffect.NONE, reservation=other)
    )
    assert decision == Escalate(REASON_IDENTITY, implicated=(B1,))


def test_result_with_a_different_reference_than_the_bound_one_escalates_with_both() -> None:
    cmd = bind(
        record_attempt(new_command(), attempt(1, AttemptOutcome.SUCCESS, SideEffect.NONE)), B1
    )
    decision = decide_create_after_attempt(
        PROVIDER_B, cmd, attempt_id(1), ProviderResult(SideEffect.NONE, reservation=reservation(B2))
    )
    assert decision == Escalate(REASON_DUPLICATE, implicated=(B1, B2))


def test_reservation_for_a_negatively_settled_command_escalates() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.REJECTED, SideEffect.NONE))
    cmd = settle(cmd, Disposition.REJECTED, DispositionBasis.PROVIDER_RESULT, caps=PROVIDER_B)
    decision = decide_create_after_lookup(
        PROVIDER_B, note_lookup(cmd), (reservation(B1),), lookup_budget=3
    )
    assert decision == Escalate(REASON_SETTLED_CONTRADICTED, implicated=(B1,))


def test_lookup_finds_the_reservation_and_binds() -> None:
    cmd = note_lookup(
        record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    )
    decision = decide_create_after_lookup(
        PROVIDER_B, cmd, (reservation(ProviderBookingRef("B7")),), lookup_budget=3
    )
    assert isinstance(decision, Bind)
    assert (decision.reservation.ref, decision.disposition, decision.basis) == (
        "B7",
        Disposition.SUCCEEDED,
        DispositionBasis.LOOKUP,
    )


def test_lookup_ignores_other_clients_reservations() -> None:
    cmd = note_lookup(
        record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    )
    other = reservation(ProviderBookingRef("B9"), client=BookingId("bk_other"))
    assert decide_create_after_lookup(PROVIDER_B, cmd, (other,), lookup_budget=3) == KeepLooking(1)


def test_negative_lookups_keep_looking_then_escalate_never_fail() -> None:
    """Section 6.6 #44 and #45: a possible effect at B ends in review, never in FAILED."""
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    for i in range(1, 4):
        cmd = note_lookup(cmd)
        decision = decide_create_after_lookup(PROVIDER_B, cmd, (), lookup_budget=3)
        assert decision == (
            KeepLooking(i) if i < 3 else Escalate(REASON_CANNOT_SETTLE, remediable=False)
        )


def test_two_reservations_open_a_duplicate_case_including_the_bound_one() -> None:
    cmd = note_lookup(
        record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    )
    assert decide_create_after_lookup(
        PROVIDER_B, cmd, (reservation(B1), reservation(B2)), lookup_budget=3
    ) == Escalate(REASON_DUPLICATE, implicated=(B1, B2), remediable=False)
    bound = bind(cmd, ProviderBookingRef("B0"))
    assert decide_create_after_lookup(
        PROVIDER_B, bound, (reservation(B1), reservation(B2)), lookup_budget=3
    ) == Escalate(REASON_DUPLICATE, implicated=(ProviderBookingRef("B0"), B1, B2), remediable=False)


def test_lookup_without_a_possible_effect_just_reschedules() -> None:
    cmd = note_lookup(
        record_attempt(
            new_command(),
            attempt(1, AttemptOutcome.NOT_DISPATCHED, SideEffect.NOT_DISPATCHED, marked=False),
        )
    )
    assert decide_create_after_lookup(PROVIDER_B, cmd, (), lookup_budget=3) == Reschedule()


def test_a_bind_on_a_terminal_booking_reopens_to_review() -> None:
    """Section 6.6 #46: a reservation appearing for a FAILED booking is verified evidence."""
    b = Bind(
        reservation(B1), Trigger.PROVIDER_CONFIRMED, Disposition.SUCCEEDED, DispositionBasis.LOOKUP
    )
    assert trigger_for(BookingState.FAILED, b) is Trigger.VERIFIED_RESERVATION_ON_TERMINAL
    assert trigger_for(BookingState.UNKNOWN, b) is Trigger.PROVIDER_CONFIRMED


def test_discovered_cancelled_reservation_succeeds_the_create() -> None:
    cmd = note_lookup(
        record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    )
    decision = decide_create_after_lookup(
        PROVIDER_B, cmd, (reservation(B1, ReservationState.CANCELLED),), lookup_budget=3
    )
    assert decision == Bind(
        reservation(B1, ReservationState.CANCELLED),
        Trigger.PROVIDER_CANCELLED,
        Disposition.SUCCEEDED,
        DispositionBasis.LOOKUP,
    )


def test_a_reservation_is_a_certain_effect() -> None:
    with pytest.raises(ValueError, match="certain effect"):
        ProviderResult(SideEffect.POSSIBLE, reservation=reservation(B1))


def test_reobserving_the_current_state_is_a_no_op_and_escalation_is_state_aware() -> None:
    confirmed = Bind(
        reservation(B1), Trigger.PROVIDER_CONFIRMED, Disposition.SUCCEEDED, DispositionBasis.LOOKUP
    )
    assert trigger_for(BookingState.CONFIRMED, confirmed) is None
    assert trigger_for(BookingState.NEEDS_REVIEW, confirmed) is Trigger.PROVIDER_CONFIRMED
    cancelled = Bind(
        reservation(B1, ReservationState.CANCELLED),
        Trigger.PROVIDER_CANCELLED,
        Disposition.SUCCEEDED,
        DispositionBasis.LOOKUP,
    )
    assert trigger_for(BookingState.CANCELLED, cancelled) is None
    assert trigger_for(BookingState.FAILED, cancelled) is Trigger.VERIFIED_RESERVATION_ON_TERMINAL
    assert trigger_for_escalation(BookingState.NEEDS_REVIEW) is None
    assert trigger_for_escalation(BookingState.CONFIRMED) is Trigger.ESCALATE_REVIEW
    assert trigger_for_escalation(BookingState.FAILED) is Trigger.VERIFIED_RESERVATION_ON_TERMINAL


def test_reschedule_only_moves_a_dispatch_marked_booking() -> None:
    assert trigger_for_reschedule(BookingState.CREATED) is None
    assert trigger_for_reschedule(BookingState.SUBMITTING) is Trigger.NOT_DISPATCHED
    assert trigger_for_reschedule(BookingState.CONFIRMING) is Trigger.NOT_DISPATCHED


def test_a_reservation_for_a_never_dispatched_command_is_not_adopted() -> None:
    cmd = note_lookup(new_command())
    decision = decide_create_after_lookup(PROVIDER_B, cmd, (reservation(B1),), lookup_budget=3)
    assert decision == Escalate(REASON_UNEXPECTED, implicated=(B1,))

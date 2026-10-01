from __future__ import annotations

from datetime import timedelta

import pytest

from orchestrator.domain import (
    BookingId,
    BookingState,
    CommandKind,
    Disposition,
    Evidence,
    EvidenceKind,
    Money,
    NotClosable,
    ProviderBookingRef,
    Reservation,
    ReservationState,
    ReviewCase,
    closable_into,
    replay_for,
)
from tests.unit.domain.helpers import T0

# Replay ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("disposition", "state", "status", "code"),
    [
        (Disposition.OPEN, BookingState.SUBMITTING, 202, None),
        (Disposition.OPEN, BookingState.UNKNOWN, 202, None),
        (Disposition.UNRESOLVED, BookingState.NEEDS_REVIEW, 202, None),
        (Disposition.SUCCEEDED, BookingState.CONFIRMED, 201, None),
        (Disposition.SUCCEEDED, BookingState.NEEDS_REVIEW, 201, None),
        (Disposition.SUCCEEDED, BookingState.CANCELLED, 201, None),
        (Disposition.REJECTED, BookingState.FAILED, 422, "booking-rejected"),
        (Disposition.ABANDONED, BookingState.FAILED, 422, "booking-not-submitted"),
    ],
)
def test_create_replay_table_carries_the_current_state(
    disposition: Disposition, state: BookingState, status: int, code: str | None
) -> None:
    replay = replay_for(
        CommandKind.CREATE,
        disposition,
        booking_state=state,
        unresolved_reason="x" if status == 202 else None,
    )
    assert (replay.status, replay.problem_code, replay.booking_state) == (status, code, state)
    assert replay.unresolved == (status == 202)


def test_replay_rejects_impossible_combinations() -> None:
    with pytest.raises(ValueError):
        replay_for(CommandKind.CREATE, Disposition.REFUSED, booking_state=BookingState.CONFIRMED)
    with pytest.raises(ValueError, match="unresolved reason"):
        replay_for(
            CommandKind.CREATE,
            Disposition.SUCCEEDED,
            booking_state=BookingState.CONFIRMED,
            unresolved_reason="no",
        )
    cancel = replay_for(
        CommandKind.CANCEL, Disposition.SUCCEEDED, booking_state=BookingState.CANCELLED
    )
    assert cancel.status == 200
    refused = replay_for(
        CommandKind.CANCEL, Disposition.REFUSED, booking_state=BookingState.CONFIRMED
    )
    assert (refused.status, refused.problem_code) == (409, "booking-not-cancellable")
    with pytest.raises(NotImplementedError):
        replay_for(CommandKind.CONFIRM, Disposition.SUCCEEDED, booking_state=BookingState.CONFIRMED)


# Review ------------------------------------------------------------------------------------

B1, B2 = ProviderBookingRef("B1"), ProviderBookingRef("B2")
BK = BookingId("bk_1")


def res(ref: ProviderBookingRef, state: ReservationState) -> Reservation:
    return Reservation(ref, BK, state, T0)


def ev(
    ref: ProviderBookingRef,
    state: ReservationState | None,
    *,
    at: int = 0,
    valid_for: int | None = None,
    kind: EvidenceKind = EvidenceKind.LOOKUP_BY_REF,
) -> Evidence:
    return Evidence(
        kind=kind,
        initiated_at=T0 + timedelta(seconds=at),
        subject_ref=ref,
        reservation=res(ref, state) if state else None,
        valid_until=T0 + timedelta(seconds=at + valid_for) if valid_for is not None else None,
    )


def case(
    *evidence: Evidence,
    implicated: tuple[ProviderBookingRef, ...] = (B1,),
    outstanding: CommandKind | None = None,
) -> ReviewCase:
    return ReviewCase(
        "test",
        remediable=True,
        outstanding_command=outstanding,
        implicated=implicated,
        evidence=evidence,
    )


def test_no_implicated_reservation_needs_discovery() -> None:
    assert closable_into(case(implicated=()), None, now=T0) == NotClosable(
        "no implicated reservation; discovery by client reference required"
    )


def test_close_into_confirmed_when_bound_reference_is_the_sole_live_one() -> None:
    assert (
        closable_into(case(ev(B1, ReservationState.CONFIRMED)), B1, now=T0)
        is BookingState.CONFIRMED
    )


def test_unbound_booking_cannot_close_into_a_live_state() -> None:
    assert closable_into(case(ev(B1, ReservationState.CONFIRMED)), None, now=T0) == NotClosable(
        "no bound reference; bind through settlement before closing"
    )


def test_wrong_survivor_keeps_the_case_open() -> None:
    """Section 6.6 #38: R1 bound and cancelled, R2 live: never close, never rebind."""
    c = case(
        ev(B1, ReservationState.CANCELLED), ev(B2, ReservationState.CONFIRMED), implicated=(B1, B2)
    )
    assert closable_into(c, B1, now=T0) == NotClosable(
        "the live reservation is not the bound reference"
    )


def test_two_live_reservations_keep_the_case_open() -> None:
    c = case(
        ev(B1, ReservationState.CONFIRMED), ev(B2, ReservationState.CONFIRMED), implicated=(B1, B2)
    )
    assert closable_into(c, B1, now=T0) == NotClosable("more than one live reservation")


def test_a_negative_plain_lookup_never_accounts_for_a_duplicate() -> None:
    """A B lookup that finds nothing does not prove the duplicate is gone."""
    c = case(ev(B1, ReservationState.CONFIRMED), ev(B2, None), implicated=(B1, B2))
    assert closable_into(c, B1, now=T0) == NotClosable("no affirmative evidence for B2")
    c2 = case(ev(B1, ReservationState.CANCELLED), ev(B2, None), implicated=(B1, B2))
    assert closable_into(c2, B1, now=T0) == NotClosable("no affirmative evidence for B2")


def test_missing_evidence_for_an_implicated_reservation_keeps_it_open() -> None:
    assert closable_into(
        case(ev(B1, ReservationState.CONFIRMED), implicated=(B1, B2)), B1, now=T0
    ) == NotClosable("no evidence for B2")


def test_expired_newer_evidence_does_not_revive_older_evidence() -> None:
    """Section 6.1: the newest evidence governs; if it has expired, fresh evidence is required."""
    c = case(
        ev(B1, ReservationState.CONFIRMED, at=0), ev(B1, ReservationState.HELD, at=10, valid_for=10)
    )
    assert closable_into(c, B1, now=T0 + timedelta(seconds=15)) is BookingState.HELD
    assert closable_into(c, B1, now=T0 + timedelta(seconds=30)) == NotClosable(
        "evidence for B1 has expired; fresh evidence required"
    )


def test_hold_evidence_needs_a_validity_bound() -> None:
    assert closable_into(case(ev(B1, ReservationState.HELD)), B1, now=T0) == NotClosable(
        "hold evidence for B1 needs a validity bound"
    )


def test_outstanding_cancel_resumes_when_the_bound_reservation_is_live() -> None:
    resumed = closable_into(
        case(ev(B1, ReservationState.CONFIRMED), outstanding=CommandKind.CANCEL), B1, now=T0
    )
    assert resumed is BookingState.CANCELLING
    done = closable_into(
        case(ev(B1, ReservationState.CANCELLED), outstanding=CommandKind.CANCEL), B1, now=T0
    )
    assert done is BookingState.CANCELLED


def test_no_live_reservation_without_finality_stays_open() -> None:
    """Section 6.6 #45: Provider B cases may stay open indefinitely."""
    assert closable_into(case(ev(B1, None)), B1, now=T0) == NotClosable(
        "no affirmative evidence for B1"
    )


def test_bound_reference_affirmatively_cancelled_closes_cancelled() -> None:
    assert (
        closable_into(case(ev(B1, ReservationState.CANCELLED)), B1, now=T0)
        is BookingState.CANCELLED
    )


def test_superseded_evidence_is_ignored() -> None:
    stale = Evidence(
        EvidenceKind.LOOKUP_BY_REF,
        T0 + timedelta(seconds=10),
        B1,
        res(B1, ReservationState.CANCELLED),
        superseded=True,
    )
    c = case(ev(B1, ReservationState.CONFIRMED, at=0), stale)
    assert closable_into(c, B1, now=T0 + timedelta(seconds=20)) is BookingState.CONFIRMED


def test_evidence_must_be_about_its_subject() -> None:
    with pytest.raises(ValueError):
        Evidence(EvidenceKind.LOOKUP_BY_REF, T0, B1, res(B2, ReservationState.CONFIRMED))


# Money -------------------------------------------------------------------------------------


def test_money_is_integer_minor_units_with_a_currency() -> None:
    assert Money(1050, "EUR") + Money(50, "EUR") == Money(1100, "EUR")
    assert Money(1050, "EUR") - Money(50, "EUR") == Money(1000, "EUR")
    assert Money.zero("GBP") == Money(0, "GBP")
    with pytest.raises(ValueError, match="mismatch"):
        Money(1, "EUR") + Money(1, "GBP")
    with pytest.raises(TypeError):
        Money(1.5, "EUR")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ISO 4217"):
        Money(1, "eur")

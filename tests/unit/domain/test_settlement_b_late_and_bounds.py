"""Late results, bounded safe failures, and strict identity."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from orchestrator.domain import (
    AttemptOutcome,
    Bind,
    BookingId,
    BookingState,
    CreateIntent,
    Disposition,
    DispositionBasis,
    Escalate,
    InvariantError,
    ProviderBookingRef,
    ProviderResult,
    Reservation,
    ReservationState,
    SideEffect,
    bind,
    decide_create_after_lookup,
    decide_exhausted,
    decide_late_result,
    note_lookup,
    record_attempt,
    settle,
)
from orchestrator.domain.settlement import (
    REASON_DUPLICATE,
    REASON_EXHAUSTED,
    REASON_IDENTITY,
    REASON_SETTLED_CONTRADICTED,
)
from tests.unit.domain.helpers import PROVIDER_B, T0, attempt, new_command, open_attempt

BK = BookingId("bk_1")
B1 = ProviderBookingRef("B1")
B2 = ProviderBookingRef("B2")


def _confirmed(ref: ProviderBookingRef) -> Reservation:
    return Reservation(ref, BK, ReservationState.CONFIRMED, T0)


def _settled_by_lookup() -> object:
    """An attempt in flight, then a lookup that found and bound B1."""
    cmd = record_attempt(new_command(), open_attempt(1))
    cmd = bind(cmd, B1)
    return settle(cmd, Disposition.SUCCEEDED, DispositionBasis.LOOKUP, caps=PROVIDER_B)


def test_a_settled_command_still_closes_its_own_unfinished_attempt() -> None:
    cmd = _settled_by_lookup()
    finished = attempt(1, AttemptOutcome.SUCCESS, SideEffect.NONE)
    closed = record_attempt(cmd, finished)  # type: ignore[arg-type]
    assert closed.attempts[0].finished_at is not None
    with pytest.raises(InvariantError, match="no further attempts"):
        record_attempt(closed, open_attempt(2))
    with pytest.raises(InvariantError, match="no further attempts"):
        record_attempt(cmd, attempt(2, AttemptOutcome.SUCCESS, SideEffect.NONE))  # type: ignore[arg-type]


def test_late_result_agreeing_with_the_binding_is_a_no_op() -> None:
    cmd = _settled_by_lookup()
    assert (
        decide_late_result(cmd, ProviderResult(SideEffect.NONE, reservation=_confirmed(B1))) is None
    )  # type: ignore[arg-type]
    assert decide_late_result(cmd, ProviderResult(SideEffect.POSSIBLE)) is None  # type: ignore[arg-type]


def test_late_result_with_another_reference_or_a_rejection_contradicts() -> None:
    cmd = _settled_by_lookup()
    other = decide_late_result(cmd, ProviderResult(SideEffect.NONE, reservation=_confirmed(B2)))  # type: ignore[arg-type]
    assert other == Escalate(REASON_DUPLICATE, implicated=(B1, B2))
    rejected = decide_late_result(cmd, ProviderResult(SideEffect.NONE, definitive_rejection=True))  # type: ignore[arg-type]
    assert rejected == Escalate(REASON_SETTLED_CONTRADICTED, implicated=(B1,))
    with pytest.raises(InvariantError):
        decide_late_result(new_command(), ProviderResult(SideEffect.NONE))


def test_exhausted_safe_failures_escalate_never_abandon_and_never_after_a_possible_effect() -> None:
    cmd = new_command()
    for n in range(1, 4):
        cmd = record_attempt(cmd, attempt(n, AttemptOutcome.UNKNOWN, SideEffect.NONE))
    assert decide_exhausted(cmd, max_attempts=4) is None
    assert decide_exhausted(cmd, max_attempts=3) == Escalate(
        REASON_EXHAUSTED, implicated=(), remediable=True
    )
    with pytest.raises(InvariantError, match="ever dispatched"):
        settle(cmd, Disposition.ABANDONED, DispositionBasis.LOCAL, caps=PROVIDER_B)
    parked = replace(cmd, disposition=Disposition.UNRESOLVED, basis=DispositionBasis.LOCAL)
    assert decide_exhausted(parked, max_attempts=1) is None, "not OPEN: nothing to decide"

    risky = record_attempt(cmd, attempt(4, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    assert decide_exhausted(risky, max_attempts=1) is None

    denied = new_command()
    for n in range(1, 4):
        denied = record_attempt(
            denied,
            attempt(n, AttemptOutcome.NOT_DISPATCHED, SideEffect.NOT_DISPATCHED, marked=False),
        )
    assert decide_exhausted(denied, max_attempts=3) is None, "never dispatched: age decides"


def test_review_closure_requires_the_live_reservation_to_be_verifiably_ours() -> None:
    from orchestrator.domain import Evidence, EvidenceKind, NotClosable, ReviewCase, closable_into

    intent = CreateIntent(
        "off_1", ("Ada",), "a@x.io", product_ref="J1", service_date=date(2026, 6, 15)
    )
    cmd = bind(replace(new_command(), intent=intent), B1)
    wrong = Reservation(
        B1, BK, ReservationState.CONFIRMED, T0, product_ref="J2", service_date=date(2026, 6, 15)
    )
    right = Reservation(
        B1, BK, ReservationState.CONFIRMED, T0, product_ref="J1", service_date=date(2026, 6, 15)
    )
    case_wrong = ReviewCase(
        "x", True, None, (B1,), (Evidence(EvidenceKind.OBSERVATION, T0, B1, wrong),)
    )
    case_right = ReviewCase(
        "x", True, None, (B1,), (Evidence(EvidenceKind.OBSERVATION, T0, B1, right),)
    )
    assert closable_into(case_wrong, B1, now=T0) is BookingState.CONFIRMED, (
        "without the command: refs only"
    )
    assert isinstance(closable_into(case_wrong, B1, now=T0, command=cmd), NotClosable)
    assert closable_into(case_right, B1, now=T0, command=cmd) is BookingState.CONFIRMED


def test_identity_the_command_expects_must_be_present_to_bind() -> None:
    intent = CreateIntent(
        "off_1", ("Ada",), "a@x.io", product_ref="J1", service_date=date(2026, 6, 15)
    )
    cmd = replace(new_command(), intent=intent)
    cmd = note_lookup(record_attempt(cmd, attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE)))
    anonymous = _confirmed(B1)  # no product, no date: unverifiable
    assert decide_create_after_lookup(PROVIDER_B, cmd, (anonymous,), lookup_budget=3) == Escalate(
        REASON_IDENTITY, implicated=(B1,)
    )
    verified = Reservation(
        B1, BK, ReservationState.CONFIRMED, T0, product_ref="J1", service_date=date(2026, 6, 15)
    )
    assert isinstance(
        decide_create_after_lookup(PROVIDER_B, cmd, (verified,), lookup_budget=3), Bind
    )

"""Evidence rules: exclusion of in-flight attempts, late
responses that cannot reopen a settled question, rejections that are read rather than presumed,
watermarks and regressions in observation ordering, finality of a key-bound terminal read."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from orchestrator.domain import (
    AttemptOutcome,
    Bind,
    BookingId,
    BookingState,
    CommandKind,
    Disposition,
    DispositionBasis,
    InvariantError,
    ProviderBookingRef,
    ProviderResult,
    Reservation,
    ReservationState,
    SideEffect,
    Uncertain,
    decide_create_after_lookup,
    exclude_all_possible,
    exclude_attempt,
    record_attempt,
    settle,
)
from orchestrator.domain.confirmation import (
    AttemptExcluded,
    HoldExpired,
    Reconcile,
    decide_confirm_after_attempt,
)
from orchestrator.domain.observations import ObservationOutcome, order_observation
from orchestrator.providers.bus_legacy import BUS_LEGACY_CAPABILITIES as B
from orchestrator.providers.mobility_async import MOBILITY_ASYNC_CAPABILITIES as C
from orchestrator.providers.rail_osdm import RAIL_OSDM_CAPABILITIES as A
from tests.unit.domain.helpers import T0, attempt, attempt_id, new_command, open_attempt

BK = BookingId("bk_1")
R1 = ProviderBookingRef("MB1")


def test_an_in_flight_attempt_is_excluded_by_evidence_and_its_late_response_keeps_that() -> None:
    cmd = record_attempt(new_command(), open_attempt(1))
    assert cmd.possibly_executed, "journaled, dispatched, no answer: it may have executed"
    settled = exclude_all_possible(cmd, at=T0 + timedelta(seconds=5))
    assert not settled.possibly_executed, "the key's outcome accounts for it, answer or not"
    assert settled.attempts[0].finished_at is None, "its record stays open for the response"

    late = replace(
        open_attempt(1),
        finished_at=T0 + timedelta(seconds=9),
        outcome=AttemptOutcome.UNKNOWN,
        side_effect=SideEffect.POSSIBLE,
    )
    closed = record_attempt(settled, late)
    assert closed.attempts[0].finished_at is not None
    assert closed.attempts[0].excluded_at == T0 + timedelta(seconds=5), (
        "the late response closes the record but never reopens the question of its effect"
    )
    assert not closed.possibly_executed

    never_sent = record_attempt(
        new_command(), attempt(1, AttemptOutcome.NOT_DISPATCHED, None, marked=False)
    )
    with pytest.raises(InvariantError):
        exclude_attempt(never_sent, attempt_id(1), at=T0)


def test_a_rejection_that_is_not_an_expiry_is_read_not_presumed() -> None:
    cmd = new_command(kind=CommandKind.CONFIRM)
    cmd = replace(cmd, submission_ref=R1)
    rejected = attempt(1, AttemptOutcome.REJECTED, SideEffect.NONE)
    cmd = record_attempt(cmd, rejected)
    plain = ProviderResult(SideEffect.NONE, definitive_rejection=True)
    assert decide_confirm_after_attempt(cmd, attempt_id(1), plain, bound=R1) == Reconcile(
        "confirmation-rejected"
    )
    expired_request = ProviderResult(
        SideEffect.NONE, definitive_rejection=True, request_expired=True
    )
    assert isinstance(
        decide_confirm_after_attempt(cmd, attempt_id(1), expired_request, bound=R1),
        AttemptExcluded,
    ), "our request expired before execution: the hold lives on"
    expired_hold = ProviderResult(SideEffect.NONE, definitive_rejection=True, hold_expired=True)
    assert isinstance(
        decide_confirm_after_attempt(cmd, attempt_id(1), expired_hold, bound=R1), HoldExpired
    )
    with_doubt = record_attempt(cmd, open_attempt(2))
    assert decide_confirm_after_attempt(with_doubt, attempt_id(1), plain, bound=R1) == Uncertain()


def _obs(state: ReservationState, generation: int, revision: int) -> Reservation:
    return Reservation(R1, BK, state, T0, generation=generation, revision=revision)


def test_no_change_advances_the_watermark_and_a_regressed_read_is_quarantined() -> None:
    same_fact = order_observation(
        _obs(ReservationState.PENDING, 1, 1),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=1,
        last_revision=1,
        authoritative=True,
    )
    assert same_fact.outcome is ObservationOutcome.NO_CHANGE
    newer_same_state = order_observation(
        _obs(ReservationState.PENDING, 2, 1),
        bound_ref=R1,
        state=BookingState.PENDING_PROVIDER,
        generation=1,
        last_revision=1,
        authoritative=True,
    )
    assert newer_same_state.outcome is ObservationOutcome.NO_CHANGE
    assert newer_same_state.advances_watermark, "generation 2 is adopted from the read"
    behind = order_observation(
        _obs(ReservationState.CONFIRMED, 1, 1),
        bound_ref=R1,
        state=BookingState.CONFIRMED,
        generation=1,
        last_revision=3,
        authoritative=True,
    )
    assert behind.outcome is ObservationOutcome.REGRESSED, "the provider lost its own history"
    pushed_behind = order_observation(
        _obs(ReservationState.CONFIRMED, 1, 1),
        bound_ref=R1,
        state=BookingState.CONFIRMED,
        generation=1,
        last_revision=3,
        authoritative=False,
    )
    assert pushed_behind.outcome is ObservationOutcome.STALE, "an old event is simply late"
    same_revision_other_state = order_observation(
        _obs(ReservationState.FAILED, 1, 3),
        bound_ref=R1,
        state=BookingState.CONFIRMED,
        generation=1,
        last_revision=3,
        authoritative=True,
    )
    assert same_revision_other_state.outcome is ObservationOutcome.REGRESSED


def test_a_terminal_reservation_of_a_key_bound_provider_with_finality_is_final() -> None:
    cmd = record_attempt(new_command(), open_attempt(1))
    failed = Reservation(R1, BK, ReservationState.FAILED, T0, generation=1, revision=2)
    for caps in (C, A):
        decision = decide_create_after_lookup(caps, cmd, (failed,), lookup_budget=3)
        assert isinstance(decision, Bind) and decision.disposition is Disposition.REJECTED
        assert decision.basis is DispositionBasis.FENCED_LOOKUP, (
            "the one reservation the key produced is terminal; nothing else can carry the key"
        )
        settled = settle(
            exclude_all_possible(cmd, at=T0), Disposition.REJECTED, decision.basis, caps=caps
        )
        assert settled.disposition is Disposition.REJECTED
    without_key = decide_create_after_lookup(B, cmd, (failed,), lookup_budget=3)
    assert isinstance(without_key, Bind) and without_key.basis is DispositionBasis.LOOKUP, (
        "without key binding and finality a plain lookup stays a plain lookup"
    )


def test_a_live_reservation_of_another_client_does_not_keep_a_case_open() -> None:
    from orchestrator.domain.review import Evidence, EvidenceKind, ReviewCase, closable_into

    ours = Reservation(R1, BK, ReservationState.PENDING, T0, generation=1, revision=1)
    theirs = Reservation(
        ProviderBookingRef("MB9"), BookingId("bk_other"), ReservationState.CONFIRMED, T0
    )
    case = ReviewCase(
        reason="identity",
        remediable=True,
        outstanding_command=CommandKind.CREATE,
        implicated=(R1, theirs.ref),
        evidence=(
            Evidence(EvidenceKind.LOOKUP_BY_REF, T0, R1, ours),
            Evidence(EvidenceKind.LOOKUP_BY_REF, T0, theirs.ref, theirs),
        ),
    )
    cmd = new_command()
    assert closable_into(case, R1, now=T0, command=cmd) is BookingState.PENDING_PROVIDER, (
        "the other client's reservation is accounted for; ours is the sole live one"
    )
    from orchestrator.domain.review import NotClosable

    assert isinstance(closable_into(case, R1, now=T0, command=None), NotClosable), (
        "without the command, two live reservations cannot be told apart"
    )


def test_pushed_events_never_close_a_case_and_cannot_outrank_a_read() -> None:
    from orchestrator.domain.review import (
        Evidence,
        EvidenceKind,
        NotClosable,
        ReviewCase,
        closable_into,
    )

    read_gen2 = Reservation(R1, BK, ReservationState.CONFIRMED, T0, generation=2, revision=2)
    pushed_gen1 = Reservation(R1, BK, ReservationState.FAILED, T0, generation=1, revision=9)
    later = T0 + timedelta(minutes=1)
    case = ReviewCase(
        reason="observation-from-a-newer-generation",
        remediable=True,
        outstanding_command=CommandKind.CREATE,
        implicated=(R1,),
        evidence=(
            Evidence(EvidenceKind.LOOKUP_BY_REF, T0, R1, read_gen2),
            Evidence(EvidenceKind.WEBHOOK, later, R1, pushed_gen1),  # received later, older fact
        ),
    )
    assert closable_into(case, R1, now=later, command=new_command()) is BookingState.CONFIRMED, (
        "the authoritative generation-2 read decides, not the later-received generation-1 event"
    )
    pushed_gen5 = Reservation(R1, BK, ReservationState.FAILED, T0, generation=5, revision=1)
    louder = replace(
        case, evidence=(*case.evidence, Evidence(EvidenceKind.WEBHOOK, later, R1, pushed_gen5))
    )
    assert isinstance(closable_into(louder, R1, now=later, command=new_command()), NotClosable), (
        "a pushed event claiming a newer generation than any read keeps the case open"
    )
    only_pushed = replace(case, evidence=(Evidence(EvidenceKind.WEBHOOK, later, R1, read_gen2),))
    assert isinstance(closable_into(only_pushed, R1, now=later, command=new_command()), NotClosable)

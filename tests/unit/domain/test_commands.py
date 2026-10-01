from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from orchestrator.domain import (
    Attempt,
    AttemptOutcome,
    Command,
    CommandId,
    CommandKind,
    Disposition,
    DispositionBasis,
    InvariantError,
    NaiveDatetimeError,
    ProviderBookingRef,
    SideEffect,
    bind,
    dispatch_allowed,
    record_attempt,
    settle,
)
from tests.unit.domain.helpers import (
    DEFAULT_BOOKING,
    DEFAULT_COMMAND,
    INTENT,
    PROVIDER_B,
    REQUEST,
    T0,
    attempt,
    attempt_id,
    new_command,
    open_attempt,
)


def test_journaled_attempt_is_possible_until_the_provider_answers_for_it() -> None:
    cmd = record_attempt(new_command(), open_attempt(1))
    assert cmd.possibly_executed, "an open dispatch-marked attempt is POSSIBLE by definition"
    assert cmd.first_dispatch_at == T0 + timedelta(seconds=1)

    answered = replace(
        open_attempt(1),
        finished_at=T0 + timedelta(seconds=2),
        outcome=AttemptOutcome.REJECTED,
        side_effect=SideEffect.NONE,
    )
    cmd = record_attempt(cmd, answered)
    assert not cmd.possibly_executed, "a definitive answer for the same attempt clears it"
    assert cmd.max_side_effect is SideEffect.NONE


def test_possible_effect_from_an_earlier_attempt_is_sticky() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    cmd = record_attempt(
        cmd, attempt(2, AttemptOutcome.NOT_DISPATCHED, SideEffect.NOT_DISPATCHED, marked=False)
    )
    cmd = record_attempt(cmd, attempt(3, AttemptOutcome.REJECTED, SideEffect.NONE))
    assert cmd.possibly_executed
    assert cmd.other_attempts_possibly_executed(attempt_id(3))
    assert not cmd.other_attempts_possibly_executed(attempt_id(1))


def test_first_dispatch_anchor_is_never_renewed() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    anchor = cmd.first_dispatch_at
    cmd = record_attempt(cmd, attempt(2, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    assert cmd.first_dispatch_at == anchor


def test_dispatch_history_cannot_be_rewritten() -> None:
    cmd = record_attempt(new_command(), open_attempt(1))
    erased = replace(
        open_attempt(1),
        dispatch_marked_at=None,
        finished_at=T0,
        outcome=AttemptOutcome.NOT_DISPATCHED,
        side_effect=SideEffect.NOT_DISPATCHED,
    )
    with pytest.raises(InvariantError, match="dispatch mark"):
        record_attempt(cmd, erased)
    with pytest.raises(InvariantError, match="immutable"):
        record_attempt(cmd, replace(open_attempt(1), n=2))
    with pytest.raises(InvariantError, match="immutable"):
        record_attempt(cmd, replace(open_attempt(1), request=replace(REQUEST, payload=())))

    done = record_attempt(cmd, attempt(1, AttemptOutcome.REJECTED, SideEffect.NONE))
    with pytest.raises(InvariantError, match="finished attempt is immutable"):
        record_attempt(done, attempt(1, AttemptOutcome.SUCCESS, SideEffect.NONE))
    with pytest.raises(InvariantError, match="already exists"):
        record_attempt(
            done, replace(attempt(1, AttemptOutcome.REJECTED, SideEffect.NONE), id=attempt_id(9))
        )
    unmarked_done = record_attempt(
        new_command(),
        attempt(1, AttemptOutcome.NOT_DISPATCHED, SideEffect.NOT_DISPATCHED, marked=False),
    )
    with pytest.raises(InvariantError, match="finished attempt is immutable"):
        record_attempt(
            unmarked_done, attempt(1, AttemptOutcome.NOT_DISPATCHED, SideEffect.NOT_DISPATCHED)
        )
    with pytest.raises(InvariantError, match="belongs to"):
        record_attempt(new_command(), replace(open_attempt(1), command_id=CommandId("cmd_other")))


def test_abandoned_requires_zero_dispatch() -> None:
    never = record_attempt(
        new_command(),
        attempt(1, AttemptOutcome.NOT_DISPATCHED, SideEffect.NOT_DISPATCHED, marked=False),
    )
    abandoned = settle(never, Disposition.ABANDONED, DispositionBasis.LOCAL, caps=PROVIDER_B)
    assert abandoned.disposition is Disposition.ABANDONED

    dispatched = record_attempt(
        new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE)
    )
    with pytest.raises(InvariantError):
        settle(dispatched, Disposition.ABANDONED, DispositionBasis.LOCAL, caps=PROVIDER_B)
    with pytest.raises(InvariantError):
        settle(never, Disposition.REJECTED, DispositionBasis.LOCAL, caps=PROVIDER_B)
    with pytest.raises(InvariantError):
        settle(never, Disposition.ABANDONED, DispositionBasis.PROVIDER_RESULT, caps=PROVIDER_B)


def test_negative_settlement_rules() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    cmd = record_attempt(cmd, attempt(2, AttemptOutcome.REJECTED, SideEffect.NONE))
    with pytest.raises(InvariantError, match="may have executed"):
        settle(cmd, Disposition.REJECTED, DispositionBasis.PROVIDER_RESULT, caps=PROVIDER_B)
    with pytest.raises(InvariantError, match="plain lookup"):
        settle(cmd, Disposition.REJECTED, DispositionBasis.LOOKUP, caps=PROVIDER_B)
    with pytest.raises(InvariantError, match="no fenced lookup"):
        settle(cmd, Disposition.REJECTED, DispositionBasis.FENCED_LOOKUP, caps=PROVIDER_B)


def test_definitive_rejection_without_prior_effect_settles_and_freezes() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.REJECTED, SideEffect.NONE))
    settled = settle(cmd, Disposition.REJECTED, DispositionBasis.PROVIDER_RESULT, caps=PROVIDER_B)
    assert settled.is_settled
    with pytest.raises(InvariantError):
        record_attempt(settled, attempt(2, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    with pytest.raises(InvariantError):
        settle(settled, Disposition.SUCCEEDED, DispositionBasis.PROVIDER_RESULT, caps=PROVIDER_B)
    assert not dispatch_allowed(PROVIDER_B, settled)


def test_success_requires_a_bound_reference() -> None:
    cmd = record_attempt(new_command(), attempt(1, AttemptOutcome.SUCCESS, SideEffect.NONE))
    with pytest.raises(InvariantError, match="bound"):
        settle(cmd, Disposition.SUCCEEDED, DispositionBasis.PROVIDER_RESULT, caps=PROVIDER_B)
    bound = bind(cmd, ProviderBookingRef("B1"))
    settled = settle(
        bound, Disposition.SUCCEEDED, DispositionBasis.PROVIDER_RESULT, caps=PROVIDER_B
    )
    assert settled.is_settled


def test_bound_reference_is_immutable() -> None:
    cmd = bind(new_command(), ProviderBookingRef("B1"))
    assert bind(cmd, ProviderBookingRef("B1")).submission_ref == "B1"
    with pytest.raises(InvariantError):
        bind(cmd, ProviderBookingRef("B2"))


def test_no_mutating_dispatch_after_a_possible_effect_at_provider_b() -> None:
    fresh = new_command()
    assert dispatch_allowed(PROVIDER_B, fresh)
    uncertain = record_attempt(fresh, attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    assert not dispatch_allowed(PROVIDER_B, uncertain)
    rejected = record_attempt(fresh, attempt(1, AttemptOutcome.REJECTED, SideEffect.NONE))
    assert dispatch_allowed(PROVIDER_B, rejected), "a definitive answer leaves no possible effect"


def test_unmarked_attempt_can_only_be_not_dispatched() -> None:
    with pytest.raises(InvariantError):
        record_attempt(
            new_command(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE, marked=False)
        )
    with pytest.raises(InvariantError, match="must have an outcome"):
        record_attempt(
            new_command(), replace(open_attempt(1), finished_at=T0 + timedelta(seconds=5))
        )


def test_naive_timestamps_are_rejected() -> None:
    naive = datetime(2026, 1, 1)
    with pytest.raises(NaiveDatetimeError):
        Command(
            id=DEFAULT_COMMAND,
            booking_id=DEFAULT_BOOKING,
            kind=CommandKind.CREATE,
            intent=INTENT,
            provider_key="k",
            created_at=naive,
        )
    with pytest.raises(NaiveDatetimeError):
        Attempt(
            id=attempt_id(1),
            command_id=DEFAULT_COMMAND,
            n=1,
            request=REQUEST,
            dispatch_marked_at=naive,
        )

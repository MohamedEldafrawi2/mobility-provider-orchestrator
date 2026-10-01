"""The transition table is checked against an expected table transcribed independently from
the diagram in docs/booking-state-machine.md, so a change to either shows up here."""

from __future__ import annotations

from collections import deque

from hypothesis import given
from hypothesis import strategies as st

from orchestrator.domain import (
    TERMINAL_STATES,
    BookingState,
    InvalidTransition,
    Trigger,
    transition,
)
from orchestrator.domain.states import TRANSITIONS

S, T = BookingState, Trigger

# (from, trigger, to), transcribed from the diagram.
EXPECTED: set[tuple[BookingState, Trigger, BookingState]] = {
    (S.CREATED, T.ATTEMPT_DISPATCHED, S.SUBMITTING),
    (S.CREATED, T.ABANDONED, S.FAILED),
    (S.CREATED, T.ESCALATE_REVIEW, S.NEEDS_REVIEW),
    (S.SUBMITTING, T.NOT_DISPATCHED, S.CREATED),
    (S.SUBMITTING, T.PROVIDER_CONFIRMED, S.CONFIRMED),
    (S.SUBMITTING, T.PROVIDER_HELD, S.HELD),
    (S.SUBMITTING, T.PROVIDER_PENDING, S.PENDING_PROVIDER),
    (S.SUBMITTING, T.PROVIDER_REJECTED, S.FAILED),
    (S.SUBMITTING, T.PROVIDER_CANCELLED, S.CANCELLED),
    (S.SUBMITTING, T.OUTCOME_UNCERTAIN, S.UNKNOWN),
    (S.SUBMITTING, T.ESCALATE_REVIEW, S.NEEDS_REVIEW),
    (S.HELD, T.ATTEMPT_DISPATCHED, S.CONFIRMING),
    (S.HELD, T.HOLD_EXPIRED, S.FAILED),
    (S.HELD, T.ESCALATE_REVIEW, S.NEEDS_REVIEW),
    (S.CONFIRMING, T.NOT_DISPATCHED, S.HELD),
    (S.CONFIRMING, T.ATTEMPT_EXCLUDED, S.HELD),
    (S.CONFIRMING, T.PROVIDER_CONFIRMED, S.CONFIRMED),
    (S.CONFIRMING, T.HOLD_EXPIRED, S.FAILED),
    (S.CONFIRMING, T.OUTCOME_UNCERTAIN, S.UNKNOWN),
    (S.CONFIRMING, T.ESCALATE_REVIEW, S.NEEDS_REVIEW),
    (S.UNKNOWN, T.PROVIDER_CONFIRMED, S.CONFIRMED),
    (S.UNKNOWN, T.PROVIDER_HELD, S.HELD),
    (S.UNKNOWN, T.PROVIDER_PENDING, S.PENDING_PROVIDER),
    (S.UNKNOWN, T.PROVIDER_REJECTED, S.FAILED),
    (S.UNKNOWN, T.PROVIDER_CANCELLED, S.CANCELLED),
    (S.UNKNOWN, T.ESCALATE_REVIEW, S.NEEDS_REVIEW),
    (S.PENDING_PROVIDER, T.PROVIDER_CONFIRMED, S.CONFIRMED),
    (S.PENDING_PROVIDER, T.PROVIDER_REJECTED, S.FAILED),
    (S.PENDING_PROVIDER, T.ESCALATE_REVIEW, S.NEEDS_REVIEW),
    (S.CONFIRMED, T.CANCEL_ACCEPTED, S.CANCELLING),
    (S.CONFIRMED, T.ESCALATE_REVIEW, S.NEEDS_REVIEW),
    (S.CANCELLING, T.PROVIDER_CANCELLED, S.CANCELLED),
    (S.CANCELLING, T.CANCEL_REFUSED, S.CONFIRMED),
    (S.CANCELLING, T.ESCALATE_REVIEW, S.NEEDS_REVIEW),
    (S.NEEDS_REVIEW, T.PROVIDER_CONFIRMED, S.CONFIRMED),
    (S.NEEDS_REVIEW, T.PROVIDER_HELD, S.HELD),
    (S.NEEDS_REVIEW, T.PROVIDER_PENDING, S.PENDING_PROVIDER),
    (S.NEEDS_REVIEW, T.CANCEL_RESUMED, S.CANCELLING),
    (S.NEEDS_REVIEW, T.PROVIDER_CANCELLED, S.CANCELLED),
    (S.NEEDS_REVIEW, T.PROVIDER_REJECTED, S.FAILED),
    (S.FAILED, T.VERIFIED_RESERVATION_ON_TERMINAL, S.NEEDS_REVIEW),
    (S.CANCELLED, T.VERIFIED_RESERVATION_ON_TERMINAL, S.NEEDS_REVIEW),
}


def test_table_matches_the_specification_exactly() -> None:
    actual = {(src, trig, dst) for (src, trig), dst in TRANSITIONS.items()}
    assert actual == EXPECTED


def test_transition_is_total_and_consistent_with_the_table() -> None:
    for state in S:
        for trigger in T:
            result = transition(state, trigger)
            expected = next((dst for s, t, dst in EXPECTED if s is state and t is trigger), None)
            if expected is None:
                assert result == InvalidTransition(state, trigger)
            else:
                assert result is expected


def test_every_state_is_reachable_from_created() -> None:
    seen = {S.CREATED}
    frontier = deque([S.CREATED])
    while frontier:
        state = frontier.popleft()
        for src, _, dst in EXPECTED:
            if src is state and dst not in seen:
                seen.add(dst)
                frontier.append(dst)
    assert seen == set(S)


def test_terminal_states_reopen_only_on_verified_evidence() -> None:
    for state in TERMINAL_STATES:
        for trigger in T:
            result = transition(state, trigger)
            if trigger is T.VERIFIED_RESERVATION_ON_TERMINAL:
                assert result is S.NEEDS_REVIEW
            else:
                assert isinstance(result, InvalidTransition)


def test_failed_is_never_reached_by_silence() -> None:
    into_failed = {trig for (_, trig), dst in TRANSITIONS.items() if dst is S.FAILED}
    assert into_failed == {T.PROVIDER_REJECTED, T.HOLD_EXPIRED, T.ABANDONED}
    assert transition(S.UNKNOWN, T.ABANDONED) == InvalidTransition(S.UNKNOWN, T.ABANDONED)


def test_review_is_reachable_from_every_non_terminal_state() -> None:
    """Section 6.4: contradictory evidence opens a review case from any state."""
    for state in S:
        if state in TERMINAL_STATES or state is S.NEEDS_REVIEW:
            continue
        assert transition(state, T.ESCALATE_REVIEW) is S.NEEDS_REVIEW, state


def test_cancellation_requires_confirmed() -> None:
    for state in S:
        assert (transition(state, T.CANCEL_ACCEPTED) is S.CANCELLING) == (state is S.CONFIRMED)


@given(st.lists(st.sampled_from(list(T)), max_size=30))
def test_random_trigger_walks_stay_consistent(triggers: list[Trigger]) -> None:
    state = S.CREATED
    for trigger in triggers:
        nxt = transition(state, trigger)
        if isinstance(nxt, InvalidTransition):
            assert nxt.state is state and nxt.trigger is trigger
            continue
        assert (state, trigger, nxt) in EXPECTED
        state = nxt

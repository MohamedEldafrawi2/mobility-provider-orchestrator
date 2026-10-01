"""The domain's Provider B lifecycle must agree with the independent model.

The harness drives the *real* lifecycle: it journals an attempt (open, dispatch-marked), lets
the model decide what the provider did, records the finished attempt, applies the domain's
decision through the transition table, runs lookups, injects duplicates, and abandons. After
every step it checks the command disposition and basis, the booking state, the bound reference,
the replay status, and the safe-dispatch rule against the model.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from hypothesis import example, given, settings
from hypothesis import strategies as st

from orchestrator.domain import (
    Abandon,
    Attempt,
    AttemptId,
    AttemptOutcome,
    Bind,
    BookingId,
    BookingState,
    Command,
    CommandId,
    Disposition,
    DispositionBasis,
    Escalate,
    InvalidTransition,
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
    decide_exhausted,
    dispatch_allowed,
    note_lookup,
    record_attempt,
    replay_for,
    settle,
    transition,
    trigger_for,
    trigger_for_escalation,
    trigger_for_reschedule,
)
from orchestrator.domain.commands import CommandKind
from orchestrator.domain.review import (
    Evidence,
    EvidenceKind,
    NotClosable,
    ReviewCase,
    closable_into,
)
from tests.model.b_reference_model import BReferenceModel, DispatchMode
from tests.unit.domain.helpers import PROVIDER_B, REQUEST, T0, new_command

LOOKUP_BUDGET = 3
MAX_AGE = timedelta(minutes=5)
MAX_ATTEMPTS = 3

steps = st.lists(
    st.sampled_from(["dispatch", "lookup", "expose", "duplicate", "abandon", "review"]),
    max_size=8,
)
modes = st.lists(st.sampled_from(list(DispatchMode)), min_size=1, max_size=9)
# Ages around the abandonment boundary, generated independently of both implementations.
ages = st.lists(st.sampled_from([-2, -1, 0, 1, 60]), max_size=8)


class Harness:
    """Runs the platform side of one CREATE command against the model's provider truth."""

    def __init__(self) -> None:
        self.model = BReferenceModel(
            client_ref="bk_model",
            lookup_budget=LOOKUP_BUDGET,
            max_age=MAX_AGE,
            max_attempts=MAX_ATTEMPTS,
        )
        self.command: Command = new_command(
            CommandKind.CREATE, BookingId("bk_model"), CommandId("cmd_model")
        )
        self.state = BookingState.CREATED
        self.clock = T0
        self.next_n = 1
        self.seen: list[ProviderBookingRef] = []  # every reference the domain was ever shown

    # Steps ---------------------------------------------------------------------------------

    def dispatch(self, mode: DispatchMode) -> None:
        if mode is DispatchMode.LOCAL_DENIAL:
            # Admission can deny a submission the worker picked up, including one that is
            # still uncertain: the denial is journaled and must not disturb the uncertainty.
            if self.command.disposition is not Disposition.OPEN:
                return  # the worker never picks up a settled or unresolved command
        elif not dispatch_allowed(PROVIDER_B, self.command):
            return  # the platform refuses; the model must never see an unsafe dispatch
        n, self.next_n = self.next_n, self.next_n + 1
        att_id = AttemptId(f"att_{n}")
        self.clock += timedelta(seconds=1)
        if mode is DispatchMode.LOCAL_DENIAL:
            denied = Attempt(
                att_id,
                self.command.id,
                n,
                REQUEST,
                None,
                self.clock,
                AttemptOutcome.NOT_DISPATCHED,
                SideEffect.NOT_DISPATCHED,
            )
            self.command = record_attempt(self.command, denied)
            self.model.dispatch(mode)
            self._apply(
                decide_create_after_attempt(
                    PROVIDER_B, self.command, att_id, ProviderResult(SideEffect.NOT_DISPATCHED)
                )
            )
            return
        # Journal first: the attempt is dispatch-marked before any network IO.
        self.command = record_attempt(
            self.command, Attempt(att_id, self.command.id, n, REQUEST, self.clock)
        )
        self._transition(Trigger.ATTEMPT_DISPATCHED)
        self.model.dispatch(mode)
        result = self._provider_result(mode)
        finished = Attempt(
            att_id,
            self.command.id,
            n,
            REQUEST,
            self.clock,
            self.clock + timedelta(seconds=1),
            self._outcome(result),
            result.side_effect,
        )
        self.command = record_attempt(self.command, finished)
        if result.reservation is not None:
            self._note_seen(result.reservation.ref)
        self._apply(decide_create_after_attempt(PROVIDER_B, self.command, att_id, result))

    def lookup(self) -> None:
        if self.command.is_settled and self.command.disposition is not Disposition.SUCCEEDED:
            return
        reservations = self._observe()
        self.command = note_lookup(self.command)
        self._apply(
            decide_create_after_lookup(
                PROVIDER_B, self.command, reservations, lookup_budget=LOOKUP_BUDGET
            )
        )

    def review(self) -> None:
        """An operator reconciles, then tries to resolve: closure must follow the evidence."""
        if self.state is not BookingState.NEEDS_REVIEW:
            return
        self.clock += timedelta(seconds=1)
        found = {r.ref: r for r in self._observe()}
        # Reconciliation first lets settlement bind a single discovered reservation.
        self.command = note_lookup(self.command)
        self._apply(
            decide_create_after_lookup(
                PROVIDER_B, self.command, tuple(found.values()), lookup_budget=10**9
            )
        )
        evidence = tuple(
            Evidence(EvidenceKind.LOOKUP_BY_CLIENT_REF, self.clock, ref, found.get(ref))
            for ref in self.seen
        )
        case = ReviewCase("model", True, CommandKind.CREATE, tuple(self.seen), evidence)
        target = closable_into(case, self.command.submission_ref, now=self.clock)
        expected = self.model.review_closable_into()
        if isinstance(target, NotClosable):
            assert expected is None, (
                f"domain keeps the case open ({target.reason}) but the model closes {expected}"
            )
            return
        assert target.value == expected, f"domain closes into {target}; model: {expected}"
        self.model.close_review()
        if self.command.disposition is not Disposition.SUCCEEDED:
            self.command = settle(
                self.command, Disposition.SUCCEEDED, DispositionBasis.LOOKUP, caps=PROVIDER_B
            )
        if self.state is BookingState.NEEDS_REVIEW:
            self._transition(Trigger.PROVIDER_CONFIRMED)

    def abandon(self, age_delta: int) -> None:
        self.clock = max(self.clock, T0 + MAX_AGE + timedelta(seconds=age_delta))
        decision = decide_abandon(self.command, now=self.clock, max_age=MAX_AGE)
        eligible = self.model.abandon_eligible(self.clock - T0)
        assert isinstance(decision, Abandon) == eligible, (
            f"abandon decision {decision!r} at age {self.clock - T0} "
            f"but the model says eligible={eligible}"
        )
        if isinstance(decision, Abandon):
            self.model.abandon(self.clock - T0)  # raises if it abandoned a possible booking
            self.command = settle(
                self.command, Disposition.ABANDONED, DispositionBasis.LOCAL, caps=PROVIDER_B
            )
            self._transition(Trigger.ABANDONED)

    # Plumbing ------------------------------------------------------------------------------

    def _observe(self) -> tuple[Reservation, ...]:
        reservations = tuple(
            Reservation(
                ProviderBookingRef(r.res_id),
                BookingId(r.client_ref),
                ReservationState.CONFIRMED,
                self.clock,
            )
            for r in self.model.lookup()
        )
        for r in reservations:
            self._note_seen(r.ref)
        return reservations

    def _note_seen(self, ref: ProviderBookingRef) -> None:
        if ref not in self.seen:
            self.seen.append(ref)

    def _provider_result(self, mode: DispatchMode) -> ProviderResult:
        match mode:
            case DispatchMode.OK:
                res = self.model.reservations[-1]
                return ProviderResult(
                    SideEffect.NONE,
                    reservation=Reservation(
                        ProviderBookingRef(res.res_id),
                        BookingId(res.client_ref),
                        ReservationState.CONFIRMED,
                        self.clock,
                    ),
                )
            case DispatchMode.REJECT:
                return ProviderResult(SideEffect.NONE, definitive_rejection=True)
            case DispatchMode.SAFE_FAILURE:
                return ProviderResult(SideEffect.NONE)
            case _:
                return ProviderResult(SideEffect.POSSIBLE)

    @staticmethod
    def _outcome(result: ProviderResult) -> AttemptOutcome:
        if result.reservation is not None:
            return AttemptOutcome.SUCCESS
        if result.definitive_rejection:
            return AttemptOutcome.REJECTED
        return AttemptOutcome.UNKNOWN

    def _apply(self, decision: object) -> None:
        match decision:
            case Bind(reservation=res, disposition=disp, basis=basis):
                self.command = bind(self.command, res.ref)
                if disp is not Disposition.OPEN:
                    self.command = settle(self.command, disp, basis, caps=PROVIDER_B)
                trigger = trigger_for(self.state, decision)
                if trigger is not None:
                    self._transition(trigger)
            case Rejected(basis=basis):
                self.command = settle(self.command, Disposition.REJECTED, basis, caps=PROVIDER_B)
                self._transition(Trigger.PROVIDER_REJECTED)
            case Uncertain():
                if self.state is not BookingState.UNKNOWN:
                    self._transition(Trigger.OUTCOME_UNCERTAIN)
            case Reschedule():
                exhausted = decide_exhausted(self.command, max_attempts=MAX_ATTEMPTS)
                if exhausted is not None:
                    self._apply(exhausted)
                    return
                trigger = trigger_for_reschedule(self.state)
                if trigger is not None:
                    self._transition(trigger)
            case KeepLooking():
                pass
            case Escalate():
                if not self.command.is_settled:
                    self.command = replace(
                        self.command,
                        disposition=Disposition.UNRESOLVED,
                        basis=DispositionBasis.LOOKUP,
                    )
                trigger = trigger_for_escalation(self.state)
                if trigger is not None:
                    self._transition(trigger)
            case _:
                raise AssertionError(f"unexpected decision for Provider B: {decision!r}")

    def _transition(self, trigger: Trigger) -> None:
        nxt = transition(self.state, trigger)
        assert not isinstance(nxt, InvalidTransition), f"{trigger} illegal from {self.state}"
        self.state = nxt

    # Checks --------------------------------------------------------------------------------

    def check(self) -> None:
        m, c = self.model, self.command
        assert c.disposition.value in m.allowed_dispositions(), (
            f"domain holds {c.disposition} but the model allows {m.allowed_dispositions()}; "
            f"truth={m.reservations} observed={m.observed_refs} possible={m.possible_effect}"
        )
        assert self.state.value in m.allowed_booking_states(), (
            f"booking {self.state} but the model allows {m.allowed_booking_states()}"
        )
        assert m.unsafe_dispatches == 0, "a second network dispatch followed a possible effect"
        if m.possible_effect and not m.settled_success:
            assert c.possibly_executed, "uncertainty must be sticky until settled by evidence"
        assert c.submission_ref == m.bound_ref, (
            f"bound to {c.submission_ref} but the model bound {m.bound_ref}"
        )
        if c.is_settled:
            assert c.basis is not None
            if c.disposition is Disposition.SUCCEEDED:
                assert c.basis in (DispositionBasis.PROVIDER_RESULT, DispositionBasis.LOOKUP)
            if c.disposition is Disposition.REJECTED:
                assert c.basis is DispositionBasis.PROVIDER_RESULT and not m.possible_effect
            if c.disposition is Disposition.ABANDONED:
                assert c.basis is DispositionBasis.LOCAL and m.dispatched_to_network == 0
        replay = replay_for(CommandKind.CREATE, c.disposition, booking_state=self.state)
        assert replay.status == m.expected_replay_status()


@settings(max_examples=400, deadline=None)
@example(
    modes=[DispatchMode.COMMIT_INVISIBLE],
    later=["lookup", "lookup", "lookup", "expose", "review"],
    age_deltas=[],
)
@example(modes=[DispatchMode.OK], later=["duplicate", "lookup", "review"], age_deltas=[])
@example(
    modes=[DispatchMode.COMMIT_LOSE_RESPONSE, DispatchMode.LOCAL_DENIAL],
    later=["dispatch", "lookup"],
    age_deltas=[],
)
@example(modes=[DispatchMode.LOCAL_DENIAL], later=["abandon", "abandon"], age_deltas=[-1, 0])
@example(
    modes=[DispatchMode.SAFE_FAILURE] * 3 + [DispatchMode.OK],
    later=["dispatch", "dispatch", "dispatch", "lookup", "abandon"],
    age_deltas=[60],
)
@given(modes=modes, later=steps, age_deltas=ages)
def test_domain_lifecycle_agrees_with_the_model(
    modes: list[DispatchMode], later: list[str], age_deltas: list[int]
) -> None:
    h = Harness()
    mode_iter = iter(modes)
    age_iter = iter(age_deltas)
    h.dispatch(next(mode_iter))
    h.check()
    for step in later:
        if step == "dispatch":
            mode = next(mode_iter, None)
            if mode is None:
                continue
            h.dispatch(mode)
        elif step == "lookup":
            h.lookup()
        elif step == "expose":
            h.model.expose()
        elif step == "duplicate":
            h.model.inject_duplicate()
        elif step == "abandon":
            h.abandon(next(age_iter, 60))
        elif step == "review":
            h.review()
        h.check()

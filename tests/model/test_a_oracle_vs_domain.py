"""The domain's Provider A lifecycle (hold then confirm) must agree with the independent model.

The harness drives the real predicates the application uses, in the order the application uses
them: anchor and journal a CREATE attempt inside the cutoff, let the model decide what the
provider did, settle by the answer or recover (resubmit, wait, fence), then confirm the hold
inside its deadline, settle by the answer or by an authoritative read. After every step it
checks the safety properties of docs/booking-state-machine.md against the model's truth:

- a CREATE is never settled negatively unless the key is fenced and produced nothing, or the
  provider rejected it definitively before anything could execute;
- no attempt is dispatched after the command cutoff, and no resubmission ever creates a second
  hold;
- no confirm attempt may execute after the hold's deadline (its expiry stays inside it);
- the hold's expiry is never presumed from time alone: ``FAILED`` follows a provider answer or
  an authoritative read that says so;
- ``CONFIRMED`` means the provider holds a confirmed reservation, and an excluded confirm
  attempt is one that really did not confirm.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from orchestrator.domain import (
    Attempt,
    AttemptId,
    AttemptOutcome,
    Bind,
    BookingId,
    BookingState,
    Command,
    CommandId,
    CommandKind,
    ConfirmIntent,
    Disposition,
    DispositionBasis,
    Escalate,
    ProviderBookingRef,
    ProviderRequest,
    ProviderResult,
    Rejected,
    Reschedule,
    Reservation,
    ReservationState,
    SideEffect,
    Uncertain,
    anchor,
    bind,
    decide_create_after_attempt,
    dispatch_allowed,
    exclude_all_possible,
    record_attempt,
    settle,
)
from orchestrator.domain.confirmation import (
    AttemptExcluded,
    Confirmed,
    HoldExpired,
    Reconcile,
    confirm_dispatch_window,
    decide_confirm_after_attempt,
    decide_confirm_after_lookup,
)
from orchestrator.domain.cutoffs import ExpiryPolicy
from orchestrator.domain.settlement_fenced import (
    FencedLookupDue,
    Resubmit,
    WaitForExclusion,
    decide_create_after_fenced_lookup,
    decide_create_recovery,
)
from orchestrator.providers.rail_osdm import RAIL_OSDM_CAPABILITIES as A
from tests.model.a_reference_model import AReferenceModel, ConfirmMode, CreateMode, Hold, Observed
from tests.unit.domain.helpers import INTENT, T0

POLICY = ExpiryPolicy()
BK = BookingId("bk_a")
SKEW = A.max_clock_skew or timedelta(0)


def _reservation(hold: Hold, *, now: datetime) -> Reservation:
    hold.refresh(now)
    state = {
        "PREBOOKED": ReservationState.HELD,
        "CONFIRMED": ReservationState.CONFIRMED,
        "EXPIRED": ReservationState.FAILED,
        "CANCELLED": ReservationState.CANCELLED,
    }[hold.status]
    return Reservation(
        ProviderBookingRef(hold.ref),
        BK,
        state,
        now,
        valid_until=hold.deadline if state is ReservationState.HELD else None,
        product_ref=INTENT.offer_id,
        service_date=None,
    )


def _result(observed: Observed, hold: Hold | None, *, now: datetime) -> ProviderResult:
    match observed:
        case Observed.ANSWER:
            assert hold is not None
            return ProviderResult(SideEffect.NONE, reservation=_reservation(hold, now=now))
        case Observed.LOST | Observed.IN_PROGRESS:
            return ProviderResult(SideEffect.POSSIBLE)
        case Observed.NOTHING:
            return ProviderResult(SideEffect.NONE)
        case Observed.REJECTED | Observed.FENCED:
            return ProviderResult(SideEffect.NONE, definitive_rejection=True)
        case Observed.EXPIRED_REQUEST:
            return ProviderResult(SideEffect.NONE, definitive_rejection=True, request_expired=True)
        case Observed.HOLD_EXPIRED:
            return ProviderResult(SideEffect.NONE, definitive_rejection=True, hold_expired=True)
    raise AssertionError(observed)


def _outcome(result: ProviderResult) -> AttemptOutcome:
    if result.reservation is not None:
        return AttemptOutcome.SUCCESS
    if result.definitive_rejection:
        return AttemptOutcome.REJECTED
    return AttemptOutcome.UNKNOWN


class Harness:
    """The application's control flow for one booking at A, over the real domain predicates."""

    def __init__(self) -> None:
        self.model = AReferenceModel()
        self.now = T0
        self.state = BookingState.CREATED
        self.create = Command(
            id=CommandId("cmd_create"),
            booking_id=BK,
            kind=CommandKind.CREATE,
            intent=INTENT,
            provider_key=str(BK),
            created_at=T0,
        )
        self.confirm: Command | None = None
        self.bound: str | None = None
        self.hold_deadline: datetime | None = None
        self.attempts = 0
        self.excluded_confirm_attempts: list[int] = []  # model call numbers judged excluded
        self.confirm_calls: dict[AttemptId, int] = {}

    # Helpers -------------------------------------------------------------------------------

    def _attempt(self, command: Command, expiry: datetime) -> tuple[Command, Attempt]:
        self.attempts += 1
        attempt = Attempt(
            id=AttemptId(f"att_{self.attempts}"),
            command_id=command.id,
            n=len(command.attempts) + 1,
            request=ProviderRequest(payload=(("key", command.provider_key),), expiry=expiry),
            dispatch_marked_at=self.now,
        )
        return record_attempt(command, attempt), attempt

    def _finish(self, command: Command, attempt: Attempt, result: ProviderResult) -> Command:
        finished = replace(
            attempt,
            finished_at=self.now,
            outcome=_outcome(result),
            side_effect=result.side_effect,
        )
        return record_attempt(command, finished)

    # CREATE --------------------------------------------------------------------------------

    def dispatch_create(self, mode: CreateMode) -> None:
        if self.state not in (BookingState.CREATED, BookingState.UNKNOWN) or self.create.is_settled:
            return
        if self.state is BookingState.UNKNOWN:
            recovery = decide_create_recovery(A, self.create, now=self.now, policy=POLICY)
            if not isinstance(recovery, Resubmit):
                return
        if not dispatch_allowed(A, self.create, now=self.now):
            return
        command = self.create
        if command.first_dispatch_at is None:
            command = anchor(command, first_dispatch_at=self.now, cutoff=POLICY.cutoff(A, self.now))
        expiry = POLICY.attempt_expiry(self.now, command.execution_cutoff)
        assert expiry is not None, "inside the cutoff an attempt always has an expiry"
        assert command.execution_cutoff is not None and expiry <= command.execution_cutoff
        command, attempt = self._attempt(command, expiry)
        holds_before = self.model.holds_created
        observed = self.model.create(mode, now=self.now, expiry=expiry)
        assert self.model.holds_created <= 1, "a resubmission never creates a second hold"
        assert self.model.holds_created - holds_before == 0 or holds_before == 0
        result = _result(observed, self.model.hold, now=self.now)
        command = self._finish(command, attempt, result)
        decision = decide_create_after_attempt(A, command, attempt.id, result)
        self._apply_create(command, decision)

    def recover_create(self) -> None:
        if self.state is not BookingState.UNKNOWN or self.create.is_settled:
            return
        if self.confirm is not None:
            return  # a CONFIRM is outstanding: its own recovery applies
        recovery = decide_create_recovery(A, self.create, now=self.now, policy=POLICY)
        if isinstance(recovery, Resubmit | WaitForExclusion):
            return
        if isinstance(recovery, Reschedule):
            self.state = BookingState.CREATED
            return
        assert isinstance(recovery, FencedLookupDue)
        cutoff = self.create.execution_cutoff
        assert cutoff is not None and self.now >= cutoff + SKEW, "fenced only after exclusion"
        found = self.model.fenced_lookup(now=self.now)
        reservations = (_reservation(found, now=self.now),) if found is not None else ()
        decision = decide_create_after_fenced_lookup(A, self.create, reservations)
        self._apply_create(self.create, decision)

    def _apply_create(self, command: Command, decision: object) -> None:
        match decision:
            case Bind(reservation=res, disposition=disp, basis=basis):
                command = exclude_all_possible(bind(command, res.ref), at=self.now)
                if disp is not Disposition.OPEN:
                    command = settle(command, disp, basis, caps=A)
                self.create = command
                self.bound = str(res.ref)
                if res.state is ReservationState.HELD:
                    self.state = BookingState.HELD
                    self.hold_deadline = res.valid_until
                    if self.confirm is None:
                        self.confirm = bind(
                            Command(
                                id=CommandId("cmd_confirm"),
                                booking_id=BK,
                                kind=CommandKind.CONFIRM,
                                intent=ConfirmIntent(res.ref),
                                provider_key=f"{BK}:confirm",
                                created_at=self.now,
                            ),
                            res.ref,
                        )
                elif res.state is ReservationState.CONFIRMED:
                    self.state = BookingState.CONFIRMED
                elif res.state is ReservationState.FAILED:
                    self.state = BookingState.FAILED
                else:
                    self.state = BookingState.NEEDS_REVIEW
            case Rejected(basis=basis):
                self.create = settle(command, Disposition.REJECTED, basis, caps=A)
                self.state = BookingState.FAILED
                self._check_negative_create(basis)
            case Uncertain():
                self.create = command
                self.state = BookingState.UNKNOWN
            case Reschedule():
                self.create = command
                self.state = BookingState.CREATED
            case Escalate():
                self.create = command
                self.state = BookingState.NEEDS_REVIEW
            case _:
                raise AssertionError(decision)

    def _check_negative_create(self, basis: DispositionBasis) -> None:
        hold = self.model.hold
        assert hold is None or hold.status == "EXPIRED", (
            "CREATE settled negatively while the provider holds a live reservation for it"
        )
        if basis is DispositionBasis.FENCED_LOOKUP:
            assert self.model.fenced, "a fenced basis needs a fence"
        else:
            assert basis is DispositionBasis.PROVIDER_RESULT
            assert not any(c.executed for c in self.model.calls if c.kind == "create"), (
                "a definitive rejection settled a command whose earlier attempt executed"
            )

    # CONFIRM -------------------------------------------------------------------------------

    def dispatch_confirm(self, mode: ConfirmMode) -> None:
        if self.state is not BookingState.HELD or self.confirm is None or self.confirm.is_settled:
            return
        assert self.hold_deadline is not None
        latest = confirm_dispatch_window(
            now=self.now, hold_deadline=self.hold_deadline, caps=A, policy=POLICY
        )
        if latest is None:
            return  # too close to the deadline: wait for the provider to report
        if self.confirm.possibly_executed or not dispatch_allowed(A, self.confirm, now=self.now):
            return
        command = self.confirm
        if command.first_dispatch_at is None:
            command = anchor(command, first_dispatch_at=self.now, cutoff=POLICY.cutoff(A, self.now))
        expiry = POLICY.attempt_expiry(self.now, command.execution_cutoff)
        expiry = min(expiry, latest) if expiry is not None else latest
        assert expiry <= self.hold_deadline - SKEW - POLICY.margin, (
            "a confirm attempt could execute after the hold's deadline"
        )
        command, attempt = self._attempt(command, expiry)
        observed = self.model.confirm(mode, now=self.now, expiry=expiry)
        self.confirm_calls[attempt.id] = self.model.last_call().n
        result = _result(observed, self.model.hold, now=self.now)
        command = self._finish(command, attempt, result)
        assert self.bound is not None
        decision = decide_confirm_after_attempt(command, attempt.id, result, bound=self.bound)
        self._apply_confirm(command, decision, attempt_id=attempt.id)

    def lookup_confirm(self) -> None:
        if self.confirm is None or self.confirm.is_settled:
            return
        if self.state not in (BookingState.UNKNOWN, BookingState.HELD):
            return
        if self.state is BookingState.HELD and not self.confirm.possibly_executed:
            return  # nothing to settle by a read
        hold = self.model.read(now=self.now)
        assert hold is not None and self.bound is not None
        decision = decide_confirm_after_lookup(
            A,
            self.confirm,
            _reservation(hold, now=self.now),
            bound=self.bound,
            now=self.now,
            policy=POLICY,
        )
        self._apply_confirm(self.confirm, decision, attempt_id=None)

    def _apply_confirm(
        self, command: Command, decision: object, *, attempt_id: AttemptId | None
    ) -> None:
        truth = self.model.status(now=self.now)
        match decision:
            case Confirmed(basis=basis):
                final = (
                    DispositionBasis.FENCED_LOOKUP if basis is DispositionBasis.LOOKUP else basis
                )
                self.confirm = settle(
                    exclude_all_possible(command, at=self.now), Disposition.SUCCEEDED, final, caps=A
                )
                if self.create.disposition is Disposition.OPEN:
                    self.create = settle(
                        exclude_all_possible(self.create, at=self.now),
                        Disposition.SUCCEEDED,
                        final,
                        caps=A,
                    )
                self.state = BookingState.CONFIRMED
                assert truth == "CONFIRMED", "CONFIRMED without a confirmed reservation"
            case HoldExpired(basis=basis):
                final = (
                    DispositionBasis.FENCED_LOOKUP if basis is DispositionBasis.LOOKUP else basis
                )
                self.confirm = settle(
                    exclude_all_possible(command, at=self.now), Disposition.REJECTED, final, caps=A
                )
                if self.create.disposition is Disposition.OPEN:
                    self.create = settle(
                        exclude_all_possible(self.create, at=self.now),
                        Disposition.REJECTED,
                        final,
                        caps=A,
                    )
                self.state = BookingState.FAILED
                assert truth == "EXPIRED", "hold expiry concluded although the hold is not expired"
            case AttemptExcluded():
                before = {a.id: a.excluded_at for a in command.attempts}
                self.confirm = exclude_all_possible(command, at=self.now)
                self.state = BookingState.HELD
                hold = self.model.hold
                assert hold is not None and hold.status == "PREBOOKED", (
                    "an attempt was excluded although the hold is not live at the provider"
                )
                newly = [a for a in self.confirm.attempts if a.excluded_at and not before[a.id]]
                for att in newly:
                    n = self.confirm_calls.get(att.id)
                    assert n is None or hold.confirmed_by != n, (
                        "an attempt that confirmed the hold was judged excluded"
                    )
            case Reconcile() | Uncertain():
                self.confirm = command
                self.state = BookingState.UNKNOWN
            case Reschedule():
                self.confirm = command
                self.state = BookingState.HELD
            case Escalate():
                self.confirm = command
                self.state = BookingState.NEEDS_REVIEW
            case _:
                raise AssertionError(decision)

    # Invariants ------------------------------------------------------------------------------

    def check(self) -> None:
        for command in (self.create, self.confirm):
            if command is None or command.execution_cutoff is None:
                continue
            for att in command.attempts:
                assert att.request.expiry is not None
                assert att.request.expiry <= command.execution_cutoff
        if self.state is BookingState.CONFIRMED:
            assert self.model.status(now=self.now) == "CONFIRMED"
        if self.state is BookingState.HELD:
            assert self.model.status(now=self.now) != "CONFIRMED", (
                "the platform believes in a live hold the provider has confirmed"
            )
        if self.state is BookingState.FAILED and self.confirm is not None:
            assert self.model.status(now=self.now) in ("EXPIRED", None)
        if self.state is BookingState.FAILED and self.confirm is None:
            hold = self.model.hold
            assert hold is None or hold.status == "EXPIRED"
        assert self.model.holds_created <= 1


create_modes = st.sampled_from(list(CreateMode))
confirm_modes = st.sampled_from(list(ConfirmMode))
advances = st.sampled_from([1, 10, 29, 31, 60, 300, 301, 600, 606])
steps = st.lists(
    st.one_of(
        st.tuples(st.just("create"), create_modes),
        st.tuples(st.just("recover"), st.none()),
        st.tuples(st.just("confirm"), confirm_modes),
        st.tuples(st.just("lookup"), st.none()),
        st.tuples(st.just("advance"), advances),
    ),
    min_size=1,
    max_size=14,
)


@settings(max_examples=400, deadline=None)
@given(steps=steps)
def test_domain_lifecycle_at_a_agrees_with_the_model(steps: list[tuple[str, object]]) -> None:
    h = Harness()
    for kind, arg in steps:
        if kind == "create":
            assert isinstance(arg, CreateMode)
            h.dispatch_create(arg)
        elif kind == "recover":
            h.recover_create()
        elif kind == "confirm":
            assert isinstance(arg, ConfirmMode)
            h.dispatch_confirm(arg)
        elif kind == "lookup":
            h.lookup_confirm()
        else:
            assert isinstance(arg, int)
            h.now = h.now + timedelta(seconds=arg)
        h.check()
    # Quiescence: with time past every bound, recovery settles whatever is left.
    h.now = h.now + timedelta(minutes=11)
    h.recover_create()
    h.lookup_confirm()
    h.check()
    if h.state is BookingState.UNKNOWN and h.confirm is None:
        h.recover_create()
        assert h.state is not BookingState.UNKNOWN, "a fenced provider never stays unknown"

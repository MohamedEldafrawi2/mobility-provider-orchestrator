"""Provider A's settlement predicates: cutoffs, resubmission, fenced lookups, confirmation with a
hold deadline, cancellation by exact refund offer (docs/booking-state-machine.md)."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from orchestrator.domain import (
    AttemptOutcome,
    Bind,
    BookingId,
    Command,
    CommandId,
    CommandKind,
    ConfirmIntent,
    Disposition,
    DispositionBasis,
    Escalate,
    InvariantError,
    Money,
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
    dispatch_allowed,
    record_attempt,
    settle,
)
from orchestrator.domain.cancellation import (
    AcceptanceExcluded,
    CancelIntent,
    Cancelled,
    Requote,
    TermsChanged,
    check_terms,
    decide_cancel_after_acceptance,
    decide_cancel_after_lookup,
)
from orchestrator.domain.confirmation import (
    AttemptExcluded,
    Confirmed,
    HoldExpired,
    confirm_dispatch_window,
    decide_confirm_after_attempt,
    decide_confirm_after_lookup,
)
from orchestrator.domain.cutoffs import ExpiryPolicy
from orchestrator.domain.refunds import RefundOfferState, RefundOfferStatus, RefundQuote
from orchestrator.domain.settlement import REASON_DUPLICATE, REASON_IDENTITY
from orchestrator.domain.settlement_fenced import (
    FencedLookupDue,
    Resubmit,
    WaitForExclusion,
    decide_create_after_fenced_lookup,
    decide_create_recovery,
)
from orchestrator.providers.rail_osdm import RAIL_OSDM_CAPABILITIES as A
from tests.unit.domain.helpers import T0, attempt, attempt_id, new_command

BK = BookingId("bk_1")
R1 = ProviderBookingRef("RB1")
R2 = ProviderBookingRef("RB2")
POLICY = ExpiryPolicy()
CHF = "CHF"


def _held(ref: ProviderBookingRef = R1, **kw: object) -> Reservation:
    return Reservation(
        ref, BK, ReservationState.HELD, T0, valid_until=T0 + timedelta(minutes=10), **kw
    )  # type: ignore[arg-type]


def _anchored() -> Command:
    cmd = new_command()
    return anchor(cmd, first_dispatch_at=T0, cutoff=POLICY.cutoff(A, T0))


# Cutoffs ------------------------------------------------------------------------------------


def test_cutoff_is_anchored_once_and_bounds_every_attempt() -> None:
    cmd = _anchored()
    assert cmd.execution_cutoff == T0 + timedelta(minutes=10), "the lifetime, not the 24 h window"
    with pytest.raises(InvariantError, match="anchored once"):
        anchor(cmd, first_dispatch_at=T0, cutoff=T0)
    late = replace(
        attempt(1, None, None, finished=False),
        request=ProviderRequest((), expiry=T0 + timedelta(hours=1)),
    )
    with pytest.raises(InvariantError, match="may not exceed"):
        record_attempt(cmd, late)
    assert POLICY.attempt_expiry(T0, cmd.execution_cutoff) == T0 + timedelta(seconds=30)
    assert (
        POLICY.attempt_expiry(T0 + timedelta(minutes=9, seconds=50), cmd.execution_cutoff)
        == cmd.execution_cutoff
    )
    assert POLICY.cutoff(A, T0) is not None
    from orchestrator.providers.bus_legacy import BUS_LEGACY_CAPABILITIES as B

    assert POLICY.cutoff(B, T0) is None, "no execution expiry: nothing bounds B"


def test_resubmission_is_allowed_only_inside_the_cutoff() -> None:
    cmd = _anchored()
    cmd = record_attempt(cmd, attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    assert dispatch_allowed(A, cmd, now=T0 + timedelta(minutes=1)), "key bound before execution"
    assert not dispatch_allowed(A, cmd, now=T0 + timedelta(minutes=11)), "past the cutoff"
    from orchestrator.providers.bus_legacy import BUS_LEGACY_CAPABILITIES as B

    assert not dispatch_allowed(B, cmd), "B: never after a possible effect"


# CREATE recovery for a fenced provider ------------------------------------------------------


def test_create_recovery_resubmits_then_waits_then_fences() -> None:
    cmd = record_attempt(_anchored(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    early = decide_create_recovery(A, cmd, now=T0 + timedelta(minutes=1), policy=POLICY)
    assert isinstance(early, Resubmit) and early.expiry_at_or_before == cmd.execution_cutoff
    just_after = decide_create_recovery(
        A, cmd, now=T0 + timedelta(minutes=10, seconds=1), policy=POLICY
    )
    assert isinstance(just_after, WaitForExclusion)
    assert just_after.until == T0 + timedelta(minutes=10, seconds=5), "cutoff plus the 5 s skew"
    due = decide_create_recovery(A, cmd, now=T0 + timedelta(minutes=10, seconds=6), policy=POLICY)
    assert isinstance(due, FencedLookupDue)
    safe = record_attempt(_anchored(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.NONE))
    assert decide_create_recovery(A, safe, now=T0, policy=POLICY) == Reschedule()


def test_fenced_lookup_settles_negatively_or_binds() -> None:
    cmd = record_attempt(_anchored(), attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE))
    nothing = decide_create_after_fenced_lookup(A, cmd, ())
    assert nothing == Rejected(DispositionBasis.FENCED_LOOKUP)
    settled = settle(cmd, Disposition.REJECTED, DispositionBasis.FENCED_LOOKUP, caps=A)
    assert settled.disposition is Disposition.REJECTED
    found = decide_create_after_fenced_lookup(A, cmd, (_held(),))
    assert isinstance(found, Bind) and found.disposition is Disposition.OPEN, (
        "a hold keeps CREATE open"
    )
    assert found.basis is DispositionBasis.FENCED_LOOKUP
    two = decide_create_after_fenced_lookup(A, cmd, (_held(R1), _held(R2)))
    assert two == Escalate(REASON_DUPLICATE, implicated=(R1, R2))
    foreign = Reservation(R2, BookingId("bk_other"), ReservationState.HELD, T0)
    assert decide_create_after_fenced_lookup(A, cmd, (foreign,)) == Escalate(
        REASON_IDENTITY, implicated=(R2,)
    )
    with pytest.raises(InvariantError):
        from orchestrator.providers.bus_legacy import BUS_LEGACY_CAPABILITIES as B

        decide_create_after_fenced_lookup(B, cmd, ())


def test_expired_request_rejection_reschedules_instead_of_rejecting() -> None:
    from orchestrator.domain import decide_create_after_attempt

    cmd = record_attempt(_anchored(), attempt(1, AttemptOutcome.REJECTED, SideEffect.NONE))
    expired = ProviderResult(SideEffect.NONE, definitive_rejection=True, request_expired=True)
    assert decide_create_after_attempt(A, cmd, attempt_id(1), expired) == Reschedule()
    sold_out = ProviderResult(SideEffect.NONE, definitive_rejection=True)
    assert decide_create_after_attempt(A, cmd, attempt_id(1), sold_out) == Rejected(
        DispositionBasis.PROVIDER_RESULT
    )


# CONFIRM ------------------------------------------------------------------------------------


def _confirm_command() -> Command:
    cmd = Command(
        id=CommandId("cmd_confirm"),
        booking_id=BK,
        kind=CommandKind.CONFIRM,
        intent=ConfirmIntent(R1),
        provider_key="bk_1",
        created_at=T0,
    )
    return bind(cmd, R1)


def _confirm_attempt(
    n: int, outcome: AttemptOutcome | None, side_effect: SideEffect | None, *, finished: bool = True
):  # type: ignore[no-untyped-def]
    a = attempt(n, outcome, side_effect, finished=finished)
    return replace(
        a,
        command_id=CommandId("cmd_confirm"),
        request=ProviderRequest((), expiry=T0 + timedelta(seconds=30)),
    )


def test_confirm_dispatch_window_respects_the_hold_deadline() -> None:
    deadline = T0 + timedelta(minutes=10)
    assert confirm_dispatch_window(
        now=T0, hold_deadline=deadline, caps=A, policy=POLICY
    ) == T0 + timedelta(seconds=30)
    late = T0 + timedelta(minutes=9, seconds=40)
    assert confirm_dispatch_window(
        now=late, hold_deadline=deadline, caps=A, policy=POLICY
    ) == deadline - timedelta(seconds=10)
    assert (
        confirm_dispatch_window(
            now=T0 + timedelta(minutes=9, seconds=51), hold_deadline=deadline, caps=A, policy=POLICY
        )
        is None
    )


def test_confirm_after_attempt() -> None:
    cmd = record_attempt(
        _confirm_command(), _confirm_attempt(1, AttemptOutcome.SUCCESS, SideEffect.NONE)
    )
    confirmed = Reservation(R1, BK, ReservationState.CONFIRMED, T0)
    ok = decide_confirm_after_attempt(
        cmd, attempt_id(1), ProviderResult(SideEffect.NONE, reservation=confirmed), bound=R1
    )
    assert isinstance(ok, Confirmed) and ok.basis is DispositionBasis.PROVIDER_RESULT
    other = Reservation(R2, BK, ReservationState.CONFIRMED, T0)
    assert decide_confirm_after_attempt(
        cmd, attempt_id(1), ProviderResult(SideEffect.NONE, reservation=other), bound=R1
    ) == Escalate(REASON_IDENTITY, implicated=(R1, R2))
    expired = ProviderResult(SideEffect.NONE, definitive_rejection=True, hold_expired=True)
    assert isinstance(
        decide_confirm_after_attempt(cmd, attempt_id(1), expired, bound=R1), HoldExpired
    )
    late_request = ProviderResult(SideEffect.NONE, definitive_rejection=True, request_expired=True)
    assert (
        decide_confirm_after_attempt(cmd, attempt_id(1), late_request, bound=R1)
        == AttemptExcluded()
    )
    assert (
        decide_confirm_after_attempt(
            cmd, attempt_id(1), ProviderResult(SideEffect.POSSIBLE), bound=R1
        )
        == Uncertain()
    )
    safe = record_attempt(
        _confirm_command(), _confirm_attempt(1, AttemptOutcome.UNKNOWN, SideEffect.NONE)
    )
    assert (
        decide_confirm_after_attempt(safe, attempt_id(1), ProviderResult(SideEffect.NONE), bound=R1)
        == Reschedule()
    )


def test_confirm_after_lookup_excludes_only_after_expiry_plus_skew() -> None:
    cmd = record_attempt(
        _confirm_command(), _confirm_attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE)
    )
    still_held = _held()
    early = decide_confirm_after_lookup(
        A, cmd, still_held, bound=R1, now=T0 + timedelta(seconds=32), policy=POLICY
    )
    assert early == Uncertain(), "the attempt may still execute until expiry plus skew"
    later = decide_confirm_after_lookup(
        A, cmd, still_held, bound=R1, now=T0 + timedelta(seconds=36), policy=POLICY
    )
    assert later == AttemptExcluded()
    confirmed = Reservation(R1, BK, ReservationState.CONFIRMED, T0)
    assert isinstance(
        decide_confirm_after_lookup(A, cmd, confirmed, bound=R1, now=T0, policy=POLICY), Confirmed
    )
    gone = Reservation(R1, BK, ReservationState.FAILED, T0)
    assert isinstance(
        decide_confirm_after_lookup(A, cmd, gone, bound=R1, now=T0, policy=POLICY), HoldExpired
    )


# CANCEL -------------------------------------------------------------------------------------


def _cancel_command(quote: RefundQuote) -> Command:
    cmd = Command(
        id=CommandId("cmd_cancel"),
        booking_id=BK,
        kind=CommandKind.CANCEL,
        intent=CancelIntent(max_fee=Money(700, CHF)),
        provider_key="bk_1:cancel",
        created_at=T0,
        quote=quote,
    )
    return bind(cmd, R1)


def _quote(fee: int = 680, offer_id: str = "RF1") -> RefundQuote:
    return RefundQuote(offer_id, Money(3400 - fee, CHF), Money(fee, CHF), T0 + timedelta(minutes=2))


def _accept_attempt(n: int, outcome: AttemptOutcome | None, side_effect: SideEffect | None):  # type: ignore[no-untyped-def]
    a = attempt(n, outcome, side_effect)
    return replace(
        a,
        command_id=CommandId("cmd_cancel"),
        request=ProviderRequest((), expiry=T0 + timedelta(seconds=60)),
    )


def test_terms_are_checked_against_what_the_client_authorised() -> None:
    assert check_terms(CancelIntent(Money(700, CHF)), _quote(680)) is None
    assert isinstance(check_terms(CancelIntent(Money(600, CHF)), _quote(680)), TermsChanged)
    assert isinstance(check_terms(CancelIntent(Money(700, "EUR")), _quote(680)), TermsChanged)
    assert isinstance(check_terms(CancelIntent(None), _quote(1)), TermsChanged)
    assert check_terms(CancelIntent(None), _quote(0)) is None


def test_acceptance_settles_only_by_the_exact_offer() -> None:
    quote = _quote()
    cmd = record_attempt(
        _cancel_command(quote), _accept_attempt(1, AttemptOutcome.SUCCESS, SideEffect.NONE)
    )
    cancelled = Reservation(
        R1,
        BK,
        ReservationState.CANCELLED,
        T0,
        refund_offers=(
            RefundOfferStatus("RF1", RefundOfferState.CONFIRMED),
            RefundOfferStatus("RF2", RefundOfferState.REJECTED),
        ),
    )
    ok = decide_cancel_after_acceptance(
        cmd,
        attempt_id(1),
        ProviderResult(SideEffect.NONE, reservation=cancelled),
        bound=R1,
        quote=quote,
    )
    assert isinstance(ok, Cancelled)
    by_other = Reservation(
        R1,
        BK,
        ReservationState.CANCELLED,
        T0,
        refund_offers=(
            RefundOfferStatus("RF1", RefundOfferState.REJECTED),
            RefundOfferStatus("RF2", RefundOfferState.CONFIRMED),
        ),
    )
    assert isinstance(
        decide_cancel_after_acceptance(
            cmd,
            attempt_id(1),
            ProviderResult(SideEffect.NONE, reservation=by_other),
            bound=R1,
            quote=quote,
        ),
        Escalate,
    )
    expired = ProviderResult(SideEffect.NONE, definitive_rejection=True)
    assert (
        decide_cancel_after_acceptance(cmd, attempt_id(1), expired, bound=R1, quote=quote)
        == AcceptanceExcluded()
    )


def test_lookup_settles_an_uncertain_acceptance_and_requotes_only_when_nothing_can_commit() -> None:
    quote = _quote()
    cmd = record_attempt(
        _cancel_command(quote), _accept_attempt(1, AttemptOutcome.UNKNOWN, SideEffect.POSSIBLE)
    )
    still = Reservation(
        R1,
        BK,
        ReservationState.CONFIRMED,
        T0,
        refund_offers=(RefundOfferStatus("RF1", RefundOfferState.PROPOSED),),
    )
    assert (
        decide_cancel_after_lookup(
            A, cmd, still, bound=R1, quote=quote, now=T0 + timedelta(seconds=61), policy=POLICY
        )
        == Uncertain()
    )
    assert (
        decide_cancel_after_lookup(
            A, cmd, still, bound=R1, quote=quote, now=T0 + timedelta(seconds=66), policy=POLICY
        )
        == AcceptanceExcluded()
    )
    gone = Reservation(
        R1,
        BK,
        ReservationState.CONFIRMED,
        T0,
        refund_offers=(RefundOfferStatus("RF1", RefundOfferState.EXPIRED),),
    )
    assert (
        decide_cancel_after_lookup(
            A, cmd, gone, bound=R1, quote=quote, now=T0 + timedelta(seconds=66), policy=POLICY
        )
        == Requote()
    )
    done = Reservation(
        R1,
        BK,
        ReservationState.CANCELLED,
        T0,
        refund_offers=(RefundOfferStatus("RF1", RefundOfferState.CONFIRMED),),
    )
    assert isinstance(
        decide_cancel_after_lookup(A, cmd, done, bound=R1, quote=quote, now=T0, policy=POLICY),
        Cancelled,
    )
    with pytest.raises(InvariantError):
        settle(new_command(), Disposition.REFUSED, DispositionBasis.PROVIDER_RESULT, caps=A)

"""Commands and attempts: certainty is tracked per command (docs/architecture.md).

A command is one immutable client intent with one provider key and one execution cutoff.
Attempts are its provider requests, each with its own immutable request. An attempt's side
effect is the worst the platform knows about *that* attempt: ``POSSIBLE`` from the moment it
is dispatch-marked until the provider answers definitively for it. The command's certainty is
the worst across its attempts, so a later local rejection cannot make an earlier possible
effect go away, while a definitive answer for the same attempt can.

Dispositions carry their basis. ``LOCAL`` justifies only ``ABANDONED`` with proven zero
dispatch. A negative disposition based on a provider result requires that no other attempt
could have had an effect. A fenced lookup can only be claimed for a provider that offers one.
Violations raise ``InvariantError``: they are programming errors, not runtime conditions.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime
from enum import StrEnum
from typing import Any

from orchestrator.domain.capabilities import ProviderCapabilities
from orchestrator.domain.ids import AttemptId, BookingId, CommandId, ProviderBookingRef
from orchestrator.domain.time import require_aware


class SideEffect(StrEnum):
    NOT_DISPATCHED = "NOT_DISPATCHED"
    NONE = "NONE"
    POSSIBLE = "POSSIBLE"


class CommandKind(StrEnum):
    CREATE = "CREATE"
    CONFIRM = "CONFIRM"
    CANCEL = "CANCEL"
    CANCEL_EXTRA = "CANCEL_EXTRA"


class Disposition(StrEnum):
    OPEN = "OPEN"
    SUCCEEDED = "SUCCEEDED"
    REJECTED = "REJECTED"
    REFUSED = "REFUSED"
    TERMS_CHANGED = "TERMS_CHANGED"
    UNRESOLVED = "UNRESOLVED"
    ABANDONED = "ABANDONED"


NEGATIVE_DISPOSITIONS: frozenset[Disposition] = frozenset(
    {Disposition.REJECTED, Disposition.REFUSED, Disposition.TERMS_CHANGED, Disposition.ABANDONED}
)
FINAL_DISPOSITIONS: frozenset[Disposition] = frozenset(
    {Disposition.SUCCEEDED, *NEGATIVE_DISPOSITIONS}
)


class DispositionBasis(StrEnum):
    PROVIDER_RESULT = "PROVIDER_RESULT"
    FENCED_LOOKUP = "FENCED_LOOKUP"
    LOOKUP = "LOOKUP"
    LOCAL = "LOCAL"


class AttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    REJECTED = "REJECTED"
    NOT_DISPATCHED = "NOT_DISPATCHED"
    UNKNOWN = "UNKNOWN"


class InvariantError(Exception):
    """A domain rule was broken by the caller. Never expected at runtime."""


@dataclass(frozen=True, slots=True)
class CreateIntent:
    """The client's immutable intent for a CREATE command, plus what the provider must echo."""

    offer_id: str
    passenger_names: tuple[str, ...]
    contact_email: str
    product_ref: str | None = None  # the provider's product the offer resolved to
    service_date: date | None = None


@dataclass(frozen=True, slots=True)
class ConfirmIntent:
    """Confirm one immutable hold: the reservation the CREATE bound."""

    reservation_ref: ProviderBookingRef


class CancelPhase(StrEnum):
    NONE = "NONE"
    QUOTED = "QUOTED"
    ACCEPTING = "ACCEPTING"


@dataclass(frozen=True, slots=True)
class ProviderRequest:
    """The immutable request one attempt sent (or was about to send) to the provider."""

    payload: tuple[tuple[str, str], ...]
    expiry: datetime | None = None

    def __post_init__(self) -> None:
        require_aware(self.expiry, "ProviderRequest.expiry")


@dataclass(frozen=True, slots=True)
class Attempt:
    id: AttemptId
    command_id: CommandId
    n: int
    request: ProviderRequest
    dispatch_marked_at: datetime | None
    finished_at: datetime | None = None
    outcome: AttemptOutcome | None = None
    side_effect: SideEffect | None = None
    error: str | None = None  # the provider error kind a failed attempt ended with
    excluded_at: datetime | None = None  # a possible effect settled as "did not happen"

    def __post_init__(self) -> None:
        require_aware(self.dispatch_marked_at, "Attempt.dispatch_marked_at")
        require_aware(self.finished_at, "Attempt.finished_at")
        require_aware(self.excluded_at, "Attempt.excluded_at")

    @property
    def is_open(self) -> bool:
        return self.dispatch_marked_at is not None and self.finished_at is None

    @property
    def effective_side_effect(self) -> SideEffect:
        """What the platform must assume about this attempt right now."""
        if self.dispatch_marked_at is None:
            return SideEffect.NOT_DISPATCHED
        if self.excluded_at is not None:
            return SideEffect.NONE  # settled: the effect was confirmed elsewhere or excluded
        if self.is_open or self.side_effect is None:
            return SideEffect.POSSIBLE
        return self.side_effect


@dataclass(frozen=True, slots=True)
class Command:
    id: CommandId
    booking_id: BookingId
    kind: CommandKind
    intent: CreateIntent | ConfirmIntent | Any
    provider_key: str
    created_at: datetime
    first_dispatch_at: datetime | None = None
    execution_cutoff: datetime | None = None
    disposition: Disposition = Disposition.OPEN
    basis: DispositionBasis | None = None
    submission_ref: ProviderBookingRef | None = None
    attempts: tuple[Attempt, ...] = field(default_factory=tuple)
    lookups_performed: int = 0
    phase: CancelPhase | None = None  # CANCEL only
    quote: Any = None  # CANCEL only: the RefundQuote the acceptance is bound to
    target_ref: ProviderBookingRef | None = None  # CANCEL_EXTRA: the duplicate

    def __post_init__(self) -> None:
        require_aware(self.created_at, "Command.created_at")
        require_aware(self.first_dispatch_at, "Command.first_dispatch_at")
        require_aware(self.execution_cutoff, "Command.execution_cutoff")

    @property
    def ever_dispatched(self) -> bool:
        return any(a.dispatch_marked_at is not None for a in self.attempts)

    @property
    def max_side_effect(self) -> SideEffect:
        effects = {a.effective_side_effect for a in self.attempts}
        if SideEffect.POSSIBLE in effects:
            return SideEffect.POSSIBLE
        if SideEffect.NONE in effects:
            return SideEffect.NONE
        return SideEffect.NOT_DISPATCHED

    @property
    def possibly_executed(self) -> bool:
        return self.max_side_effect is SideEffect.POSSIBLE

    def other_attempts_possibly_executed(self, attempt_id: AttemptId) -> bool:
        return any(
            a.effective_side_effect is SideEffect.POSSIBLE
            for a in self.attempts
            if a.id != attempt_id
        )

    @property
    def is_settled(self) -> bool:
        return self.disposition in FINAL_DISPOSITIONS


def record_attempt(command: Command, attempt: Attempt) -> Command:
    """Add an attempt, or update an existing one without rewriting its history.

    A settled command accepts no *new* attempt. It does accept the completion of an attempt
    it already holds unfinished: the response of a call that was in flight while a lookup
    settled the command still closes that attempt with its outcome (ADR 007).
    """
    if attempt.dispatch_marked_at is None and attempt.outcome not in (
        None,
        AttemptOutcome.NOT_DISPATCHED,
    ):
        raise InvariantError("an attempt without a dispatch mark can only be NOT_DISPATCHED")
    if attempt.finished_at is not None and attempt.outcome is None:
        raise InvariantError("a finished attempt must have an outcome")

    if attempt.command_id != command.id:
        raise InvariantError(
            f"attempt {attempt.id} belongs to {attempt.command_id}, not {command.id}"
        )
    existing = next((a for a in command.attempts if a.id == attempt.id), None)
    if command.is_settled and (existing is None or existing.finished_at is not None):
        raise InvariantError(f"command {command.id} is settled; no further attempts")
    if existing is not None:
        if existing.excluded_at is not None and attempt.excluded_at is None:
            # The attempt was settled by evidence while its call was in flight; the late
            # response closes the record but never reopens the question of its effect.
            attempt = replace(attempt, excluded_at=existing.excluded_at)
        if existing.n != attempt.n or existing.request != attempt.request:
            raise InvariantError("attempt identity and request are immutable")
        if existing.dispatch_marked_at is not None and (
            attempt.dispatch_marked_at != existing.dispatch_marked_at
        ):
            raise InvariantError("a dispatch mark, once written, is immutable")
        if (
            existing.finished_at is not None
            and replace(attempt, excluded_at=existing.excluded_at) != existing
        ):
            raise InvariantError("a finished attempt is immutable")
    elif any(a.n == attempt.n for a in command.attempts):
        raise InvariantError(f"attempt number {attempt.n} already exists")

    others = tuple(a for a in command.attempts if a.id != attempt.id)
    attempts = tuple(sorted((*others, attempt), key=lambda a: a.n))
    first_dispatch = command.first_dispatch_at
    if first_dispatch is None and attempt.dispatch_marked_at is not None:
        first_dispatch = attempt.dispatch_marked_at
    if (
        command.execution_cutoff is not None
        and attempt.request.expiry is not None
        and attempt.request.expiry > command.execution_cutoff
    ):
        raise InvariantError("an attempt's expiry may not exceed the command cutoff")
    return replace(command, attempts=attempts, first_dispatch_at=first_dispatch)


def exclude_attempt(command: Command, attempt_id: AttemptId, *, at: datetime) -> Command:
    """Settle one attempt's possible effect as excluded (docs/booking-state-machine.md): the
    evidence said it did not happen, or the reservation the command bound accounts for it.
    The attempt's own record (outcome, side effect) is untouched; only its settlement is.

    An attempt whose response never arrived (the caller died) is excluded like any other: the
    evidence speaks about the key or the reservation, not about the response. Its record is
    closed by recovery, or by the response if it arrives after all."""
    require_aware(at, "at")
    attempts = []
    for a in command.attempts:
        if a.id == attempt_id:
            if a.dispatch_marked_at is None:
                raise InvariantError("an attempt that was never dispatched has no effect")
            a = replace(a, excluded_at=at)
        attempts.append(a)
    return replace(command, attempts=tuple(attempts))


def exclude_all_possible(command: Command, *, at: datetime) -> Command:
    """Every attempt with a possible effect, finished or not, is settled (a bound reservation
    or a final fenced answer accounts for all of them at once)."""
    out = command
    for a in command.attempts:
        if a.effective_side_effect is SideEffect.POSSIBLE:
            out = exclude_attempt(out, a.id, at=at)
    return out


def anchor(command: Command, *, first_dispatch_at: datetime, cutoff: datetime | None) -> Command:
    """Set the anchor and the absolute cutoff once, at the first dispatch mark. Never renewed."""
    require_aware(first_dispatch_at, "first_dispatch_at")
    require_aware(cutoff, "cutoff")
    if command.first_dispatch_at is not None or command.execution_cutoff is not None:
        raise InvariantError("a command is anchored once")
    return replace(command, first_dispatch_at=first_dispatch_at, execution_cutoff=cutoff)


def settle(
    command: Command,
    disposition: Disposition,
    basis: DispositionBasis,
    *,
    caps: ProviderCapabilities,
) -> Command:
    """Apply a disposition, enforcing the evidence rules of docs/booking-state-machine.md."""
    if command.is_settled and disposition != command.disposition:
        raise InvariantError(f"command {command.id} already settled as {command.disposition}")
    if basis is DispositionBasis.LOCAL:
        if disposition is not Disposition.ABANDONED:
            raise InvariantError("LOCAL basis justifies only ABANDONED")
        if command.ever_dispatched or command.possibly_executed:
            raise InvariantError("ABANDONED requires that no attempt was ever dispatched")
    if disposition is Disposition.ABANDONED and basis is not DispositionBasis.LOCAL:
        raise InvariantError("ABANDONED must have LOCAL basis")
    if disposition in (Disposition.REFUSED, Disposition.TERMS_CHANGED) and command.kind not in (
        CommandKind.CANCEL,
        CommandKind.CANCEL_EXTRA,
    ):
        raise InvariantError(f"{disposition} is a cancellation disposition")
    if disposition in NEGATIVE_DISPOSITIONS:
        if basis is DispositionBasis.LOOKUP:
            raise InvariantError("a plain lookup never settles a command negatively")
        if basis is DispositionBasis.PROVIDER_RESULT and command.possibly_executed:
            raise InvariantError(
                "a provider result cannot settle a command negatively while an attempt may "
                "have executed; exclusion needs a fenced lookup or review"
            )
        if basis is DispositionBasis.FENCED_LOOKUP and not caps.finality_lookup:
            raise InvariantError("this provider offers no fenced lookup")
    if disposition is Disposition.SUCCEEDED and command.submission_ref is None:
        raise InvariantError("SUCCEEDED requires a bound reservation reference")
    return replace(command, disposition=disposition, basis=basis)


def bind(command: Command, ref: ProviderBookingRef) -> Command:
    """Bind the provider reference. Identity is immutable once bound."""
    if command.submission_ref is not None and command.submission_ref != ref:
        raise InvariantError(
            f"command {command.id} is bound to {command.submission_ref}; cannot rebind to {ref}"
        )
    return replace(command, submission_ref=ref)


def note_lookup(command: Command) -> Command:
    """Count a lookup against the reconciliation budget.

    Only lookups made while the command has an unsettled possible effect count: they are the
    ones trying to settle it. A lookup before any dispatch reconciles nothing.
    """
    if not command.possibly_executed:
        return command
    return replace(command, lookups_performed=command.lookups_performed + 1)


def dispatch_allowed(
    caps: ProviderCapabilities, command: Command, *, now: datetime | None = None
) -> bool:
    """May the platform send another mutating attempt for this command?

    For a provider that cannot deduplicate or settle (``REVIEW``), no mutating dispatch may
    follow a possible effect: that would be the second unsafe dispatch the guarantee forbids.
    For a provider that binds the key before execution, a resubmission is safe, but only
    inside the command's cutoff: after it the provider may have forgotten the key.
    """
    if command.disposition is not Disposition.OPEN:
        return False  # settled, or parked in review: nothing more to send
    if command.possibly_executed and not caps.key_bound_before_execution:
        return False
    if now is not None and command.execution_cutoff is not None:
        require_aware(now, "now")
        if now >= command.execution_cutoff:
            return False
    return True

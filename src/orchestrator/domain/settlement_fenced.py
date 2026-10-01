"""Settlement for providers with execution expiry and a fenced lookup
(docs/booking-state-machine.md).

Provider B settles only positively. A provider that binds its idempotency key before execution,
rejects late requests, and offers a fenced lookup can be settled *negatively* too, and its
uncertainty is recovered by **resubmission** rather than review:

- An attempt with a possible effect is resubmitted with the same key and a fresh short expiry
  while the command's cutoff allows: the provider replays the original outcome, reports "in
  progress", or creates exactly one reservation.
- Once the cutoff (plus the declared clock skew) has passed, a **fenced lookup** by key answers
  finally: every reservation the key produced, or nothing, with the guarantee that no later
  execution can commit. Nothing found: ``REJECTED`` on a ``FENCED_LOOKUP`` basis, booking
  ``FAILED``. Something found: it binds like any discovered reservation.

The plain B rules (definitive rejection settles immediately when no earlier attempt could have
executed; identity and duplicates escalate) apply unchanged; this module adds the two paths
above and the decisions that name them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from orchestrator.domain.capabilities import ProviderCapabilities
from orchestrator.domain.commands import (
    AttemptOutcome,
    Command,
    Disposition,
    DispositionBasis,
    InvariantError,
    SideEffect,
)
from orchestrator.domain.cutoffs import ExpiryPolicy
from orchestrator.domain.settlement import (
    REASON_DUPLICATE,
    REASON_IDENTITY,
    Bind,
    Decision,
    Escalate,
    Rejected,
    Reschedule,
    Reservation,
    Uncertain,
    _bind_for,
    _refs,
    identity_mismatch,
)
from orchestrator.domain.time import require_aware


@dataclass(frozen=True, slots=True)
class Resubmit:
    """Dispatch the same key again with a fresh expiry: the provider deduplicates."""

    expiry_at_or_before: datetime


@dataclass(frozen=True, slots=True)
class FencedLookupDue:
    """No attempt can execute any more (cutoff plus skew passed): ask the fenced lookup."""


@dataclass(frozen=True, slots=True)
class WaitForExclusion:
    """An attempt may still execute at the provider; nothing to do until ``until``."""

    until: datetime


def _require_fenced(caps: ProviderCapabilities) -> None:
    if not (caps.finality_lookup and caps.key_bound_before_execution and caps.execution_expiry):
        raise InvariantError("fenced settlement needs finality, key binding and execution expiry")


def decide_create_recovery(
    caps: ProviderCapabilities, command: Command, *, now: datetime, policy: ExpiryPolicy
) -> Decision | Resubmit | FencedLookupDue | WaitForExclusion:
    """What to do about a CREATE with an unsettled possible effect (state ``UNKNOWN``)."""
    _require_fenced(caps)
    require_aware(now, "now")
    if command.disposition is not Disposition.OPEN:
        raise InvariantError("recovery is for open commands")
    if not command.possibly_executed:
        return Reschedule()
    if command.execution_cutoff is not None and policy.usable(now, command.execution_cutoff):
        return Resubmit(expiry_at_or_before=command.execution_cutoff)
    if command.execution_cutoff is None:
        raise InvariantError("a dispatched command for this provider carries a cutoff")
    excluded_at = policy.excluded_after(caps, command.execution_cutoff)
    if now < excluded_at:
        return WaitForExclusion(until=excluded_at)
    return FencedLookupDue()


def decide_create_after_fenced_lookup(
    caps: ProviderCapabilities, command: Command, reservations: tuple[Reservation, ...]
) -> Decision:
    """The final answer for the key: bind what it produced, or settle negatively."""
    _require_fenced(caps)
    matching = tuple(r for r in reservations if r.client_ref == command.booking_id)
    foreign = tuple(r for r in reservations if r.client_ref != command.booking_id)
    if foreign:
        # Our key produced a reservation for another reference: identity is broken.
        return Escalate(REASON_IDENTITY, implicated=_refs(command, *(r.ref for r in reservations)))
    if len(matching) > 1:
        return Escalate(REASON_DUPLICATE, implicated=_refs(command, *(r.ref for r in matching)))
    if len(matching) == 1:
        return _bind_for(command, matching[0], DispositionBasis.FENCED_LOOKUP)
    if command.is_settled:
        if command.disposition is Disposition.SUCCEEDED:
            # We had bound a reservation the key did not produce: contradiction.
            return Escalate(REASON_IDENTITY, implicated=_refs(command))
        return Uncertain()  # already settled negatively: nothing new
    return Rejected(DispositionBasis.FENCED_LOOKUP)


def attempt_certainly_expired(
    caps: ProviderCapabilities, command: Command, *, now: datetime, policy: ExpiryPolicy
) -> bool:
    """True when every attempt's expiry plus skew has passed: none can execute any more."""
    for a in command.attempts:
        if a.effective_side_effect is not SideEffect.POSSIBLE:
            continue
        if a.request.expiry is None or now < policy.excluded_after(caps, a.request.expiry):
            return False
    return True


__all__ = [
    "AttemptOutcome",
    "Bind",
    "FencedLookupDue",
    "Resubmit",
    "WaitForExclusion",
    "attempt_certainly_expired",
    "decide_create_after_fenced_lookup",
    "decide_create_recovery",
    "identity_mismatch",
]

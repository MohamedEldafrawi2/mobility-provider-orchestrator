"""An independent reference model of Provider A (hold then confirm) and of what the platform may
conclude about it.

The model shares no code with ``orchestrator.domain``. It is the provider's truth: a key bound
before execution with a stored outcome, at most one hold per key, lazy hold expiry, requests
rejected once their ``executeBefore`` has passed, a fence that stops later executions, and the
confirmation of a live hold. Every call returns what the platform observes (an answer, a lost
answer, a definitive rejection) and records what actually happened, so the harness can judge
the domain's decisions against reality.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum

HOLD_LIFETIME = timedelta(minutes=10)


class CreateMode(StrEnum):
    OK = "ok"  # executed, hold created, answer delivered
    LOSE_ANSWER = "lose_answer"  # executed, hold created, answer lost
    DROPPED = "dropped"  # never reached the provider, answer lost
    REJECT = "reject"  # validation rejected it, nothing created
    EDGE_FAILURE = "edge_failure"  # refused at the edge, certainly nothing


class ConfirmMode(StrEnum):
    OK = "ok"
    LOSE_ANSWER = "lose_answer"
    DROPPED = "dropped"
    EDGE_FAILURE = "edge_failure"
    CANCEL_FIRST = "cancel_first"  # someone cancelled the hold at the provider before the call


class Observed(StrEnum):
    """What the platform sees of a call."""

    ANSWER = "answer"  # a reservation body
    LOST = "lost"  # timeout after the send: possible effect
    NOTHING = "nothing"  # certainly no effect (edge, connect)
    REJECTED = "rejected"  # definitive, nothing created
    EXPIRED_REQUEST = "expired_request"  # definitive: executeBefore had passed
    HOLD_EXPIRED = "hold_expired"  # definitive: the hold is gone
    FENCED = "fenced"  # definitive: a fence preceded the commit
    IN_PROGRESS = "in_progress"  # the key is still executing: possible effect


@dataclass
class Hold:
    ref: str
    created_at: datetime
    deadline: datetime
    status: str = "PREBOOKED"  # PREBOOKED | CONFIRMED | EXPIRED | CANCELLED
    confirmed_by: int | None = None  # the confirm call that did it

    def refresh(self, now: datetime) -> None:
        if self.status == "PREBOOKED" and now >= self.deadline:
            self.status = "EXPIRED"


@dataclass
class Call:
    kind: str  # create | confirm | read | fenced
    n: int
    observed: Observed
    executed: bool  # did the provider change state because of this call


@dataclass
class AReferenceModel:
    """Truth for one client key / one booking at Provider A."""

    key_outcome: tuple[str, Observed] | None = None  # ("hold", ...) or ("rejected", ...)
    key_in_progress: bool = False
    hold: Hold | None = None
    fenced: bool = False
    holds_created: int = 0
    calls: list[Call] = field(default_factory=list)
    counter: int = 0

    # Creates -------------------------------------------------------------------------------

    def create(self, mode: CreateMode, *, now: datetime, expiry: datetime) -> Observed:
        """One create (or resubmission) under the key, with its ``executeBefore``."""
        self.counter += 1
        n = self.counter
        if mode is CreateMode.EDGE_FAILURE:
            return self._call("create", n, Observed.NOTHING, executed=False)
        if mode is CreateMode.DROPPED:
            return self._call("create", n, Observed.LOST, executed=False)
        # The request reached the provider: the key decides first (bound before execution).
        if self.key_in_progress:
            return self._call("create", n, Observed.IN_PROGRESS, executed=False)
        if self.key_outcome is not None:
            # A replay of the stored outcome; the answer may still be lost on the way back.
            _kind, stored = self.key_outcome
            seen = Observed.LOST if mode is CreateMode.LOSE_ANSWER else stored
            return self._call("create", n, seen, executed=False)
        if now >= expiry:
            return self._call("create", n, Observed.EXPIRED_REQUEST, executed=False)
        if self.fenced:
            self.key_outcome = ("rejected", Observed.FENCED)
            return self._call("create", n, Observed.FENCED, executed=False)
        if mode is CreateMode.REJECT:
            self.key_outcome = ("rejected", Observed.REJECTED)
            return self._call("create", n, Observed.REJECTED, executed=False)
        self.holds_created += 1
        self.hold = Hold(f"RB{self.holds_created}", now, now + HOLD_LIFETIME)
        self.key_outcome = ("hold", Observed.ANSWER)
        seen = Observed.LOST if mode is CreateMode.LOSE_ANSWER else Observed.ANSWER
        return self._call("create", n, seen, executed=True)

    # Confirms ------------------------------------------------------------------------------

    def confirm(self, mode: ConfirmMode, *, now: datetime, expiry: datetime) -> Observed:
        self.counter += 1
        n = self.counter
        assert self.hold is not None, "a confirm addresses a hold the platform knows"
        if mode is ConfirmMode.EDGE_FAILURE:
            return self._call("confirm", n, Observed.NOTHING, executed=False)
        if mode is ConfirmMode.DROPPED:
            return self._call("confirm", n, Observed.LOST, executed=False)
        if mode is ConfirmMode.CANCEL_FIRST and self.hold.status == "PREBOOKED":
            self.hold.status = "CANCELLED"  # an operator at the provider, not this call
        self.hold.refresh(now)
        if now >= expiry:
            return self._call("confirm", n, Observed.EXPIRED_REQUEST, executed=False)
        if self.hold.status == "EXPIRED":
            return self._call("confirm", n, Observed.HOLD_EXPIRED, executed=False)
        if self.hold.status == "CANCELLED":
            return self._call("confirm", n, Observed.REJECTED, executed=False)
        executed = self.hold.status == "PREBOOKED"
        if executed:
            self.hold.status = "CONFIRMED"
            self.hold.confirmed_by = n
        seen = Observed.LOST if mode is ConfirmMode.LOSE_ANSWER else Observed.ANSWER
        return self._call("confirm", n, seen, executed=executed)

    # Reads ---------------------------------------------------------------------------------

    def read(self, *, now: datetime) -> Hold | None:
        """An authoritative read of the hold by reference (lazy expiry applies)."""
        if self.hold is not None:
            self.hold.refresh(now)
        self.counter += 1
        self.calls.append(Call("read", self.counter, Observed.ANSWER, executed=False))
        return self.hold

    def fenced_lookup(self, *, now: datetime) -> Hold | None:
        """Fence the key: nothing can commit for it afterwards; answer what it produced."""
        self.fenced = True
        self.counter += 1
        self.calls.append(Call("fenced", self.counter, Observed.ANSWER, executed=False))
        if self.hold is not None:
            self.hold.refresh(now)
        return self.hold

    # Judgement -------------------------------------------------------------------------------

    def _call(self, kind: str, n: int, observed: Observed, *, executed: bool) -> Observed:
        self.calls.append(Call(kind, n, observed, executed))
        return observed

    def status(self, *, now: datetime) -> str | None:
        if self.hold is None:
            return None
        self.hold.refresh(now)
        return self.hold.status

    def last_call(self) -> Call:
        return self.calls[-1]

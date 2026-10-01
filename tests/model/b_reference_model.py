"""An independent reference model of Provider B and of what the platform may conclude.

This model shares no code with ``orchestrator.domain``. It records what actually happened at
the provider (the truth), what the platform did (dispatches, lookups, abandonment), and what
the platform observed, and computes the only conclusions the design permits from that. The
domain's decisions are checked against it, including the replay status and the booking state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum


class DispatchMode(StrEnum):
    OK = "ok"  # reservation committed, response delivered
    REJECT = "reject"  # provider validation rejected it; nothing created
    DROP_REQUEST = "drop_request"  # request never arrived; nothing created; no response
    COMMIT_LOSE_RESPONSE = "commit_lose_response"  # reservation committed; response lost
    COMMIT_INVISIBLE = "commit_invisible"  # committed; the yourRef index never exposes it
    LOCAL_DENIAL = "local_denial"  # admission refused it before any network IO
    SAFE_FAILURE = "safe_failure"  # sent, but certainly nothing happened (edge 429/503)


@dataclass
class TruthReservation:
    res_id: str
    client_ref: str
    visible: bool


@dataclass
class BReferenceModel:
    """Truth plus the platform's observations for one CREATE command against Provider B."""

    client_ref: str
    lookup_budget: int
    max_age: timedelta
    max_attempts: int | None = None  # dispatched attempts without effect before review
    reservations: list[TruthReservation] = field(default_factory=list)
    dispatched_to_network: int = 0  # attempts that actually left the platform
    unsafe_dispatches: int = 0  # network dispatches made while an earlier one may have executed
    possible_effect: bool = False
    definitively_rejected: bool = False
    abandoned: bool = False
    observed_refs: list[str] = field(default_factory=list)
    bound_ref: str | None = None  # the one reservation the platform could bind to
    duplicate_case: bool = False
    unexpected_reservation: bool = False  # found something although nothing was dispatched
    lookups_after_uncertain: int = 0
    review_closed: bool = False
    safe_failures: int = 0  # dispatched, certainly without effect
    counter: int = 0

    # Truth ---------------------------------------------------------------------------------

    def dispatch(self, mode: DispatchMode) -> None:
        if mode is DispatchMode.LOCAL_DENIAL:
            return  # nothing left the platform; earlier uncertainty, if any, remains
        if self.possible_effect:
            self.unsafe_dispatches += 1
        self.dispatched_to_network += 1
        match mode:
            case DispatchMode.SAFE_FAILURE:
                self.safe_failures += 1  # dispatched, no effect, no uncertainty
            case DispatchMode.OK:
                self._commit(visible=True)
                self.observed_refs.append(self.reservations[-1].res_id)
                self.bound_ref = self.reservations[-1].res_id
            case DispatchMode.REJECT:
                if not self.possible_effect:
                    self.definitively_rejected = True
            case DispatchMode.DROP_REQUEST:
                self.possible_effect = True
            case DispatchMode.COMMIT_LOSE_RESPONSE:
                self._commit(visible=True)
                self.possible_effect = True
            case DispatchMode.COMMIT_INVISIBLE:
                self._commit(visible=False)
                self.possible_effect = True

    def _commit(self, *, visible: bool) -> None:
        self.counter += 1
        self.reservations.append(
            TruthReservation(f"B{self.counter}", self.client_ref, visible=visible)
        )

    def expose(self) -> None:
        for r in self.reservations:
            r.visible = True

    def inject_duplicate(self) -> None:
        """A second reservation appears for our reference (legacy replay, operator error)."""
        self._commit(visible=True)

    def abandon_eligible(self, age: timedelta) -> bool:
        """Only a command that never left the platform, and only once it is old enough."""
        return age >= self.max_age and not (
            self.dispatched_to_network
            or self.possible_effect
            or self.unexpected_reservation
            or self.abandoned
            or self.definitively_rejected
            or self.exhausted
        )

    def abandon(self, age: timedelta) -> None:
        if not self.abandon_eligible(age):
            raise AssertionError("the platform abandoned a command that may have executed")
        self.abandoned = True

    def review_closable_into(self) -> str | None:
        """The state a review case may close into from what the platform has observed.

        Provider B has no finality, so a case closes only into CONFIRMED, and only when the
        platform has bound one reservation and has never observed any other.
        """
        if self.bound_ref is None or self.observed_refs != [self.bound_ref]:
            return None
        return "CONFIRMED"

    def close_review(self) -> None:
        if self.review_closable_into() is None:
            raise AssertionError("the platform closed a case the evidence does not support")
        self.review_closed = True

    # Observation ---------------------------------------------------------------------------

    def lookup(self) -> list[TruthReservation]:
        if self.possible_effect and not self.observed_refs:
            self.lookups_after_uncertain += 1
        found = [r for r in self.reservations if r.visible]
        for r in found:
            if r.res_id not in self.observed_refs:
                self.observed_refs.append(r.res_id)
        if found and self.bound_ref is None and not self.possible_effect:
            self.unexpected_reservation = True  # nothing we sent could have made it
        elif self.bound_ref is None:
            if len(found) == 1:
                self.bound_ref = found[0].res_id  # the only bindable observation
            elif len(found) >= 2:
                self.duplicate_case = True  # nothing to bind to; review, never a guess
        return found

    # Expectations --------------------------------------------------------------------------

    @property
    def settled_success(self) -> bool:
        """The command succeeded once the platform could bind one confirmed reservation."""
        return self.bound_ref is not None

    @property
    def exhausted(self) -> bool:
        """Bounded escalation (6.2): every dispatched attempt certainly had no effect and the
        attempt budget is spent. Not a negative settlement: nothing at the provider is claimed."""
        # What the platform *observed* decides, not the truth: a reservation nobody has seen
        # yet changes nothing until a lookup finds it (and then it is "unexpected").
        return (
            self.max_attempts is not None
            and self.safe_failures >= self.max_attempts
            and not self.possible_effect
            and not self.observed_refs
            and not self.definitively_rejected
        )

    def allowed_dispositions(self) -> set[str]:
        """Every command disposition the design permits the platform to hold right now."""
        if self.abandoned:
            return {"ABANDONED"}
        if self.settled_success:
            # Later duplicates make the *booking* a review case; the command stays succeeded.
            return {"SUCCEEDED"}
        if self.duplicate_case or self.unexpected_reservation:
            return {"UNRESOLVED"}
        if self.definitively_rejected and not self.possible_effect:
            return {"REJECTED"}
        if self.exhausted:
            return {"UNRESOLVED"}
        if self.dispatched_to_network == 0:
            return {"OPEN"}
        if self.possible_effect and self.lookups_after_uncertain >= self.lookup_budget:
            return {"UNRESOLVED"}
        return {"OPEN"}

    def allowed_booking_states(self) -> set[str]:
        if self.abandoned:
            return {"FAILED"}
        if len(self.observed_refs) >= 2 or self.unexpected_reservation:
            return {"NEEDS_REVIEW"}
        if self.settled_success:
            return {"CONFIRMED"}
        if self.definitively_rejected and not self.possible_effect:
            return {"FAILED"}
        if self.exhausted:
            return {"NEEDS_REVIEW"}
        if self.dispatched_to_network == 0:
            return {"CREATED", "SUBMITTING"}
        if self.possible_effect and self.lookups_after_uncertain >= self.lookup_budget:
            return {"NEEDS_REVIEW"}
        if self.possible_effect:
            return {"UNKNOWN", "SUBMITTING"}
        return {"CREATED", "SUBMITTING"}

    def expected_replay_status(self) -> int:
        table = {
            "OPEN": 202,
            "UNRESOLVED": 202,
            "SUCCEEDED": 201,
            "REJECTED": 422,
            "ABANDONED": 422,
        }
        return table[next(iter(self.allowed_dispositions()))]

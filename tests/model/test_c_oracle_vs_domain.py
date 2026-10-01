"""Observation ordering at Provider C must agree with an independent model of the provider's
history (docs/architecture.md, observations).

The provider's truth is a sequence of facts ``(generation, revision, state)``: legal paths within
a generation, a later generation restarting its revisions. The platform hears about them as
pushed events (any order, duplicated) and as authoritative reads (always the newest fact). The
model, which shares no code with the domain, computes what the platform may believe:

- its watermark ``(generation, revision)`` never moves backwards;
- a pushed fact from a newer generation is never adopted on its own word;
- a fact is adopted only if the provider's state is a legal successor of the platform's; an
  illegal one (a skipped fact, or a new generation that contradicts a settled state) is
  contradictory and review decides, however it arrived;
- after the newest fact was heard (pushed in order or read), the platform's state is the truth.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from hypothesis import given, settings
from hypothesis import strategies as st

from orchestrator.domain import (
    BookingId,
    BookingState,
    ProviderBookingRef,
    Reservation,
    ReservationState,
)
from orchestrator.domain.observations import ObservationOutcome, order_observation
from tests.unit.domain.helpers import T0

BK = BookingId("bk_c")
REF = ProviderBookingRef("MB1")

PATHS = [
    ["PENDING", "CONFIRMED"],
    ["PENDING", "FAILED"],
]
STATE_OF = {
    "PENDING": BookingState.PENDING_PROVIDER,
    "CONFIRMED": BookingState.CONFIRMED,
    "FAILED": BookingState.FAILED,
    "CANCELLED": BookingState.CANCELLED,
}
RES_STATE = {
    "PENDING": ReservationState.PENDING,
    "CONFIRMED": ReservationState.CONFIRMED,
    "FAILED": ReservationState.FAILED,
    "CANCELLED": ReservationState.CANCELLED,
}
# What the platform's state machine lets a provider observation do: a pending booking settles,
# a settled one never moves again on an observation (that is a contradiction for review).
LEGAL_SUCCESSORS = {"PENDING": {"CONFIRMED", "FAILED"}}


@dataclass(frozen=True, slots=True)
class Fact:
    generation: int
    revision: int
    state: str


@dataclass
class CModel:
    """What the platform may believe, computed from the deliveries alone."""

    watermark: tuple[int, int] = (1, 1)
    state: str = "PENDING"
    quarantined: bool = False  # a newer generation was pushed: review until a read adopts it
    contradiction: bool = False
    skipped: bool = False  # a fact arrived with a predecessor still unheard
    heard: set[tuple[int, int]] = field(default_factory=set)

    def push(self, fact: Fact) -> None:
        if self.contradiction:
            return
        key = (fact.generation, fact.revision)
        if fact.generation > self.watermark[0]:
            self.quarantined = True
            return
        if key <= self.watermark:
            return  # old news
        if fact.revision > self.watermark[1] + 1 and fact.generation == self.watermark[0]:
            self.skipped = True
        self.heard.add(key)
        self.adopt(fact)

    def read(self, fact: Fact) -> None:
        if self.contradiction:
            return
        key = (fact.generation, fact.revision)
        if key <= self.watermark:
            return
        if fact.generation == self.watermark[0] and fact.revision > self.watermark[1] + 1:
            self.skipped = True
        self.quarantined = False
        self.adopt(fact)

    def adopt(self, fact: Fact) -> None:
        if fact.state != self.state and fact.state not in LEGAL_SUCCESSORS.get(self.state, ()):
            self.contradiction = True  # the provider says something the history forbids
            return
        self.watermark = (fact.generation, fact.revision)
        self.state = fact.state


def _facts(paths: list[int]) -> list[Fact]:
    facts: list[Fact] = []
    for generation, path_index in enumerate(paths, start=1):
        for revision, state in enumerate(PATHS[path_index], start=1):
            facts.append(Fact(generation, revision, state))
    return facts


def _observation(fact: Fact) -> Reservation:
    return Reservation(
        REF, BK, RES_STATE[fact.state], T0, generation=fact.generation, revision=fact.revision
    )


deliveries = st.lists(
    st.one_of(
        st.tuples(st.just("push"), st.integers(min_value=0, max_value=5)),
        st.tuples(st.just("read"), st.integers(min_value=0, max_value=5)),
    ),
    min_size=1,
    max_size=12,
)
paths = st.lists(st.integers(min_value=0, max_value=len(PATHS) - 1), min_size=1, max_size=2)


@settings(max_examples=500, deadline=None)
@given(paths=paths, deliveries=deliveries, finish=st.booleans())
def test_observation_ordering_agrees_with_the_model(
    paths: list[int], deliveries: list[tuple[str, int]], finish: bool
) -> None:
    facts = _facts(paths)
    # The platform starts where the create response left it: generation 1, revision 1, PENDING.
    state = BookingState.PENDING_PROVIDER
    generation: int | None = 1
    revision: int | None = 1
    model = CModel()
    in_review = False
    for kind, index in deliveries:
        if in_review:
            break
        if kind == "push":
            fact = facts[index % len(facts)]
            authoritative = False
        else:
            # A read reports the newest fact that exists so far in the truth: the index picks
            # how far the provider has progressed, monotonically.
            fact = facts[min(index, len(facts) - 1)]
            authoritative = True
        ordering = order_observation(
            _observation(fact),
            bound_ref=REF,
            state=state,
            generation=generation,
            last_revision=revision,
            authoritative=authoritative,
        )
        before = (generation, revision)
        match ordering.outcome:
            case ObservationOutcome.APPLIED:
                assert ordering.next_state is not None
                state, generation, revision = ordering.next_state, fact.generation, fact.revision
            case ObservationOutcome.NO_CHANGE:
                if ordering.advances_watermark:
                    generation, revision = fact.generation, fact.revision
            case ObservationOutcome.NEWER_GENERATION:
                assert not authoritative, "a read adopts a newer generation"
                in_review = True
            case ObservationOutcome.CONTRADICTORY | ObservationOutcome.REGRESSED:
                in_review = True
            case ObservationOutcome.STALE | ObservationOutcome.SUPERSEDED_GENERATION:
                pass
            case _:
                raise AssertionError(ordering.outcome)
        assert (generation or 0, revision or 0) >= (before[0] or 0, before[1] or 0), (
            "the watermark moved backwards"
        )
        if authoritative:
            model.read(fact)
        else:
            model.push(fact)
        if ordering.outcome is ObservationOutcome.CONTRADICTORY:
            assert model.contradiction, "the domain saw a contradiction the model does not"
            assert model.skipped or fact.generation != before[0], (
                "in-order delivery within a generation never contradicts"
            )
        else:
            assert not model.contradiction, "the model saw a contradiction the domain missed"
        if ordering.outcome is ObservationOutcome.REGRESSED:
            # The read reported something behind what a push already applied: only possible
            # when the pushes ran ahead of the truth the read picked, which the model saw as
            # a stale read and ignored. The domain quarantines instead of guessing.
            assert (fact.generation, fact.revision) <= model.watermark
            model.contradiction = True
        if not in_review:
            assert (generation, revision) == model.watermark, "watermarks disagree"
            assert state is STATE_OF[model.state], "states disagree"
    if finish and not in_review:
        # The newest fact is read: the platform must now say what the provider says.
        newest = facts[-1]
        ordering = order_observation(
            _observation(newest),
            bound_ref=REF,
            state=state,
            generation=generation,
            last_revision=revision,
            authoritative=True,
        )
        if ordering.outcome is ObservationOutcome.APPLIED:
            assert ordering.next_state is STATE_OF[newest.state]
        elif ordering.outcome is ObservationOutcome.NO_CHANGE:
            assert state is STATE_OF[newest.state]
        else:
            assert ordering.outcome is ObservationOutcome.CONTRADICTORY
            model.read(newest)
            assert model.contradiction
            assert model.skipped or newest.generation != (generation or 0), (
                "a contradiction on the final read means a skipped fact or a new generation"
            )

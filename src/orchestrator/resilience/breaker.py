"""A sliding-window circuit breaker (decision D3: in-repo, a teaching objective).

The window is time based and bucketed: the last ``window_seconds`` are split into ``buckets``
slices, each counting successes and failures; a slice older than the window is discarded when
touched. The breaker opens when the window holds at least ``minimum_calls`` calls and the failure
rate reaches ``failure_rate_threshold``; it stays open for ``open_seconds``, then lets exactly
``half_open_max_calls`` probes through: all must succeed to close it, one failure reopens it.

Every admission returns a **permit** bound to the breaker's *generation* (a counter that moves
on every state change). A completion is counted only against the generation that admitted it:
a call admitted while closed and finishing during half-open is not a probe, and a probe of an
earlier half-open period cannot close a later one. In half-open, permits are counted in total,
not in flight, so a finished probe does not make room for an extra one.

What counts as a failure is the caller's decision (see ``admission.Ticket``): a provider that
answers "no" quickly and definitively is healthy; one that times out, resets, or throttles is
not.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from time import monotonic

from orchestrator.resilience.errors import REASON_CIRCUIT, NotDispatchedError


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(slots=True)
class _Bucket:
    index: int = -1  # which window slice this bucket currently holds; -1 is empty
    successes: int = 0
    failures: int = 0


@dataclass(frozen=True, slots=True)
class Permit:
    """Proof of admission, bound to the breaker generation that granted it."""

    generation: int
    probe: bool  # granted while half open: one of the counted probes


@dataclass(frozen=True, slots=True)
class BreakerSnapshot:
    state: CircuitState
    calls: int
    failures: int
    failure_rate: float
    opens_at: float | None  # monotonic instant the open period ends, when open


class CircuitBreaker:
    def __init__(
        self,
        *,
        window_seconds: float = 30.0,
        buckets: int = 10,
        minimum_calls: int = 10,
        failure_rate_threshold: float = 0.5,
        open_seconds: float = 10.0,
        half_open_max_calls: int = 2,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if window_seconds <= 0 or buckets < 1 or minimum_calls < 1 or half_open_max_calls < 1:
            raise ValueError("breaker parameters must be positive")
        if not 0 < failure_rate_threshold <= 1:
            raise ValueError("failure_rate_threshold is a fraction in (0, 1]")
        self._slice = window_seconds / buckets
        self._buckets = [_Bucket() for _ in range(buckets)]
        self.minimum_calls = minimum_calls
        self.failure_rate_threshold = failure_rate_threshold
        self.open_seconds = open_seconds
        self.half_open_max_calls = half_open_max_calls
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._generation = 0
        self._opened_at: float | None = None
        self._probes_granted = 0
        self._probe_successes = 0
        self.transitions = 0

    # Window ----------------------------------------------------------------------------------

    def _bucket(self, now: float) -> _Bucket:
        index = int(now // self._slice)
        bucket = self._buckets[index % len(self._buckets)]
        if bucket.index != index:
            bucket.index, bucket.successes, bucket.failures = index, 0, 0
        return bucket

    def _totals(self, now: float) -> tuple[int, int]:
        oldest = int(now // self._slice) - len(self._buckets) + 1
        successes = failures = 0
        for bucket in self._buckets:
            if bucket.index >= oldest:
                successes += bucket.successes
                failures += bucket.failures
        return successes + failures, failures

    def _reset_window(self) -> None:
        for bucket in self._buckets:
            bucket.index, bucket.successes, bucket.failures = -1, 0, 0

    # State changes -----------------------------------------------------------------------------

    def _move(self, state: CircuitState, now: float) -> None:
        self._state = state
        self._generation += 1
        self.transitions += 1
        self._probes_granted = 0
        self._probe_successes = 0
        self._opened_at = now if state is CircuitState.OPEN else None

    def _maybe_half_open(self, now: float) -> None:
        if (
            self._state is CircuitState.OPEN
            and self._opened_at is not None
            and now >= self._opened_at + self.open_seconds
        ):
            self._move(CircuitState.HALF_OPEN, now)

    # Protocol --------------------------------------------------------------------------------

    @property
    def state(self) -> CircuitState:
        self._maybe_half_open(self._clock())
        return self._state

    @property
    def generation(self) -> int:
        return self._generation

    def check(self) -> None:
        """Refuse when open, or when the half-open probes are all granted. No side effect
        beyond the timed open-to-half-open move."""
        now = self._clock()
        self._maybe_half_open(now)
        if self._state is CircuitState.OPEN:
            assert self._opened_at is not None
            raise NotDispatchedError(
                REASON_CIRCUIT, retry_after=max(self._opened_at + self.open_seconds - now, 0.0)
            )
        if (
            self._state is CircuitState.HALF_OPEN
            and self._probes_granted >= self.half_open_max_calls
        ):
            raise NotDispatchedError(REASON_CIRCUIT, retry_after=None)

    def begin(self) -> Permit:
        """Grant a permit for one call. Call right before the network IO."""
        self.check()
        if self._state is CircuitState.HALF_OPEN:
            self._probes_granted += 1
            return Permit(self._generation, probe=True)
        return Permit(self._generation, probe=False)

    def record(self, permit: Permit, *, success: bool) -> None:
        """Count a completion against the generation that admitted it, and only that one."""
        now = self._clock()
        self._maybe_half_open(now)
        if permit.generation != self._generation:
            return  # admitted under an earlier state: it says nothing about this one
        if self._state is CircuitState.HALF_OPEN:
            if not permit.probe:
                return  # cannot happen (a permit of this generation is a probe); be safe
            if not success:
                self._move(CircuitState.OPEN, now)
                return
            self._probe_successes += 1
            if self._probe_successes >= self.half_open_max_calls:
                self._move(CircuitState.CLOSED, now)
                self._reset_window()
            return
        if self._state is CircuitState.OPEN:
            return
        bucket = self._bucket(now)
        if success:
            bucket.successes += 1
        else:
            bucket.failures += 1
        calls, failures = self._totals(now)
        if calls >= self.minimum_calls and failures / calls >= self.failure_rate_threshold:
            self._move(CircuitState.OPEN, now)

    def snapshot(self) -> BreakerSnapshot:
        now = self._clock()
        self._maybe_half_open(now)
        calls, failures = self._totals(now)
        return BreakerSnapshot(
            state=self._state,
            calls=calls,
            failures=failures,
            failure_rate=failures / calls if calls else 0.0,
            opens_at=(
                self._opened_at + self.open_seconds
                if self._state is CircuitState.OPEN and self._opened_at is not None
                else None
            ),
        )

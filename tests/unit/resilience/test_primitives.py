"""Bulkhead, breaker, quota arithmetic and retry tokens: each rule on its own."""

from __future__ import annotations

import asyncio

import pytest

from orchestrator.domain import ProviderCode
from orchestrator.resilience import (
    DEFAULT_SHARES,
    REASON_BULKHEAD,
    REASON_CIRCUIT,
    Bulkhead,
    CircuitBreaker,
    CircuitState,
    LocalQuota,
    NotDispatchedError,
    Purpose,
    QuotaPolicy,
    RetryTokenBucket,
    Share,
)
from orchestrator.resilience.purpose import validate_shares

P = ProviderCode("prov")


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# Bulkhead ------------------------------------------------------------------------------------


async def test_bulkhead_bounds_in_flight_calls_and_the_wait_for_a_slot() -> None:
    bulkhead = Bulkhead(1, max_wait=0.05)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold() -> None:
        async with bulkhead.slot():
            entered.set()
            await release.wait()

    holder = asyncio.create_task(hold())
    await entered.wait()
    assert bulkhead.in_use == 1
    with pytest.raises(NotDispatchedError) as refused:
        async with bulkhead.slot():
            pass
    assert refused.value.reason == REASON_BULKHEAD
    release.set()
    await holder
    assert bulkhead.in_use == 0
    async with bulkhead.slot() as waited:
        assert waited >= 0


async def test_bulkhead_never_waits_past_the_callers_deadline() -> None:
    clock = Clock()
    bulkhead = Bulkhead(1, max_wait=10.0, clock=clock)
    release = asyncio.Event()

    async def hold() -> None:
        async with bulkhead.slot():
            await release.wait()

    holder = asyncio.create_task(hold())
    await asyncio.sleep(0)
    started = asyncio.get_running_loop().time()
    with pytest.raises(NotDispatchedError):
        async with bulkhead.slot(deadline=clock.now + 0.02):
            pass
    assert asyncio.get_running_loop().time() - started < 1.0, "bounded by the deadline"
    with pytest.raises(NotDispatchedError):
        async with bulkhead.slot(deadline=clock.now - 1):  # already past: no wait at all
            pass
    release.set()
    await holder


def test_bulkhead_rejects_nonsense() -> None:
    with pytest.raises(ValueError):
        Bulkhead(0, max_wait=1)
    with pytest.raises(ValueError):
        Bulkhead(1, max_wait=-1)


# Circuit breaker ----------------------------------------------------------------------------


def _breaker(clock: Clock) -> CircuitBreaker:
    return CircuitBreaker(
        window_seconds=10,
        buckets=5,
        minimum_calls=4,
        failure_rate_threshold=0.5,
        open_seconds=5,
        half_open_max_calls=2,
        clock=clock,
    )


def _fail(b: CircuitBreaker) -> None:
    b.record(b.begin(), success=False)


def _ok(b: CircuitBreaker) -> None:
    b.record(b.begin(), success=True)


def test_breaker_opens_on_the_failure_rate_over_the_window_not_before_minimum_calls() -> None:
    clock = Clock()
    b = _breaker(clock)
    for _ in range(3):
        _fail(b)
    assert b.state is CircuitState.CLOSED, "below minimum calls"
    _fail(b)
    assert b.state is CircuitState.OPEN
    with pytest.raises(NotDispatchedError) as refused:
        b.check()
    assert refused.value.reason == REASON_CIRCUIT
    assert refused.value.retry_after == pytest.approx(5.0)


def test_breaker_forgets_failures_that_left_the_window() -> None:
    clock = Clock()
    b = _breaker(clock)
    _fail(b)
    _fail(b)
    clock.advance(11)  # both failures are older than the window now
    for _ in range(4):
        _ok(b)
    assert b.snapshot().failures == 0
    _fail(b)
    assert b.state is CircuitState.CLOSED, "one failure in five is under the threshold"


def test_breaker_half_open_probes_then_closes_or_reopens() -> None:
    clock = Clock()
    b = _breaker(clock)
    for _ in range(4):
        _fail(b)
    clock.advance(5)
    assert b.state is CircuitState.HALF_OPEN
    first = b.begin()
    second = b.begin()
    with pytest.raises(NotDispatchedError):
        b.begin()  # exactly two probes per half-open period, in total
    b.record(first, success=True)
    with pytest.raises(NotDispatchedError):
        b.begin()  # a finished probe does not make room for a third
    b.record(second, success=False)  # a failed probe reopens
    assert b.state is CircuitState.OPEN
    clock.advance(5)
    p1 = b.begin()
    b.record(p1, success=True)
    p2 = b.begin()
    b.record(p2, success=True)
    assert b.state is CircuitState.CLOSED
    assert b.snapshot().calls == 0, "a fresh window after closing"


def test_breaker_counts_a_completion_only_against_the_generation_that_admitted_it() -> None:
    clock = Clock()
    b = _breaker(clock)
    stale = b.begin()  # admitted while closed
    for _ in range(4):
        _fail(b)
    clock.advance(5)
    assert b.state is CircuitState.HALF_OPEN
    b.record(stale, success=True)  # an old call finishing now is not a probe
    assert b.state is CircuitState.HALF_OPEN
    probe = b.begin()
    b.record(probe, success=True)
    assert b.state is CircuitState.HALF_OPEN, "one probe of two: not closed yet"


def test_breaker_snapshot_reports_when_it_reopens() -> None:
    clock = Clock()
    b = _breaker(clock)
    for _ in range(4):
        _fail(b)
    snap = b.snapshot()
    assert snap.state is CircuitState.OPEN and snap.opens_at == pytest.approx(clock.now + 5)
    assert snap.failure_rate == 1.0


# Quota ---------------------------------------------------------------------------------------


def _policy(allowance: float = 100.0) -> QuotaPolicy:
    return QuotaPolicy(allowance_per_second=allowance, shares=DEFAULT_SHARES, burst_seconds=1.0)


async def test_capped_purposes_cannot_exceed_their_share_even_when_the_rest_is_idle() -> None:
    clock = Clock()
    quota = LocalQuota({P: _policy()}, clock=clock)
    granted = 0
    while (await quota.take(P, Purpose.SEARCH)).allowed:
        granted += 1
    assert granted == 45, "45% of a 100/s allowance with a one-second burst"
    refused = await quota.take(P, Purpose.SEARCH)
    assert refused.retry_after is not None and 0 < refused.retry_after <= 1 / 45 + 1e-9


async def test_reserved_purposes_keep_their_floor_and_borrow_what_capped_ones_leave() -> None:
    clock = Clock()
    quota = LocalQuota({P: _policy()}, clock=clock)
    # search takes its whole share: 45 tokens, also 45 from the shared bucket (70 capacity)
    for _ in range(45):
        assert (await quota.take(P, Purpose.SEARCH)).allowed
    granted = 0
    while (await quota.take(P, Purpose.CONFIRM)).allowed:
        granted += 1
    assert granted == 10 + 25, "its own 10% floor plus the 25 the shared bucket still holds"
    # cancel keeps its own floor untouched by confirm's borrowing
    assert (await quota.take(P, Purpose.CANCEL)).allowed


async def test_quota_refills_at_the_share_rate() -> None:
    clock = Clock()
    quota = LocalQuota({P: _policy()}, clock=clock)
    while (await quota.take(P, Purpose.CREATE)).allowed:
        pass
    clock.advance(0.5)
    granted = 0
    while (await quota.take(P, Purpose.CREATE)).allowed:
        granted += 1
    assert granted == 12, "half a second of a 25/s refill, rounded down"


def test_shares_are_validated() -> None:
    with pytest.raises(ValueError):
        validate_shares({p: Share(0.5, reserved=True) for p in Purpose})
    with pytest.raises(ValueError):
        validate_shares({Purpose.SEARCH: Share(0.5, reserved=False)})
    with pytest.raises(ValueError):
        Share(0, reserved=False)
    with pytest.raises(ValueError):
        QuotaPolicy(allowance_per_second=0, shares=DEFAULT_SHARES)


# Retry tokens --------------------------------------------------------------------------------


def test_retry_tokens_charge_retries_more_after_timeouts_and_refund_on_success() -> None:
    bucket = RetryTokenBucket(capacity=20, retry_cost=5, timeout_retry_cost=10, refund=1)
    assert bucket.take(after_timeout=False) and bucket.tokens == 15
    assert bucket.take(after_timeout=True) and bucket.tokens == 5
    assert bucket.take(after_timeout=False) and bucket.tokens == 0
    assert not bucket.take(after_timeout=False)
    assert bucket.exhausted_count == 1
    for _ in range(30):
        bucket.succeeded()
    assert bucket.tokens == 20, "refunds never exceed capacity"
    with pytest.raises(ValueError):
        RetryTokenBucket(capacity=0)

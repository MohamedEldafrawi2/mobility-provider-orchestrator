"""The admission pipeline and the attempt loop, end to end in memory."""

from __future__ import annotations

import asyncio
import random
from datetime import timedelta

import pytest

from orchestrator.domain import ProviderCode, SideEffect
from orchestrator.providers.errors import ErrorKind, ProviderError
from orchestrator.resilience import (
    DEFAULT_SHARES,
    REASON_BULKHEAD,
    REASON_CIRCUIT,
    REASON_DEADLINE,
    REASON_QUOTA,
    REASON_QUOTA_OUTAGE,
    REASON_RETRY_TOKENS,
    AdmissionController,
    AttemptContext,
    BreakerConfig,
    CircuitState,
    LocalQuota,
    NotDispatchedError,
    Purpose,
    PurposeConfig,
    QuotaPolicy,
    QuotaResult,
    QuotaUnavailableError,
    RetryPolicy,
    retry_verdict,
    run_attempts,
)

P = ProviderCode("prov")


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class BrokenQuota:
    def policy_for(self, provider: ProviderCode) -> QuotaPolicy:
        return QuotaPolicy(100.0, DEFAULT_SHARES)

    def peek(self) -> list[tuple[ProviderCode, str, float]]:
        return []

    async def take(self, provider: ProviderCode, purpose: Purpose) -> QuotaResult:
        raise QuotaUnavailableError("connection refused")


def _controller(clock: Clock, quota: object | None = None) -> AdmissionController:
    breaker = BreakerConfig(
        window_seconds=10, buckets=5, minimum_calls=4, failure_rate_threshold=0.6, open_seconds=5
    )
    cfg = PurposeConfig(
        bulkhead_limit=2, bulkhead_max_wait=0.02, breaker=breaker, retry_token_capacity=10
    )
    return AdmissionController(
        quota or LocalQuota({P: QuotaPolicy(100.0, DEFAULT_SHARES)}, clock=clock),  # type: ignore[arg-type]
        purposes=dict.fromkeys(Purpose, cfg),
        clock=clock,
    )


async def test_admission_order_tokens_bulkhead_breaker_quota() -> None:
    clock = Clock()
    admission = _controller(clock)
    async with admission.admit(P, Purpose.CREATE, operation="create_booking") as ticket:
        ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
    res = admission.resources(P, Purpose.CREATE)
    assert res.breaker.state is CircuitState.CLOSED and res.breaker.snapshot().calls == 1
    assert list(admission.quota.peek()), "the quota gauge has something to report"

    # retry tokens: retries after a provider failure spend them; refused before any other
    # gate. A retry after one of our own refusals costs nothing.
    res.retry_tokens._tokens = 0
    with pytest.raises(NotDispatchedError) as refused:
        async with admission.admit(P, Purpose.CREATE, operation="create_booking", attempt_n=2):
            pass
    assert refused.value.reason == REASON_RETRY_TOKENS
    async with admission.admit(
        P, Purpose.CREATE, operation="create_booking", attempt_n=2, charged=False
    ) as ticket:
        ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)

    # breaker: with one healthy call in the window, three unhealthy ones open it
    for _ in range(3):
        async with admission.admit(P, Purpose.CREATE, operation="create_booking") as ticket:
            ticket.record(outcome="UNKNOWN", side_effect="POSSIBLE", healthy=False)
    with pytest.raises(NotDispatchedError) as opened:
        async with admission.admit(P, Purpose.CREATE, operation="create_booking"):
            pass
    assert opened.value.reason == REASON_CIRCUIT and opened.value.retry_after == pytest.approx(5)

    # a deadline that already passed is refused before anything is consumed
    with pytest.raises(NotDispatchedError) as late:
        async with admission.admit(P, Purpose.LOOKUP, operation="lookup", deadline=clock.now - 1):
            pass
    assert late.value.reason == REASON_DEADLINE


async def test_retry_tokens_are_refunded_when_a_later_gate_refuses() -> None:
    clock = Clock()
    admission = _controller(clock)
    res = admission.resources(P, Purpose.CREATE)
    for _ in range(4):
        with pytest.raises(RuntimeError):
            async with admission.admit(P, Purpose.CREATE, operation="create_booking"):
                raise RuntimeError("boom")
    assert res.breaker.state is CircuitState.OPEN
    before = res.retry_tokens.tokens
    with pytest.raises(NotDispatchedError) as refused:
        async with admission.admit(P, Purpose.CREATE, operation="create_booking", attempt_n=2):
            pass
    assert refused.value.reason == REASON_CIRCUIT
    assert res.retry_tokens.tokens == before, "our own refusal spends no retry tokens"


class _SlowQuota:
    """A quota that answers only after the caller's deadline has passed."""

    def __init__(self, clock: Clock, delay: float) -> None:
        self.clock, self.delay = clock, delay

    def policy_for(self, provider: ProviderCode) -> QuotaPolicy:
        return QuotaPolicy(100.0, DEFAULT_SHARES)

    def peek(self) -> list[tuple[ProviderCode, str, float]]:
        return []

    async def take(self, provider: ProviderCode, purpose: Purpose) -> QuotaResult:
        self.clock.advance(self.delay)
        return QuotaResult(True, 1.0, 1.0, None)


async def test_deadline_is_rechecked_after_the_quota_gate() -> None:
    clock = Clock()
    admission = _controller(clock, _SlowQuota(clock, delay=3.0))
    with pytest.raises(NotDispatchedError) as late:
        async with admission.admit(P, Purpose.LOOKUP, operation="lookup", deadline=clock.now + 1):
            pass
    assert late.value.reason == REASON_DEADLINE
    # and the ticket lets the caller check again right before the network IO
    async with admission.admit(P, Purpose.LOOKUP, operation="lookup", deadline=clock.now + 10) as t:
        assert t.remaining() is not None and t.remaining() > 0
        clock.advance(11)
        with pytest.raises(NotDispatchedError):
            t.require_time()


async def test_definitive_rejections_are_healthy_and_exceptions_are_not() -> None:
    clock = Clock()
    admission = _controller(clock)
    for _ in range(3):
        async with admission.admit(P, Purpose.CREATE, operation="create_booking") as ticket:
            ticket.record(outcome="REJECTED", side_effect="NONE", healthy=True)
    assert admission.resources(P, Purpose.CREATE).breaker.state is CircuitState.CLOSED
    for _ in range(4):
        with pytest.raises(RuntimeError):
            async with admission.admit(P, Purpose.LOOKUP, operation="lookup"):
                raise RuntimeError("boom")
    assert admission.resources(P, Purpose.LOOKUP).breaker.state is CircuitState.OPEN


async def test_quota_exhaustion_and_outage_are_distinct_refusals() -> None:
    clock = Clock()
    admission = _controller(clock)
    granted = 0
    while True:
        try:
            async with admission.admit(P, Purpose.SEARCH, operation="search_trips") as ticket:
                ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
            granted += 1
        except NotDispatchedError as exc:
            assert exc.reason == REASON_QUOTA and exc.retry_after is not None
            break
    assert granted == 45
    closed = _controller(clock, BrokenQuota())
    with pytest.raises(NotDispatchedError) as outage:
        async with closed.admit(P, Purpose.CONFIRM, operation="confirm"):
            pass
    assert outage.value.reason == REASON_QUOTA_OUTAGE, "fails closed: nothing is presumed"


async def test_purposes_are_isolated_from_each_other() -> None:
    """A search storm fills search's bulkhead and share; confirm is untouched."""
    clock = Clock()
    admission = _controller(clock)
    release = asyncio.Event()
    entered = asyncio.Semaphore(0)

    async def storm() -> None:
        async with admission.admit(P, Purpose.SEARCH, operation="search_trips") as ticket:
            entered.release()
            await release.wait()
            ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)

    stormers = [asyncio.create_task(storm()) for _ in range(2)]
    await entered.acquire()
    await entered.acquire()
    with pytest.raises(NotDispatchedError) as full:
        async with admission.admit(P, Purpose.SEARCH, operation="search_trips"):
            pass
    assert full.value.reason == REASON_BULKHEAD
    async with admission.admit(P, Purpose.CONFIRM, operation="confirm") as ticket:
        ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
    release.set()
    await asyncio.gather(*stormers)


# The attempt loop ----------------------------------------------------------------------------


def _transient(kind: ErrorKind, side_effect: SideEffect = SideEffect.NONE) -> ProviderError:
    return ProviderError(kind, side_effect, "x", retry_after=timedelta(seconds=0.3))


def test_retry_verdicts_only_for_failures_that_certainly_had_no_effect() -> None:
    assert retry_verdict(_transient(ErrorKind.TRANSIENT)) is not None
    assert retry_verdict(_transient(ErrorKind.RATE_LIMITED)).retry_after == pytest.approx(0.3)  # type: ignore[union-attr]
    assert retry_verdict(_transient(ErrorKind.TIMEOUT)).after_timeout is True  # type: ignore[union-attr]
    assert retry_verdict(_transient(ErrorKind.TIMEOUT)).charged is True  # type: ignore[union-attr]
    assert retry_verdict(NotDispatchedError(REASON_BULKHEAD)).charged is False  # type: ignore[union-attr]
    assert retry_verdict(_transient(ErrorKind.TIMEOUT, SideEffect.POSSIBLE)) is None
    assert retry_verdict(_transient(ErrorKind.REJECTED)) is None
    assert retry_verdict(_transient(ErrorKind.MALFORMED)) is None
    assert retry_verdict(NotDispatchedError(REASON_QUOTA, retry_after=2.0)).retry_after == 2.0  # type: ignore[union-attr]
    assert retry_verdict(NotDispatchedError(REASON_RETRY_TOKENS)) is None
    assert retry_verdict(NotDispatchedError(REASON_DEADLINE)) is None
    assert retry_verdict(RuntimeError()) is None


async def test_attempt_loop_uses_full_jitter_honours_retry_after_and_stops_at_the_deadline() -> (
    None
):
    clock = Clock()
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(seconds)

    calls: list[tuple[int, bool, bool]] = []

    async def attempt(context: AttemptContext) -> str:
        calls.append((context.n, context.after_timeout, context.charged))
        if context.n == 1:
            raise _transient(ErrorKind.TIMEOUT)
        if context.n == 2:
            raise NotDispatchedError(REASON_QUOTA, retry_after=0.5)
        return "ok"

    result = await run_attempts(
        attempt,
        policy=RetryPolicy(max_attempts=4, base_seconds=0.1, cap_seconds=1.0),
        deadline=clock.now + 10,
        labels={"provider": P, "purpose": "lookup"},
        clock=clock,
        sleep=sleep,
        rng=random.Random(7),
    )
    assert result == "ok"
    assert calls == [(1, False, False), (2, True, True), (3, False, False)]
    assert sleeps[0] >= 0.3, "Retry-After from the provider is a floor"
    assert sleeps[1] >= 0.5, "retry_after from the refusing resource is a floor"

    # the deadline: a wait that would end past it is not taken, the last error is raised
    calls.clear()

    async def always(context: AttemptContext) -> str:
        calls.append((context.n, context.after_timeout, context.charged))
        raise _transient(ErrorKind.TRANSIENT)

    with pytest.raises(ProviderError):
        await run_attempts(
            always,
            policy=RetryPolicy(max_attempts=10, base_seconds=0.1, cap_seconds=1.0),
            deadline=clock.now + 0.2,
            labels={},
            clock=clock,
            sleep=sleep,
            rng=random.Random(7),
        )
    assert len(calls) <= 2


async def test_attempt_loop_never_retries_a_possible_effect_or_a_final_refusal() -> None:
    calls = 0

    async def possible(context: AttemptContext) -> None:
        nonlocal calls
        calls += 1
        raise ProviderError(ErrorKind.TIMEOUT, SideEffect.POSSIBLE, "sent")

    with pytest.raises(ProviderError):
        await run_attempts(possible, policy=RetryPolicy(), deadline=None, labels={})
    assert calls == 1

    async def final(context: AttemptContext) -> None:
        nonlocal calls
        calls += 1
        raise NotDispatchedError(REASON_RETRY_TOKENS)

    calls = 0
    with pytest.raises(NotDispatchedError):
        await run_attempts(final, policy=RetryPolicy(), deadline=None, labels={})
    assert calls == 1

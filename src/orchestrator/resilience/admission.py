"""Admission: the gate every provider call passes, per provider and per purpose.

    retry tokens reserved (retries after a provider failure only)
      -> bulkhead slot (bounded wait, never past the deadline)
        -> circuit breaker check
          -> quota token (Redis, fails closed, bounded by the remaining time)
            -> deadline rechecked, breaker permit granted
              -> the caller commits its dispatch mark and makes the call inside the slot

A refusal is ``NotDispatchedError`` with a reason: nothing was sent, nothing happened, and the
journal may say so with certainty. Reserved retry tokens are released on a refusal, so our own
gates never spend the provider's protection budget. The ticket the caller holds carries the
deadline and the breaker permit, and records the attempt's outcome for the breaker, the retry
tokens and the metrics when the block exits.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from time import monotonic

from opentelemetry.metrics import Observation

from orchestrator.domain import ProviderCode
from orchestrator.providers.errors import ErrorKind, ProviderError
from orchestrator.resilience.breaker import BreakerSnapshot, CircuitBreaker, CircuitState, Permit
from orchestrator.resilience.bulkhead import Bulkhead
from orchestrator.resilience.errors import (
    REASON_DEADLINE,
    REASON_QUOTA,
    REASON_QUOTA_OUTAGE,
    REASON_RETRY_TOKENS,
    NotDispatchedError,
)
from orchestrator.resilience.purpose import Purpose
from orchestrator.resilience.quota import Quota, QuotaResult, QuotaUnavailableError
from orchestrator.resilience.retry_tokens import RetryTokenBucket
from orchestrator.telemetry import metrics


@dataclass(frozen=True, slots=True)
class BreakerConfig:
    window_seconds: float = 30.0
    buckets: int = 10
    minimum_calls: int = 10
    failure_rate_threshold: float = 0.5
    open_seconds: float = 10.0
    half_open_max_calls: int = 2


@dataclass(frozen=True, slots=True)
class PurposeConfig:
    bulkhead_limit: int
    bulkhead_max_wait: float
    breaker: BreakerConfig = field(default_factory=BreakerConfig)
    retry_token_capacity: int = 50


@dataclass(slots=True)
class PurposeResources:
    bulkhead: Bulkhead
    breaker: CircuitBreaker
    retry_tokens: RetryTokenBucket


_UNHEALTHY_KINDS = frozenset({ErrorKind.TIMEOUT, ErrorKind.TRANSIENT, ErrorKind.RATE_LIMITED})


class Ticket:
    """What the caller holds while admitted. Record the outcome before leaving the block."""

    def __init__(
        self,
        resources: PurposeResources,
        *,
        provider: ProviderCode,
        purpose: Purpose,
        operation: str,
        attempt_n: int,
        permit: Permit,
        deadline: float | None,
        started: float,
        clock: Callable[[], float],
    ) -> None:
        self._resources = resources
        self.provider = provider
        self.purpose = purpose
        self.operation = operation
        self.attempt_n = attempt_n
        self.permit = permit
        self.deadline = deadline
        self._started = started
        self._clock = clock
        self.recorded = False
        self.sent = False  # the caller reached the network: the reservation is spent

    def remaining(self) -> float | None:
        """Seconds left before the deadline, or None without one. Check before dispatching."""
        if self.deadline is None:
            return None
        return self.deadline - self._clock()

    def require_time(self) -> None:
        """Raise ``NotDispatchedError(deadline-passed)`` if the deadline has been reached."""
        remaining = self.remaining()
        if remaining is not None and remaining <= 0:
            raise NotDispatchedError(REASON_DEADLINE)

    def record(
        self, *, outcome: str, side_effect: str, healthy: bool, timeout: bool = False
    ) -> None:
        """``healthy``: the provider behaved (a definitive rejection is healthy); a timeout,
        a reset, a 5xx or a throttle is not. Only unhealthy outcomes count against the breaker."""
        if self.recorded:
            return
        self.recorded = True
        if side_effect != "NOT_DISPATCHED":
            self.sent = True
        self._resources.breaker.record(self.permit, success=healthy)
        if healthy:
            self._resources.retry_tokens.succeeded()
        labels = {
            "provider": self.provider,
            "purpose": self.purpose.value,
            "operation": self.operation,
        }
        metrics.provider_attempts.add(1, {**labels, "outcome": outcome, "side_effect": side_effect})
        metrics.provider_attempt_duration.record(
            self._clock() - self._started, {**labels, "outcome": outcome}
        )
        if timeout:
            metrics.provider_timeouts.add(
                1, {"provider": self.provider, "operation": self.operation}
            )

    def record_provider_error(self, exc: ProviderError) -> None:
        """A provider error's kind says whether the provider is unhealthy (timeouts, resets,
        throttling) or merely said no (a definitive rejection is a healthy answer)."""
        self.record(
            outcome=exc.kind.value,
            side_effect=exc.side_effect.value,
            healthy=exc.kind not in _UNHEALTHY_KINDS,
            timeout=exc.kind is ErrorKind.TIMEOUT,
        )


class AdmissionController:
    def __init__(
        self,
        quota: Quota,
        *,
        purposes: Mapping[Purpose, PurposeConfig],
        clock: Callable[[], float] = monotonic,
        quota_timeout: float = 0.25,
    ) -> None:
        if set(purposes) != set(Purpose):
            raise ValueError("every purpose needs a configuration")
        self.quota = quota  # public: tests swap it for an unavailable one
        self._configs = dict(purposes)
        self._clock = clock
        self._quota_timeout = quota_timeout
        self._resources: dict[tuple[ProviderCode, Purpose], PurposeResources] = {}
        metrics.register_gauge_source(
            "provider_circuit_state",
            self._observe_breakers,
            description="0 closed, 1 half open, 2 open",
        )
        metrics.register_gauge_source(
            "provider_quota_tokens",
            self._observe_quota,
            description="Quota tokens left per purpose, as last observed",
        )

    def close(self) -> None:
        metrics.unregister_gauge_source("provider_circuit_state", self._observe_breakers)
        metrics.unregister_gauge_source("provider_quota_tokens", self._observe_quota)

    def resources(self, provider: ProviderCode, purpose: Purpose) -> PurposeResources:
        key = (provider, purpose)
        found = self._resources.get(key)
        if found is None:
            cfg = self._configs[purpose]
            b = cfg.breaker
            found = PurposeResources(
                bulkhead=Bulkhead(
                    cfg.bulkhead_limit, max_wait=cfg.bulkhead_max_wait, clock=self._clock
                ),
                breaker=CircuitBreaker(
                    window_seconds=b.window_seconds,
                    buckets=b.buckets,
                    minimum_calls=b.minimum_calls,
                    failure_rate_threshold=b.failure_rate_threshold,
                    open_seconds=b.open_seconds,
                    half_open_max_calls=b.half_open_max_calls,
                    clock=self._clock,
                ),
                retry_tokens=RetryTokenBucket(capacity=cfg.retry_token_capacity),
            )
            self._resources[key] = found
        return found

    @asynccontextmanager
    async def admit(
        self,
        provider: ProviderCode,
        purpose: Purpose,
        *,
        operation: str,
        attempt_n: int = 1,
        after_timeout: bool = False,
        charged: bool = True,
        deadline: float | None = None,
    ) -> AsyncIterator[Ticket]:
        """Admit one attempt or raise ``NotDispatchedError``. ``deadline`` is a monotonic instant.

        ``charged``: whether this retry follows a provider failure and so spends retry tokens;
        a retry after one of our own refusals does not (the provider saw nothing)."""
        res = self.resources(provider, purpose)
        labels = {"provider": provider, "purpose": purpose.value}
        if deadline is not None and self._clock() >= deadline:
            self._refused(labels, REASON_DEADLINE)
            raise NotDispatchedError(REASON_DEADLINE)
        reserved: int | None = None
        if attempt_n > 1 and charged:
            reserved = res.retry_tokens.reserve(after_timeout=after_timeout)
            if reserved is None:
                metrics.provider_retry_tokens_exhausted.add(1, labels)
                self._refused(labels, REASON_RETRY_TOKENS)
                raise NotDispatchedError(REASON_RETRY_TOKENS)
        try:
            async with res.bulkhead.slot(deadline=deadline) as waited:
                metrics.provider_admission_wait.record(waited, labels)
                res.breaker.check()
                taken = await self._take_quota(provider, purpose, deadline)
                if not taken.allowed:
                    raise NotDispatchedError(REASON_QUOTA, retry_after=taken.retry_after)
                if deadline is not None and self._clock() >= deadline:
                    raise NotDispatchedError(REASON_DEADLINE)  # waiting used the time up
                permit = res.breaker.begin()
                ticket = Ticket(
                    res,
                    provider=provider,
                    purpose=purpose,
                    operation=operation,
                    attempt_n=attempt_n,
                    permit=permit,
                    deadline=deadline,
                    started=self._clock(),
                    clock=self._clock,
                )
                try:
                    yield ticket
                except NotDispatchedError:
                    # The caller refused after admission (deadline before IO): nothing sent.
                    ticket.record(
                        outcome="NOT_DISPATCHED", side_effect="NOT_DISPATCHED", healthy=True
                    )
                    raise
                except BaseException as exc:
                    if not ticket.recorded and not ticket.sent and reserved is not None:
                        # Cancelled or failed before any IO: the reservation goes back.
                        res.retry_tokens.release(reserved)
                        reserved = None
                    ticket.record(
                        outcome="error",
                        side_effect="unknown",
                        healthy=False,
                        timeout=isinstance(exc, TimeoutError),
                    )
                    raise
                else:
                    ticket.record(outcome="unrecorded", side_effect="unknown", healthy=True)
                finally:
                    if ticket.sent:
                        reserved = None  # spent on a real dispatch
        except NotDispatchedError as exc:
            if reserved is not None:
                res.retry_tokens.release(reserved)  # nothing was sent: the tokens go back
            self._refused(labels, exc.reason)
            raise

    async def _take_quota(
        self, provider: ProviderCode, purpose: Purpose, deadline: float | None
    ) -> QuotaResult:
        budget = self._quota_timeout
        if deadline is not None:
            budget = min(budget, max(deadline - self._clock(), 0.0))
        try:
            async with asyncio.timeout(budget):
                return await self.quota.take(provider, purpose)
        except QuotaUnavailableError as exc:
            raise NotDispatchedError(REASON_QUOTA_OUTAGE) from exc
        except TimeoutError as exc:
            raise NotDispatchedError(REASON_QUOTA_OUTAGE) from exc

    @staticmethod
    def _refused(labels: Mapping[str, str], reason: str) -> None:
        metrics.provider_not_dispatched.add(1, {**labels, "reason": reason})

    # Live state ------------------------------------------------------------------------------

    def snapshot(self) -> dict[ProviderCode, dict[Purpose, BreakerSnapshot]]:
        out: dict[ProviderCode, dict[Purpose, BreakerSnapshot]] = {}
        for (provider, purpose), res in self._resources.items():
            out.setdefault(provider, {})[purpose] = res.breaker.snapshot()
        return out

    def _observe_breakers(self) -> Iterable[Observation]:
        levels = {CircuitState.CLOSED: 0, CircuitState.HALF_OPEN: 1, CircuitState.OPEN: 2}
        for (provider, purpose), res in list(self._resources.items()):
            yield Observation(
                levels[res.breaker.state], {"provider": provider, "purpose": purpose.value}
            )

    def _observe_quota(self) -> Iterable[Observation]:
        for provider, purpose, tokens in self.quota.peek():
            yield Observation(tokens, {"provider": provider, "purpose": purpose})

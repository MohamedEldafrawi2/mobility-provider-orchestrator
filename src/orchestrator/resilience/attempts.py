"""The deadline-aware attempt loop.

Written by hand rather than with tenacity: the three things it must do together (full jitter,
honour ``Retry-After`` from the provider or the refusing resource, and never sleep past the
caller's deadline) are clearer as one short loop than as a custom ``wait`` and ``stop`` pair.

Retrying is only ever offered for attempts that certainly had no effect: a local refusal
(``NotDispatchedError``) or a provider failure whose side effect is ``NONE``. A ``POSSIBLE`` effect
is never retried here; the journal and the reconciler own it.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import monotonic

from orchestrator.domain import SideEffect
from orchestrator.providers.errors import ErrorKind, ProviderError
from orchestrator.resilience.errors import REASON_DEADLINE, REASON_RETRY_TOKENS, NotDispatchedError
from orchestrator.telemetry import metrics

_RETRYABLE_KINDS = frozenset({ErrorKind.TRANSIENT, ErrorKind.RATE_LIMITED, ErrorKind.TIMEOUT})
_FINAL_REASONS = frozenset({REASON_RETRY_TOKENS, REASON_DEADLINE})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 4
    base_seconds: float = 0.2
    cap_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1 or self.base_seconds <= 0 or self.cap_seconds <= 0:
            raise ValueError("retry policy values must be positive")


@dataclass(frozen=True, slots=True)
class RetryVerdict:
    retry_after: float | None
    after_timeout: bool  # the provider timed out: the costlier symptom
    charged: bool  # the failure came from the provider: the retry spends retry tokens


@dataclass(frozen=True, slots=True)
class AttemptContext:
    """What an attempt needs to know about the ones before it."""

    n: int = 1
    after_timeout: bool = False
    charged: bool = False


def retry_verdict(exc: BaseException) -> RetryVerdict | None:
    """Whether ``exc`` describes a failure that certainly had no effect and may be retried.

    A local refusal (``NotDispatchedError``) never reached the provider, so retrying it costs no
    retry tokens: those protect the provider, not the platform's own gates.
    """
    if isinstance(exc, NotDispatchedError):
        if exc.reason in _FINAL_REASONS:
            return None
        return RetryVerdict(exc.retry_after, after_timeout=False, charged=False)
    if isinstance(exc, ProviderError):
        if exc.side_effect is not SideEffect.NONE or exc.kind not in _RETRYABLE_KINDS:
            return None
        after = exc.retry_after.total_seconds() if exc.retry_after is not None else None
        return RetryVerdict(after, after_timeout=exc.kind is ErrorKind.TIMEOUT, charged=True)
    return None


async def run_attempts[T](
    attempt: Callable[[AttemptContext], Awaitable[T]],
    *,
    policy: RetryPolicy,
    deadline: float | None,
    labels: dict[str, str],
    verdict: Callable[[BaseException], RetryVerdict | None] = retry_verdict,
    clock: Callable[[], float] = monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    rng: random.Random | None = None,
    first: AttemptContext | None = None,
) -> T:
    """Call ``attempt(context)`` until it returns, is final, or the budget is spent.

    Sleeps happen here, outside any bulkhead slot. Every wait is full-jitter exponential
    (``random(0, min(cap, base * 2**(n-1)))``), raised to the ``retry_after`` the failure
    carried, and skipped entirely (the last error is raised) when it would end past the
    ``deadline``.
    """
    rng = rng or random.Random()  # noqa: S311 - jitter, not security
    context = first or AttemptContext()
    made = 0  # attempts this loop made, regardless of where the history started
    while True:
        try:
            return await attempt(context)
        except Exception as exc:
            decision = verdict(exc)
            n = context.n
            made += 1
            if decision is None or made >= policy.max_attempts:
                raise
            delay = rng.uniform(0, min(policy.cap_seconds, policy.base_seconds * 2 ** (n - 1)))
            if decision.retry_after is not None:
                delay = max(delay, decision.retry_after)
            if deadline is not None and clock() + delay > deadline:
                raise
            metrics.provider_retries.add(1, labels)
            await sleep(delay)
            context = AttemptContext(
                n + 1, after_timeout=decision.after_timeout, charged=decision.charged
            )

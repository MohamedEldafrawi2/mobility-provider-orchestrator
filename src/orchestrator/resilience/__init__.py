"""Resilience wrapper: purposes, bulkheads, breakers, quota, retry tokens, the attempt loop."""

from orchestrator.resilience.admission import (
    AdmissionController,
    BreakerConfig,
    PurposeConfig,
    Ticket,
)
from orchestrator.resilience.attempts import (
    AttemptContext,
    RetryPolicy,
    RetryVerdict,
    retry_verdict,
    run_attempts,
)
from orchestrator.resilience.breaker import BreakerSnapshot, CircuitBreaker, CircuitState
from orchestrator.resilience.bulkhead import Bulkhead
from orchestrator.resilience.errors import (
    REASON_BULKHEAD,
    REASON_CIRCUIT,
    REASON_DEADLINE,
    REASON_QUOTA,
    REASON_QUOTA_OUTAGE,
    REASON_RETRY_TOKENS,
    NotDispatchedError,
)
from orchestrator.resilience.purpose import DEFAULT_SHARES, Purpose, Share
from orchestrator.resilience.quota import (
    LocalQuota,
    Quota,
    QuotaPolicy,
    QuotaResult,
    QuotaUnavailableError,
    RedisQuota,
)
from orchestrator.resilience.retry_tokens import RetryTokenBucket

__all__ = [
    "DEFAULT_SHARES",
    "REASON_BULKHEAD",
    "REASON_CIRCUIT",
    "REASON_DEADLINE",
    "REASON_QUOTA",
    "REASON_QUOTA_OUTAGE",
    "REASON_RETRY_TOKENS",
    "AdmissionController",
    "AttemptContext",
    "BreakerConfig",
    "BreakerSnapshot",
    "Bulkhead",
    "CircuitBreaker",
    "CircuitState",
    "LocalQuota",
    "NotDispatchedError",
    "Purpose",
    "PurposeConfig",
    "Quota",
    "QuotaPolicy",
    "QuotaResult",
    "QuotaUnavailableError",
    "RedisQuota",
    "RetryPolicy",
    "RetryTokenBucket",
    "RetryVerdict",
    "Share",
    "Ticket",
    "retry_verdict",
    "run_attempts",
]

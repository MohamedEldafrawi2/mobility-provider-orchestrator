"""The one way admission says no."""

from __future__ import annotations

REASON_RETRY_TOKENS = "retry-tokens-exhausted"
REASON_BULKHEAD = "bulkhead-full"
REASON_CIRCUIT = "circuit-open"
REASON_QUOTA = "quota-exhausted"
REASON_QUOTA_OUTAGE = "quota-outage"
REASON_DEADLINE = "deadline-passed"


class NotDispatchedError(Exception):
    """Admission refused the attempt before any network IO. Nothing happened anywhere.

    ``retry_after`` is the earliest sensible moment to try again, in seconds, when the refusing
    resource knows it (quota refill, breaker reopening); ``None`` means "use the backoff".
    """

    def __init__(
        self, reason: str, *, retry_after: float | None = None, journaled: bool = False
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retry_after = retry_after
        self.journaled = journaled  # the caller already recorded this refusal as an attempt

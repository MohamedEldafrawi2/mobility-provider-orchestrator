"""Retry tokens: a budget for *retries*, separate from the quota for calls.

Modelled on the retry quota in the AWS SDKs: the first attempt of any call is free; every retry
after a provider failure spends tokens from a per-purpose bucket (more for a retry after a
timeout, which is the costlier symptom), and every success refunds a little. When the bucket is
empty, retries stop and the attempt is ``NOT_DISPATCHED`` with reason ``retry-tokens-exhausted``:
a provider in trouble gets first attempts, not retry storms.

Tokens are *reserved* at admission and *released* if a later gate (bulkhead, breaker, quota,
deadline) refuses the attempt before anything is sent: a refusal of our own must not spend the
provider's protection budget.
"""

from __future__ import annotations


class RetryTokenBucket:
    def __init__(
        self,
        *,
        capacity: int = 50,
        retry_cost: int = 5,
        timeout_retry_cost: int = 10,
        refund: int = 1,
    ) -> None:
        if capacity < 1 or retry_cost < 1 or timeout_retry_cost < 1 or refund < 0:
            raise ValueError("retry token parameters must be positive")
        self.capacity = capacity
        self.retry_cost = retry_cost
        self.timeout_retry_cost = timeout_retry_cost
        self.refund = refund
        self._tokens = capacity
        self.exhausted_count = 0

    @property
    def tokens(self) -> int:
        return self._tokens

    def reserve(self, *, after_timeout: bool) -> int | None:
        """Reserve the tokens a retry costs. Returns the cost, or None when exhausted."""
        cost = self.timeout_retry_cost if after_timeout else self.retry_cost
        if self._tokens < cost:
            self.exhausted_count += 1
            return None
        self._tokens -= cost
        return cost

    def release(self, cost: int) -> None:
        """Give a reservation back: the attempt was refused locally and never sent."""
        self._tokens = min(self.capacity, self._tokens + cost)

    def take(self, *, after_timeout: bool) -> bool:
        return self.reserve(after_timeout=after_timeout) is not None

    def succeeded(self) -> None:
        self._tokens = min(self.capacity, self._tokens + self.refund)

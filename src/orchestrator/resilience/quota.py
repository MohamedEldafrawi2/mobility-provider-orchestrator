"""Per-provider quota, partitioned by purpose, shared across processes through Redis.

Two token buckets decide every take (docs/resilience-strategy.md):

- a *purpose* bucket, refilled at the purpose's share of the provider allowance;
- a *shared* bucket, refilled at what the reserved purposes leave: allowance minus the sum of the
  reserved shares.

A **capped** purpose (search, create) needs a token from its own bucket *and* from the shared
bucket, so it can never exceed its share nor eat into a reserve. A **reserved** purpose
(confirm, cancel, lookup) takes from its own bucket first and borrows from the shared bucket
only when that is empty, so it always has its floor and may use what the capped purposes leave.

The Redis script is atomic per call and uses the Redis clock, so every process sees one bucket
per provider and purpose. ``LocalQuota`` is the same arithmetic in memory with an injectable
clock: tests use it against ``RedisQuota`` to show both agree.

Quota **fails closed**: when Redis cannot answer, ``QuotaUnavailableError`` is raised and admission
refuses the attempt as ``NOT_DISPATCHED`` with reason ``quota-outage``. Nothing is presumed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from time import monotonic
from typing import Protocol

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from orchestrator.domain import ProviderCode
from orchestrator.resilience.purpose import Purpose, Share, validate_shares


class QuotaUnavailableError(Exception):
    """The quota service did not answer. The caller must fail closed."""


@dataclass(frozen=True, slots=True)
class QuotaResult:
    allowed: bool
    purpose_tokens: float  # what is left in the purpose bucket after the take
    shared_tokens: float
    retry_after: float | None  # seconds until a token exists again, when refused


@dataclass(frozen=True, slots=True)
class QuotaPolicy:
    """The allowance of one provider and how it is split."""

    allowance_per_second: float
    shares: Mapping[Purpose, Share]
    burst_seconds: float = 1.0  # bucket capacity, in seconds of refill

    def __post_init__(self) -> None:
        if self.allowance_per_second <= 0 or self.burst_seconds <= 0:
            raise ValueError("allowance and burst must be positive")
        validate_shares(dict(self.shares))

    def purpose_rate(self, purpose: Purpose) -> float:
        return self.allowance_per_second * self.shares[purpose].fraction

    @property
    def shared_rate(self) -> float:
        reserved = sum(s.fraction for s in self.shares.values() if s.reserved)
        return self.allowance_per_second * (1 - reserved)

    def capacity(self, rate: float) -> float:
        return max(rate * self.burst_seconds, 1.0)


class Quota(Protocol):
    async def take(self, provider: ProviderCode, purpose: Purpose) -> QuotaResult: ...

    def policy_for(self, provider: ProviderCode) -> QuotaPolicy: ...

    def peek(self) -> Iterable[tuple[ProviderCode, str, float]]:
        """(provider, purpose, tokens left) as last observed, for the quota gauge. A shared
        Redis bucket is only known as of the last take this process made."""
        ...


def _decide(
    *,
    purpose_tokens: float,
    shared_tokens: float,
    reserved: bool,
    purpose_rate: float,
    shared_rate: float,
) -> tuple[bool, float, float, float | None]:
    """The two-bucket rule. Returns (allowed, purpose left, shared left, retry_after)."""
    if reserved:
        if purpose_tokens >= 1:
            return True, purpose_tokens - 1, shared_tokens, None
        if shared_tokens >= 1:
            return True, purpose_tokens, shared_tokens - 1, None
        wait = min((1 - purpose_tokens) / purpose_rate, (1 - shared_tokens) / shared_rate)
        return False, purpose_tokens, shared_tokens, wait
    if purpose_tokens >= 1 and shared_tokens >= 1:
        return True, purpose_tokens - 1, shared_tokens - 1, None
    wait = max(
        (1 - purpose_tokens) / purpose_rate if purpose_tokens < 1 else 0.0,
        (1 - shared_tokens) / shared_rate if shared_tokens < 1 else 0.0,
    )
    return False, purpose_tokens, shared_tokens, wait


class LocalQuota:
    """In-memory quota for one process: tests, the benchmark, and nothing else."""

    def __init__(
        self,
        policies: Mapping[ProviderCode, QuotaPolicy],
        *,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._policies = dict(policies)
        self._clock = clock
        self._buckets: dict[tuple[ProviderCode, str], tuple[float, float]] = {}  # tokens, at

    def policy_for(self, provider: ProviderCode) -> QuotaPolicy:
        return self._policies[provider]

    def _refill(self, key: tuple[ProviderCode, str], rate: float, capacity: float) -> float:
        now = self._clock()
        tokens, at = self._buckets.get(key, (capacity, now))
        tokens = min(capacity, tokens + (now - at) * rate)
        self._buckets[key] = (tokens, now)
        return tokens

    async def take(self, provider: ProviderCode, purpose: Purpose) -> QuotaResult:
        policy = self._policies[provider]
        purpose_rate, shared_rate = policy.purpose_rate(purpose), policy.shared_rate
        purpose_key, shared_key = (provider, purpose.value), (provider, "shared")
        purpose_tokens = self._refill(purpose_key, purpose_rate, policy.capacity(purpose_rate))
        shared_tokens = self._refill(shared_key, shared_rate, policy.capacity(shared_rate))
        allowed, purpose_left, shared_left, retry_after = _decide(
            purpose_tokens=purpose_tokens,
            shared_tokens=shared_tokens,
            reserved=policy.shares[purpose].reserved,
            purpose_rate=purpose_rate,
            shared_rate=shared_rate,
        )
        now = self._clock()
        self._buckets[purpose_key] = (purpose_left, now)
        self._buckets[shared_key] = (shared_left, now)
        return QuotaResult(allowed, purpose_left, shared_left, retry_after)

    def peek(self) -> Iterable[tuple[ProviderCode, str, float]]:
        for (provider, purpose), (tokens, _) in list(self._buckets.items()):
            yield provider, purpose, tokens


# The same rule in Redis. KEYS: purpose bucket, shared bucket. ARGV: purpose rate, purpose
# capacity, shared rate, shared capacity, reserved (1/0), key ttl seconds.
_TAKE = """
local function refill(key, rate, capacity, now)
  local tokens = tonumber(redis.call('HGET', key, 'tokens'))
  local at = tonumber(redis.call('HGET', key, 'at'))
  if tokens == nil or at == nil then
    return capacity
  end
  local elapsed = now - at
  if elapsed < 0 then elapsed = 0 end
  local filled = tokens + elapsed * rate
  if filled > capacity then filled = capacity end
  return filled
end

local function store(key, tokens, now, ttl)
  redis.call('HSET', key, 'tokens', tokens, 'at', now)
  redis.call('EXPIRE', key, ttl)
end

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local p_rate, p_cap = tonumber(ARGV[1]), tonumber(ARGV[2])
local s_rate, s_cap = tonumber(ARGV[3]), tonumber(ARGV[4])
local reserved = ARGV[5] == '1'
local ttl = tonumber(ARGV[6])
local p = refill(KEYS[1], p_rate, p_cap, now)
local s = refill(KEYS[2], s_rate, s_cap, now)
local allowed = 0
local wait = -1
if reserved then
  if p >= 1 then
    allowed = 1; p = p - 1
  elseif s >= 1 then
    allowed = 1; s = s - 1
  else
    local wp = (1 - p) / p_rate
    local ws = (1 - s) / s_rate
    wait = math.min(wp, ws)
  end
else
  if p >= 1 and s >= 1 then
    allowed = 1; p = p - 1; s = s - 1
  else
    local wp = 0
    local ws = 0
    if p < 1 then wp = (1 - p) / p_rate end
    if s < 1 then ws = (1 - s) / s_rate end
    wait = math.max(wp, ws)
  end
end
store(KEYS[1], p, now, ttl)
store(KEYS[2], s, now, ttl)
return {allowed, tostring(p), tostring(s), tostring(wait)}
"""


class RedisQuota:
    def __init__(
        self,
        client: aioredis.Redis,
        policies: Mapping[ProviderCode, QuotaPolicy],
        *,
        key_prefix: str = "mpo:quota",
        timeout_seconds: float = 0.25,
    ) -> None:
        self._redis = client
        self._policies = dict(policies)
        self._prefix = key_prefix
        self._timeout = timeout_seconds
        self._script = client.register_script(_TAKE)
        self._observed: dict[tuple[ProviderCode, str], float] = {}

    def policy_for(self, provider: ProviderCode) -> QuotaPolicy:
        return self._policies[provider]

    def peek(self) -> Iterable[tuple[ProviderCode, str, float]]:
        """The balances this process last saw. Freshness: the last take per purpose."""
        yield from ((p, purpose, tokens) for (p, purpose), tokens in list(self._observed.items()))

    @staticmethod
    def _ttl_seconds(policy: QuotaPolicy, purpose_rate: float, shared_rate: float) -> int:
        """Long enough for either bucket to refill from empty, twice over: an expired key is
        recreated full, which must never be a shortcut around the allowance."""
        refill = max(
            policy.capacity(purpose_rate) / purpose_rate, policy.capacity(shared_rate) / shared_rate
        )
        return int(refill * 2) + 60

    async def take(self, provider: ProviderCode, purpose: Purpose) -> QuotaResult:
        policy = self._policies[provider]
        purpose_rate, shared_rate = policy.purpose_rate(purpose), policy.shared_rate
        keys = [
            f"{self._prefix}:{provider}:{purpose.value}",
            f"{self._prefix}:{provider}:shared",
        ]
        args = [
            purpose_rate,
            policy.capacity(purpose_rate),
            shared_rate,
            policy.capacity(shared_rate),
            "1" if policy.shares[purpose].reserved else "0",
            self._ttl_seconds(policy, purpose_rate, shared_rate),
        ]
        try:
            async with asyncio.timeout(self._timeout):
                raw = await self._script(keys=keys, args=args)
        except (RedisError, OSError, TimeoutError) as exc:
            raise QuotaUnavailableError(str(exc) or exc.__class__.__name__) from exc
        allowed, purpose_left, shared_left, wait = raw
        retry_after = float(wait)
        self._observed[(provider, purpose.value)] = float(purpose_left)
        self._observed[(provider, "shared")] = float(shared_left)
        return QuotaResult(
            allowed=int(allowed) == 1,
            purpose_tokens=float(purpose_left),
            shared_tokens=float(shared_left),
            retry_after=retry_after if retry_after >= 0 else None,
        )

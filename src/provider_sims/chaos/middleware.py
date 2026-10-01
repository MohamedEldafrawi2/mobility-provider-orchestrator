"""ASGI middleware applying the generic chaos knobs to every non-admin request.

Every response it produces itself (rate limit, unavailability, random failure) carries the
edge header, because those responses come from the provider's front door before any handler
ran and therefore can never have had a side effect. A provider's real edge usually has some
such discriminator (a gateway body, a header); this fictional one documents it.
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import UTC, datetime

from starlette.types import ASGIApp, Receive, Scope, Send

from provider_sims.chaos.config import ChaosConfig, register_random_source

EDGE_HEADER = b"x-bus-edge"
_rng = register_random_source(random.Random())  # noqa: S311 - simulated faults, not security


class _TokenBucket:
    def __init__(self) -> None:
        self.tokens = 1.0
        self.updated = time.monotonic()

    def take(self, rate: float) -> bool:
        now = time.monotonic()
        capacity = max(rate, 1.0)  # always room for at least one request
        self.tokens = min(capacity, self.tokens + (now - self.updated) * rate)
        self.updated = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class ChaosMiddleware:
    def __init__(self, app: ASGIApp, config: ChaosConfig, *, exempt_prefixes: tuple[str, ...]):
        self.app = app
        self.config = config
        self.exempt = exempt_prefixes
        self.bucket = _TokenBucket()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"].startswith(self.exempt):
            await self.app(scope, receive, send)
            return
        cfg = self.config

        if cfg.unavailable_until and datetime.now(UTC) < cfg.unavailable_until:
            await _edge(send, 503, b"service unavailable")
            return
        if cfg.rate_limit_per_second is not None and not self.bucket.take(
            cfg.rate_limit_per_second
        ):
            # Deliberately no Retry-After: the legacy provider gives no hint.
            await _edge(send, 429, b"too many requests")
            return
        if cfg.latency_ms or cfg.jitter_ms:
            delay = cfg.latency_ms + (
                _rng.uniform(-cfg.jitter_ms, cfg.jitter_ms) if cfg.jitter_ms else 0
            )
            await asyncio.sleep(max(delay, 0) / 1000)
        if cfg.failure_rate and _rng.random() < cfg.failure_rate:
            await _edge(send, 503, b"transient failure")
            return
        await self.app(scope, receive, send)


async def _edge(send: Send, status: int, body: bytes) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"text/plain"), (EDGE_HEADER, b"1")],
        }
    )
    await send({"type": "http.response.body", "body": body})

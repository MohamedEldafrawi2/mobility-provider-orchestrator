"""Execution cutoffs and attempt expiries (docs/architecture.md).

For a provider that binds the idempotency key before execution and rejects requests received
after their ``executeBefore``, the platform can resubmit a possibly executed command safely,
but only while the provider still remembers the key. The **command cutoff** is absolute and
anchored to the first dispatch: ``first_dispatch_at + min(idempotency_window - max_clock_skew -
margin, max_command_lifetime)``. Every attempt carries an expiry at or below the cutoff, short
enough that several attempts fit inside one cutoff and that an attempt's possible effect can be
excluded soon after its expiry plus the declared clock skew.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from orchestrator.domain.capabilities import ProviderCapabilities
from orchestrator.domain.time import require_aware


@dataclass(frozen=True, slots=True)
class ExpiryPolicy:
    attempt_ttl: timedelta = timedelta(seconds=30)  # creates and confirms
    acceptance_ttl: timedelta = timedelta(seconds=60)  # refund acceptances
    max_command_lifetime: timedelta = timedelta(minutes=10)
    margin: timedelta = timedelta(seconds=5)  # transmission and processing slack

    def cutoff(self, caps: ProviderCapabilities, first_dispatch_at: datetime) -> datetime | None:
        """The absolute instant after which no attempt of the command may execute, or None
        for a provider without execution expiry (nothing bounds it, nothing can be excluded)."""
        require_aware(first_dispatch_at, "first_dispatch_at")
        if not caps.execution_expiry:
            return None
        skew = caps.max_clock_skew or timedelta(0)
        horizon = self.max_command_lifetime
        if caps.idempotency_window is not None:
            horizon = min(horizon, caps.idempotency_window - skew - self.margin)
        return first_dispatch_at + horizon

    def attempt_expiry(
        self, now: datetime, cutoff: datetime | None, *, acceptance: bool = False
    ) -> datetime | None:
        """This attempt's ``executeBefore``: short, and never past the command cutoff."""
        require_aware(now, "now")
        ttl = self.acceptance_ttl if acceptance else self.attempt_ttl
        expiry = now + ttl
        if cutoff is None:
            return None
        return min(expiry, cutoff)

    def excluded_after(self, caps: ProviderCapabilities, expiry: datetime) -> datetime:
        """When a possibly executed attempt with this expiry can be excluded: its expiry plus
        the provider's clock skew. Before that instant the provider might still execute it."""
        require_aware(expiry, "expiry")
        return expiry + (caps.max_clock_skew or timedelta(0))

    def usable(self, now: datetime, cutoff: datetime | None) -> bool:
        """Is there still room for a supported attempt before the cutoff?"""
        if cutoff is None:
            return True
        return now + self.margin < cutoff

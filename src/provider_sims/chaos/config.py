"""Chaos configuration shared by every simulator (docs/provider-integration-guide.md).

The generic knobs (latency, failure rate, rate limit, unavailability) apply at the edge, before
any handler. Failpoints are named per provider; each simulator documents which ones it honours
and ignores the rest. Values are bounded and validated so a chaos experiment can never wedge a
simulator in a state its tests cannot recover from.
"""

from __future__ import annotations

import random
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Failpoints(BaseModel):
    """Named failpoints. Each provider documents which ones it honours."""

    model_config = ConfigDict(allow_inf_nan=False)

    # Provider B, bus-legacy
    drop_request: bool = False  # the request never reaches the provider: no effect, no response
    after_reserve_commit: str | None = Field(
        default=None, pattern="^(drop|503)$"
    )  # reservation committed, then the response is lost or replaced by a 503
    slow_commit_seconds: float = Field(default=0.0, ge=0.0, le=300.0)  # hold, then commit
    never_index: bool = False  # new reservations never appear in the by-reference index
    hold_seconds: float = Field(default=30.0, ge=0.0, le=3600.0)  # how long "drop" holds the socket

    # Provider A, rail-osdm
    after_prebook_commit: str | None = Field(default=None, pattern="^(drop|503)$")
    after_confirm_commit: str | None = Field(default=None, pattern="^(drop|503)$")
    after_refund_accept_commit: str | None = Field(default=None, pattern="^(drop|503)$")
    slow_refund_accept_commit_seconds: float = Field(default=0.0, ge=0.0, le=300.0)
    pause_before_execute_seconds: float = Field(default=0.0, ge=0.0, le=300.0)
    pause_after_check_before_commit_seconds: float = Field(default=0.0, ge=0.0, le=300.0)
    admit_then_stall_then_commit_seconds: float = Field(default=0.0, ge=0.0, le=300.0)
    lose_response: bool = False  # execute, then close the connection without answering

    # Provider C, mobility-async
    slow_commit_seconds_async: float = Field(default=0.0, ge=0.0, le=300.0)
    validate_then_stall_seconds: float = Field(default=0.0, ge=0.0, le=300.0)


# Every simulator's source of randomness registers here, so a chaos experiment can be seeded
# and reproduced (``seed`` in the configuration reseeds all of them).
RANDOM_SOURCES: list[random.Random] = []


def register_random_source(source: random.Random) -> random.Random:
    RANDOM_SOURCES.append(source)
    return source


def reseed(seed: int) -> None:
    for source in RANDOM_SOURCES:
        source.seed(seed)


class ChaosConfig(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    seed: int | None = None  # reseeds every simulator random source when set
    latency_ms: int = Field(default=0, ge=0, le=600_000)
    jitter_ms: int = Field(default=0, ge=0, le=600_000)
    failure_rate: float = Field(default=0.0, ge=0.0, le=1.0)  # probability of a random 503
    rate_limit_per_second: float | None = Field(default=None, gt=0.0, le=1_000_000.0)
    unavailable_until: datetime | None = None
    lookup_lag_seconds: float = Field(default=0.0, ge=0.0, le=3600.0)
    failpoints: Failpoints = Field(default_factory=Failpoints)

    # Provider A, rail-osdm
    hold_expiry_seconds: float = Field(default=600.0, ge=0.0, le=86_400.0)
    confirm_delay_ms: int = Field(default=0, ge=0, le=600_000)
    generation_bump: int = Field(default=0, ge=0, le=1_000_000)  # added to the reported generation
    clock_offset_ms: int = Field(default=0, ge=-600_000, le=600_000)  # the provider's clock skew

    # Provider C, mobility-async
    pending_seconds: float = Field(default=1.0, ge=0.0, le=3600.0)  # the pending window
    fail_confirmation_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    webhook_duplicate_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    webhook_before_response: bool = False  # the webhook overtakes the create response
    webhook_disabled: bool = False
    stall_pending: bool = False  # pending bookings never progress
    webhook_wrong_booking: bool = False  # the next webhook names another booking

    @field_validator("unavailable_until")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("unavailable_until must be timezone-aware")
        return value

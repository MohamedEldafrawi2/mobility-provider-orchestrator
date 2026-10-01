"""Provider capabilities and policies (docs/provider-integration-guide.md).

Booleans say what a provider can do; policies say how it behaves. The orchestration layer
reasons about these, never about provider identities. Each adapter declares its instance and
contract tests verify every claim against the provider's simulator.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum


class BookingFlow(StrEnum):
    DIRECT = "DIRECT"
    HOLD_THEN_CONFIRM = "HOLD_THEN_CONFIRM"


class Confirmation(StrEnum):
    SYNC = "SYNC"
    ASYNC = "ASYNC"


class LookupByClientRef(StrEnum):
    NONE = "NONE"
    IMMEDIATE = "IMMEDIATE"
    EVENTUAL = "EVENTUAL"


class IdempotentCreate(StrEnum):
    NONE = "NONE"
    KEY = "KEY"
    CLIENT_REF = "CLIENT_REF"


class UnknownResolution(StrEnum):
    RESUBMIT = "RESUBMIT"
    REVIEW = "REVIEW"


class Cancellation(StrEnum):
    NONE = "NONE"
    CONFIRMED_ONLY = "CONFIRMED_ONLY"


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    booking_flow: BookingFlow
    confirmation: Confirmation
    supports_webhooks: bool
    supports_status_lookup: bool
    lookup_by_client_ref: LookupByClientRef
    idempotent_create: IdempotentCreate
    key_bound_before_execution: bool
    idempotency_window: timedelta | None
    execution_expiry: bool
    finality_lookup: bool
    max_clock_skew: timedelta | None
    unknown_resolution: UnknownResolution
    confirm_is_idempotent: bool
    cancellation: Cancellation
    cancel_is_idempotent: bool
    supports_refund: bool
    reports_refund_offer_status: bool
    revisioned: bool
    reports_generation: bool

    def __post_init__(self) -> None:
        if self.unknown_resolution is UnknownResolution.RESUBMIT and (
            self.idempotent_create is IdempotentCreate.NONE or not self.key_bound_before_execution
        ):
            raise ValueError(
                "RESUBMIT requires idempotent create with the key bound before execution"
            )
        if self.finality_lookup and not self.execution_expiry:
            raise ValueError("a finality lookup is only meaningful with execution expiry")
        if self.execution_expiry and self.max_clock_skew is None:
            raise ValueError("execution expiry requires a declared max_clock_skew")

    @property
    def can_settle_negatively(self) -> bool:
        """Only a provider with a fenced lookup lets the platform conclude 'did not happen'."""
        return self.finality_lookup

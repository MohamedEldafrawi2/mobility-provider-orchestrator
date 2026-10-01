"""What Provider B can do, as declared by its adapter and verified by contract tests."""

from __future__ import annotations

from orchestrator.domain.capabilities import (
    BookingFlow,
    Cancellation,
    Confirmation,
    IdempotentCreate,
    LookupByClientRef,
    ProviderCapabilities,
    UnknownResolution,
)

BUS_LEGACY_CAPABILITIES = ProviderCapabilities(
    booking_flow=BookingFlow.DIRECT,
    confirmation=Confirmation.SYNC,
    supports_webhooks=False,
    supports_status_lookup=False,  # no lookup by reservation id; only by our reference
    lookup_by_client_ref=LookupByClientRef.EVENTUAL,
    idempotent_create=IdempotentCreate.NONE,
    key_bound_before_execution=False,
    idempotency_window=None,
    execution_expiry=False,
    finality_lookup=False,
    max_clock_skew=None,
    unknown_resolution=UnknownResolution.REVIEW,
    confirm_is_idempotent=False,
    cancellation=Cancellation.NONE,
    cancel_is_idempotent=False,
    supports_refund=False,
    reports_refund_offer_status=False,
    revisioned=False,
    reports_generation=False,
)

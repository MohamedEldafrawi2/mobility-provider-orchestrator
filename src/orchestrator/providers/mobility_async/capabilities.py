"""What Provider C can do, as declared by its adapter and verified by contract tests."""

from __future__ import annotations

from datetime import timedelta

from orchestrator.domain.capabilities import (
    BookingFlow,
    Cancellation,
    Confirmation,
    IdempotentCreate,
    LookupByClientRef,
    ProviderCapabilities,
    UnknownResolution,
)

MOBILITY_ASYNC_CAPABILITIES = ProviderCapabilities(
    booking_flow=BookingFlow.DIRECT,
    confirmation=Confirmation.ASYNC,
    supports_webhooks=True,
    supports_status_lookup=True,
    lookup_by_client_ref=LookupByClientRef.IMMEDIATE,
    idempotent_create=IdempotentCreate.CLIENT_REF,
    key_bound_before_execution=True,
    idempotency_window=timedelta(hours=24),
    execution_expiry=True,
    finality_lookup=True,
    max_clock_skew=timedelta(seconds=5),
    unknown_resolution=UnknownResolution.RESUBMIT,
    confirm_is_idempotent=False,
    cancellation=Cancellation.CONFIRMED_ONLY,
    cancel_is_idempotent=True,
    supports_refund=False,
    reports_refund_offer_status=False,
    revisioned=True,
    reports_generation=True,
)

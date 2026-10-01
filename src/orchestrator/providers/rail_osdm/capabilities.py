"""What Provider A can do, as declared by its adapter and verified by contract tests."""

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

RAIL_OSDM_CAPABILITIES = ProviderCapabilities(
    booking_flow=BookingFlow.HOLD_THEN_CONFIRM,
    confirmation=Confirmation.SYNC,
    supports_webhooks=False,
    supports_status_lookup=True,
    lookup_by_client_ref=LookupByClientRef.IMMEDIATE,
    idempotent_create=IdempotentCreate.KEY,
    key_bound_before_execution=True,
    idempotency_window=timedelta(hours=24),
    execution_expiry=True,
    finality_lookup=True,
    max_clock_skew=timedelta(seconds=5),
    unknown_resolution=UnknownResolution.RESUBMIT,
    confirm_is_idempotent=True,
    cancellation=Cancellation.CONFIRMED_ONLY,
    cancel_is_idempotent=True,
    supports_refund=True,
    reports_refund_offer_status=True,
    revisioned=True,
    reports_generation=True,
)

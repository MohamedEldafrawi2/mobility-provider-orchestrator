"""The provider port (docs/provider-integration-guide.md). Adapters speak canonical types only.

Every operation an adapter may not support raises ``CapabilityNotSupportedError``; the
capabilities it declares say in advance which ones those are, and contract tests verify each
claim against the provider's simulator.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

from orchestrator.domain import (
    BookingId,
    ProviderBookingRef,
    ProviderCapabilities,
    ProviderCode,
    Reservation,
)
from orchestrator.domain.offers import Location, Offer, PassengerComposition
from orchestrator.domain.refunds import RefundQuote


@dataclass(frozen=True, slots=True)
class TripQuery:
    origin: Location
    destination: Location
    departure_date: date
    passengers: PassengerComposition


@dataclass(frozen=True, slots=True)
class Passenger:
    full_name: str


@dataclass(frozen=True, slots=True)
class CreateBookingRequest:
    booking_id: BookingId  # sent as the client reference, always
    offer: Offer
    passengers: tuple[Passenger, ...]
    contact_email: str


@dataclass(frozen=True, slots=True)
class SearchWarning:
    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class SearchResult:
    offers: tuple[Offer, ...]
    warnings: tuple[SearchWarning, ...] = ()
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class FencedResult:
    """What a key produced, and the guarantee that nothing later can commit for it."""

    key: str
    reservations: tuple[Reservation, ...]
    final: bool = True


class ProviderAdapter(Protocol):
    code: ProviderCode
    capabilities: ProviderCapabilities

    async def search_locations(self, query: str, *, limit: int) -> list[Location]: ...

    async def search_trips(self, query: TripQuery, *, limit: int) -> SearchResult: ...

    async def create_booking(
        self, request: CreateBookingRequest, key: str, expiry: datetime | None
    ) -> Reservation: ...

    async def confirm_booking(self, ref: ProviderBookingRef, expiry: datetime) -> Reservation:
        """Confirm one immutable hold. Never creates another reservation."""
        ...

    async def get_booking(self, ref: ProviderBookingRef) -> Reservation: ...

    async def find_bookings_by_client_ref(self, client_ref: BookingId) -> list[Reservation]: ...

    async def fenced_lookup(self, key: str) -> FencedResult:
        """Finality providers only: every reservation the key produced, or an empty final
        answer, with the guarantee that no later execution of the key can commit."""
        ...

    async def quote_cancellation(self, ref: ProviderBookingRef) -> RefundQuote: ...

    async def cancel_booking(
        self, ref: ProviderBookingRef, quote: RefundQuote | None, expiry: datetime
    ) -> Reservation:
        """Accept the exact refund offer quoted (or cancel for free where no quote exists)."""
        ...

"""Locations, trips, and offers: the canonical shapes every provider is translated into.

An offer is priced for a passenger composition and carries its cancellation terms, so a
booking can be validated against it and a cancellation can be checked against what was
disclosed. Times are always timezone-aware; adapters are responsible for attaching zones.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from orchestrator.domain.ids import ProviderCode
from orchestrator.domain.money import Money


class LocationKind(StrEnum):
    STATION = "STATION"
    STOP = "STOP"
    CITY = "CITY"


class TransportMode(StrEnum):
    RAIL = "RAIL"
    BUS = "BUS"
    SHUTTLE = "SHUTTLE"


@dataclass(frozen=True, slots=True)
class Location:
    id: str  # canonical, platform-minted
    name: str
    country: str
    timezone: str  # IANA
    kind: LocationKind
    provider_ref: str  # this provider's own reference for it


@dataclass(frozen=True, slots=True)
class Segment:
    origin: Location
    destination: Location
    departure: datetime
    arrival: datetime
    mode: TransportMode
    carrier: str
    vehicle_ref: str

    def __post_init__(self) -> None:
        if self.departure.tzinfo is None or self.arrival.tzinfo is None:
            raise ValueError("segment times must be timezone-aware")
        if self.arrival < self.departure:
            raise ValueError("arrival precedes departure")


@dataclass(frozen=True, slots=True)
class Trip:
    id: str
    provider: ProviderCode
    segments: tuple[Segment, ...]

    @property
    def departure(self) -> datetime:
        return self.segments[0].departure

    @property
    def arrival(self) -> datetime:
        return self.segments[-1].arrival


@dataclass(frozen=True, slots=True)
class PassengerComposition:
    adults: int
    children: int = 0

    def __post_init__(self) -> None:
        if self.adults < 1 or self.children < 0:
            raise ValueError("at least one adult; children cannot be negative")

    @property
    def total(self) -> int:
        return self.adults + self.children


@dataclass(frozen=True, slots=True)
class FareConditions:
    refundable: bool
    max_cancellation_fee: Money | None  # None when not refundable


@dataclass(frozen=True, slots=True)
class Offer:
    id: str  # opaque, platform-minted, encodes the provider
    provider: ProviderCode
    trip: Trip
    passengers: PassengerComposition
    total_price: Money
    conditions: FareConditions
    expires_at: datetime
    provider_offer_ref: str

    def __post_init__(self) -> None:
        if self.expires_at.tzinfo is None:
            raise ValueError("expires_at must be timezone-aware")

"""Adapter for Provider C, "mobility-async" (anti-corruption layer).

The provider accepts a booking and confirms it later. A create's success is ``PENDING``: a
reservation that exists and will progress on the provider's own schedule, reported through
signed webhooks and readable by id. Failures are classified by side effect like every adapter:
edge and connect failures ``NONE``; lost answers after the send ``POSSIBLE`` (the client
reference is bound before execution, so the platform resubmits with the same reference or
fences it); definitive rejections ``NONE``.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
from pydantic import BaseModel, Field, ValidationError

from orchestrator.domain import (
    BookingId,
    Money,
    ProviderBookingRef,
    ProviderCapabilities,
    ProviderCode,
    Reservation,
    ReservationState,
    SideEffect,
)
from orchestrator.domain.offers import (
    FareConditions,
    Location,
    LocationKind,
    Offer,
    Segment,
    TransportMode,
    Trip,
)
from orchestrator.domain.refunds import RefundQuote
from orchestrator.providers.errors import CapabilityNotSupportedError, ErrorKind, ProviderError
from orchestrator.providers.mobility_async.capabilities import MOBILITY_ASYNC_CAPABILITIES
from orchestrator.providers.port import (
    CreateBookingRequest,
    FencedResult,
    SearchResult,
    TripQuery,
)
from orchestrator.providers.purpose import Purpose
from orchestrator.providers.transport import PurposeClients

MOBILITY_ASYNC = ProviderCode("mobility-async")
EDGE_HEADER = "x-bus-edge"
OFFER_TTL = timedelta(minutes=30)

_STATES = {
    "PENDING": ReservationState.PENDING,
    "CONFIRMED": ReservationState.CONFIRMED,
    "FAILED": ReservationState.FAILED,
    "CANCELLED": ReservationState.CANCELLED,
}
# The event types the provider documents; anything else is not an observation of a booking.
SUPPORTED_EVENT_TYPES = frozenset(f"booking.{status.lower()}" for status in _STATES)


class _Stop(BaseModel):
    id: str
    name: str
    timezone: str
    country: str


class _StopsBody(BaseModel):
    stops: list[_Stop]


class _MoneyBody(BaseModel):
    amount: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)


class _Product(BaseModel):
    productId: str
    origin: _Stop
    destination: _Stop
    departure: datetime
    arrival: datetime
    pricePerPassenger: _MoneyBody


class _ProductsBody(BaseModel):
    date: date
    products: list[_Product]


class _BookingBody(BaseModel):
    providerBookingId: str
    clientRef: str
    productId: str
    serviceDate: date
    status: str
    generation: int = Field(ge=0)
    sequence: int = Field(ge=0)


class _EventBody(BaseModel):
    """A webhook names the booking and its state; product and date are not repeated."""

    providerBookingId: str
    clientRef: str
    status: str
    generation: int = Field(ge=0)
    sequence: int = Field(ge=0)
    productId: str | None = None
    serviceDate: date | None = None
    type: str | None = None  # present on events, absent on reads


class _BookingsBody(BaseModel):
    bookings: list[_BookingBody]


class _FencedBody(BaseModel):
    clientRef: str
    final: bool
    bookings: list[_BookingBody]


class MobilityAsyncAdapter:
    code = MOBILITY_ASYNC
    capabilities: ProviderCapabilities = MOBILITY_ASYNC_CAPABILITIES

    def __init__(self, clients: PurposeClients | httpx.AsyncClient) -> None:
        self._clients = (
            clients if isinstance(clients, PurposeClients) else PurposeClients.shared(clients)
        )

    # Reads ---------------------------------------------------------------------------------

    async def search_locations(self, query: str, *, limit: int) -> list[Location]:
        body = await self._read(
            "GET", "/stops", params={"q": query}, model=_StopsBody, purpose=Purpose.SEARCH
        )
        return [self._location(s) for s in body.stops[:limit]]

    async def search_trips(self, query: TripQuery, *, limit: int) -> SearchResult:
        params = {
            "origin": query.origin.provider_ref,
            "destination": query.destination.provider_ref,
            "date": query.departure_date.isoformat(),
        }
        body = await self._read(
            "GET", "/products", params=params, model=_ProductsBody, purpose=Purpose.SEARCH
        )
        offers = tuple(self._offer(p, query) for p in body.products[:limit])
        return SearchResult(offers, truncated=len(body.products) > limit)

    async def get_booking(self, ref: ProviderBookingRef) -> Reservation:
        body = await self._read(
            "GET", f"/bookings/{ref}", model=_BookingBody, purpose=Purpose.LOOKUP
        )
        return self.reservation_from(body.model_dump())

    async def find_bookings_by_client_ref(self, client_ref: BookingId) -> list[Reservation]:
        body = await self._read(
            "GET",
            "/bookings",
            params={"clientRef": client_ref},
            model=_BookingsBody,
            purpose=Purpose.LOOKUP,
        )
        return [self.reservation_from(b.model_dump()) for b in body.bookings]

    async def fenced_lookup(self, key: str) -> FencedResult:
        body = await self._read(
            "POST",
            "/bookings/fenced-lookup",
            json={"clientRef": key},
            model=_FencedBody,
            purpose=Purpose.LOOKUP,
        )
        if not body.final or body.clientRef != key:
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, "fenced lookup not final")
        return FencedResult(
            key, tuple(self.reservation_from(b.model_dump()) for b in body.bookings)
        )

    # Writes --------------------------------------------------------------------------------

    async def create_booking(
        self, request: CreateBookingRequest, key: str, expiry: datetime | None
    ) -> Reservation:
        if expiry is None:
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NOT_DISPATCHED, "expiry required")
        payload = {
            "productId": request.offer.provider_offer_ref,
            "date": request.offer.trip.departure.date().isoformat(),
            "clientRef": str(request.booking_id),
            "passengers": [{"name": p.full_name} for p in request.passengers],
            "contactEmail": request.contact_email,
            "executeBefore": expiry.isoformat(),
        }
        body = await self._mutate(
            "POST",
            "/bookings",
            json=payload,
            model=_BookingBody,
            purpose=Purpose.CREATE,
            ok=(200, 202),
        )
        if (
            body.clientRef != request.booking_id
            or body.productId != request.offer.provider_offer_ref
        ):
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.POSSIBLE, "success body does not echo our request"
            )
        return self._mutation_result(body)

    async def confirm_booking(self, ref: ProviderBookingRef, expiry: datetime) -> Reservation:
        raise CapabilityNotSupportedError("mobility-async confirms on its own schedule")

    async def quote_cancellation(self, ref: ProviderBookingRef) -> RefundQuote:
        raise CapabilityNotSupportedError("mobility-async cancels for free: no quote")

    async def cancel_booking(
        self, ref: ProviderBookingRef, quote: RefundQuote | None, expiry: datetime
    ) -> Reservation:
        body = await self._mutate(
            "POST",
            f"/bookings/{ref}/cancel",
            json={"executeBefore": expiry.isoformat()},
            model=_BookingBody,
            purpose=Purpose.CANCEL,
            ok=(200,),
        )
        if body.providerBookingId != ref:
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.POSSIBLE, "cancel body names another booking"
            )
        return self._mutation_result(body)

    def _mutation_result(self, body: _BookingBody) -> Reservation:
        """Semantic decoding of a successful mutation's body: a body the adapter cannot map
        is malformed *with the side effect of the mutation* (something was accepted; what, the
        platform does not know), never a plain NONE as for a read."""
        try:
            return self.reservation_from(body.model_dump())
        except ProviderError as exc:
            raise ProviderError(exc.kind, SideEffect.POSSIBLE, exc.detail) from exc

    # Mapping -------------------------------------------------------------------------------

    @staticmethod
    def _location(stop: _Stop) -> Location:
        return Location(
            id=f"loc_mob_{stop.id}",
            name=stop.name,
            country=stop.country,
            timezone=stop.timezone,
            kind=LocationKind.STOP,
            provider_ref=stop.id,
        )

    def _offer(self, product: _Product, query: TripQuery) -> Offer:
        if product.departure.tzinfo is None or product.arrival.tzinfo is None:
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, "naive product time")
        segment = Segment(
            origin=self._location(product.origin),
            destination=self._location(product.destination),
            departure=product.departure,
            arrival=product.arrival,
            mode=TransportMode.SHUTTLE,
            carrier="Fictional Shuttles",
            vehicle_ref=product.productId,
        )
        unit = Money(product.pricePerPassenger.amount, product.pricePerPassenger.currency)
        composition = f"a{query.passengers.adults}c{query.passengers.children}"
        return Offer(
            id=f"off_mob_{product.productId}_{query.departure_date.isoformat()}_{composition}",
            provider=self.code,
            trip=Trip(
                id=f"trip_mob_{product.productId}_{query.departure_date.isoformat()}",
                provider=self.code,
                segments=(segment,),
            ),
            passengers=query.passengers,
            total_price=Money(unit.amount_minor * query.passengers.total, unit.currency),
            conditions=FareConditions(
                refundable=True, max_cancellation_fee=Money(0, unit.currency)
            ),
            expires_at=datetime.now(UTC) + OFFER_TTL,
            provider_offer_ref=product.productId,
        )

    @classmethod
    def event_from(cls, body: dict[str, Any]) -> Reservation:
        """A pushed event: it must name a documented event type consistent with its status.
        A payload without a type is not an event the provider documents, whatever else it says."""
        if not isinstance(body.get("type"), str):
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, "event type required")
        return cls.reservation_from(body)

    @staticmethod
    def reservation_from(body: dict[str, Any]) -> Reservation:
        """One mapping for pulled observations (reads) and, through ``event_from``, pushed ones."""
        try:
            parsed = _EventBody.model_validate(body)
        except ValidationError as exc:
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, f"unparsable: {exc}") from exc
        state = _STATES.get(parsed.status)
        if state is None:
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.NONE, f"undocumented status {parsed.status!r}"
            )
        if parsed.type is not None and (
            parsed.type not in SUPPORTED_EVENT_TYPES
            or parsed.type != f"booking.{parsed.status.lower()}"
        ):
            # An undocumented event type, or one that disagrees with the status it carries, is
            # not an observation of this booking's state.
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.NONE, f"unsupported event type {parsed.type!r}"
            )
        return Reservation(
            ProviderBookingRef(parsed.providerBookingId),
            BookingId(parsed.clientRef),
            state,
            datetime.now(UTC),
            product_ref=parsed.productId,
            service_date=parsed.serviceDate,
            generation=parsed.generation,
            revision=parsed.sequence,
        )

    # Transport -----------------------------------------------------------------------------

    async def _read[T: BaseModel](
        self,
        method: str,
        path: str,
        *,
        model: type[T],
        purpose: Purpose,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> T:
        try:
            response = await self._clients.for_purpose(purpose).request(
                method, path, params=params, json=json
            )
        except httpx.TimeoutException as exc:
            raise ProviderError(ErrorKind.TIMEOUT, SideEffect.NONE, f"timeout: {exc!r}") from exc
        except httpx.HTTPError as exc:
            raise ProviderError(
                ErrorKind.TRANSIENT, SideEffect.NONE, f"transport: {exc!r}"
            ) from exc
        if response.status_code != 200:
            raise self._translate(response, mutation=False)
        return self._parse(response, model, SideEffect.NONE)

    async def _mutate[T: BaseModel](
        self,
        method: str,
        path: str,
        *,
        model: type[T],
        purpose: Purpose,
        ok: tuple[int, ...],
        json: dict[str, Any] | None = None,
    ) -> T:
        try:
            response = await self._clients.for_purpose(purpose).request(method, path, json=json)
        except httpx.ConnectError as exc:
            raise ProviderError(ErrorKind.TRANSIENT, SideEffect.NONE, f"connect: {exc!r}") from exc
        except (httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            raise ProviderError(
                ErrorKind.TIMEOUT, SideEffect.NONE, f"before send: {exc!r}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise ProviderError(
                ErrorKind.TIMEOUT, SideEffect.POSSIBLE, f"timeout: {exc!r}"
            ) from exc
        except httpx.HTTPError as exc:
            raise ProviderError(
                ErrorKind.TRANSIENT, SideEffect.POSSIBLE, f"transport: {exc!r}"
            ) from exc
        if response.status_code not in ok:
            raise self._translate(response, mutation=True)
        return self._parse(response, model, SideEffect.POSSIBLE)

    @staticmethod
    def _parse[T: BaseModel](response: httpx.Response, model: type[T], on_error: SideEffect) -> T:
        try:
            return model.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise ProviderError(ErrorKind.MALFORMED, on_error, f"unparsable: {exc}") from exc

    @staticmethod
    def _translate(response: httpx.Response, *, mutation: bool) -> ProviderError:
        status = response.status_code
        edge = response.headers.get(EDGE_HEADER) == "1"
        code = ""
        with contextlib.suppress(ValueError, AttributeError):
            code = str(response.json().get("code", ""))
        retry_after = None
        if response.headers.get("retry-after"):
            try:
                retry_after = timedelta(seconds=float(response.headers["retry-after"]))
            except ValueError:
                retry_after = None
        if status == 429:
            return ProviderError(
                ErrorKind.RATE_LIMITED, SideEffect.NONE, "rate limited", retry_after=retry_after
            )
        if edge:
            return ProviderError(ErrorKind.TRANSIENT, SideEffect.NONE, f"edge {status}")
        possible = SideEffect.POSSIBLE if mutation else SideEffect.NONE
        match (status, code):
            case (409, "IN_PROGRESS"):
                return ProviderError(
                    ErrorKind.TRANSIENT, SideEffect.POSSIBLE, "reference in progress"
                )
            case (409, "FENCED"):
                return ProviderError(ErrorKind.FENCED, SideEffect.NONE, "fenced")
            case (422, "EXPIRED_REQUEST"):
                return ProviderError(
                    ErrorKind.EXPIRED_REQUEST, SideEffect.NONE, "executeBefore passed"
                )
            case (422, _) | (404, "PRODUCT_NOT_FOUND") | (409, "NOT_CANCELLABLE"):
                return ProviderError(ErrorKind.REJECTED, SideEffect.NONE, f"{code or status}")
            case (404, _):
                return ProviderError(ErrorKind.NOT_FOUND, SideEffect.NONE, f"not found: {code}")
            case (400, _):
                return ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, f"bad request: {code}")
        if status >= 500:
            return ProviderError(ErrorKind.TRANSIENT, possible, f"{status} after the handler")
        return ProviderError(ErrorKind.MALFORMED, possible, f"unexpected {status} {code}")

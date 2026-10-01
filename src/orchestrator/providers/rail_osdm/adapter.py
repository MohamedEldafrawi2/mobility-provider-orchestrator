"""Adapter for Provider A, "rail-osdm" (anti-corruption layer).

Translates the OSDM-flavoured API into canonical types and every failure into a
``ProviderError`` that says whether a side effect may have happened:

- nothing was sent (DNS or connect failure, connect or pool timeout, an edge response marked
  by the provider's front door): ``NONE``, retry-safe;
- the request was sent and no definitive answer came back (read timeout, reset, a 5xx after
  the handler, an unparsable body, ``IN_PROGRESS`` for our key): ``POSSIBLE``; the key is
  bound before execution, so the platform resubmits with the same key or fences it;
- a rejection the provider made final (expired request, expired offer, sold out, hold expired,
  fenced): ``NONE`` and definitive.

A success is only a success if the provider echoes our reference and the product we asked
for; anything else is an inconsistent body and stays uncertain.
"""

from __future__ import annotations

from collections.abc import Callable
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
from orchestrator.domain.refunds import RefundOfferState, RefundOfferStatus, RefundQuote
from orchestrator.providers.errors import ErrorKind, ProviderError
from orchestrator.providers.port import (
    CreateBookingRequest,
    FencedResult,
    SearchResult,
    TripQuery,
)
from orchestrator.providers.purpose import Purpose
from orchestrator.providers.rail_osdm.capabilities import RAIL_OSDM_CAPABILITIES
from orchestrator.providers.transport import PurposeClients

RAIL_OSDM = ProviderCode("rail-osdm")
# The provider's edge marks responses it produced before reaching a handler (documented).
EDGE_HEADER = "x-bus-edge"

_STATES = {
    "PREBOOKED": ReservationState.HELD,
    "CONFIRMED": ReservationState.CONFIRMED,
    "CANCELLED": ReservationState.CANCELLED,
    "EXPIRED": ReservationState.FAILED,
}
_REFUND_STATES = {s.value: s for s in RefundOfferState}


class _Place(BaseModel):
    id: str
    name: str
    timezone: str
    country: str


class _PlacesBody(BaseModel):
    places: list[_Place]


class _MoneyBody(BaseModel):
    amount: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)


class _Conditions(BaseModel):
    refundable: bool
    feePercent: int = Field(ge=0, le=100)


class _Leg(BaseModel):
    origin: _PlaceRef
    destination: _PlaceRef
    departure: datetime
    arrival: datetime
    vehicle: str


class _PlaceRef(BaseModel):
    id: str
    name: str
    timezone: str


class _TripBody(BaseModel):
    id: str
    legs: list[_Leg] = Field(min_length=1)


class _OfferBody(BaseModel):
    offerId: str
    validUntil: datetime
    price: _MoneyBody
    cancellationConditions: _Conditions
    trip: _TripBody


class _OffersBody(BaseModel):
    offers: list[_OfferBody]


class _RefundOfferRef(BaseModel):
    id: str
    status: str
    validUntil: datetime


class _BookingBody(BaseModel):
    bookingId: str
    externalRef: str
    offerId: str
    tripId: str
    serviceDate: date
    status: str
    confirmationTimeLimit: datetime
    generation: int = Field(ge=0)
    version: int = Field(ge=0)
    refundOffers: list[_RefundOfferRef] = Field(default_factory=list)


class _BookingsBody(BaseModel):
    bookings: list[_BookingBody]


class _FencedBody(BaseModel):
    idempotencyKey: str
    final: bool
    bookings: list[_BookingBody]


class _RefundQuoteBody(BaseModel):
    refundOfferId: str
    bookingId: str
    status: str
    validUntil: datetime
    refund: _MoneyBody
    fee: _MoneyBody


class _AcceptBody(BaseModel):
    refundOfferId: str
    status: str
    booking: _BookingBody


_Leg.model_rebuild()


class RailOsdmAdapter:
    code = RAIL_OSDM
    capabilities: ProviderCapabilities = RAIL_OSDM_CAPABILITIES

    def __init__(self, clients: PurposeClients | httpx.AsyncClient) -> None:
        self._clients = (
            clients if isinstance(clients, PurposeClients) else PurposeClients.shared(clients)
        )

    # Reads ---------------------------------------------------------------------------------

    async def search_locations(self, query: str, *, limit: int) -> list[Location]:
        body = await self._read(
            "GET", "/places", params={"name": query}, model=_PlacesBody, purpose=Purpose.SEARCH
        )
        return [self._location(p.id, p.name, p.timezone, p.country) for p in body.places[:limit]]

    async def search_trips(self, query: TripQuery, *, limit: int) -> SearchResult:
        payload = {
            "origin": query.origin.provider_ref,
            "destination": query.destination.provider_ref,
            "date": query.departure_date.isoformat(),
            "adults": query.passengers.adults,
            "children": query.passengers.children,
        }
        body = await self._read(
            "POST", "/offers", json=payload, model=_OffersBody, purpose=Purpose.SEARCH
        )
        offers = tuple(self._offer(o, query) for o in body.offers[:limit])
        return SearchResult(offers, truncated=len(body.offers) > limit)

    async def get_booking(self, ref: ProviderBookingRef) -> Reservation:
        body = await self._read(
            "GET", f"/bookings/{ref}", model=_BookingBody, purpose=Purpose.LOOKUP
        )
        return self._mapped(SideEffect.NONE, lambda: self._reservation(body))  # a read

    async def find_bookings_by_client_ref(self, client_ref: BookingId) -> list[Reservation]:
        body = await self._read(
            "GET",
            "/bookings",
            params={"externalRef": client_ref},
            model=_BookingsBody,
            purpose=Purpose.LOOKUP,
        )
        return [self._reservation(b) for b in body.bookings]

    async def fenced_lookup(self, key: str) -> FencedResult:
        body = await self._read(
            "POST",
            "/bookings/fenced-lookup",
            json={"idempotencyKey": key},
            model=_FencedBody,
            purpose=Purpose.LOOKUP,
        )
        if not body.final or body.idempotencyKey != key:
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, "fenced lookup not final")
        return FencedResult(key, tuple(self._reservation(b) for b in body.bookings))

    # Writes --------------------------------------------------------------------------------

    async def create_booking(
        self, request: CreateBookingRequest, key: str, expiry: datetime | None
    ) -> Reservation:
        if expiry is None:
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NOT_DISPATCHED, "expiry required")
        payload = {
            "offerId": request.offer.provider_offer_ref,
            "externalRef": str(request.booking_id),
            "passengers": [self._passenger(p.full_name) for p in request.passengers],
            "contactEmail": request.contact_email,
            "executeBefore": expiry.isoformat(),
        }
        body = await self._mutate(
            "POST",
            "/bookings",
            json=payload,
            headers={"Idempotency-Key": key},
            model=_BookingBody,
            purpose=Purpose.CREATE,
            ok=(200, 201),
        )
        if (
            body.externalRef != request.booking_id
            or body.offerId != request.offer.provider_offer_ref
        ):
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.POSSIBLE, "success body does not echo our request"
            )
        return self._mapped(SideEffect.POSSIBLE, lambda: self._reservation(body))

    async def confirm_booking(self, ref: ProviderBookingRef, expiry: datetime) -> Reservation:
        body = await self._mutate(
            "PATCH",
            f"/bookings/{ref}",
            json={"status": "CONFIRMED", "executeBefore": expiry.isoformat()},
            model=_BookingBody,
            purpose=Purpose.CONFIRM,
            ok=(200,),
        )
        if body.bookingId != ref:
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.POSSIBLE, "success body names another booking"
            )
        return self._mapped(SideEffect.POSSIBLE, lambda: self._reservation(body))

    async def quote_cancellation(self, ref: ProviderBookingRef) -> RefundQuote:
        body = await self._mutate(
            "POST",
            f"/bookings/{ref}/refund-offers",
            model=_RefundQuoteBody,
            purpose=Purpose.CANCEL,
            ok=(200, 201),
        )
        if body.bookingId != ref:
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, "quote names another booking")
        # A quote is an offer, not a commitment: an unmappable one is simply asked for again.
        return self._mapped(
            SideEffect.NONE,
            lambda: RefundQuote(
                body.refundOfferId,
                Money(body.refund.amount, body.refund.currency),
                Money(body.fee.amount, body.fee.currency),
                body.validUntil,
            ),
        )

    async def cancel_booking(
        self, ref: ProviderBookingRef, quote: RefundQuote | None, expiry: datetime
    ) -> Reservation:
        if quote is None:
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.NOT_DISPATCHED, "this provider cancels by quote"
            )
        body = await self._mutate(
            "PATCH",
            f"/bookings/{ref}/refund-offers/{quote.offer_id}",
            json={"status": "CONFIRMED", "executeBefore": expiry.isoformat()},
            model=_AcceptBody,
            purpose=Purpose.CANCEL,
            ok=(200,),
        )
        if body.refundOfferId != quote.offer_id or body.booking.bookingId != ref:
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.POSSIBLE, "acceptance body does not echo the offer"
            )
        if body.status != "CONFIRMED":
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.POSSIBLE, f"acceptance status {body.status!r}"
            )
        return self._mapped(SideEffect.POSSIBLE, lambda: self._reservation(body.booking))

    # Mapping -------------------------------------------------------------------------------

    @staticmethod
    def _passenger(full_name: str) -> dict[str, str]:
        first, _, last = full_name.strip().partition(" ")
        return {"firstName": first or full_name, "lastName": last or "-"}

    @staticmethod
    def _location(place_id: str, name: str, timezone: str, country: str) -> Location:
        return Location(
            id=f"loc_rail_{place_id}",
            name=name,
            country=country,
            timezone=timezone,
            kind=LocationKind.STATION,
            provider_ref=place_id,
        )

    def _offer(self, body: _OfferBody, query: TripQuery) -> Offer:
        segments = []
        for leg in body.trip.legs:
            if leg.departure.tzinfo is None or leg.arrival.tzinfo is None:
                raise ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, "naive leg time")
            segments.append(
                Segment(
                    origin=self._location(
                        leg.origin.id, leg.origin.name, leg.origin.timezone, query.origin.country
                    ),
                    destination=self._location(
                        leg.destination.id,
                        leg.destination.name,
                        leg.destination.timezone,
                        query.destination.country,
                    ),
                    departure=leg.departure,
                    arrival=leg.arrival,
                    mode=TransportMode.RAIL,
                    carrier="Fictional Rail",
                    vehicle_ref=leg.vehicle,
                )
            )
        price = Money(body.price.amount, body.price.currency)
        conditions = body.cancellationConditions
        max_fee = (
            Money(price.amount_minor * conditions.feePercent // 100, price.currency)
            if conditions.refundable
            else None
        )
        composition = f"a{query.passengers.adults}c{query.passengers.children}"
        return Offer(
            id=f"off_rail_{body.offerId}_{composition}",
            provider=self.code,
            trip=Trip(
                id=f"trip_rail_{body.trip.id}_{query.departure_date.isoformat()}",
                provider=self.code,
                segments=tuple(segments),
            ),
            passengers=query.passengers,
            total_price=price,
            conditions=FareConditions(
                refundable=conditions.refundable, max_cancellation_fee=max_fee
            ),
            expires_at=body.validUntil,
            provider_offer_ref=body.offerId,
        )

    @staticmethod
    def _reservation(body: _BookingBody) -> Reservation:
        state = _STATES.get(body.status)
        if state is None:
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.NONE, f"undocumented booking status {body.status!r}"
            )
        refund_offers = []
        for r in body.refundOffers:
            refund_state = _REFUND_STATES.get(r.status)
            if refund_state is None:
                raise ProviderError(
                    ErrorKind.MALFORMED, SideEffect.NONE, f"undocumented refund status {r.status!r}"
                )
            refund_offers.append(RefundOfferStatus(r.id, refund_state, r.validUntil))
        return Reservation(
            ProviderBookingRef(body.bookingId),
            BookingId(body.externalRef),
            state,
            datetime.now(UTC),
            product_ref=body.offerId,
            service_date=body.serviceDate,
            generation=body.generation,
            revision=body.version,
            valid_until=body.confirmationTimeLimit if state is ReservationState.HELD else None,
            refund_offers=tuple(refund_offers),
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
            raise self._translate(response, sent=True, mutation=False)
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
        headers: dict[str, str] | None = None,
    ) -> T:
        try:
            response = await self._clients.for_purpose(purpose).request(
                method, path, json=json, headers=headers
            )
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
            raise self._translate(response, sent=True, mutation=True)
        return self._parse(response, model, SideEffect.POSSIBLE)

    @staticmethod
    def _parse[T: BaseModel](response: httpx.Response, model: type[T], on_error: SideEffect) -> T:
        try:
            return model.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise ProviderError(ErrorKind.MALFORMED, on_error, f"unparsable: {exc}") from exc

    @staticmethod
    def _mapped[T](on_error: SideEffect, mapping: Callable[[], T]) -> T:
        """Semantic decoding after a successful response: a body the adapter cannot map is
        malformed with the side effect of the call it answered (a mutation stays POSSIBLE)."""
        try:
            return mapping()
        except ProviderError as exc:
            if exc.side_effect is SideEffect.NONE and on_error is SideEffect.POSSIBLE:
                raise ProviderError(exc.kind, on_error, exc.detail) from exc
            raise
        except (ValueError, TypeError, KeyError) as exc:
            raise ProviderError(ErrorKind.MALFORMED, on_error, f"unmappable: {exc}") from exc

    @staticmethod
    def _translate(response: httpx.Response, *, sent: bool, mutation: bool) -> ProviderError:
        status = response.status_code
        edge = response.headers.get(EDGE_HEADER) == "1"
        code = ""
        retry_after = None
        try:
            problem = response.json()
            code = str(problem.get("code", "")) if isinstance(problem, dict) else ""
        except ValueError:
            problem = {}
        if response.headers.get("retry-after"):
            try:
                retry_after = timedelta(seconds=float(response.headers["retry-after"]))
            except ValueError:
                retry_after = None
        if status == 429:
            return ProviderError(
                ErrorKind.RATE_LIMITED, SideEffect.NONE, "rate limited", retry_after=retry_after
            )
        if edge or (status in (502, 504) and not mutation):
            return ProviderError(ErrorKind.TRANSIENT, SideEffect.NONE, f"edge {status}")
        possible = SideEffect.POSSIBLE if mutation else SideEffect.NONE
        match (status, code):
            case (409, "IN_PROGRESS"):
                return ProviderError(ErrorKind.TRANSIENT, SideEffect.POSSIBLE, "key in progress")
            case (409, "FENCED"):
                return ProviderError(ErrorKind.FENCED, SideEffect.NONE, "fenced")
            case (422, "EXPIRED_REQUEST"):
                return ProviderError(
                    ErrorKind.EXPIRED_REQUEST, SideEffect.NONE, "executeBefore passed"
                )
            case (422, "OFFER_EXPIRED"):
                return ProviderError(ErrorKind.OFFER_EXPIRED, SideEffect.NONE, "offer expired")
            case (
                (422, _)
                | (404, "OFFER_NOT_FOUND")
                | (409, "NOT_CANCELLABLE")
                | (409, "BOOKING_CANCELLED")
                | (409, "REFUND_OFFER_REJECTED")
            ):
                return ProviderError(ErrorKind.REJECTED, SideEffect.NONE, f"{code or status}")
            case (410, "HOLD_EXPIRED"):
                return ProviderError(ErrorKind.HOLD_EXPIRED, SideEffect.NONE, "hold expired")
            case (410, "REFUND_OFFER_EXPIRED"):
                return ProviderError(
                    ErrorKind.OFFER_EXPIRED, SideEffect.NONE, "refund offer expired"
                )
            case (404, _):
                return ProviderError(ErrorKind.NOT_FOUND, SideEffect.NONE, f"not found: {code}")
            case (400, _):
                return ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, f"bad request: {code}")
        if status >= 500:
            return ProviderError(ErrorKind.TRANSIENT, possible, f"{status} after the handler")
        return ProviderError(ErrorKind.MALFORMED, possible, f"unexpected {status} {code}")

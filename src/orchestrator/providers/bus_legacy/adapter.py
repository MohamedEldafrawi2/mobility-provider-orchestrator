"""Adapter for Provider B, "bus-legacy" (anti-corruption layer).

Translates the proprietary REST API into canonical types and every failure into a
``ProviderError`` that says whether a side effect may have happened:

- nothing was sent (DNS or connect failure, connect or pool timeout, 429, a 503 from the
  provider's edge before the handler ran): ``NONE``, retry-safe
- the request was sent and no definitive answer came back (read or write timeout, reset,
  a 503 after the handler may have committed, an unparsable or inconsistent success body):
  ``POSSIBLE``, never retried by anyone; the reconciler discovers by our reference instead
- 400 with a documented numeric code: definitive rejection, ``NONE``

A success is only a success if the provider echoes our reference, our journey, and our
service date; anything else is an inconsistent body and stays uncertain.
"""

from __future__ import annotations

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
from orchestrator.providers.bus_legacy.capabilities import BUS_LEGACY_CAPABILITIES
from orchestrator.providers.bus_legacy.timepolicy import AmbiguousLocalTimeError, localize
from orchestrator.providers.errors import CapabilityNotSupportedError, ErrorKind, ProviderError
from orchestrator.providers.port import (
    CreateBookingRequest,
    FencedResult,
    SearchResult,
    SearchWarning,
    TripQuery,
)
from orchestrator.providers.purpose import Purpose
from orchestrator.providers.transport import PurposeClients

BUS_LEGACY = ProviderCode("bus-legacy")
OFFER_TTL = timedelta(minutes=30)

# The legacy error table, as documented by the (fictional) provider.
_DEFINITIVE_CODES = {12: "bad date", 17: "unknown journey", 21: "sold out", 40: "bad request"}
# The provider's edge marks responses it produced before reaching a handler (documented).
EDGE_HEADER = "x-bus-edge"


class _Stop(BaseModel):
    id: int
    name: str
    tz: str
    country: str


class _StopsBody(BaseModel):
    stops: list[_Stop]


class _Journey(BaseModel):
    jid: str
    src: int
    dst: int
    dep: str
    arr: str
    arrDayOffset: int
    priceCents: str
    cur: str
    seats: int | None = None


class _JourneysBody(BaseModel):
    journeys: list[_Journey]


class _ReserveOk(BaseModel):
    resId: int
    state: str
    jid: str
    yourRef: str
    date: str


class _ReservationRow(BaseModel):
    resId: int
    yourRef: str
    state: str
    jid: str
    date: str
    pax: int = Field(ge=1)


class _ReservationsBody(BaseModel):
    reservations: list[_ReservationRow]


class BusLegacyAdapter:
    code = BUS_LEGACY
    capabilities: ProviderCapabilities = BUS_LEGACY_CAPABILITIES

    def __init__(self, clients: PurposeClients | httpx.AsyncClient) -> None:
        # One transport per purpose (docs/resilience-strategy.md); a single client is a test
        # convenience.
        self._clients = (
            clients if isinstance(clients, PurposeClients) else PurposeClients.shared(clients)
        )

    # Reads ---------------------------------------------------------------------------------

    async def search_locations(self, query: str, *, limit: int) -> list[Location]:
        body = await self._get(
            "/api/v1/stops", params={"q": query}, model=_StopsBody, purpose=Purpose.SEARCH
        )
        return [self._location(s) for s in body.stops[:limit]]

    async def search_trips(self, query: TripQuery, *, limit: int) -> SearchResult:
        params = {
            "src": query.origin.provider_ref,
            "dst": query.destination.provider_ref,
            "date": query.departure_date.strftime("%d-%m-%Y"),
        }
        body = await self._get(
            "/api/v1/journeys", params=params, model=_JourneysBody, purpose=Purpose.SEARCH
        )
        offers: list[Offer] = []
        warnings: list[SearchWarning] = []
        for journey in body.journeys:
            try:
                offers.append(self._offer(journey, query))
            except AmbiguousLocalTimeError as exc:
                warnings.append(SearchWarning("ambiguous-local-time", f"{journey.jid}: {exc}"))
            except ValueError as exc:
                raise ProviderError(
                    ErrorKind.MALFORMED, SideEffect.NONE, f"journey {journey.jid}: {exc}"
                ) from exc
        return SearchResult(
            offers=tuple(offers[:limit]), warnings=tuple(warnings), truncated=len(offers) > limit
        )

    async def get_booking(self, ref: ProviderBookingRef) -> Reservation:
        raise CapabilityNotSupportedError("bus-legacy has no lookup by reservation id")

    async def find_bookings_by_client_ref(self, client_ref: BookingId) -> list[Reservation]:
        body = await self._get(
            "/api/v1/reservations",
            params={"yourRef": client_ref},
            model=_ReservationsBody,
            purpose=Purpose.LOOKUP,
        )
        now = datetime.now(UTC)
        found: list[Reservation] = []
        for r in body.reservations:
            if r.state != "OK":
                raise ProviderError(
                    ErrorKind.MALFORMED,
                    SideEffect.NONE,
                    f"undocumented reservation state {r.state!r}",
                )
            found.append(
                Reservation(
                    ProviderBookingRef(str(r.resId)),
                    BookingId(r.yourRef),
                    ReservationState.CONFIRMED,
                    now,
                    product_ref=r.jid,
                    service_date=_parse_legacy_date(r.date),
                )
            )
        return found

    # Writes --------------------------------------------------------------------------------

    async def create_booking(
        self, request: CreateBookingRequest, key: str, expiry: datetime | None
    ) -> Reservation:
        # ``key`` and ``expiry`` are ignored: this provider supports neither. That is exactly
        # why every failure below that follows a send is POSSIBLE.
        service_date = request.offer.trip.departure.date().strftime("%d-%m-%Y")
        payload = {
            "jid": request.offer.provider_offer_ref,
            "date": service_date,
            "yourRef": request.booking_id,
            "pax": request.offer.passengers.total,
            "name": request.passengers[0].full_name,
        }
        try:
            response = await self._clients.for_purpose(Purpose.CREATE).post(
                "/api/v1/reserve", json=payload
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            raise ProviderError(ErrorKind.TRANSIENT, SideEffect.NONE, f"not sent: {exc!r}") from exc
        except httpx.TimeoutException as exc:
            raise ProviderError(
                ErrorKind.TIMEOUT, SideEffect.POSSIBLE, f"timeout: {exc!r}"
            ) from exc
        except httpx.HTTPError as exc:
            raise ProviderError(
                ErrorKind.TRANSIENT, SideEffect.POSSIBLE, f"transport: {exc!r}"
            ) from exc

        if response.status_code != 200:
            raise self._translate_failure(response, after_send=True)
        try:
            ok = _ReserveOk.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise ProviderError(
                ErrorKind.MALFORMED, SideEffect.POSSIBLE, f"unparsable success: {exc}"
            ) from exc
        if (
            ok.state != "OK"
            or ok.yourRef != request.booking_id
            or ok.jid != request.offer.provider_offer_ref
            or ok.date != service_date
        ):
            # A success that does not echo our request is not evidence for our booking.
            raise ProviderError(
                ErrorKind.MALFORMED,
                SideEffect.POSSIBLE,
                f"success body does not match the request: {ok.model_dump()}",
            )
        return Reservation(
            ProviderBookingRef(str(ok.resId)),
            request.booking_id,
            ReservationState.CONFIRMED,
            datetime.now(UTC),
            product_ref=ok.jid,
            service_date=request.offer.trip.departure.date(),
        )

    # Operations this provider does not offer (declared in its capabilities) --------------------

    async def confirm_booking(self, ref: ProviderBookingRef, expiry: datetime) -> Reservation:
        raise CapabilityNotSupportedError("bus-legacy confirms on reservation; nothing to confirm")

    async def fenced_lookup(self, key: str) -> FencedResult:
        raise CapabilityNotSupportedError("bus-legacy has no finality lookup")

    async def quote_cancellation(self, ref: ProviderBookingRef) -> RefundQuote:
        raise CapabilityNotSupportedError("bus-legacy has no cancellation")

    async def cancel_booking(
        self, ref: ProviderBookingRef, quote: RefundQuote | None, expiry: datetime
    ) -> Reservation:
        raise CapabilityNotSupportedError("bus-legacy has no cancellation")

    # Helpers -------------------------------------------------------------------------------

    async def _get[T: BaseModel](
        self, path: str, *, params: dict[str, Any], model: type[T], purpose: Purpose
    ) -> T:
        try:
            response = await self._clients.for_purpose(purpose).get(path, params=params)
        except httpx.TimeoutException as exc:
            raise ProviderError(ErrorKind.TIMEOUT, SideEffect.NONE, f"timeout: {exc!r}") from exc
        except httpx.HTTPError as exc:
            raise ProviderError(
                ErrorKind.TRANSIENT, SideEffect.NONE, f"transport: {exc!r}"
            ) from exc
        if response.status_code != 200:
            raise self._translate_failure(response, after_send=False)
        try:
            return model.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise ProviderError(ErrorKind.MALFORMED, SideEffect.NONE, f"unparsable: {exc}") from exc

    @staticmethod
    def _translate_failure(response: httpx.Response, *, after_send: bool) -> ProviderError:
        status = response.status_code
        if status == 429:
            return ProviderError(ErrorKind.RATE_LIMITED, SideEffect.NONE, "429 without Retry-After")
        if status == 400:
            code, msg = _error_code(response)
            if code in _DEFINITIVE_CODES:
                return ProviderError(ErrorKind.REJECTED, SideEffect.NONE, f"err {code}: {msg}")
            return ProviderError(
                ErrorKind.MALFORMED,
                SideEffect.POSSIBLE if after_send else SideEffect.NONE,
                f"undocumented err {code}: {msg}",
            )
        if status == 503 and response.headers.get(EDGE_HEADER) == "1":
            # The provider's edge answered before any handler ran: nothing could have happened.
            return ProviderError(ErrorKind.TRANSIENT, SideEffect.NONE, "edge 503")
        # Any other status after a mutation was sent: the handler may have committed first.
        effect = SideEffect.POSSIBLE if after_send else SideEffect.NONE
        return ProviderError(ErrorKind.TRANSIENT, effect, f"HTTP {status}")

    @staticmethod
    def _location(stop: _Stop) -> Location:
        return Location(
            id=f"loc_bus_{stop.id}",
            name=stop.name,
            country=stop.country,
            timezone=stop.tz,
            kind=LocationKind.STOP,
            provider_ref=str(stop.id),
        )

    def _offer(self, journey: _Journey, query: TripQuery) -> Offer:
        service_date: date = query.departure_date
        departure = localize(service_date, journey.dep, query.origin.timezone)
        arrival = localize(
            service_date, journey.arr, query.destination.timezone, day_offset=journey.arrDayOffset
        )
        segment = Segment(
            origin=query.origin,
            destination=query.destination,
            departure=departure,
            arrival=arrival,
            mode=TransportMode.BUS,
            carrier="Legacy Bus Lines",
            vehicle_ref=journey.jid,
        )
        unit = Money(int(journey.priceCents), journey.cur)
        composition = f"a{query.passengers.adults}c{query.passengers.children}"
        return Offer(
            id=f"off_bus_{journey.jid}_{service_date.isoformat()}_{composition}",
            provider=self.code,
            trip=Trip(
                id=f"trip_bus_{journey.jid}_{service_date.isoformat()}",
                provider=self.code,
                segments=(segment,),
            ),
            passengers=query.passengers,
            total_price=Money(unit.amount_minor * query.passengers.total, unit.currency),
            conditions=FareConditions(refundable=False, max_cancellation_fee=None),
            expires_at=datetime.now(UTC) + OFFER_TTL,
            provider_offer_ref=journey.jid,
        )


def _parse_legacy_date(value: str) -> date:
    """A discovery row with an undocumented date is malformed, never "no date"."""
    try:
        return datetime.strptime(value, "%d-%m-%Y").date()
    except ValueError as exc:
        raise ProviderError(
            ErrorKind.MALFORMED, SideEffect.NONE, f"undocumented date {value!r}"
        ) from exc


def _error_code(response: httpx.Response) -> tuple[int, str]:
    try:
        body = response.json()
        code = body.get("err") if isinstance(body, dict) else None
        msg = body.get("msg") if isinstance(body, dict) else None
        return (int(code) if code is not None else -1), str(msg or "")[:200]
    except (ValueError, TypeError, AttributeError):
        return -1, response.text[:200]

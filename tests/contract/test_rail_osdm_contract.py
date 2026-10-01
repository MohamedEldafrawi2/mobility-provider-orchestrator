"""Contract tests: the Provider A adapter against the rail-osdm simulator on a real socket.

Every capability the adapter declares is exercised here, and every failure the simulator can
produce is checked for the side-effect verdict the adapter attaches to it.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta

import httpx
import pytest
import uvicorn

from orchestrator.domain import BookingId, ProviderBookingRef, ReservationState, SideEffect
from orchestrator.domain.capabilities import BookingFlow, IdempotentCreate, UnknownResolution
from orchestrator.domain.offers import Location, LocationKind, Offer, PassengerComposition
from orchestrator.domain.refunds import RefundOfferState
from orchestrator.providers import (
    CapabilityNotSupportedError,
    CreateBookingRequest,
    ErrorKind,
    Passenger,
    ProviderError,
    TripQuery,
)
from orchestrator.providers.rail_osdm import RAIL_OSDM_CAPABILITIES, RailOsdmAdapter
from provider_sims.rail_osdm.app import create_app

TOKEN = "t"
ADMIN = {"X-Admin-Token": TOKEN}
BASEL = Location(
    "loc_rail_8500010", "Basel SBB", "CH", "Europe/Zurich", LocationKind.STATION, "8500010"
)
ZURICH = Location(
    "loc_rail_8503000", "Zürich HB", "CH", "Europe/Zurich", LocationKind.STATION, "8503000"
)


class Sim:
    def __init__(self, client: httpx.AsyncClient, admin: httpx.AsyncClient) -> None:
        self.client = client
        self.admin = admin

    async def chaos(self, **fields: object) -> None:
        current = (await self.admin.get("/_chaos", headers=ADMIN)).json()
        failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}  # type: ignore[arg-type]
        current.update(fields)
        current["failpoints"] = failpoints
        response = await self.admin.put("/_chaos", json=current, headers=ADMIN)
        assert response.status_code == 200, response.text

    async def truth(self) -> list[dict[str, object]]:
        body = (await self.admin.get("/_truth/bookings", headers=ADMIN)).json()
        return body["bookings"]  # type: ignore[no-any-return]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
async def sim(tmp_path: object) -> AsyncIterator[Sim]:
    app = create_app(db_path=f"{tmp_path}/rail.sqlite3", admin_token=TOKEN)
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            lifespan="off",
            timeout_graceful_shutdown=1,
        )
    )
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - uvicorn exposes a flag, not an Event
        await asyncio.sleep(0.01)
    base = f"http://127.0.0.1:{port}"
    try:
        async with (
            httpx.AsyncClient(base_url=base, timeout=0.5) as client,
            httpx.AsyncClient(base_url=base, timeout=5) as admin,
        ):
            yield Sim(client, admin)
    finally:
        server.should_exit = True
        await task


@pytest.fixture
def adapter(sim: Sim) -> RailOsdmAdapter:
    return RailOsdmAdapter(sim.client)


def _soon(seconds: float = 30.0) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


async def _offer(adapter: RailOsdmAdapter, trip: str = "IC-BS-ZH-0704") -> Offer:
    query = TripQuery(BASEL, ZURICH, date(2026, 6, 15), PassengerComposition(adults=1))
    result = await adapter.search_trips(query, limit=10)
    return next(o for o in result.offers if o.trip.segments[0].vehicle_ref == trip)


def _request(offer: Offer, booking_id: str) -> CreateBookingRequest:
    return CreateBookingRequest(
        BookingId(booking_id), offer, (Passenger("Ada Lovelace"),), "ada@example.org"
    )


async def _fail(coro: object) -> ProviderError:
    with pytest.raises(ProviderError) as exc:
        await coro  # type: ignore[misc]
    return exc.value


# Capabilities ---------------------------------------------------------------------------------


def test_declared_capabilities() -> None:
    caps = RailOsdmAdapter.capabilities
    assert caps is RAIL_OSDM_CAPABILITIES
    assert caps.booking_flow is BookingFlow.HOLD_THEN_CONFIRM
    assert caps.idempotent_create is IdempotentCreate.KEY and caps.key_bound_before_execution
    assert caps.finality_lookup and caps.execution_expiry and caps.supports_refund
    assert caps.unknown_resolution is UnknownResolution.RESUBMIT
    assert caps.max_clock_skew == timedelta(seconds=5)


# Reads ------------------------------------------------------------------------------------


async def test_search_maps_places_and_offers(adapter: RailOsdmAdapter) -> None:
    places = await adapter.search_locations("bas", limit=5)
    assert places == [BASEL]
    offer = await _offer(adapter)
    assert offer.provider == "rail-osdm" and offer.total_price.amount_minor == 3400
    assert offer.total_price.currency == "CHF"
    assert offer.conditions.refundable and offer.conditions.max_cancellation_fee is not None
    assert offer.conditions.max_cancellation_fee.amount_minor == 680
    segment = offer.trip.segments[0]
    assert segment.departure.isoformat().startswith("2026-06-15T07:04:00+02:00")
    assert offer.expires_at > datetime.now(UTC)


# Hold then confirm ----------------------------------------------------------------------------


async def test_create_holds_then_confirm_is_idempotent(adapter: RailOsdmAdapter, sim: Sim) -> None:
    offer = await _offer(adapter)
    held = await adapter.create_booking(_request(offer, "bk_1"), key="key-1", expiry=_soon())
    assert held.state is ReservationState.HELD and held.client_ref == "bk_1"
    assert held.valid_until is not None and held.valid_until > datetime.now(UTC)
    assert held.generation == 1 and held.revision == 1
    assert held.product_ref == offer.provider_offer_ref and held.service_date == date(2026, 6, 15)
    confirmed = await adapter.confirm_booking(held.ref, expiry=_soon())
    assert confirmed.state is ReservationState.CONFIRMED and confirmed.revision == 2
    again = await adapter.confirm_booking(held.ref, expiry=_soon())
    assert again.state is ReservationState.CONFIRMED and again.revision == 2
    seen = await adapter.get_booking(held.ref)
    assert seen.state is ReservationState.CONFIRMED and seen.valid_until is None
    assert [r.ref for r in await adapter.find_bookings_by_client_ref(BookingId("bk_1"))] == [
        held.ref
    ]


async def test_same_key_replays_the_original_hold(adapter: RailOsdmAdapter, sim: Sim) -> None:
    offer = await _offer(adapter)
    first = await adapter.create_booking(_request(offer, "bk_2"), key="key-2", expiry=_soon())
    second = await adapter.create_booking(_request(offer, "bk_2"), key="key-2", expiry=_soon())
    assert first.ref == second.ref
    assert len(await sim.truth()) == 1


async def test_expired_request_is_definitive_and_has_no_effect(
    adapter: RailOsdmAdapter, sim: Sim
) -> None:
    offer = await _offer(adapter)
    err = await _fail(
        adapter.create_booking(_request(offer, "bk_3"), key="key-3", expiry=_soon(-1))
    )
    assert (err.kind, err.side_effect, err.definitive) == (
        ErrorKind.EXPIRED_REQUEST,
        SideEffect.NONE,
        True,
    )
    assert await sim.truth() == []


async def test_hold_expiry_makes_confirmation_a_definitive_rejection(
    adapter: RailOsdmAdapter, sim: Sim
) -> None:
    await sim.chaos(hold_expiry_seconds=0.3)
    offer = await _offer(adapter)
    held = await adapter.create_booking(_request(offer, "bk_4"), key="key-4", expiry=_soon())
    await asyncio.sleep(0.4)
    err = await _fail(adapter.confirm_booking(held.ref, expiry=_soon()))
    assert (err.kind, err.side_effect, err.definitive) == (
        ErrorKind.HOLD_EXPIRED,
        SideEffect.NONE,
        True,
    )
    seen = await adapter.get_booking(held.ref)
    assert seen.state is ReservationState.FAILED, "expired holds read as failed"


async def test_in_progress_key_preserves_uncertainty(adapter: RailOsdmAdapter, sim: Sim) -> None:
    await sim.chaos(failpoints={"admit_then_stall_then_commit_seconds": 0.4})
    offer = await _offer(adapter)
    slow = httpx.AsyncClient(base_url=str(sim.client.base_url), timeout=5)
    first = asyncio.create_task(
        RailOsdmAdapter(slow).create_booking(_request(offer, "bk_5"), key="key-5", expiry=_soon())
    )
    await asyncio.sleep(0.1)
    err = await _fail(adapter.create_booking(_request(offer, "bk_5"), key="key-5", expiry=_soon()))
    assert (err.kind, err.side_effect) == (ErrorKind.TRANSIENT, SideEffect.POSSIBLE)
    assert (await first).state is ReservationState.HELD
    await slow.aclose()


async def test_lost_response_after_commit_is_possible_and_replayable(
    adapter: RailOsdmAdapter, sim: Sim
) -> None:
    await sim.chaos(failpoints={"after_prebook_commit": "503"})
    offer = await _offer(adapter)
    err = await _fail(adapter.create_booking(_request(offer, "bk_6"), key="key-6", expiry=_soon()))
    assert (err.kind, err.side_effect) == (ErrorKind.TRANSIENT, SideEffect.POSSIBLE)
    await sim.chaos(failpoints={"after_prebook_commit": None})
    replay = await adapter.create_booking(_request(offer, "bk_6"), key="key-6", expiry=_soon())
    assert replay.state is ReservationState.HELD and len(await sim.truth()) == 1


async def test_read_timeout_on_a_mutation_is_possible(adapter: RailOsdmAdapter, sim: Sim) -> None:
    await sim.chaos(latency_ms=800)
    offer_client = httpx.AsyncClient(base_url=str(sim.client.base_url), timeout=5)
    offer = await _offer(RailOsdmAdapter(offer_client))
    await offer_client.aclose()
    err = await _fail(adapter.create_booking(_request(offer, "bk_7"), key="key-7", expiry=_soon()))
    assert (err.kind, err.side_effect) == (ErrorKind.TIMEOUT, SideEffect.POSSIBLE)
    await sim.chaos(latency_ms=0)


# Fenced lookup --------------------------------------------------------------------------------


async def test_fenced_lookup_is_final_and_fences_the_key(
    adapter: RailOsdmAdapter, sim: Sim
) -> None:
    empty = await adapter.fenced_lookup("key-8")
    assert empty.final and empty.reservations == () and empty.key == "key-8"
    offer = await _offer(adapter)
    err = await _fail(adapter.create_booking(_request(offer, "bk_8"), key="key-8", expiry=_soon()))
    assert (err.kind, err.side_effect, err.definitive) == (ErrorKind.FENCED, SideEffect.NONE, True)
    assert await sim.truth() == []
    held = await adapter.create_booking(_request(offer, "bk_9"), key="key-9", expiry=_soon())
    found = await adapter.fenced_lookup("key-9")
    assert [r.ref for r in found.reservations] == [held.ref] and found.final


async def test_writer_paused_after_check_is_seen_or_aborted(
    adapter: RailOsdmAdapter, sim: Sim
) -> None:
    """6.6 #3: the fence decides; either the lookup sees the reservation or the writer aborts."""
    await sim.chaos(failpoints={"pause_after_check_before_commit_seconds": 0.5})
    offer = await _offer(adapter)
    slow = httpx.AsyncClient(base_url=str(sim.client.base_url), timeout=5)
    writer = asyncio.create_task(
        RailOsdmAdapter(slow).create_booking(_request(offer, "bk_10"), key="key-10", expiry=_soon())
    )
    await asyncio.sleep(0.15)
    fenced = await adapter.fenced_lookup("key-10")
    try:
        result = await writer
        assert [r.ref for r in fenced.reservations] == [result.ref]
    except ProviderError as err:
        assert err.kind is ErrorKind.FENCED and fenced.reservations == ()
        assert await sim.truth() == []
    await slow.aclose()


# Cancellation with refund -----------------------------------------------------------------------


async def test_quote_then_accept_cancels_and_reports_offer_statuses(
    adapter: RailOsdmAdapter, sim: Sim
) -> None:
    offer = await _offer(adapter)
    held = await adapter.create_booking(_request(offer, "bk_11"), key="key-11", expiry=_soon())
    err = await _fail(adapter.quote_cancellation(held.ref))
    assert (err.kind, err.side_effect) == (ErrorKind.REJECTED, SideEffect.NONE), (
        "held: not cancellable"
    )
    await adapter.confirm_booking(held.ref, expiry=_soon())
    quote = await adapter.quote_cancellation(held.ref)
    assert quote.fee.amount_minor == 680 and quote.refund.amount_minor == 2720
    assert quote.within(offer.conditions.max_cancellation_fee)
    other = await adapter.quote_cancellation(held.ref)
    assert other.offer_id != quote.offer_id, "one offer per quote"
    cancelled = await adapter.cancel_booking(held.ref, quote, expiry=_soon())
    assert cancelled.state is ReservationState.CANCELLED
    statuses = {o.offer_id: o.state for o in cancelled.refund_offers}
    assert statuses == {
        quote.offer_id: RefundOfferState.CONFIRMED,
        other.offer_id: RefundOfferState.REJECTED,
    }
    again = await adapter.cancel_booking(held.ref, quote, expiry=_soon())
    assert again.state is ReservationState.CANCELLED, "acceptance is idempotent"
    rejected = await _fail(adapter.cancel_booking(held.ref, other, expiry=_soon()))
    assert (rejected.kind, rejected.side_effect) == (ErrorKind.REJECTED, SideEffect.NONE)


async def test_expired_refund_offer_is_definitive(adapter: RailOsdmAdapter, sim: Sim) -> None:
    offer = await _offer(adapter)
    held = await adapter.create_booking(_request(offer, "bk_12"), key="key-12", expiry=_soon())
    await adapter.confirm_booking(held.ref, expiry=_soon())
    quote = await adapter.quote_cancellation(held.ref)
    await sim.chaos(clock_offset_ms=200_000)  # the provider's clock is far ahead: the offer expired
    err = await _fail(adapter.cancel_booking(held.ref, quote, expiry=_soon(600)))
    assert (err.kind, err.side_effect, err.definitive) == (
        ErrorKind.OFFER_EXPIRED,
        SideEffect.NONE,
        True,
    )
    await sim.chaos(clock_offset_ms=0)
    seen = await adapter.get_booking(held.ref)
    assert seen.state is ReservationState.CONFIRMED
    assert {o.state for o in seen.refund_offers} == {RefundOfferState.EXPIRED}


# Failures before anything was sent ------------------------------------------------------------


async def test_connection_refused_is_none(sim: Sim) -> None:
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{_free_port()}", timeout=0.5) as dead:
        offer_source = await _offer(RailOsdmAdapter(sim.client))
        err = await _fail(
            RailOsdmAdapter(dead).create_booking(
                _request(offer_source, "bk_13"), key="k", expiry=_soon()
            )
        )
    # A closed port is refused or times out at connect, depending on the platform: both NONE.
    assert (
        err.kind in (ErrorKind.TRANSIENT, ErrorKind.TIMEOUT) and err.side_effect is SideEffect.NONE
    )


async def test_edge_rejections_are_none(adapter: RailOsdmAdapter, sim: Sim) -> None:
    offer = await _offer(adapter)
    await sim.chaos(failure_rate=1.0)
    err = await _fail(
        adapter.create_booking(_request(offer, "bk_14"), key="key-14", expiry=_soon())
    )
    assert (err.kind, err.side_effect) == (ErrorKind.TRANSIENT, SideEffect.NONE)
    await sim.chaos(failure_rate=0.0)
    assert await sim.truth() == []


async def test_malformed_bodies_never_bind(adapter: RailOsdmAdapter, sim: Sim) -> None:
    offer = await _offer(adapter)

    class Liar(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            headers = {"content-type": "application/json"}
            if request.method == "POST" and request.url.path == "/bookings":
                body = (
                    b'{"bookingId": "RB1", "externalRef": "bk_other", "offerId": "OF1",'
                    b' "tripId": "T", "serviceDate": "2026-06-15", "status": "PREBOOKED",'
                    b' "confirmationTimeLimit": "2026-06-15T07:00:00+00:00",'
                    b' "generation": 1, "version": 1}'
                )
                return httpx.Response(201, content=body, headers=headers)
            return httpx.Response(200, content=b"[1]", headers=headers)

    async with httpx.AsyncClient(transport=Liar(), base_url="http://liar") as client:
        liar = RailOsdmAdapter(client)
        err = await _fail(liar.create_booking(_request(offer, "bk_mine"), key="k", expiry=_soon()))
        assert (err.kind, err.side_effect) == (ErrorKind.MALFORMED, SideEffect.POSSIBLE)
        read = await _fail(liar.get_booking(ProviderBookingRef("RB1")))
        assert (read.kind, read.side_effect) == (ErrorKind.MALFORMED, SideEffect.NONE)


async def test_unsupported_operations_are_declared_not_discovered() -> None:
    from orchestrator.providers.bus_legacy import BusLegacyAdapter

    bus = BusLegacyAdapter(httpx.AsyncClient(base_url="http://unused"))
    with pytest.raises(CapabilityNotSupportedError):
        await bus.fenced_lookup("k")
    with pytest.raises(CapabilityNotSupportedError):
        await bus.quote_cancellation(ProviderBookingRef("1"))

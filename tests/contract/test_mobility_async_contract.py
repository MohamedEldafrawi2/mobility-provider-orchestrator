"""Contract tests: the Provider C adapter against the mobility-async simulator on a real socket."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta

import httpx
import pytest
import uvicorn
from asgi_lifespan import LifespanManager

from orchestrator.domain import BookingId, ProviderBookingRef, ReservationState, SideEffect
from orchestrator.domain.capabilities import Confirmation, IdempotentCreate
from orchestrator.domain.offers import Location, LocationKind, Offer, PassengerComposition
from orchestrator.providers import (
    CapabilityNotSupportedError,
    CreateBookingRequest,
    ErrorKind,
    Passenger,
    ProviderError,
    TripQuery,
)
from orchestrator.providers.mobility_async import MOBILITY_ASYNC_CAPABILITIES, MobilityAsyncAdapter
from provider_sims.mobility_async.app import create_app

TOKEN = "t"
ADMIN = {"X-Admin-Token": TOKEN}
BERLIN = Location(
    "loc_mob_MOB-BER",
    "Berlin Hauptbahnhof (shuttle stand)",
    "DE",
    "Europe/Berlin",
    LocationKind.STOP,
    "MOB-BER",
)
AIRPORT = Location(
    "loc_mob_MOB-BER-AIR",
    "Berlin Brandenburg Airport",
    "DE",
    "Europe/Berlin",
    LocationKind.STOP,
    "MOB-BER-AIR",
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
        assert (await self.admin.put("/_chaos", json=current, headers=ADMIN)).status_code == 200

    async def truth(self) -> list[dict[str, object]]:
        return (await self.admin.get("/_truth/bookings", headers=ADMIN)).json()["bookings"]  # type: ignore[no-any-return]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
async def sim(tmp_path: object) -> AsyncIterator[Sim]:
    app = create_app(db_path=f"{tmp_path}/async.sqlite3", admin_token=TOKEN, webhook_url="")
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="off")
    )
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110
        await asyncio.sleep(0.01)
    base = f"http://127.0.0.1:{port}"
    try:
        async with (
            LifespanManager(app),
            httpx.AsyncClient(base_url=base, timeout=0.5) as client,
            httpx.AsyncClient(base_url=base, timeout=5) as admin,
        ):
            yield Sim(client, admin)
    finally:
        server.should_exit = True
        await task


@pytest.fixture
def adapter(sim: Sim) -> MobilityAsyncAdapter:
    return MobilityAsyncAdapter(sim.client)


def _soon(seconds: float = 30.0) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


async def _offer(adapter: MobilityAsyncAdapter, product: str = "SHUTTLE-BER-AIR-0630") -> Offer:
    result = await adapter.search_trips(
        TripQuery(BERLIN, AIRPORT, date(2026, 6, 15), PassengerComposition(adults=1)), limit=10
    )
    return next(o for o in result.offers if o.provider_offer_ref == product)


def _request(offer: Offer, booking_id: str) -> CreateBookingRequest:
    return CreateBookingRequest(
        BookingId(booking_id), offer, (Passenger("Ada Lovelace"),), "ada@example.org"
    )


async def _fail(coro: object) -> ProviderError:
    with pytest.raises(ProviderError) as exc:
        await coro  # type: ignore[misc]
    return exc.value


def test_declared_capabilities() -> None:
    caps = MobilityAsyncAdapter.capabilities
    assert caps is MOBILITY_ASYNC_CAPABILITIES
    assert caps.confirmation is Confirmation.ASYNC and caps.supports_webhooks
    assert caps.idempotent_create is IdempotentCreate.CLIENT_REF and caps.key_bound_before_execution
    assert caps.finality_lookup and caps.cancel_is_idempotent and not caps.supports_refund


async def test_search_and_pending_create(adapter: MobilityAsyncAdapter, sim: Sim) -> None:
    await sim.chaos(stall_pending=True)
    stops = await adapter.search_locations("berlin", limit=5)
    assert [s.id for s in stops] == ["loc_mob_MOB-BER", "loc_mob_MOB-BER-AIR"]
    offer = await _offer(adapter)
    assert (
        offer.total_price.amount_minor == 1900 and offer.conditions.max_cancellation_fee is not None
    )
    assert offer.conditions.max_cancellation_fee.amount_minor == 0, "free cancellation"
    pending = await adapter.create_booking(_request(offer, "bk_1"), key="bk_1", expiry=_soon())
    assert (
        pending.state is ReservationState.PENDING
        and pending.generation == 1
        and pending.revision == 1
    )
    assert pending.product_ref == "SHUTTLE-BER-AIR-0630" and pending.service_date == date(
        2026, 6, 15
    )
    replay = await adapter.create_booking(_request(offer, "bk_1"), key="bk_1", expiry=_soon())
    assert replay.ref == pending.ref and len(await sim.truth()) == 1
    seen = await adapter.get_booking(pending.ref)
    assert seen.state is ReservationState.PENDING
    assert [r.ref for r in await adapter.find_bookings_by_client_ref(BookingId("bk_1"))] == [
        pending.ref
    ]


async def test_failures_are_classified(adapter: MobilityAsyncAdapter, sim: Sim) -> None:
    await sim.chaos(stall_pending=True)
    offer = await _offer(adapter)
    expired = await _fail(
        adapter.create_booking(_request(offer, "bk_2"), key="bk_2", expiry=_soon(-1))
    )
    assert (expired.kind, expired.side_effect, expired.definitive) == (
        ErrorKind.EXPIRED_REQUEST,
        SideEffect.NONE,
        True,
    )
    await sim.chaos(failpoints={"lose_response": True})
    slow_client = httpx.AsyncClient(base_url=str(sim.client.base_url), timeout=0.5)
    lost = await _fail(
        MobilityAsyncAdapter(slow_client).create_booking(
            _request(offer, "bk_3"), key="bk_3", expiry=_soon()
        )
    )
    assert (lost.kind, lost.side_effect) == (ErrorKind.TIMEOUT, SideEffect.POSSIBLE)
    await slow_client.aclose()
    await sim.chaos(failpoints={"lose_response": False})
    assert [b["clientRef"] for b in await sim.truth()] == ["bk_3"], "committed, answer lost"
    fenced = await adapter.fenced_lookup("bk_4")
    assert fenced.final and fenced.reservations == ()
    late = await _fail(adapter.create_booking(_request(offer, "bk_4"), key="bk_4", expiry=_soon()))
    assert (late.kind, late.side_effect, late.definitive) == (
        ErrorKind.FENCED,
        SideEffect.NONE,
        True,
    )
    with pytest.raises(CapabilityNotSupportedError):
        await adapter.confirm_booking(ProviderBookingRef("MB000001"), _soon())
    with pytest.raises(CapabilityNotSupportedError):
        await adapter.quote_cancellation(ProviderBookingRef("MB000001"))


async def test_cancel_is_free_and_idempotent(adapter: MobilityAsyncAdapter, sim: Sim) -> None:
    await sim.chaos(pending_seconds=0.1)
    offer = await _offer(adapter)
    pending = await adapter.create_booking(_request(offer, "bk_5"), key="bk_5", expiry=_soon())
    for _ in range(30):
        seen = await adapter.get_booking(pending.ref)
        if seen.state is ReservationState.CONFIRMED:
            break
        await asyncio.sleep(0.05)
    assert seen.state is ReservationState.CONFIRMED and seen.revision == 2
    cancelled = await adapter.cancel_booking(pending.ref, None, _soon())
    assert cancelled.state is ReservationState.CANCELLED and cancelled.revision == 3
    again = await adapter.cancel_booking(pending.ref, None, _soon())
    assert again.state is ReservationState.CANCELLED and again.revision == 3
    expired = await _fail(adapter.cancel_booking(pending.ref, None, _soon(-1)))
    assert (expired.kind, expired.side_effect) == (ErrorKind.EXPIRED_REQUEST, SideEffect.NONE)


def test_unsupported_or_inconsistent_event_types_are_not_observations() -> None:
    body = {
        "providerBookingId": "MB000001",
        "clientRef": "bk_1",
        "status": "CONFIRMED",
        "generation": 1,
        "sequence": 2,
    }
    assert MobilityAsyncAdapter.reservation_from(body).state is ReservationState.CONFIRMED
    assert (
        MobilityAsyncAdapter.reservation_from({**body, "type": "booking.confirmed"}).state
        is ReservationState.CONFIRMED
    )
    for event_type in ("booking.note_added", "booking.failed", "payment.settled"):
        with pytest.raises(ProviderError) as info:
            MobilityAsyncAdapter.reservation_from({**body, "type": event_type})
        assert (info.value.kind, info.value.side_effect) == (ErrorKind.MALFORMED, SideEffect.NONE)


def test_a_pushed_event_must_carry_a_documented_type() -> None:
    body = {
        "providerBookingId": "MB000001",
        "clientRef": "bk_1",
        "status": "CONFIRMED",
        "generation": 1,
        "sequence": 2,
    }
    assert MobilityAsyncAdapter.reservation_from(body).state is ReservationState.CONFIRMED, (
        "a read has no type"
    )
    with pytest.raises(ProviderError) as info:
        MobilityAsyncAdapter.event_from(body)
    assert info.value.kind is ErrorKind.MALFORMED
    with pytest.raises(ProviderError):
        MobilityAsyncAdapter.event_from({**body, "type": None})
    typed = MobilityAsyncAdapter.event_from({**body, "type": "booking.confirmed"})
    assert typed.state is ReservationState.CONFIRMED

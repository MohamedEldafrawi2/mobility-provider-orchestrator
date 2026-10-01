"""Contract tests: every capability the bus-legacy adapter declares is verified against the
simulator on a real socket (so client timeouts and dropped connections are real), and every
failure is translated with the right side-effect certainty."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, date, datetime

import httpx
import pytest
import uvicorn

from orchestrator.domain import BookingId, ProviderBookingRef, ReservationState, SideEffect
from orchestrator.domain.capabilities import (
    Cancellation,
    IdempotentCreate,
    LookupByClientRef,
    UnknownResolution,
)
from orchestrator.domain.offers import Location, LocationKind, Offer, PassengerComposition
from orchestrator.providers import (
    CapabilityNotSupportedError,
    CreateBookingRequest,
    ErrorKind,
    Passenger,
    ProviderError,
    TripQuery,
)
from orchestrator.providers.bus_legacy import BUS_LEGACY, BusLegacyAdapter
from provider_sims.bus_legacy.app import create_app

TOKEN = "t"
ADMIN = {"X-Admin-Token": TOKEN}
ROME = Location("loc_bus_101", "Roma Tiburtina", "IT", "Europe/Rome", LocationKind.STOP, "101")
MILAN = Location("loc_bus_102", "Milano Lampugnano", "IT", "Europe/Rome", LocationKind.STOP, "102")


class Sim:
    def __init__(self, client: httpx.AsyncClient, admin: httpx.AsyncClient, port: int) -> None:
        self.client = client
        self.admin = admin
        self.port = port

    async def chaos(self, **fields: object) -> None:
        current = (await self.admin.get("/_chaos", headers=ADMIN)).json()
        failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}  # type: ignore[arg-type]
        current.update(fields)
        current["failpoints"] = failpoints
        response = await self.admin.put("/_chaos", json=current, headers=ADMIN)
        assert response.status_code == 200, response.text

    async def truth(self) -> list[dict[str, object]]:
        body = (await self.admin.get("/_truth/reservations", headers=ADMIN)).json()
        return body["reservations"]  # type: ignore[no-any-return]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
async def sim(tmp_path: object) -> AsyncIterator[Sim]:
    app = create_app(db_path=f"{tmp_path}/bus.sqlite3", admin_token=TOKEN)
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
            yield Sim(client, admin, port)
    finally:
        server.should_exit = True
        await task


@pytest.fixture
def adapter(sim: Sim) -> BusLegacyAdapter:
    return BusLegacyAdapter(sim.client)


async def _offer(
    adapter: BusLegacyAdapter, jid: str = "BUS-ROM-MIL-0715", on: date = date(2026, 6, 15)
) -> Offer:
    query = TripQuery(ROME, MILAN, on, PassengerComposition(adults=1))
    result = await adapter.search_trips(query, limit=10)
    return next(o for o in result.offers if o.provider_offer_ref == jid)


def _request(offer: Offer, booking_id: str = "bk_c1") -> CreateBookingRequest:
    return CreateBookingRequest(
        BookingId(booking_id), offer, (Passenger("Ada Lovelace"),), "ada@example.org"
    )


async def _create(adapter: BusLegacyAdapter, offer: Offer, booking_id: str) -> ProviderError:
    with pytest.raises(ProviderError) as exc:
        await adapter.create_booking(_request(offer, booking_id), key="k", expiry=None)
    return exc.value


# Declared capabilities ----------------------------------------------------------------------


def test_declared_capabilities_are_the_worst_case() -> None:
    caps = BusLegacyAdapter.capabilities
    assert caps.idempotent_create is IdempotentCreate.NONE
    assert caps.lookup_by_client_ref is LookupByClientRef.EVENTUAL
    assert caps.unknown_resolution is UnknownResolution.REVIEW
    assert not caps.execution_expiry and not caps.finality_lookup
    assert not caps.supports_status_lookup
    assert caps.cancellation is Cancellation.NONE
    assert not caps.can_settle_negatively


async def test_search_translates_legacy_shapes_and_rejects_ambiguous_times(
    adapter: BusLegacyAdapter,
) -> None:
    # 25 October 2026 is the autumn DST change in Europe/Rome: 02:30 happens twice.
    query = TripQuery(ROME, MILAN, date(2026, 10, 25), PassengerComposition(adults=2))
    result = await adapter.search_trips(query, limit=10)
    refs = {o.provider_offer_ref for o in result.offers}
    assert "BUS-ROM-MIL-0715" in refs and "BUS-ROM-MIL-0230" not in refs
    assert any(w.code == "ambiguous-local-time" and "fold" in w.detail for w in result.warnings)

    # 29 March 2026 is the spring change: 02:30 does not exist.
    spring = await adapter.search_trips(replace(query, departure_date=date(2026, 3, 29)), limit=10)
    assert any("gap" in w.detail for w in spring.warnings)

    overnight = next(o for o in result.offers if o.provider_offer_ref == "BUS-ROM-MIL-2350")
    assert overnight.trip.departure.isoformat() == "2026-10-25T23:50:00+01:00"
    assert overnight.trip.arrival.date().isoformat() == "2026-10-26", "arrDayOffset applied"
    assert overnight.total_price.amount_minor == 1890 * 2
    assert overnight.provider == BUS_LEGACY and not overnight.conditions.refundable
    assert overnight.trip.segments[0].origin.country == "IT"

    family = await adapter.search_trips(
        replace(query, passengers=PassengerComposition(adults=1, children=1)), limit=10
    )
    assert {o.id for o in family.offers}.isdisjoint({o.id for o in result.offers}), (
        "different compositions never share an offer id"
    )


async def test_create_is_not_idempotent_and_the_adapter_never_retries(
    adapter: BusLegacyAdapter, sim: Sim
) -> None:
    offer = await _offer(adapter)
    first = await adapter.create_booking(_request(offer), key="bk_c1", expiry=None)
    second = await adapter.create_booking(_request(offer), key="bk_c1", expiry=None)
    assert first.state is ReservationState.CONFIRMED and first.client_ref == "bk_c1"
    assert first.ref != second.ref, "the same client reference created two reservations"
    assert len(await sim.truth()) == 2


async def test_inventory_is_per_service_date(adapter: BusLegacyAdapter) -> None:
    nearly_full = await _offer(adapter, "BUS-ROM-MIL-0230", on=date(2026, 6, 15))
    for i in range(2):
        await adapter.create_booking(_request(nearly_full, f"bk_d{i}"), key="k", expiry=None)
    sold_out = await _create(adapter, nearly_full, "bk_d3")
    assert sold_out.kind is ErrorKind.REJECTED and sold_out.definitive
    other_day = await _offer(adapter, "BUS-ROM-MIL-0230", on=date(2026, 6, 16))
    created = await adapter.create_booking(_request(other_day, "bk_d4"), key="k", expiry=None)
    assert created.state is ReservationState.CONFIRMED


async def test_expiry_is_not_enforced(adapter: BusLegacyAdapter, sim: Sim) -> None:
    """A past expiry changes nothing: this provider has no execution expiry, on the wire either."""
    offer = await _offer(adapter)
    past = datetime(2000, 1, 1, tzinfo=UTC)
    created = await adapter.create_booking(_request(offer, "bk_exp"), key="k", expiry=past)
    assert created.state is ReservationState.CONFIRMED
    wire = await sim.client.post(
        "/api/v1/reserve",
        json={
            "jid": "BUS-ROM-MIL-0715",
            "date": "15-06-2026",
            "yourRef": "bk_exp_wire",
            "pax": 1,
            "name": "Ada",
            "executeBefore": past.isoformat(),
        },
    )
    assert wire.status_code == 200, "executeBefore is accepted and ignored"
    assert [r["yourRef"] for r in await sim.truth()] == ["bk_exp", "bk_exp_wire"]


async def test_discovery_carries_product_and_date(adapter: BusLegacyAdapter) -> None:
    offer = await _offer(adapter)
    created = await adapter.create_booking(_request(offer, "bk_disc"), key="k", expiry=None)
    [found] = await adapter.find_bookings_by_client_ref(BookingId("bk_disc"))
    assert found.ref == created.ref
    assert found.product_ref == "BUS-ROM-MIL-0715" == created.product_ref
    assert found.service_date == date(2026, 6, 15) == created.service_date


async def test_lookup_by_client_ref_is_eventual_and_returns_every_match(
    adapter: BusLegacyAdapter, sim: Sim
) -> None:
    await sim.chaos(lookup_lag_seconds=0.3)
    offer = await _offer(adapter)
    a = await adapter.create_booking(_request(offer, "bk_lag"), key="k", expiry=None)
    b = await adapter.create_booking(_request(offer, "bk_lag"), key="k", expiry=None)
    assert await adapter.find_bookings_by_client_ref(BookingId("bk_lag")) == []
    await asyncio.sleep(0.35)
    found = await adapter.find_bookings_by_client_ref(BookingId("bk_lag"))
    assert {r.ref for r in found} == {a.ref, b.ref}, "discovery reports duplicates"
    assert found[0].observed_at.tzinfo is not None


async def test_lookup_by_id_is_declared_unsupported(adapter: BusLegacyAdapter) -> None:
    with pytest.raises(CapabilityNotSupportedError):
        await adapter.get_booking(ProviderBookingRef("anything"))


# Failure translation ------------------------------------------------------------------------


async def test_definitive_rejections_have_no_side_effect(
    adapter: BusLegacyAdapter, sim: Sim
) -> None:
    offer = await _offer(adapter)
    unknown = await _create(adapter, replace(offer, provider_offer_ref="nope"), "bk_x")
    assert (unknown.kind, unknown.side_effect) == (ErrorKind.REJECTED, SideEffect.NONE)
    assert unknown.definitive

    # Malformed input is a definitive 400 (code 40), not FastAPI's 422.
    bad = CreateBookingRequest(BookingId("bk_y"), offer, (Passenger(""),), "a@b.c")
    with pytest.raises(ProviderError) as exc:
        await adapter.create_booking(bad, key="k", expiry=None)
    assert exc.value.kind is ErrorKind.REJECTED and exc.value.side_effect is SideEffect.NONE
    assert await sim.truth() == []


async def test_commit_then_503_is_a_possible_effect(adapter: BusLegacyAdapter, sim: Sim) -> None:
    await sim.chaos(failpoints={"after_reserve_commit": "503"})
    err = await _create(adapter, await _offer(adapter), "bk_503")
    assert (err.kind, err.side_effect) == (ErrorKind.TRANSIENT, SideEffect.POSSIBLE)
    assert [r["yourRef"] for r in await sim.truth()] == ["bk_503"], "the reservation exists"


async def test_commit_then_drop_is_a_timeout_with_possible_effect(
    adapter: BusLegacyAdapter, sim: Sim
) -> None:
    await sim.chaos(failpoints={"after_reserve_commit": "drop", "hold_seconds": 2})
    err = await _create(adapter, await _offer(adapter), "bk_drop_after")
    assert (err.kind, err.side_effect) == (ErrorKind.TIMEOUT, SideEffect.POSSIBLE)
    assert [r["yourRef"] for r in await sim.truth()] == ["bk_drop_after"]


async def test_dropped_request_is_a_timeout_with_possible_effect_and_nothing_exists(
    adapter: BusLegacyAdapter, sim: Sim
) -> None:
    await sim.chaos(failpoints={"drop_request": True, "hold_seconds": 2})
    err = await _create(adapter, await _offer(adapter), "bk_drop")
    assert (err.kind, err.side_effect) == (ErrorKind.TIMEOUT, SideEffect.POSSIBLE)
    assert await sim.truth() == [], "the platform cannot know this; the model can"


async def test_slow_commit_after_the_caller_gave_up(adapter: BusLegacyAdapter, sim: Sim) -> None:
    await sim.chaos(failpoints={"slow_commit_seconds": 1.0})
    err = await _create(adapter, await _offer(adapter), "bk_slow")
    assert err.side_effect is SideEffect.POSSIBLE
    await asyncio.sleep(1.2)
    assert [r["yourRef"] for r in await sim.truth()] == ["bk_slow"], "committed after the timeout"


async def test_edge_rejections_on_a_mutation_have_no_side_effect(
    adapter: BusLegacyAdapter, sim: Sim
) -> None:
    offer = await _offer(adapter)
    await sim.chaos(rate_limit_per_second=0.0001)
    errors: list[ProviderError] = []
    for i in range(3):
        try:
            await adapter.create_booking(_request(offer, f"bk_rl{i}"), key="k", expiry=None)
        except ProviderError as exc:
            errors.append(exc)
    limited = next(e for e in errors if e.kind is ErrorKind.RATE_LIMITED)
    assert limited.side_effect is SideEffect.NONE

    await sim.chaos(
        rate_limit_per_second=None, unavailable_until=datetime(2999, 1, 1, tzinfo=UTC).isoformat()
    )
    down = await _create(adapter, offer, "bk_down")
    assert (down.kind, down.side_effect) == (ErrorKind.TRANSIENT, SideEffect.NONE)
    assert len(await sim.truth()) == 3 - len(errors)


async def test_connection_refused_has_no_side_effect(sim: Sim) -> None:
    offer = await _offer(BusLegacyAdapter(sim.client))
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{_free_port()}", timeout=0.5) as dead:
        err = await _create(BusLegacyAdapter(dead), offer, "bk_refused")
    assert (err.kind, err.side_effect) == (ErrorKind.TRANSIENT, SideEffect.NONE)


async def test_inconsistent_success_body_stays_uncertain(sim: Sim) -> None:
    """A 200 that echoes another booking's reference is not evidence for ours."""

    class Liar(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            headers = {"content-type": "application/json"}
            if request.url.path == "/api/v1/reserve":
                body = (
                    b'{"resId": 1, "state": "OK", "jid": "BUS-ROM-MIL-0715",'
                    b' "yourRef": "bk_other", "date": "15-06-2026"}'
                )
                return httpx.Response(200, content=body, headers=headers)
            if request.url.path == "/api/v1/journeys":
                body = (
                    b'{"journeys": [{"jid": "J", "src": 101, "dst": 102, "dep": "07:00",'
                    b' "arr": "08:00", "arrDayOffset": 0, "priceCents": "lots", "cur": "EUR"}]}'
                )
                return httpx.Response(200, content=body, headers=headers)
            return httpx.Response(200, content=b"[1, 2, 3]", headers=headers)

    offer = await _offer(BusLegacyAdapter(sim.client))
    async with httpx.AsyncClient(transport=Liar(), base_url="http://liar") as client:
        with pytest.raises(ProviderError) as bad_price:
            await BusLegacyAdapter(client).search_trips(
                TripQuery(ROME, MILAN, date(2026, 6, 15), PassengerComposition(adults=1)), limit=5
            )
        assert bad_price.value.kind is ErrorKind.MALFORMED
        adapter = BusLegacyAdapter(client)
        err = await _create(adapter, offer, "bk_mine")
        assert (err.kind, err.side_effect) == (ErrorKind.MALFORMED, SideEffect.POSSIBLE)
        with pytest.raises(ProviderError) as read:
            await adapter.find_bookings_by_client_ref(BookingId("bk_mine"))
    assert (read.value.kind, read.value.side_effect) == (ErrorKind.MALFORMED, SideEffect.NONE)


async def test_never_indexed_reservation_is_invisible_to_the_adapter(
    adapter: BusLegacyAdapter, sim: Sim
) -> None:
    await sim.chaos(failpoints={"never_index": True, "after_reserve_commit": "503"})
    await _create(adapter, await _offer(adapter), "bk_ghost")
    assert await adapter.find_bookings_by_client_ref(BookingId("bk_ghost")) == []
    assert [r["yourRef"] for r in await sim.truth()] == ["bk_ghost"]


async def test_discovery_with_an_undocumented_date_is_malformed_not_dateless(sim: Sim) -> None:
    """A row whose date the adapter cannot read must not become "no date", which settlement
    could not verify against the command; it is a malformed read with no side effect."""

    class Confused(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            body = (
                b'{"reservations": [{"resId": 7, "yourRef": "bk_x", "state": "OK",'
                b' "jid": "BUS-ROM-MIL-0715", "date": "31-02-2026", "pax": 1}]}'
            )
            return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    async with httpx.AsyncClient(transport=Confused(), base_url="http://confused") as client:
        with pytest.raises(ProviderError) as bad:
            await BusLegacyAdapter(client).find_bookings_by_client_ref(BookingId("bk_x"))
    assert (bad.value.kind, bad.value.side_effect) == (ErrorKind.MALFORMED, SideEffect.NONE)

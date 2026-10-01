"""Convergence under random faults (docs/architecture.md; convergence properties).

All three simulators run with faults: random edge failures, latency, duplicated and withheld
webhooks, lagging indexes. Bookings are created against every provider at once, the faults are
then switched off, and the worker ticks until nothing moves any more. Judged against the
simulators' truth, not against the platform's own records:

6. at most one live reservation per booking;
7. ``CONFIRMED`` has exactly one confirmed reservation under its bound reference; ``FAILED`` and
   ``CANCELLED`` have none live;
8. every live reservation the simulators hold belongs to a booking whose state permits it, or to
   an open review case;
9. nothing is left in an in-flight state: every booking is terminal or in review with a reason.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.test_booking_a import (
    Rail,
    rail,
    rail_url,
)
from tests.integration.test_booking_b import (
    CLIENT,
    OPERATOR,
    Stack,
    bus,
    postgres_url,
    redis_url,
    stack,
)
from tests.integration.test_booking_c import (
    Shuttle,
    async_url,
    public_port,
    shuttle,
)

pytestmark = pytest.mark.integration

LIVE = {"CONFIRMED", "PREBOOKED", "PENDING", "HELD"}
TERMINAL = {"CONFIRMED", "FAILED", "CANCELLED"}
PER_PROVIDER = 4


async def _offer(stack: Stack, params: dict[str, str], ref: str) -> str:
    response = await stack.public.get("/v1/trips", params=params, headers=CLIENT)
    assert response.status_code == 200, response.text
    offers = response.json()["offers"]
    matching = [
        o["id"]
        for o in offers
        if ref in (o["provider_offer_ref"], o["trip"]["segments"][0]["vehicle_ref"])
    ]
    assert matching, (ref, [o["provider_offer_ref"] for o in offers], response.json()["providers"])
    return matching[0]


async def _create(stack: Stack, offer_id: str, key: str) -> dict[str, Any]:
    response = await stack.public.post(
        "/v1/bookings",
        json={
            "offer_id": offer_id,
            "passengers": [{"full_name": "Ada Lovelace"}],
            "contact_email": "ada@example.org",
        },
        headers={**CLIENT, "Idempotency-Key": key},
    )
    assert response.status_code in (201, 202, 422, 503), response.text
    return response.json()


async def _truth(stack: Stack, rail: Rail, shuttle: Shuttle) -> dict[str, list[dict[str, Any]]]:
    """Every reservation every simulator holds, keyed by client reference."""
    by_ref: dict[str, list[dict[str, Any]]] = {}
    for r in await stack.bus.truth():
        # A bus reservation exists or does not: there is no hold and no cancellation at B.
        by_ref.setdefault(str(r["yourRef"]), []).append(
            {"status": "CONFIRMED", "ref": str(r["resId"])}
        )
    for b in await rail.truth():
        by_ref.setdefault(str(b["externalRef"]), []).append(
            {"status": b["status"], "ref": str(b["bookingId"])}
        )
    for b in (await shuttle.truth())["bookings"]:
        by_ref.setdefault(str(b["clientRef"]), []).append(
            {"status": b["status"], "ref": str(b["providerBookingId"])}
        )
    return by_ref


async def test_bookings_converge_after_faults_stop(
    stack: Stack, rail: Rail, shuttle: Shuttle, public_port: int
) -> None:
    # Offers first, on a quiet system: search is not what this scenario is about.
    offers = {
        "bus": await _offer(
            stack,
            {"from": "loc_bus_101", "to": "loc_bus_102", "departureDate": "2026-06-15"},
            "BUS-ROM-MIL-0715",
        ),
        "rail": await _offer(
            stack,
            {"from": "loc_rail_8500010", "to": "loc_rail_8503000", "departureDate": "2026-06-15"},
            "IC-BS-ZH-0704",
        ),
        "shuttle": await _offer(
            stack,
            {"from": "loc_mob_MOB-BER", "to": "loc_mob_MOB-BER-AIR", "departureDate": "2026-06-15"},
            "SHUTTLE-BER-AIR-0630",
        ),
    }
    faults = {"failure_rate": 0.3, "latency_ms": 20, "jitter_ms": 20, "seed": 2026}
    await stack.bus.chaos(**faults, lookup_lag_seconds=0.2)
    await rail.chaos(**faults, hold_expiry_seconds=120)
    await shuttle.chaos(**faults, pending_seconds=0.3, webhook_duplicate_rate=0.5)
    created = await asyncio.gather(
        *[
            _create(stack, offer_id, f"chaos-{name}-{i}")
            for name, offer_id in offers.items()
            for i in range(PER_PROVIDER)
        ]
    )
    booking_ids = [c["id"] for c in created if "id" in c]
    assert len(booking_ids) >= 2 * PER_PROVIDER, "most creates were accepted despite the faults"

    # The faults stop; the worker drives everything to rest.
    for sim in (stack.bus, rail, shuttle):
        await sim.chaos(failure_rate=0.0, latency_ms=0, jitter_ms=0)
    await stack.bus.chaos(lookup_lag_seconds=0.0)
    await shuttle.chaos(webhook_duplicate_rate=0.0)
    quiet = 0
    for _ in range(80):
        await asyncio.sleep(0.25)
        done = await stack.tick()
        moved = sum(v for k, v in done.items() if k != "failed")
        states = {b: (await stack.get(b))["state"] for b in booking_ids}
        if all(s in TERMINAL or s == "NEEDS_REVIEW" for s in states.values()):
            quiet = quiet + 1 if moved == 0 else 0
            if quiet >= 2:
                break
    states = {b: await stack.get(b) for b in booking_ids}
    truth = await _truth(stack, rail, shuttle)

    for booking_id, booking in states.items():
        state = booking["state"]
        reservations = truth.get(booking_id, [])
        live = [r for r in reservations if r["status"] in LIVE]
        # 9: at rest, every booking is terminal or in review with a reason.
        assert state in TERMINAL or (state == "NEEDS_REVIEW" and booking["unresolved_reason"]), (
            booking_id,
            state,
            booking["unresolved_reason"],
        )
        # 6: never more than one live reservation per booking, unless an open case lists each.
        if len(live) > 1:
            assert state == "NEEDS_REVIEW", (booking_id, reservations)
        if state == "NEEDS_REVIEW":
            case = await stack.admin.get(f"/review/{booking_id}", headers=OPERATOR)
            assert case.status_code == 200, case.text
            implicated = set(case.json()["implicated"])
            for r in live:
                assert r["ref"] in implicated, (
                    "a live reservation of a booking under review is not listed by its case",
                    booking_id,
                    r,
                    implicated,
                )
        # 7: a terminal state agrees with the provider.
        if state == "CONFIRMED":
            assert [r["status"] for r in live] == ["CONFIRMED"], (booking_id, reservations)
            assert live[0]["ref"] == booking["provider_booking_ref"]
        if state in ("FAILED", "CANCELLED"):
            assert not live, (booking_id, state, reservations)
    # 8: every reservation the simulators hold belongs to a booking this run created, and a
    # live one to a booking whose state permits it or whose case lists it.
    for client_ref, reservations in truth.items():
        assert client_ref in states, ("a reservation for no booking of this run", client_ref)
        booking = states[client_ref]
        for r in reservations:
            if r["status"] in LIVE:
                assert booking["state"] in ("CONFIRMED", "NEEDS_REVIEW"), (client_ref, r, booking)
    cases = await stack.admin.get("/review", headers=OPERATOR)
    assert cases.status_code == 200
    for case in cases.json()["cases"]:
        assert case["reason"], "every open case says why"

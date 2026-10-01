"""Search aggregation end to end (ADR 010): all three simulators on real sockets,
one deadline, coverage, partial results with a per-provider report, the offers endpoint."""

from __future__ import annotations

from time import monotonic

import httpx
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
    shuttle,
)

pytestmark = pytest.mark.integration

BUS = {"from": "loc_bus_101", "to": "loc_bus_102", "departureDate": "2026-06-15"}
RAIL = {"from": "loc_rail_8500010", "to": "loc_rail_8503000", "departureDate": "2026-06-15"}


async def _trips(stack: Stack, params: dict[str, str]) -> httpx.Response:
    return await stack.public.get("/v1/trips", params=params, headers=CLIENT)


def _reports(body: dict[str, object]) -> dict[str, dict[str, object]]:
    return {r["code"]: r for r in body["providers"]}  # type: ignore[index, union-attr]


async def test_every_provider_is_asked_and_coverage_decides_who_answers(stack: Stack) -> None:
    response = await _trips(stack, BUS)
    assert response.status_code == 200, response.text
    body = response.json()
    reports = _reports(body)
    assert set(reports) == {"bus-legacy", "rail-osdm", "mobility-async"}
    assert reports["bus-legacy"]["status"] == "ok"
    assert reports["rail-osdm"]["status"] == "not-covered"
    assert reports["mobility-async"]["status"] == "not-covered"
    assert body["complete"] is True
    assert body["offers"] and all(o["provider"] == "bus-legacy" for o in body["offers"])
    departures = [o["trip"]["segments"][0]["departure"] for o in body["offers"]]
    assert departures == sorted(departures), "deterministic order: departure first"

    # The same offers are retrievable for as long as they are valid, and nothing else is.
    first = body["offers"][0]
    fetched = await stack.public.get(f"/v1/offers/{first['id']}", headers=CLIENT)
    assert fetched.status_code == 200 and fetched.json() == first
    missing = await stack.public.get("/v1/offers/off_nothing", headers=CLIENT)
    assert missing.status_code == 404 and missing.json()["code"] == "offer-unavailable"


async def test_a_slow_provider_degrades_the_answer_but_never_blocks_it(
    stack: Stack, rail: Rail
) -> None:
    await rail.chaos(latency_ms=2_600)  # above the 2 s provider budget, below the 3 s deadline
    started = monotonic()
    response = await _trips(stack, BUS)
    elapsed = monotonic() - started
    assert response.status_code == 200, response.text
    assert elapsed < 3.5, f"the request waited {elapsed:.2f}s; the deadline bounds it"
    body = response.json()
    reports = _reports(body)
    assert reports["bus-legacy"]["status"] == "ok"
    assert reports["rail-osdm"]["status"] == "catalogue-unavailable"
    assert body["complete"] is False
    assert body["offers"], "the fast provider's offers are returned"

    await rail.chaos(latency_ms=0)
    metrics = await stack.admin.get("/metrics", headers=OPERATOR)
    assert "search_requests_total" in metrics.text
    outcomes = [
        line for line in metrics.text.splitlines() if "search_provider_outcomes_total{" in line
    ]
    assert any('provider="rail-osdm"' in line and 'status="timeout"' in line for line in outcomes)


async def test_unknown_location_is_not_found_only_when_every_catalogue_answered(
    stack: Stack, shuttle: Shuttle
) -> None:
    # One catalogue cannot be read: "not found" cannot be asserted, the search is unavailable.
    await shuttle.chaos(latency_ms=2_600)
    response = await _trips(stack, {**BUS, "to": "loc_bus_999"})
    assert response.status_code == 503, response.text
    problem = response.json()
    assert problem["code"] == "search-unavailable"
    assert response.headers["Retry-After"] == str(problem["retry_after"])

    await shuttle.chaos(latency_ms=0)
    response = await _trips(stack, {**BUS, "to": "loc_bus_999"})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == "location-not-found"


async def test_the_only_covering_provider_failing_is_unavailable(stack: Stack, rail: Rail) -> None:
    # Coverage is decided from the catalogues first (fast), then the trips call times out.
    response = await _trips(stack, RAIL)
    assert response.status_code == 200, response.text
    await rail.chaos(latency_ms=2_600)
    started = monotonic()
    response = await _trips(stack, RAIL)
    assert response.status_code == 503, response.text
    assert monotonic() - started < 3.5
    assert response.json()["code"] == "search-unavailable"
    await rail.chaos(latency_ms=0)

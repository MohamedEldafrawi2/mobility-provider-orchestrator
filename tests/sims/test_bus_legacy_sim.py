"""Provider B simulator behaviour: the properties the platform's worst case depends on."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from provider_sims.bus_legacy.app import create_app

TOKEN = "t"
ADMIN = {"X-Admin-Token": TOKEN}
DATE = "24-12-2026"


@pytest.fixture
async def bus(tmp_path: object) -> AsyncIterator[AsyncClient]:
    app = create_app(db_path=f"{tmp_path}/bus.sqlite3", admin_token=TOKEN)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://bus") as client:
        yield client


async def _set_chaos(bus: AsyncClient, **fields: object) -> None:
    current = (await bus.get("/_chaos", headers=ADMIN)).json()
    failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}  # type: ignore[arg-type]
    current.update(fields)
    current["failpoints"] = failpoints
    response = await bus.put("/_chaos", json=current, headers=ADMIN)
    assert response.status_code == 200, response.text


def _reserve_body(ref: str, jid: str = "BUS-ROM-MIL-0715") -> dict[str, object]:
    return {"jid": jid, "date": DATE, "yourRef": ref, "pax": 1, "name": "Ada"}


async def test_legacy_shapes(bus: AsyncClient) -> None:
    stops = (await bus.get("/api/v1/stops", params={"q": "rom"})).json()["stops"]
    assert stops[0] == {"id": 101, "name": "Roma Tiburtina", "tz": "Europe/Rome", "country": "IT"}

    journeys = (
        await bus.get("/api/v1/journeys", params={"src": 101, "dst": 102, "date": DATE})
    ).json()
    overnight = next(j for j in journeys["journeys"] if j["jid"] == "BUS-ROM-MIL-2350")
    assert overnight["dep"] == "23:50" and overnight["arrDayOffset"] == 1
    assert overnight["priceCents"] == "1890" and isinstance(overnight["priceCents"], str)

    bad = await bus.get("/api/v1/journeys", params={"src": 101, "dst": 102, "date": "2026-12-24"})
    assert bad.status_code == 400 and bad.json()["err"] == 12


async def test_reserve_has_no_idempotency(bus: AsyncClient) -> None:
    first = (await bus.post("/api/v1/reserve", json=_reserve_body("bk_1"))).json()
    second = (await bus.post("/api/v1/reserve", json=_reserve_body("bk_1"))).json()
    assert first["state"] == "OK" and second["state"] == "OK"
    assert first["resId"] != second["resId"], "the same yourRef creates two reservations"
    assert first["date"] == DATE and first["yourRef"] == "bk_1"


async def test_definitive_rejections_have_numeric_codes(bus: AsyncClient) -> None:
    unknown = await bus.post("/api/v1/reserve", json=_reserve_body("bk_2", jid="nope"))
    assert (unknown.status_code, unknown.json()["err"]) == (400, 17)
    malformed = await bus.post("/api/v1/reserve", json={"jid": "x"})
    assert (malformed.status_code, malformed.json()["err"]) == (400, 40)
    for i in range(2):
        ok = await bus.post("/api/v1/reserve", json=_reserve_body(f"bk_f{i}", "BUS-ROM-MIL-0230"))
        assert ok.status_code == 200
    sold_out = await bus.post("/api/v1/reserve", json=_reserve_body("bk_f3", "BUS-ROM-MIL-0230"))
    assert (sold_out.status_code, sold_out.json()["err"]) == (400, 21)
    other_day = await bus.post(
        "/api/v1/reserve", json={**_reserve_body("bk_f4", "BUS-ROM-MIL-0230"), "date": "25-12-2026"}
    )
    assert other_day.status_code == 200, "inventory is per service date"
    for alias in ("24-12-2026\n", "31-02-2026", " 24-12-2026"):
        aliased = await bus.post(
            "/api/v1/reserve", json={**_reserve_body("bk_alias", "BUS-ROM-MIL-0230"), "date": alias}
        )
        assert (aliased.status_code, aliased.json()["err"]) == (400, 12), alias


async def test_concurrent_slow_reservations_cannot_oversell(bus: AsyncClient) -> None:
    await _set_chaos(bus, failpoints={"slow_commit_seconds": 0.2})
    responses = await asyncio.gather(
        *(
            bus.post("/api/v1/reserve", json=_reserve_body(f"bk_o{i}", "BUS-ROM-MIL-0230"))
            for i in range(3)
        )
    )
    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200, 200, 400], statuses
    journeys = (
        await bus.get("/api/v1/journeys", params={"src": 101, "dst": 102, "date": DATE})
    ).json()
    assert next(j for j in journeys["journeys"] if j["jid"] == "BUS-ROM-MIL-0230")["seats"] == 0


async def test_index_lags_then_exposes(bus: AsyncClient) -> None:
    await _set_chaos(bus, lookup_lag_seconds=0.3)
    await bus.post("/api/v1/reserve", json=_reserve_body("bk_lag"))
    found = (await bus.get("/api/v1/reservations", params={"yourRef": "bk_lag"})).json()
    assert found["reservations"] == []
    await asyncio.sleep(0.35)
    found = (await bus.get("/api/v1/reservations", params={"yourRef": "bk_lag"})).json()
    assert len(found["reservations"]) == 1 and found["reservations"][0]["yourRef"] == "bk_lag"


async def test_never_index_hides_forever_but_truth_knows(bus: AsyncClient) -> None:
    await _set_chaos(bus, failpoints={"never_index": True})
    await bus.post("/api/v1/reserve", json=_reserve_body("bk_ghost"))
    found = (await bus.get("/api/v1/reservations", params={"yourRef": "bk_ghost"})).json()
    assert found["reservations"] == []
    truth = (await bus.get("/_truth/reservations", headers=ADMIN)).json()["reservations"]
    assert [r["yourRef"] for r in truth] == ["bk_ghost"]
    assert truth[0]["visibleAfter"] is None


async def test_after_reserve_commit_503_commits_then_fails_the_response(bus: AsyncClient) -> None:
    await _set_chaos(bus, failpoints={"after_reserve_commit": "503"})
    response = await bus.post("/api/v1/reserve", json=_reserve_body("bk_503"))
    assert response.status_code == 503 and "x-bus-edge" not in response.headers
    truth = (await bus.get("/_truth/reservations", headers=ADMIN)).json()["reservations"]
    assert [r["yourRef"] for r in truth] == ["bk_503"], "the reservation exists despite the 503"


async def test_chaos_requires_the_admin_token_and_rejects_bad_values(bus: AsyncClient) -> None:
    assert (await bus.get("/_chaos")).status_code == 401
    assert (await bus.get("/_truth/reservations")).status_code == 401
    current = (await bus.get("/_chaos", headers=ADMIN)).json()
    naive = await bus.put(
        "/_chaos", json={**current, "unavailable_until": "2999-01-01T00:00:00"}, headers=ADMIN
    )
    assert naive.status_code == 400, "the legacy 400 applies everywhere"


async def test_edge_rate_limit_is_429_without_retry_after_and_marked(bus: AsyncClient) -> None:
    await _set_chaos(bus, rate_limit_per_second=0.001)
    responses = [await bus.get("/api/v1/stops", params={"q": "a"}) for _ in range(3)]
    limited = [r for r in responses if r.status_code == 429]
    assert limited and "retry-after" not in limited[0].headers
    assert limited[0].headers["x-bus-edge"] == "1"
    assert responses[0].status_code == 200, "the bucket always holds one token to start"

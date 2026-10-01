"""Provider A simulator behaviour: the properties the platform's design leans on."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient

from provider_sims.rail_osdm.app import create_app

TOKEN = "t"
ADMIN = {"X-Admin-Token": TOKEN}


@pytest.fixture
async def rail(tmp_path: object) -> AsyncIterator[AsyncClient]:
    app = create_app(db_path=f"{tmp_path}/rail.sqlite3", admin_token=TOKEN)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://rail") as client:
        yield client


async def _set_chaos(rail: AsyncClient, **fields: object) -> None:
    current = (await rail.get("/_chaos", headers=ADMIN)).json()
    failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}  # type: ignore[arg-type]
    current.update(fields)
    current["failpoints"] = failpoints
    response = await rail.put("/_chaos", json=current, headers=ADMIN)
    assert response.status_code == 200, response.text


async def _offer(rail: AsyncClient, trip: str = "IC-BS-ZH-0704") -> dict[str, object]:
    body = {"origin": "8500010", "destination": "8503000", "date": "2026-06-15", "adults": 1}
    response = await rail.post("/offers", json=body)
    assert response.status_code == 200, response.text
    return next(o for o in response.json()["offers"] if o["trip"]["id"] == trip)  # type: ignore[no-any-return]


def _soon(seconds: float = 30.0) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()


def _booking_body(
    offer_id: str, ref: str, *, execute_before: str | None = None
) -> dict[str, object]:
    return {
        "offerId": offer_id,
        "externalRef": ref,
        "passengers": [{"firstName": "Ada", "lastName": "Lovelace"}],
        "contactEmail": "ada@example.org",
        "executeBefore": execute_before or _soon(),
    }


async def _hold(rail: AsyncClient, ref: str, key: str, **kw: object) -> dict[str, object]:
    offer = await _offer(rail)
    response = await rail.post(
        "/bookings",
        json=_booking_body(str(offer["offerId"]), ref, **kw),
        headers={"Idempotency-Key": key},  # type: ignore[arg-type]
    )
    assert response.status_code == 201, response.text
    return response.json()  # type: ignore[no-any-return]


async def _truth(rail: AsyncClient) -> list[dict[str, object]]:
    return (await rail.get("/_truth/bookings", headers=ADMIN)).json()["bookings"]  # type: ignore[no-any-return]


async def test_osdm_shapes(rail: AsyncClient) -> None:
    places = (await rail.get("/places", params={"name": "basel"})).json()["places"]
    assert places == [
        {"id": "8500010", "name": "Basel SBB", "timezone": "Europe/Zurich", "country": "CH"}
    ]
    offer = await _offer(rail)
    assert offer["price"] == {"amount": 3400, "currency": "CHF"}
    assert offer["cancellationConditions"] == {"refundable": True, "feePercent": 20}
    leg = offer["trip"]["legs"][0]  # type: ignore[index]
    assert leg["departure"].startswith("2026-06-15T07:04:00+02:00")
    assert datetime.fromisoformat(str(offer["validUntil"])) > datetime.now(UTC)


async def test_hold_then_confirm_with_versions_and_generation(rail: AsyncClient) -> None:
    hold = await _hold(rail, "bk_1", "key-1")
    assert hold["status"] == "PREBOOKED" and hold["version"] == 1 and hold["generation"] == 1
    assert datetime.fromisoformat(str(hold["confirmationTimeLimit"])) > datetime.now(UTC)
    confirm = await rail.patch(
        f"/bookings/{hold['bookingId']}", json={"status": "CONFIRMED", "executeBefore": _soon()}
    )
    assert confirm.status_code == 200 and confirm.json()["status"] == "CONFIRMED"
    assert confirm.json()["version"] == 2
    again = await rail.patch(
        f"/bookings/{hold['bookingId']}", json={"status": "CONFIRMED", "executeBefore": _soon()}
    )
    assert again.status_code == 200 and again.json()["version"] == 2, "confirm is idempotent"
    by_ref = (await rail.get("/bookings", params={"externalRef": "bk_1"})).json()["bookings"]
    assert [b["bookingId"] for b in by_ref] == [hold["bookingId"]]


async def test_key_is_bound_before_execution_and_replays_the_original_outcome(
    rail: AsyncClient,
) -> None:
    hold = await _hold(rail, "bk_2", "key-2")
    offer = await _offer(rail)
    replay = await rail.post(
        "/bookings",
        json=_booking_body(str(offer["offerId"]), "bk_2"),
        headers={"Idempotency-Key": "key-2"},
    )
    assert replay.status_code == 201 and replay.headers["idempotent-replayed"] == "true"
    assert replay.json()["bookingId"] == hold["bookingId"]
    assert len(await _truth(rail)) == 1, "one key, one reservation"
    # A rejected execution is replayed as the same rejection.
    bad = await rail.post(
        "/bookings",
        json=_booking_body("OF999999", "bk_3"),
        headers={"Idempotency-Key": "key-3"},
    )
    assert bad.status_code == 404 and bad.json()["code"] == "OFFER_NOT_FOUND"
    same = await rail.post(
        "/bookings", json=_booking_body("OF999999", "bk_3"), headers={"Idempotency-Key": "key-3"}
    )
    assert same.status_code == 404 and same.headers["idempotent-replayed"] == "true"


async def test_in_progress_key_is_reported_as_such(rail: AsyncClient) -> None:
    await _set_chaos(rail, failpoints={"admit_then_stall_then_commit_seconds": 0.4})
    offer = await _offer(rail)
    body = _booking_body(str(offer["offerId"]), "bk_ip")
    first = asyncio.create_task(
        rail.post("/bookings", json=body, headers={"Idempotency-Key": "key-ip"})
    )
    await asyncio.sleep(0.1)
    second = await rail.post("/bookings", json=body, headers={"Idempotency-Key": "key-ip"})
    assert second.status_code == 409 and second.json()["code"] == "IN_PROGRESS"
    assert (await first).status_code == 201
    assert len(await _truth(rail)) == 1


async def test_expired_request_is_rejected_before_execution(rail: AsyncClient) -> None:
    offer = await _offer(rail)
    past = _soon(-1)
    response = await rail.post(
        "/bookings",
        json=_booking_body(str(offer["offerId"]), "bk_exp", execute_before=past),
        headers={"Idempotency-Key": "key-exp"},
    )
    assert response.status_code == 422 and response.json()["code"] == "EXPIRED_REQUEST"
    assert await _truth(rail) == []
    # The provider's clock lags: a request we consider expired is still executed there.
    await _set_chaos(rail, clock_offset_ms=-3000)
    late = await rail.post(
        "/bookings",
        json=_booking_body(str(offer["offerId"]), "bk_skew", execute_before=_soon(-1)),
        headers={"Idempotency-Key": "key-skew"},
    )
    assert late.status_code == 201, "clock skew: the provider executed what we thought expired"


async def test_hold_expires_and_confirmation_then_fails(rail: AsyncClient) -> None:
    await _set_chaos(rail, hold_expiry_seconds=0.3)
    hold = await _hold(rail, "bk_hold", "key-hold")
    await asyncio.sleep(0.4)
    seen = (await rail.get(f"/bookings/{hold['bookingId']}")).json()
    assert seen["status"] == "EXPIRED" and seen["version"] == 2
    confirm = await rail.patch(
        f"/bookings/{hold['bookingId']}", json={"status": "CONFIRMED", "executeBefore": _soon()}
    )
    assert confirm.status_code == 410 and confirm.json()["code"] == "HOLD_EXPIRED"


async def test_fenced_lookup_is_final(rail: AsyncClient) -> None:
    empty = await rail.post("/bookings/fenced-lookup", json={"idempotencyKey": "key-f"})
    assert empty.json() == {"idempotencyKey": "key-f", "final": True, "bookings": []}
    offer = await _offer(rail)
    late = await rail.post(
        "/bookings",
        json=_booking_body(str(offer["offerId"]), "bk_f"),
        headers={"Idempotency-Key": "key-f"},
    )
    assert late.status_code == 409 and late.json()["code"] == "FENCED"
    assert await _truth(rail) == [], "nothing can commit for a fenced key"


async def test_writer_paused_after_check_is_aborted_by_the_fence(rail: AsyncClient) -> None:
    """6.6 #3: the writer checked expiry, paused, and the fenced lookup ran in between."""
    await _set_chaos(rail, failpoints={"pause_after_check_before_commit_seconds": 0.5})
    offer = await _offer(rail)
    body = _booking_body(str(offer["offerId"]), "bk_race")
    writer = asyncio.create_task(
        rail.post("/bookings", json=body, headers={"Idempotency-Key": "key-race"})
    )
    await asyncio.sleep(0.15)
    fenced = await rail.post("/bookings/fenced-lookup", json={"idempotencyKey": "key-race"})
    assert fenced.json()["bookings"] == [] and fenced.json()["final"] is True
    result = await writer
    assert result.status_code == 409 and result.json()["code"] == "FENCED"
    assert await _truth(rail) == []


async def test_writer_admitted_then_stalled_commits_before_or_is_fenced(rail: AsyncClient) -> None:
    """6.6 #4: admitted and validated, then stalled; the fence decides either way."""
    await _set_chaos(rail, failpoints={"admit_then_stall_then_commit_seconds": 0.5})
    offer = await _offer(rail)
    body = _booking_body(str(offer["offerId"]), "bk_stall")
    writer = asyncio.create_task(
        rail.post("/bookings", json=body, headers={"Idempotency-Key": "key-stall"})
    )
    await asyncio.sleep(0.15)
    fenced = (
        await rail.post("/bookings/fenced-lookup", json={"idempotencyKey": "key-stall"})
    ).json()
    result = await writer
    if fenced["bookings"]:
        assert result.status_code == 201 and len(await _truth(rail)) == 1
    else:
        assert result.status_code == 409 and await _truth(rail) == []


async def test_refund_offers_quote_accept_and_expire(rail: AsyncClient) -> None:
    hold = await _hold(rail, "bk_ref", "key-ref")
    booking_id = hold["bookingId"]
    too_early = await rail.post(f"/bookings/{booking_id}/refund-offers")
    assert too_early.status_code == 409 and too_early.json()["code"] == "NOT_CANCELLABLE"
    await rail.patch(
        f"/bookings/{booking_id}", json={"status": "CONFIRMED", "executeBefore": _soon()}
    )
    first = (await rail.post(f"/bookings/{booking_id}/refund-offers")).json()
    second = (await rail.post(f"/bookings/{booking_id}/refund-offers")).json()
    assert first["refundOfferId"] != second["refundOfferId"], "one offer per quote"
    assert first["refund"] == {"amount": 2720, "currency": "CHF"} and first["fee"]["amount"] == 680
    seen = (await rail.get(f"/bookings/{booking_id}")).json()
    assert {r["id"]: r["status"] for r in seen["refundOffers"]} == {
        first["refundOfferId"]: "PROPOSED",
        second["refundOfferId"]: "PROPOSED",
    }
    accept = await rail.patch(
        f"/bookings/{booking_id}/refund-offers/{second['refundOfferId']}",
        json={"status": "CONFIRMED", "executeBefore": _soon()},
    )
    assert accept.status_code == 200 and accept.json()["status"] == "CONFIRMED"
    assert accept.json()["booking"]["status"] == "CANCELLED"
    again = await rail.patch(
        f"/bookings/{booking_id}/refund-offers/{second['refundOfferId']}",
        json={"status": "CONFIRMED", "executeBefore": _soon()},
    )
    assert again.status_code == 200, "acceptance is idempotent"
    other = await rail.patch(
        f"/bookings/{booking_id}/refund-offers/{first['refundOfferId']}",
        json={"status": "CONFIRMED", "executeBefore": _soon()},
    )
    assert other.status_code == 409 and other.json()["code"] == "REFUND_OFFER_REJECTED"
    seen = (await rail.get(f"/bookings/{booking_id}")).json()
    assert {r["status"] for r in seen["refundOffers"]} == {"REJECTED", "CONFIRMED"}


async def test_generation_bump_changes_reports_but_not_facts(rail: AsyncClient) -> None:
    hold = await _hold(rail, "bk_gen", "key-gen")
    await _set_chaos(rail, generation_bump=1)
    seen = (await rail.get(f"/bookings/{hold['bookingId']}")).json()
    assert seen["generation"] == 2 and seen["version"] == 1
    offer = await _offer(rail)
    replay = await rail.post(
        "/bookings",
        json=_booking_body(str(offer["offerId"]), "bk_gen"),
        headers={"Idempotency-Key": "key-gen"},
    )
    assert replay.status_code == 201 and replay.headers["idempotent-replayed"] == "true", (
        "the dedupe record survives the bump"
    )


async def test_lost_response_after_commit(rail: AsyncClient) -> None:
    await _set_chaos(rail, failpoints={"after_prebook_commit": "503"})
    offer = await _offer(rail)
    response = await rail.post(
        "/bookings",
        json=_booking_body(str(offer["offerId"]), "bk_lost"),
        headers={"Idempotency-Key": "key-lost"},
    )
    assert response.status_code == 503
    assert [b["externalRef"] for b in await _truth(rail)] == ["bk_lost"], "committed, answer lost"
    await _set_chaos(rail, failpoints={"after_prebook_commit": None})
    replay = await rail.post(
        "/bookings",
        json=_booking_body(str(offer["offerId"]), "bk_lost"),
        headers={"Idempotency-Key": "key-lost"},
    )
    assert replay.status_code == 201 and replay.headers["idempotent-replayed"] == "true"


async def test_malformed_input_is_a_definitive_400(rail: AsyncClient) -> None:
    response = await rail.post("/bookings", json={"nope": 1}, headers={"Idempotency-Key": "k"})
    assert response.status_code == 400 and response.json()["code"] == "INVALID_REQUEST"
    assert await _truth(rail) == []

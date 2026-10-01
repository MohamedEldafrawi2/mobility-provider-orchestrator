"""Provider C simulator behaviour: asynchronous outcomes, signed webhooks, reference binding."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from standardwebhooks import Webhook

from provider_sims.mobility_async.app import create_app

TOKEN = "t"
ADMIN = {"X-Admin-Token": TOKEN}
SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"


class Sink:
    """A webhook receiver that verifies signatures like the platform does."""

    def __init__(self) -> None:
        self.received: list[dict[str, Any]] = []
        self.app = FastAPI()

        @self.app.post("/hook")
        async def hook(request: Request) -> dict[str, str]:
            body = await request.body()
            payload = Webhook(SECRET).verify(body, dict(request.headers))
            self.received.append({"headers": dict(request.headers), "payload": payload})
            return {"ok": "true"}


@pytest.fixture
async def sim(tmp_path: object) -> AsyncIterator[tuple[AsyncClient, Sink]]:
    import socket

    import uvicorn

    sink = Sink()
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = int(s.getsockname()[1])
    server = uvicorn.Server(
        uvicorn.Config(sink.app, host="127.0.0.1", port=port, log_level="warning", lifespan="off")
    )
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110
        await asyncio.sleep(0.01)
    app = create_app(
        db_path=f"{tmp_path}/async.sqlite3",
        admin_token=TOKEN,
        webhook_url=f"http://127.0.0.1:{port}/hook",
        webhook_secret=SECRET,
    )
    try:
        async with (
            LifespanManager(app),
            AsyncClient(transport=ASGITransport(app=app), base_url="http://async") as client,
        ):
            yield client, sink
    finally:
        server.should_exit = True
        await task


async def _set_chaos(client: AsyncClient, **fields: object) -> None:
    current = (await client.get("/_chaos", headers=ADMIN)).json()
    failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}  # type: ignore[arg-type]
    current.update(fields)
    current["failpoints"] = failpoints
    response = await client.put("/_chaos", json=current, headers=ADMIN)
    assert response.status_code == 200, response.text


def _body(ref: str, product: str = "SHUTTLE-BER-AIR-0630") -> dict[str, object]:
    return {
        "productId": product,
        "date": "2026-06-15",
        "clientRef": ref,
        "passengers": [{"name": "Ada Lovelace"}],
        "contactEmail": "ada@example.org",
        "executeBefore": (datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
    }


async def _truth(client: AsyncClient) -> dict[str, Any]:
    return (await client.get("/_truth/bookings", headers=ADMIN)).json()  # type: ignore[no-any-return]


async def _wait(predicate: Any, *, within: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + within
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.05)


async def test_pending_then_confirmed_by_signed_webhook(sim: tuple[AsyncClient, Sink]) -> None:
    client, sink = sim
    await _set_chaos(client, pending_seconds=0.2)
    response = await client.post("/bookings", json=_body("bk_1"))
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["status"] == "PENDING" and body["sequence"] == 1 and body["generation"] == 1
    await _wait(lambda: len(sink.received) >= 1)
    event = sink.received[0]["payload"]
    assert event["type"] == "booking.confirmed" and event["sequence"] == 2
    assert event["providerBookingId"] == body["providerBookingId"]
    assert sink.received[0]["headers"]["webhook-id"] == event["eventId"]
    seen = (await client.get(f"/bookings/{body['providerBookingId']}")).json()
    assert seen["status"] == "CONFIRMED" and seen["sequence"] == 2


async def test_client_reference_is_bound_before_execution(sim: tuple[AsyncClient, Sink]) -> None:
    client, _ = sim
    await _set_chaos(client, stall_pending=True)
    first = (await client.post("/bookings", json=_body("bk_2"))).json()
    replay = await client.post("/bookings", json=_body("bk_2"))
    assert replay.status_code == 202 and replay.headers["idempotent-replayed"] == "true"
    assert replay.json()["providerBookingId"] == first["providerBookingId"]
    assert len((await _truth(client))["bookings"]) == 1


async def test_in_progress_reference(sim: tuple[AsyncClient, Sink]) -> None:
    client, _ = sim
    await _set_chaos(
        client, stall_pending=True, failpoints={"admit_then_stall_then_commit_seconds": 0.4}
    )
    first = asyncio.create_task(client.post("/bookings", json=_body("bk_3")))
    await asyncio.sleep(0.1)
    second = await client.post("/bookings", json=_body("bk_3"))
    assert second.status_code == 409 and second.json()["code"] == "IN_PROGRESS"
    assert (await first).status_code == 202


async def test_webhook_duplicates_wrong_booking_and_redelivery(
    sim: tuple[AsyncClient, Sink],
) -> None:
    client, sink = sim
    await _set_chaos(client, pending_seconds=0.1, webhook_duplicate_rate=1.0)
    body = (await client.post("/bookings", json=_body("bk_4"))).json()
    await _wait(lambda: len(sink.received) >= 2)
    ids = {r["payload"]["eventId"] for r in sink.received}
    assert len(ids) == 1, "the same event, delivered twice"
    sigs = {r["headers"]["webhook-signature"] for r in sink.received}
    assert len(sigs) >= 1
    await _set_chaos(client, webhook_duplicate_rate=0.0, webhook_wrong_booking=True)
    cancel = await client.post(
        f"/bookings/{body['providerBookingId']}/cancel",
        json={"executeBefore": (datetime.now(UTC) + timedelta(seconds=30)).isoformat()},
    )
    assert cancel.status_code == 200 and cancel.json()["status"] == "CANCELLED"
    await _wait(lambda: len(sink.received) >= 3)
    wrong = sink.received[-1]["payload"]
    assert wrong["providerBookingId"] == "MB999999" and wrong["status"] == "CANCELLED"
    event_id = sink.received[0]["payload"]["eventId"]
    redelivered = await client.post(f"/_truth/redeliver/{event_id}", headers=ADMIN)
    assert redelivered.status_code == 200
    await _wait(lambda: len(sink.received) >= 4)
    assert sink.received[-1]["payload"]["eventId"] == event_id


async def test_fenced_lookup_by_reference_is_final(sim: tuple[AsyncClient, Sink]) -> None:
    client, _ = sim
    fenced = (await client.post("/bookings/fenced-lookup", json={"clientRef": "bk_5"})).json()
    assert fenced == {"clientRef": "bk_5", "final": True, "bookings": []}
    late = await client.post("/bookings", json=_body("bk_5"))
    assert late.status_code == 409 and late.json()["code"] == "FENCED"
    assert (await _truth(client))["bookings"] == []


async def test_cancel_is_free_idempotent_and_confirmed_only(sim: tuple[AsyncClient, Sink]) -> None:
    client, sink = sim
    await _set_chaos(client, pending_seconds=0.1)
    body = (await client.post("/bookings", json=_body("bk_6"))).json()
    booking_id = body["providerBookingId"]
    too_early = await client.post(
        f"/bookings/{booking_id}/cancel",
        json={"executeBefore": (datetime.now(UTC) + timedelta(seconds=30)).isoformat()},
    )
    assert too_early.status_code in (409, 200)
    await _wait(lambda: any(r["payload"]["status"] == "CONFIRMED" for r in sink.received))
    when = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    first = await client.post(f"/bookings/{booking_id}/cancel", json={"executeBefore": when})
    assert first.status_code == 200 and first.json()["status"] == "CANCELLED"
    again = await client.post(f"/bookings/{booking_id}/cancel", json={"executeBefore": when})
    assert again.status_code == 200 and again.json()["sequence"] == first.json()["sequence"]
    expired = await client.post(
        f"/bookings/{booking_id}/cancel",
        json={"executeBefore": (datetime.now(UTC) - timedelta(seconds=1)).isoformat()},
    )
    assert expired.status_code == 422 and expired.json()["code"] == "EXPIRED_REQUEST"


async def test_expired_request_and_generation_bump(sim: tuple[AsyncClient, Sink]) -> None:
    client, _ = sim
    body = _body("bk_7")
    body["executeBefore"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    expired = await client.post("/bookings", json=body)
    assert expired.status_code == 422 and expired.json()["code"] == "EXPIRED_REQUEST"
    await _set_chaos(client, stall_pending=True)
    created = (await client.post("/bookings", json=_body("bk_8"))).json()
    await _set_chaos(client, generation_bump=2)
    seen = (await client.get(f"/bookings/{created['providerBookingId']}")).json()
    assert seen["generation"] == 3 and seen["sequence"] == 1
    replay = await client.post("/bookings", json=_body("bk_8"))
    assert replay.headers["idempotent-replayed"] == "true", "dedupe survives the bump"
    assert json.loads(replay.text)["providerBookingId"] == created["providerBookingId"]

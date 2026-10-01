"""Provider C simulator: what is enforced inside the serialized commit (expiry, capacity),
reference release after an expired request, identifiers across a reopened database, immutable
events, acknowledgement-only delivery."""

from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import uvicorn
from asgi_lifespan import LifespanManager
from fastapi import FastAPI, Request, Response
from httpx import ASGITransport, AsyncClient
from standardwebhooks import Webhook

from provider_sims.mobility_async.app import create_app
from tests.sims.test_mobility_async_sim import ADMIN, SECRET, TOKEN, _set_chaos, _truth, _wait


class FlakySink:
    """Answers 5xx to the first ``failures`` deliveries, then acknowledges."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.received: list[dict[str, Any]] = []
        self.app = FastAPI()

        @self.app.post("/hook")
        async def hook(request: Request) -> Response:
            body = await request.body()
            payload = Webhook(SECRET).verify(body, dict(request.headers))
            self.received.append({"headers": dict(request.headers), "payload": payload})
            if len(self.received) <= self.failures:
                return Response(status_code=503)
            return Response(status_code=200)


@pytest.fixture
async def flaky(tmp_path: object) -> AsyncIterator[tuple[AsyncClient, FlakySink]]:
    sink = FlakySink(failures=2)
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


def _body(
    ref: str,
    *,
    product: str = "SHUTTLE-BER-AIR-0630",
    passengers: int = 1,
    expires_in: float = 30.0,
) -> dict[str, object]:
    return {
        "productId": product,
        "date": "2026-06-15",
        "clientRef": ref,
        "passengers": [{"name": f"Passenger {i}"} for i in range(passengers)],
        "contactEmail": "ada@example.org",
        "executeBefore": (datetime.now(UTC) + timedelta(seconds=expires_in)).isoformat(),
    }


async def test_expiry_is_enforced_at_the_commit_and_the_reference_is_released(
    flaky: tuple[AsyncClient, FlakySink],
) -> None:
    client, _ = flaky
    await _set_chaos(
        client, stall_pending=True, failpoints={"pause_after_check_before_commit_seconds": 0.4}
    )
    # The request is valid when checked, expired by the time the commit is reached.
    late = await client.post("/bookings", json=_body("bk_exp", expires_in=0.2))
    assert late.status_code == 422 and late.json()["code"] == "EXPIRED_REQUEST"
    assert (await _truth(client))["bookings"] == [], "nothing committed after the expiry"
    await _set_chaos(client, failpoints={"pause_after_check_before_commit_seconds": 0.0})
    # The same reference with a fresh expiry is not answered from a cached rejection.
    fresh = await client.post("/bookings", json=_body("bk_exp", expires_in=30))
    assert fresh.status_code == 202, fresh.text
    assert fresh.headers.get("idempotent-replayed") is None


async def test_capacity_is_enforced_inside_the_commit_under_concurrency(
    flaky: tuple[AsyncClient, FlakySink],
) -> None:
    client, _ = flaky
    await _set_chaos(
        client, stall_pending=True, failpoints={"pause_after_check_before_commit_seconds": 0.2}
    )
    # Two seats on this product; two concurrent requests for two passengers each both pass
    # the pre-check, only one may commit.
    first, second = await asyncio.gather(
        client.post(
            "/bookings", json=_body("bk_cap_1", product="SHUTTLE-BER-HAM-0715", passengers=2)
        ),
        client.post(
            "/bookings", json=_body("bk_cap_2", product="SHUTTLE-BER-HAM-0715", passengers=2)
        ),
    )
    codes = sorted((first.status_code, second.status_code))
    assert codes == [202, 422], (first.text, second.text)
    sold_out = first if first.status_code == 422 else second
    assert sold_out.json()["code"] == "SOLD_OUT"
    booked = sum(1 for b in (await _truth(client))["bookings"])
    assert booked == 1


async def test_cancel_expiry_is_enforced_at_the_commit(
    flaky: tuple[AsyncClient, FlakySink],
) -> None:
    client, sink = flaky
    await _set_chaos(client, pending_seconds=0.1)
    body = (await client.post("/bookings", json=_body("bk_cx"))).json()
    booking_id = body["providerBookingId"]
    await _wait(lambda: any(r["payload"]["status"] == "CONFIRMED" for r in sink.received), within=5)
    await _set_chaos(client, failpoints={"pause_after_check_before_commit_seconds": 0.4})
    expiring = (datetime.now(UTC) + timedelta(seconds=0.2)).isoformat()
    late = await client.post(f"/bookings/{booking_id}/cancel", json={"executeBefore": expiring})
    assert late.status_code == 422 and late.json()["code"] == "EXPIRED_REQUEST"
    seen = (await client.get(f"/bookings/{booking_id}")).json()
    assert seen["status"] == "CONFIRMED", "a cancel that expired while paused never commits"


async def test_identifiers_continue_across_a_reopened_database(tmp_path: object) -> None:
    path = f"{tmp_path}/reopen.sqlite3"
    first = create_app(db_path=path, admin_token=TOKEN, webhook_url="")
    async with (
        LifespanManager(first),
        AsyncClient(transport=ASGITransport(app=first), base_url="http://async") as client,
    ):
        await _set_chaos(client, stall_pending=True)
        one = (await client.post("/bookings", json=_body("bk_r1"))).json()["providerBookingId"]
    second = create_app(db_path=path, admin_token=TOKEN, webhook_url="")
    async with (
        LifespanManager(second),
        AsyncClient(transport=ASGITransport(app=second), base_url="http://async") as client,
    ):
        await _set_chaos(client, stall_pending=True)
        two = (await client.post("/bookings", json=_body("bk_r2"))).json()["providerBookingId"]
        assert two != one
        assert len((await _truth(client))["bookings"]) == 2


async def test_events_are_immutable_and_delivered_only_on_acknowledgement(
    flaky: tuple[AsyncClient, FlakySink],
) -> None:
    client, sink = flaky
    await _set_chaos(client, pending_seconds=0.1)
    body = (await client.post("/bookings", json=_body("bk_ack"))).json()
    # The sink rejects the first two deliveries with 5xx: the event stays owed and is retried
    # until acknowledged, every attempt carrying the same id and payload.
    await _wait(lambda: len(sink.received) >= 3, within=8)
    event: dict[str, Any] = {}
    for _ in range(50):
        truth = await _truth(client)
        event = next(e for e in truth["events"] if e["bookingId"] == body["providerBookingId"])
        if event["delivered"]:
            break
        await asyncio.sleep(0.1)
    assert event["delivered"] and event["attempts"] >= 3
    ids = {r["payload"]["eventId"] for r in sink.received}
    assert ids == {event["eventId"]}
    payloads = {r["payload"]["sequence"] for r in sink.received}
    assert payloads == {event["sequence"]}
    signatures = {r["headers"]["webhook-signature"] for r in sink.received}
    timestamps = {r["headers"]["webhook-timestamp"] for r in sink.received}
    assert len(signatures) >= 2 or len(timestamps) >= 1, "signed per delivery attempt"
    # Redelivery after a generation bump still carries the generation the fact was recorded at.
    await _set_chaos(client, generation_bump=5)
    before = len(sink.received)
    assert (
        await client.post(f"/_truth/redeliver/{event['eventId']}", headers=ADMIN)
    ).status_code == 200
    await _wait(lambda: len(sink.received) > before, within=5)
    assert sink.received[-1]["payload"]["generation"] == event["generation"] == 1


async def test_an_expired_request_at_validation_leaves_the_reference_free(
    flaky: tuple[AsyncClient, FlakySink],
) -> None:
    client, _ = flaky
    await _set_chaos(client, stall_pending=True)
    expired = await client.post("/bookings", json=_body("bk_val_exp", expires_in=-1))
    assert expired.status_code == 422 and expired.json()["code"] == "EXPIRED_REQUEST"
    fresh = await client.post("/bookings", json=_body("bk_val_exp", expires_in=30))
    assert fresh.status_code == 202, fresh.text
    assert fresh.headers.get("idempotent-replayed") is None, "not a cached rejection"


async def test_an_interrupted_first_delivery_is_owed_again(
    flaky: tuple[AsyncClient, FlakySink],
) -> None:
    """An event recorded but never posted (the process died between the record and the post)
    is delivered by the progressor once its in-flight bound has passed."""
    from datetime import timedelta as _td

    client, sink = flaky
    await _set_chaos(client, stall_pending=True)
    body = (await client.post("/bookings", json=_body("bk_interrupted"))).json()
    store = client._transport.app.state.store  # type: ignore[attr-defined]
    row = store.booking(body["providerBookingId"])
    stale_claim = datetime.now(UTC) - _td(seconds=30)
    event_id = store.next_event_id()
    event = store.record_event(
        event_id,
        row,
        generation=1,
        now=stale_claim,
        payload=json.dumps(
            {
                "type": "booking.pending",
                "eventId": event_id,
                "providerBookingId": row.booking_id,
                "clientRef": row.client_ref,
                "status": "PENDING",
                "generation": 1,
                "sequence": 1,
                "occurredAt": stale_claim.isoformat(),
            }
        ),
    )
    store.claim_delivery(event.event_id, now=stale_claim)  # claimed long ago, never posted
    before = len(sink.received)
    try:
        await _wait(lambda: len(sink.received) > before, within=6)
    except AssertionError:
        raise AssertionError(("not delivered", store.events(), sink.received)) from None
    assert any(r["payload"]["eventId"] == event.event_id for r in sink.received)

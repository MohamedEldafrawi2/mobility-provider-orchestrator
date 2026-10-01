"""Provider C end to end: asynchronous confirmation through signed webhooks, ordering, polling,
free cancellation inside the cutoff (docs/edge-cases.md, cases 17 to 22, 32, 33, 53, 54)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from standardwebhooks import Webhook

from orchestrator.domain import BookingId, CommandKind
from tests.integration.test_booking_b import (
    CLIENT,
    SIM_ADMIN,
    SIM_TOKEN,
    Stack,
    _free_port,
    bus,
    postgres_url,
    rail_url,
    redis_url,
    stack,
)

pytestmark = pytest.mark.integration
SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"


class Shuttle:
    def __init__(self, base: str, admin: httpx.AsyncClient) -> None:
        self.base = base
        self.admin = admin

    async def chaos(self, **fields: object) -> None:
        current = (await self.admin.get("/_chaos", headers=SIM_ADMIN)).json()
        failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}  # type: ignore[arg-type]
        current.update(fields)
        current["failpoints"] = failpoints
        assert (await self.admin.put("/_chaos", json=current, headers=SIM_ADMIN)).status_code == 200

    async def truth(self) -> dict[str, Any]:
        return (await self.admin.get("/_truth/bookings", headers=SIM_ADMIN)).json()  # type: ignore[no-any-return]

    async def point_webhooks_at(self, url: str) -> None:
        assert (
            await self.admin.post("/_truth/webhook-url", json={"url": url}, headers=SIM_ADMIN)
        ).status_code == 200

    async def redeliver(self, event_id: str) -> None:
        assert (
            await self.admin.post(f"/_truth/redeliver/{event_id}", headers=SIM_ADMIN)
        ).status_code == 200


@pytest.fixture
async def shuttle(tmp_path: object) -> AsyncIterator[Shuttle]:
    import uvicorn
    from asgi_lifespan import LifespanManager

    from provider_sims.mobility_async.app import create_app

    app = create_app(
        db_path=f"{tmp_path}/async.sqlite3",
        admin_token=SIM_TOKEN,
        webhook_url="",
        webhook_secret=SECRET,
    )
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="off")
    )
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110
        await asyncio.sleep(0.01)
    base = f"http://127.0.0.1:{port}"
    try:
        async with LifespanManager(app), httpx.AsyncClient(base_url=base, timeout=5) as admin:
            yield Shuttle(base, admin)
    finally:
        server.should_exit = True
        await task


@pytest.fixture
def async_url(shuttle: Shuttle) -> str:
    return shuttle.base


@pytest.fixture
async def public_port(stack: Stack, shuttle: Shuttle) -> AsyncIterator[int]:
    """The public listener on a real socket, so the simulator can deliver webhooks to it."""
    import uvicorn

    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            stack.public_app, host="127.0.0.1", port=port, log_level="warning", lifespan="off"
        )
    )
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110
        await asyncio.sleep(0.01)
    await shuttle.point_webhooks_at(f"http://127.0.0.1:{port}/v1/providers/mobility-async/webhooks")
    try:
        yield port
    finally:
        server.should_exit = True
        await task


async def _offer_id(stack: Stack, product: str = "SHUTTLE-BER-AIR-0630") -> str:
    response = await stack.public.get(
        "/v1/trips",
        params={
            "from": "loc_mob_MOB-BER",
            "to": "loc_mob_MOB-BER-AIR",
            "departureDate": "2026-06-15",
            "adults": 1,
        },
        headers=CLIENT,
    )
    assert response.status_code == 200, response.text
    return next(o["id"] for o in response.json()["offers"] if o["provider_offer_ref"] == product)  # type: ignore[no-any-return]


async def _create(stack: Stack, offer_id: str, key: str) -> httpx.Response:
    body = {
        "offer_id": offer_id,
        "passengers": [{"full_name": "Ada Lovelace"}],
        "contact_email": "ada@example.org",
    }
    return await stack.public.post(
        "/v1/bookings", json=body, headers={**CLIENT, "Idempotency-Key": key}
    )


async def _wait_state(
    stack: Stack, booking_id: str, *states: str, within: float = 5.0
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + within
    while True:
        booking = await stack.get(booking_id)
        if booking["state"] in states:
            return booking
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"still {booking['state']}, wanted {states}")
        await asyncio.sleep(0.05)


async def test_async_create_is_pending_then_confirmed_by_webhook(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    """6.3 DIRECT + ASYNC: 202 PENDING_PROVIDER, then the signed webhook confirms."""
    await shuttle.chaos(pending_seconds=0.3)
    offer_id = await _offer_id(stack)
    response = await _create(stack, offer_id, "c-happy")
    assert response.status_code == 202, response.text
    body = response.json()
    assert (
        body["state"] == "PENDING_PROVIDER"
        and body["unresolved"]
        and response.headers["retry-after"]
    )
    booking_id = body["id"]
    final = await _wait_state(stack, booking_id, "CONFIRMED")
    assert final["provider_generation"] == 1
    replay = await _create(stack, offer_id, "c-happy")
    assert replay.status_code == 201 and replay.headers["idempotent-replayed"] == "true"
    async with stack.creator.uow() as store:
        create = await store.command_for(BookingId(booking_id), CommandKind.CREATE)
    assert create.disposition.value == "SUCCEEDED" and create.basis is not None
    truth = await shuttle.truth()
    assert [d["status"] for d in truth["deliveries"]] == [200]


async def test_duplicate_webhook_is_a_noop_and_wrong_booking_is_unmatched(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    await shuttle.chaos(pending_seconds=0.2, webhook_duplicate_rate=1.0)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "c-dup")).json()["id"]
    await _wait_state(stack, booking_id, "CONFIRMED")
    for _ in range(20):
        truth = await shuttle.truth()
        if len(truth["deliveries"]) >= 2:
            break
        await asyncio.sleep(0.05)
    event_id = truth["events"][0]["eventId"]
    await shuttle.redeliver(event_id)
    for _ in range(20):
        truth = await shuttle.truth()
        if len(truth["deliveries"]) >= 3:
            break
        await asyncio.sleep(0.05)
    async with stack.creator.uow() as store:
        rows = (
            (
                await store.s.execute(
                    __import__("sqlalchemy").text(
                        "SELECT outcome FROM webhook_receipts WHERE booking_id = :b "
                        "ORDER BY received_at"
                    ),
                    {"b": booking_id},
                )
            )
            .scalars()
            .all()
        )
    assert rows[0] == "APPLIED" and set(rows[1:]) <= {"DUPLICATE"} and len(rows) == 1, (
        "one receipt: the duplicate hit the unique constraint and inserted nothing"
    )
    version_before = (await stack.get(booking_id))["version"]
    await shuttle.chaos(webhook_duplicate_rate=0.0, webhook_wrong_booking=True)
    payload = json.dumps(
        {
            "type": "booking.cancelled",
            "eventId": "evt_wrong",
            "providerBookingId": "MB999999",
            "clientRef": "bk_nobody",
            "status": "CANCELLED",
            "generation": 1,
            "sequence": 9,
            "occurredAt": datetime.now(UTC).isoformat(),
        }
    )
    ts = datetime.now(UTC)
    headers = {
        "webhook-id": "evt_wrong",
        "webhook-timestamp": str(int(ts.timestamp())),
        "webhook-signature": Webhook(SECRET).sign("evt_wrong", ts, payload),
        "content-type": "application/json",
    }
    unmatched = await stack.public.post(
        "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
    )
    assert unmatched.status_code == 200 and unmatched.json()["outcome"] == "UNMATCHED"
    assert (await stack.get(booking_id))["version"] == version_before
    bad = await stack.public.post(
        "/v1/providers/mobility-async/webhooks",
        content=payload,
        headers={**headers, "webhook-signature": "v1,bogus"},
    )
    assert bad.status_code == 401
    assert (
        await stack.public.post(
            "/v1/providers/bus-legacy/webhooks", content=payload, headers=headers
        )
    ).status_code == 404


async def test_stale_and_superseded_observations_are_ignored(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    """6.6 #18, #21: generation then revision; older facts never move the booking."""
    await shuttle.chaos(pending_seconds=0.2)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "c-order")).json()["id"]
    final = await _wait_state(stack, booking_id, "CONFIRMED")
    ref = final["provider_booking_ref"]

    def signed(
        event_id: str, status: str, generation: int, sequence: int
    ) -> tuple[str, dict[str, str]]:
        payload = json.dumps(
            {
                "type": f"booking.{status.lower()}",
                "eventId": event_id,
                "providerBookingId": ref,
                "clientRef": booking_id,
                "status": status,
                "generation": generation,
                "sequence": sequence,
                "occurredAt": datetime.now(UTC).isoformat(),
            }
        )
        ts = datetime.now(UTC)
        return payload, {
            "webhook-id": event_id,
            "webhook-timestamp": str(int(ts.timestamp())),
            "webhook-signature": Webhook(SECRET).sign(event_id, ts, payload),
            "content-type": "application/json",
        }

    payload, headers = signed("evt_stale", "PENDING", 1, 1)  # an old fact arriving late
    stale = await stack.public.post(
        "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
    )
    assert (
        stale.json()["outcome"] == "STALE" and (await stack.get(booking_id))["state"] == "CONFIRMED"
    )
    payload, headers = signed("evt_old_gen", "FAILED", 0, 9)
    superseded = await stack.public.post(
        "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
    )
    assert superseded.json()["outcome"] == "SUPERSEDED_GENERATION"
    payload, headers = signed("evt_new_gen", "FAILED", 5, 9)
    quarantined = await stack.public.post(
        "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
    )
    assert quarantined.json()["outcome"] == "NEWER_GENERATION"
    assert (await stack.get(booking_id))["state"] == "NEEDS_REVIEW", (
        "adopted only from an authoritative read"
    )


async def test_early_webhook_then_late_create_response(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    """6.6 #22: the outcome overtakes the create response; the response still closes its
    attempt, the CREATE settles on CONFIRMED, nothing regresses."""
    await shuttle.chaos(webhook_before_response=True)
    offer_id = await _offer_id(stack)
    response = await _create(stack, offer_id, "c-early")
    assert response.status_code in (201, 202), response.text
    booking_id = response.json()["id"]
    final = await _wait_state(stack, booking_id, "CONFIRMED")
    assert final["state"] == "CONFIRMED"
    async with stack.creator.uow() as store:
        create = await store.command_for(BookingId(booking_id), CommandKind.CREATE)
    assert create.disposition.value == "SUCCEEDED"
    assert all(a.finished_at is not None for a in create.attempts), (
        "the late response closed its attempt"
    )
    await shuttle.chaos(webhook_before_response=False)


async def test_polling_settles_when_webhooks_are_disabled_and_overdue_pending_goes_to_review(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    await shuttle.chaos(pending_seconds=0.2, webhook_disabled=True)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "c-poll")).json()["id"]
    await asyncio.sleep(0.3)
    async with stack.creator.uow() as store:
        booking = await store.get(BookingId(booking_id), for_update=True)
        await store.save_booking(booking, next_action_at=datetime.now(UTC))
    done = await stack.tick()
    assert done["polled"] == 1
    assert (await stack.get(booking_id))["state"] == "CONFIRMED"
    # overdue: a pending booking nobody progresses
    await shuttle.chaos(stall_pending=True)
    stalled = (await _create(stack, offer_id, "c-stall")).json()["id"]
    from dataclasses import replace

    impatient = replace(stack.recovery.policy, pending_max_age=timedelta(0))
    stack.recovery.policy = impatient
    try:
        async with stack.creator.uow() as store:
            booking = await store.get(BookingId(stalled), for_update=True)
            await store.save_booking(booking, next_action_at=datetime.now(UTC))
        await stack.tick()
    finally:
        stack.recovery.policy = replace(impatient, pending_max_age=timedelta(seconds=900))
    parked = await stack.get(stalled)
    assert parked["state"] == "NEEDS_REVIEW" and parked["unresolved_reason"] == "pending-overdue"
    await shuttle.chaos(stall_pending=False, webhook_disabled=False)


async def test_free_cancel_retries_inside_the_cutoff_and_is_idempotent(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    await shuttle.chaos(pending_seconds=0.2)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "c-cancel")).json()["id"]
    await _wait_state(stack, booking_id, "CONFIRMED")
    pending_cancel = await stack.public.post(
        f"/v1/bookings/{booking_id}/cancel",
        json={"max_fee": None},
        headers={**CLIENT, "Idempotency-Key": "cc-1"},
    )
    assert pending_cancel.status_code == 200, pending_cancel.text
    assert pending_cancel.json()["state"] == "CANCELLED"
    replay = await stack.public.post(
        f"/v1/bookings/{booking_id}/cancel",
        json={"max_fee": None},
        headers={**CLIENT, "Idempotency-Key": "cc-1"},
    )
    assert replay.status_code == 200 and replay.headers["idempotent-replayed"] == "true"
    # a lost cancel answer: settled by an authoritative read, retried inside the cutoff
    other = (await _create(stack, offer_id, "c-cancel-lost")).json()["id"]
    await _wait_state(stack, other, "CONFIRMED")
    await shuttle.chaos(failpoints={"lose_response": True})
    lost = await stack.public.post(
        f"/v1/bookings/{other}/cancel",
        json={"max_fee": None},
        headers={**CLIENT, "Idempotency-Key": "cc-2"},
    )
    # The answer is lost, but the provider's event about the cancellation usually arrives
    # first and settles it (200 CANCELLED); otherwise the lookup settles it on the next pass.
    assert lost.status_code in (200, 202), lost.text
    await shuttle.chaos(failpoints={"lose_response": False})
    if lost.status_code == 202:
        assert lost.json()["state"] == "CANCELLING"
        async with stack.creator.uow() as store:
            booking = await store.get(BookingId(other), for_update=True)
            await store.save_booking(booking, next_action_at=datetime.now(UTC))
        await stack.tick()
    assert (await stack.get(other))["state"] == "CANCELLED"
    async with stack.creator.uow() as store:
        cancel = await store.command_for(BookingId(other), CommandKind.CANCEL)
    assert cancel.disposition.value == "SUCCEEDED" and len(cancel.attempts) == 1
    pending_one = await _create(stack, offer_id, "c-pending")
    await shuttle.chaos(stall_pending=True)
    stalled = (await _create(stack, offer_id, "c-pending-2")).json()["id"]
    refused = await stack.public.post(
        f"/v1/bookings/{stalled}/cancel",
        json={"max_fee": None},
        headers={**CLIENT, "Idempotency-Key": "cc-3"},
    )
    assert refused.status_code == 409 and refused.json()["code"] == "booking-not-cancellable", (
        "6.6 #53"
    )
    await shuttle.chaos(stall_pending=False)
    assert pending_one.status_code in (201, 202)


def _signed_event(
    event_id: str, ref: str, booking_id: str, status: str, generation: int, sequence: int
) -> tuple[str, dict[str, str]]:
    payload = json.dumps(
        {
            "type": f"booking.{status.lower()}",
            "eventId": event_id,
            "providerBookingId": ref,
            "clientRef": booking_id,
            "status": status,
            "generation": generation,
            "sequence": sequence,
            "occurredAt": datetime.now(UTC).isoformat(),
        }
    )
    ts = datetime.now(UTC)
    return payload, {
        "webhook-id": event_id,
        "webhook-timestamp": str(int(ts.timestamp())),
        "webhook-signature": Webhook(SECRET).sign(event_id, ts, payload),
        "content-type": "application/json",
    }


async def test_events_under_review_are_quarantined_and_an_early_mismatch_escalates(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    """Section 9 and 6.1: an event never moves a booking out of review on its own word, it
    joins the case's evidence; an early event is bound only by an authoritative read that
    agrees with it, a disagreeing read is contradictory evidence."""
    await shuttle.chaos(pending_seconds=0.2)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "c-quarantine")).json()["id"]
    final = await _wait_state(stack, booking_id, "CONFIRMED")
    ref = final["provider_booking_ref"]
    payload, headers = _signed_event("evt_q1", ref, booking_id, "FAILED", 5, 9)
    first = await stack.public.post(
        "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
    )
    assert first.json()["outcome"] == "NEWER_GENERATION"
    assert (await stack.get(booking_id))["state"] == "NEEDS_REVIEW"
    payload, headers = _signed_event("evt_q2", ref, booking_id, "CANCELLED", 1, 10)
    second = await stack.public.post(
        "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
    )
    assert second.status_code == 200 and second.json()["outcome"] == "QUARANTINED"
    under_review = await stack.get(booking_id)
    assert under_review["state"] == "NEEDS_REVIEW", "a legal transition is not taken in review"
    async with stack.creator.uow() as store:
        found = await store.review_case(BookingId(booking_id))
    assert found is not None
    case, _ = found
    assert any(
        e.kind.value == "WEBHOOK" and e.reservation is not None and e.subject_ref == ref
        for e in case.evidence
    ), "the quarantined event is evidence of the case, as a pushed event that never closes it"
    again = await stack.public.post(
        "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
    )
    assert again.json()["outcome"] == "DUPLICATE"

    # An early event (the booking has no bound reference yet) that names a reservation the
    # authoritative read attributes to another booking: contradictory, never bound.
    await shuttle.chaos(stall_pending=True, failpoints={"validate_then_stall_seconds": 1.5})
    pending_create = asyncio.create_task(_create(stack, offer_id, "c-early-mismatch"))
    await asyncio.sleep(0.4)
    async with stack.creator.uow() as store:
        submitting = await store.expired_submitting(
            stale_before=datetime.now(UTC) + timedelta(seconds=5)
        )
    victim = next(b for b in submitting if b.provider_booking_ref is None)
    payload, headers = _signed_event("evt_early_wrong", ref, victim.id, "CONFIRMED", 1, 2)
    early = await stack.public.post(
        "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
    )
    assert early.status_code == 200 and early.json()["outcome"] == "CONTRADICTORY", early.text
    escalated = await stack.get(victim.id)
    assert escalated["state"] == "NEEDS_REVIEW" and escalated["provider_booking_ref"] is None, (
        "the read disagreed with the event: nothing was bound"
    )
    response = await pending_create
    assert response.status_code in (201, 202), response.text
    assert (await stack.get(victim.id))["state"] == "NEEDS_REVIEW"
    await shuttle.chaos(stall_pending=False, failpoints={"validate_then_stall_seconds": 0.0})

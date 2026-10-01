"""Forced interleavings (docs/edge-cases.md): the same event from two receivers at once,
a webhook racing a poll, identical cancel requests racing for one key, identical create requests
racing for one key. Each race must end with exactly one effect."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from standardwebhooks import Webhook

from orchestrator.domain import BookingId, CommandKind
from tests.integration.test_booking_b import (
    CLIENT,
    Stack,
    bus,
    postgres_url,
    rail_url,
    redis_url,
    stack,
)
from tests.integration.test_booking_b import (
    _offer_id as _bus_offer_id,
)
from tests.integration.test_booking_c import (
    SECRET,
    Shuttle,
    _create,
    _offer_id,
    _wait_state,
    async_url,
    public_port,
    shuttle,
)

pytestmark = pytest.mark.integration


def _signed(
    event_id: str, ref: str, booking_id: str, status: str, sequence: int
) -> tuple[str, dict[str, str]]:
    payload = json.dumps(
        {
            "type": f"booking.{status.lower()}",
            "eventId": event_id,
            "providerBookingId": ref,
            "clientRef": booking_id,
            "status": status,
            "generation": 1,
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


async def _events(stack: Stack, booking_id: str) -> int:
    async with stack.creator.uow() as store:
        rows = await store.s.execute(
            __import__("sqlalchemy").text(
                "SELECT count(*) FROM booking_events WHERE booking_id = :b"
            ),
            {"b": booking_id},
        )
        return int(rows.scalar_one())


async def test_the_same_event_from_two_receivers_applies_once(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    await shuttle.chaos(webhook_disabled=True, pending_seconds=0.1)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "r-two-receivers")).json()["id"]
    ref = (await stack.get(booking_id))["provider_booking_ref"]
    for _ in range(40):  # the provider progresses on its own; nothing is delivered
        if any(b["status"] == "CONFIRMED" for b in (await shuttle.truth())["bookings"]):
            break
        await asyncio.sleep(0.05)
    payload, headers = _signed("evt_race_same", ref, booking_id, "CONFIRMED", 2)
    events_before = await _events(stack, booking_id)
    responses = await asyncio.gather(
        *[
            stack.public.post(
                "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
            )
            for _ in range(4)
        ]
    )
    outcomes = sorted(r.json()["outcome"] for r in responses)
    assert outcomes == ["APPLIED", "DUPLICATE", "DUPLICATE", "DUPLICATE"], outcomes
    assert (await stack.get(booking_id))["state"] == "CONFIRMED"
    assert await _events(stack, booking_id) == events_before + 1, "exactly one transition"
    await shuttle.chaos(webhook_disabled=False)


async def test_a_webhook_racing_a_poll_produces_one_transition(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    await shuttle.chaos(webhook_disabled=True, pending_seconds=0.1)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "r-webhook-vs-poll")).json()["id"]
    ref = (await stack.get(booking_id))["provider_booking_ref"]
    for _ in range(40):
        if any(b["status"] == "CONFIRMED" for b in (await shuttle.truth())["bookings"]):
            break
        await asyncio.sleep(0.05)
    payload, headers = _signed("evt_race_poll", ref, booking_id, "CONFIRMED", 2)
    events_before = await _events(stack, booking_id)
    webhook, _ = await asyncio.gather(
        stack.public.post(
            "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
        ),
        stack.recovery.poll_pending(BookingId(booking_id), lease=None),
    )
    assert webhook.json()["outcome"] in ("APPLIED", "NO_CHANGE", "STALE")
    final = await stack.get(booking_id)
    assert final["state"] == "CONFIRMED" and final["provider_generation"] == 1
    assert await _events(stack, booking_id) == events_before + 1, (
        "the row lock serialised them: one applied, the other saw the same fact"
    )
    await shuttle.chaos(webhook_disabled=False)


async def test_identical_cancel_requests_race_for_one_command(
    stack: Stack, shuttle: Shuttle, public_port: int
) -> None:
    await shuttle.chaos(pending_seconds=0.1)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "r-cancel-race")).json()["id"]
    await _wait_state(stack, booking_id, "CONFIRMED")
    responses = await asyncio.gather(
        *[
            stack.public.post(
                f"/v1/bookings/{booking_id}/cancel",
                json={"max_fee": None},
                headers={**CLIENT, "Idempotency-Key": "race-cancel"},
            )
            for _ in range(3)
        ]
    )
    assert all(r.status_code in (200, 202) for r in responses), [r.text for r in responses]
    replayed = sorted(r.headers.get("idempotent-replayed") == "true" for r in responses)
    assert replayed == [False, True, True], "one request won the key, the others replayed it"
    async with stack.creator.uow() as store:
        commands = await store.commands_for(BookingId(booking_id), CommandKind.CANCEL)
    assert len(commands) == 1, "one command, however many identical requests"
    assert (await _wait_state(stack, booking_id, "CANCELLED"))["state"] == "CANCELLED"


async def test_identical_create_requests_race_for_one_booking(stack: Stack) -> None:
    offer_id = await _bus_offer_id(stack)
    body = {
        "offer_id": offer_id,
        "passengers": [{"full_name": "Ada Lovelace"}],
        "contact_email": "ada@example.org",
    }
    responses = await asyncio.gather(
        *[
            stack.public.post(
                "/v1/bookings", json=body, headers={**CLIENT, "Idempotency-Key": "race-create"}
            )
            for _ in range(4)
        ]
    )
    assert all(r.status_code in (201, 202) for r in responses), [r.text for r in responses]
    ids = {r.json()["id"] for r in responses}
    assert len(ids) == 1, "one booking for one key"
    assert sum(r.headers.get("idempotent-replayed") == "true" for r in responses) == 3
    truth = await stack.bus.truth()
    assert sum(1 for r in truth if r["yourRef"] == ids.pop()) == 1, "one reservation"


async def test_receivers_held_at_a_barrier_still_apply_the_event_once(
    stack: Stack, shuttle: Shuttle, public_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The contested overlap is forced: three receivers reach the applying transaction at the
    same instant (a barrier), then serialise on the booking's row lock and the receipts
    constraint. Exactly one applies."""
    from orchestrator.application import webhooks as module

    await shuttle.chaos(webhook_disabled=True, pending_seconds=0.1)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "r-barrier-webhook")).json()["id"]
    ref = (await stack.get(booking_id))["provider_booking_ref"]
    for _ in range(40):
        if any(b["status"] == "CONFIRMED" for b in (await shuttle.truth())["bookings"]):
            break
        await asyncio.sleep(0.05)
    barrier = asyncio.Barrier(3)
    original = module.WebhookService._apply
    overlapped = {"n": 0}

    async def gated(self: Any, store: Any, provider: Any, event_id: str, verified: Any) -> Any:
        await barrier.wait()  # everyone is inside its own transaction now
        overlapped["n"] += 1
        return await original(self, store, provider, event_id, verified)

    monkeypatch.setattr(module.WebhookService, "_apply", gated)
    payload, headers = _signed("evt_barrier", ref, booking_id, "CONFIRMED", 2)
    events_before = await _events(stack, booking_id)
    responses = await asyncio.gather(
        *[
            stack.public.post(
                "/v1/providers/mobility-async/webhooks", content=payload, headers=headers
            )
            for _ in range(3)
        ]
    )
    assert overlapped["n"] == 3, "every receiver passed the barrier: the overlap happened"
    assert sorted(r.json()["outcome"] for r in responses) == ["APPLIED", "DUPLICATE", "DUPLICATE"]
    assert await _events(stack, booking_id) == events_before + 1
    await shuttle.chaos(webhook_disabled=False)


async def test_identical_creates_held_at_a_barrier_make_one_booking(
    stack: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    from orchestrator.application import booking_create as module

    barrier = asyncio.Barrier(3)
    original = module.BookingCreator.accept

    async def gated(self: Any, request: Any) -> Any:
        await barrier.wait()  # all three race for the (client, key) row together
        return await original(self, request)

    monkeypatch.setattr(module.BookingCreator, "accept", gated)
    offer_id = await _bus_offer_id(stack)
    body = {
        "offer_id": offer_id,
        "passengers": [{"full_name": "Ada Lovelace"}],
        "contact_email": "ada@example.org",
    }
    responses = await asyncio.gather(
        *[
            stack.public.post(
                "/v1/bookings", json=body, headers={**CLIENT, "Idempotency-Key": "race-barrier"}
            )
            for _ in range(3)
        ]
    )
    assert all(r.status_code in (201, 202) for r in responses), [r.text for r in responses]
    assert len({r.json()["id"] for r in responses}) == 1
    assert sum(r.headers.get("idempotent-replayed") == "true" for r in responses) == 2


async def test_identical_cancels_held_at_a_barrier_make_one_command(
    stack: Stack, shuttle: Shuttle, public_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    from orchestrator.application import cancellation as module

    await shuttle.chaos(pending_seconds=0.1)
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "r-cancel-barrier")).json()["id"]
    await _wait_state(stack, booking_id, "CONFIRMED")
    barrier = asyncio.Barrier(3)
    original = module.Canceller.request

    async def gated(self: Any, request: Any) -> Any:
        await barrier.wait()  # three identical requests reach the key together
        return await original(self, request)

    monkeypatch.setattr(module.Canceller, "request", gated)
    responses = await asyncio.gather(
        *[
            stack.public.post(
                f"/v1/bookings/{booking_id}/cancel",
                json={"max_fee": None},
                headers={**CLIENT, "Idempotency-Key": "race-cancel-barrier"},
            )
            for _ in range(3)
        ]
    )
    assert all(r.status_code in (200, 202) for r in responses), [r.text for r in responses]
    assert sum(r.headers.get("idempotent-replayed") == "true" for r in responses) == 2
    async with stack.creator.uow() as store:
        commands = await store.commands_for(BookingId(booking_id), CommandKind.CANCEL)
    assert len(commands) == 1
    assert (await _wait_state(stack, booking_id, "CANCELLED"))["state"] == "CANCELLED"

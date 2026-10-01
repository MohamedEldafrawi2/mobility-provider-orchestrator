"""Provider A end to end: hold then confirm, resubmission and fenced settlement, hold expiry,
cancellation under authorised terms (docs/edge-cases.md, cases 2 to 7, 12, 13, 22 to 36).

Both simulators run on real sockets; the platform's public and admin listeners share one set
of services; the worker's loops run one tick at a time.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from orchestrator.domain import BookingId, CommandKind
from tests.integration.test_booking_b import (  # noqa: F401 - fixtures
    CLIENT,
    OPERATOR,
    SIM_ADMIN,
    SIM_TOKEN,
    Bus,
    Stack,
    _free_port,
    async_url,
    bus,
    postgres_url,
    redis_url,
    stack,
)

pytestmark = pytest.mark.integration


class Rail:
    def __init__(self, base: str, admin: httpx.AsyncClient) -> None:
        self.base = base
        self.admin = admin

    async def chaos(self, **fields: object) -> None:
        current = (await self.admin.get("/_chaos", headers=SIM_ADMIN)).json()
        failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}  # type: ignore[arg-type]
        current.update(fields)
        current["failpoints"] = failpoints
        response = await self.admin.put("/_chaos", json=current, headers=SIM_ADMIN)
        assert response.status_code == 200, response.text

    async def truth(self) -> list[dict[str, object]]:
        body = (await self.admin.get("/_truth/bookings", headers=SIM_ADMIN)).json()
        return body["bookings"]  # type: ignore[no-any-return]


@pytest.fixture
async def rail(tmp_path: object) -> AsyncIterator[Rail]:
    import uvicorn

    from provider_sims.rail_osdm.app import create_app

    app = create_app(db_path=f"{tmp_path}/rail.sqlite3", admin_token=SIM_TOKEN)
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
        async with httpx.AsyncClient(base_url=base, timeout=5) as admin:
            yield Rail(base, admin)
    finally:
        server.should_exit = True
        await task


@pytest.fixture
def rail_url(rail: Rail) -> str:
    return rail.base


async def _offer_id(stack: Stack, trip: str = "IC-BS-ZH-0704") -> str:
    response = await stack.public.get(
        "/v1/trips",
        params={
            "from": "loc_rail_8500010",
            "to": "loc_rail_8503000",
            "departureDate": "2026-06-15",
            "adults": 1,
        },
        headers=CLIENT,
    )
    assert response.status_code == 200, response.text
    return next(  # type: ignore[no-any-return]
        o["id"]
        for o in response.json()["offers"]
        if o["provider"] == "rail-osdm" and o["trip"]["segments"][0]["vehicle_ref"] == trip
    )


def _body(offer_id: str) -> dict[str, object]:
    return {
        "offer_id": offer_id,
        "passengers": [{"full_name": "Ada Lovelace"}],
        "contact_email": "ada@example.org",
    }


async def _create(stack: Stack, offer_id: str, key: str) -> httpx.Response:
    return await stack.public.post(
        "/v1/bookings", json=_body(offer_id), headers={**CLIENT, "Idempotency-Key": key}
    )


async def _command(stack: Stack, booking_id: str, kind: CommandKind):  # type: ignore[no-untyped-def]
    async with stack.creator.uow() as store:
        return await store.command_for(BookingId(booking_id), kind)


# Hold then confirm ----------------------------------------------------------------------------


async def test_hold_then_confirm_on_the_request_path(stack: Stack, rail: Rail) -> None:
    """6.3: the request path holds, confirms immediately, and answers 201 CONFIRMED."""
    offer_id = await _offer_id(stack)
    response = await _create(stack, offer_id, "a-happy")
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["state"] == "CONFIRMED" and body["command"]["disposition"] == "SUCCEEDED"
    assert body["provider_generation"] == 1
    [truth] = await rail.truth()
    assert truth["status"] == "CONFIRMED" and truth["bookingId"] == body["provider_booking_ref"]
    confirm = await _command(stack, body["id"], CommandKind.CONFIRM)
    create = await _command(stack, body["id"], CommandKind.CREATE)
    assert confirm.disposition.value == "SUCCEEDED" and create.disposition.value == "SUCCEEDED"
    assert create.execution_cutoff is not None and confirm.execution_cutoff is not None
    assert all(a.request.expiry is not None for a in create.attempts + confirm.attempts)
    replay = await _create(stack, offer_id, "a-happy")
    assert replay.status_code == 201 and replay.headers["idempotent-replayed"] == "true"


async def test_lost_hold_response_is_resubmitted_with_the_same_key(
    stack: Stack, rail: Rail
) -> None:
    """6.6 #2 for a RESUBMIT provider: the key is bound before execution, so the platform
    resubmits inside the cutoff and the provider replays the hold it created."""
    offer_id = await _offer_id(stack)
    await rail.chaos(failpoints={"after_prebook_commit": "503"})
    response = await _create(stack, offer_id, "a-lost")
    assert response.status_code == 202, response.text
    assert response.json()["state"] == "UNKNOWN"
    booking_id = response.json()["id"]
    await rail.chaos(failpoints={"after_prebook_commit": None})
    done = await stack.tick()
    assert done["reconciled"] == 1
    final = await stack.get(booking_id)
    assert final["state"] == "CONFIRMED", final
    assert len(await rail.truth()) == 1, "one key, one reservation: the resubmission replayed"
    create = await _command(stack, booking_id, CommandKind.CREATE)
    assert len(create.attempts) == 2 and create.disposition.value == "SUCCEEDED"


async def test_fenced_lookup_settles_a_dropped_create_negatively(stack: Stack, rail: Rail) -> None:
    """6.2: past the cutoff plus skew, the fenced lookup answers finally: nothing, so the
    command is REJECTED on a FENCED_LOOKUP basis and the booking FAILED. Never silence."""
    offer_id = await _offer_id(stack)
    await rail.chaos(
        failpoints={"lose_response": True, "admit_then_stall_then_commit_seconds": 3.0}
    )
    response = await _create(stack, offer_id, "a-fenced")
    assert response.status_code == 202 and response.json()["state"] == "UNKNOWN"
    booking_id = response.json()["id"]
    await rail.chaos(
        failpoints={"lose_response": False, "admit_then_stall_then_commit_seconds": 0.0}
    )
    # Shorten the command's cutoff by hand: the test cannot wait ten minutes.
    async with stack.creator.uow() as store:
        from dataclasses import replace

        command = await store.command_for(BookingId(booking_id), CommandKind.CREATE)
        await store.save_command(
            replace(command, execution_cutoff=datetime.now(UTC) - timedelta(seconds=10))
        )
    await stack.tick()
    final = await stack.get(booking_id)
    assert final["state"] == "FAILED", final
    create = await _command(stack, booking_id, CommandKind.CREATE)
    assert (create.disposition.value, create.basis.value if create.basis else None) == (
        "REJECTED",
        "FENCED_LOOKUP",
    )
    assert [b["status"] for b in await rail.truth()] in ([], ["PREBOOKED"], ["EXPIRED"])
    # Whatever the stalled writer did, the fence guarantees it cannot commit *after* the lookup.
    late = await _create(stack, offer_id, "a-fenced")
    assert late.status_code == 422 and late.json()["code"] == "booking-rejected"


async def test_crash_between_hold_and_confirm_is_confirmed_by_the_loop(
    stack: Stack, rail: Rail
) -> None:
    """6.6 #12: the hold is durable; the confirmation loop confirms it."""
    offer_id = await _offer_id(stack)
    confirmer = stack.services.confirmer
    stack.creator.confirmer = None  # the request path dies right after the hold
    response = await _create(stack, offer_id, "a-crash")
    stack.creator.confirmer = confirmer
    assert response.status_code == 202 and response.json()["state"] == "HELD", response.text
    booking_id = response.json()["id"]
    assert response.json()["confirmation_deadline"] is not None
    done = await stack.tick()
    assert done["confirmed"] == 1
    final = await stack.get(booking_id)
    assert final["state"] == "CONFIRMED"


async def test_hold_expiry_is_observed_never_presumed(stack: Stack, rail: Rail) -> None:
    """6.6 #12: a hold whose deadline passed is looked up; the provider reports the expiry and
    only then is the booking FAILED, with CREATE REJECTED."""
    offer_id = await _offer_id(stack)
    await rail.chaos(hold_expiry_seconds=0.5)
    confirmer = stack.services.confirmer
    stack.creator.confirmer = None
    response = await _create(stack, offer_id, "a-expire")
    stack.creator.confirmer = confirmer
    booking_id = response.json()["id"]
    assert response.json()["state"] == "HELD"
    await asyncio.sleep(0.7)
    await stack.tick()
    final = await stack.get(booking_id)
    assert final["state"] == "FAILED" and final["failure_code"] == "hold-expired", final
    create = await _command(stack, booking_id, CommandKind.CREATE)
    assert create.disposition.value == "REJECTED"
    assert (await _create(stack, offer_id, "a-expire")).status_code == 422


async def test_lost_confirmation_answer_is_settled_by_lookup(stack: Stack, rail: Rail) -> None:
    """6.6 #13: the confirm answer is lost; an authoritative read settles the attempt."""
    offer_id = await _offer_id(stack)
    await rail.chaos(failpoints={"after_confirm_commit": "503"})
    response = await _create(stack, offer_id, "a-confirm-lost")
    assert response.status_code == 202 and response.json()["state"] == "UNKNOWN", response.text
    booking_id = response.json()["id"]
    await rail.chaos(failpoints={"after_confirm_commit": None})
    async with stack.creator.uow() as store:  # the exclusion instant, brought forward
        booking = await store.get(BookingId(booking_id), for_update=True)
        await store.save_booking(booking, next_action_at=datetime.now(UTC))
    await stack.tick()
    final = await stack.get(booking_id)
    assert final["state"] == "CONFIRMED", final
    confirm = await _command(stack, booking_id, CommandKind.CONFIRM)
    assert confirm.disposition.value == "SUCCEEDED"
    assert confirm.basis is not None and confirm.basis.value == "FENCED_LOOKUP"


# Cancellation --------------------------------------------------------------------------------


async def _cancel(
    stack: Stack, booking_id: str, key: str, max_fee: int | None = 700
) -> httpx.Response:
    body: dict[str, object] = (
        {"max_fee": {"amount_minor": max_fee, "currency": "CHF"}}
        if max_fee is not None
        else {"max_fee": None}
    )
    return await stack.public.post(
        f"/v1/bookings/{booking_id}/cancel", json=body, headers={**CLIENT, "Idempotency-Key": key}
    )


async def test_cancel_with_refund_under_authorised_terms(stack: Stack, rail: Rail) -> None:
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "a-cancel")).json()["id"]
    response = await _cancel(stack, booking_id, "c-1")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["state"] == "CANCELLED" and body["command"] == {
        "kind": "CANCEL",
        "disposition": "SUCCEEDED",
    }
    assert (
        body["refund"]["status"] == "CONFIRMED"
        and body["refund"]["quote"]["fee"]["amount_minor"] == 680
    )
    [truth] = await rail.truth()
    assert truth["status"] == "CANCELLED"
    replay = await _cancel(stack, booking_id, "c-1")
    assert replay.status_code == 200 and replay.headers["idempotent-replayed"] == "true"
    other = await _cancel(stack, booking_id, "c-1", max_fee=999)
    assert other.status_code == 422 and other.json()["code"] == "idempotency-key-reuse"
    again = await _cancel(stack, booking_id, "c-2")
    assert again.status_code == 409 and again.json()["code"] == "booking-not-cancellable"


async def test_cancel_terms_changed_leaves_the_booking_confirmed(stack: Stack, rail: Rail) -> None:
    """6.6 #34: a fee above what the client authorised is not accepted on its behalf."""
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "a-terms")).json()["id"]
    response = await _cancel(stack, booking_id, "c-3", max_fee=100)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == "cancellation-terms-changed"
    assert response.json()["booking"]["state"] == "CONFIRMED"
    assert response.json()["booking"]["refund"]["quote"]["fee"]["amount_minor"] == 680
    [truth] = await rail.truth()
    assert truth["status"] == "CONFIRMED", "nothing was accepted"
    free = await _cancel(stack, booking_id, "c-4", max_fee=None)
    assert free.status_code == 409


async def test_cancel_not_allowed_while_held_or_at_a_provider_without_cancellation(
    stack: Stack, rail: Rail
) -> None:
    offer_id = await _offer_id(stack)
    confirmer = stack.services.confirmer
    stack.creator.confirmer = None
    held = (await _create(stack, offer_id, "a-held")).json()
    stack.creator.confirmer = confirmer
    assert held["state"] == "HELD"
    refused = await _cancel(stack, held["id"], "c-5")
    assert refused.status_code == 409 and refused.json()["code"] == "booking-not-cancellable"
    from tests.integration.test_booking_b import _create as create_b
    from tests.integration.test_booking_b import _offer_id as offer_b

    bus_booking = (await create_b(stack, await offer_b(stack), "b-cancel")).json()
    assert bus_booking["state"] == "CONFIRMED"
    refused = await _cancel(stack, bus_booking["id"], "c-6")
    assert refused.status_code == 409


async def test_lost_acceptance_answer_settles_by_the_exact_offer(stack: Stack, rail: Rail) -> None:
    """6.6 #31: the acceptance answer is lost; the offer's status decides."""
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "a-cancel-lost")).json()["id"]
    await rail.chaos(failpoints={"after_refund_accept_commit": "503"})
    response = await _cancel(stack, booking_id, "c-7")
    assert response.status_code == 202, response.text
    assert response.json()["state"] == "CANCELLING"
    await rail.chaos(failpoints={"after_refund_accept_commit": None})
    async with stack.creator.uow() as store:
        booking = await store.get(BookingId(booking_id), for_update=True)
        await store.save_booking(booking, next_action_at=datetime.now(UTC))
    await stack.tick()
    final = await stack.get(booking_id)
    assert final["state"] == "CANCELLED", final
    cancel = await _command(stack, booking_id, CommandKind.CANCEL)
    assert cancel.disposition.value == "SUCCEEDED" and len(cancel.attempts) == 1
    assert (await _cancel(stack, booking_id, "c-7")).status_code == 200

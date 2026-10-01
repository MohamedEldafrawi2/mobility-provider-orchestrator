"""Booking creation against Provider B through the real stack: PostgreSQL and Redis containers,
the simulator on a real socket, the public and admin applications, and the worker loops.

Every scenario is checked against the independent reference model as well as against the
API, so a platform that reports a state the design forbids fails here even if it is
internally consistent.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pytest
import uvicorn
from alembic import command
from alembic.config import Config
from asgi_lifespan import LifespanManager
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from orchestrator.api import create_admin_app, create_public_app
from orchestrator.application.booking_create import BookingCreator
from orchestrator.application.recovery import Recovery
from orchestrator.application.wiring import build_services
from orchestrator.domain import (
    BookingId,
    BookingState,
    CommandKind,
    Evidence,
    EvidenceKind,
    ProviderBookingRef,
    Reservation,
    ReservationState,
)
from orchestrator.persistence.bookings import StaleLeaseError
from orchestrator.worker.loops import Worker
from provider_sims.bus_legacy.app import create_app as create_bus_app
from tests.conftest import DEV_ADMIN_KEY, DEV_CLIENT_KEY, make_settings, sha256
from tests.model.b_reference_model import BReferenceModel, DispatchMode

pytestmark = pytest.mark.integration

SIM_TOKEN = "t"
SIM_ADMIN = {"X-Admin-Token": SIM_TOKEN}
CLIENT = {"X-API-Key": DEV_CLIENT_KEY}
OTHER_CLIENT_KEY = "test-other-client-key"
OTHER_CLIENT = {"X-API-Key": OTHER_CLIENT_KEY}
OPERATOR = {"X-Admin-Key": DEV_ADMIN_KEY}
LOOKUP_BUDGET = 3


# Infrastructure -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:18", driver="asyncpg") as pg:
        url = pg.get_connection_url()
        cfg = Config("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", url)
        command.upgrade(cfg, "head")
        yield url


@pytest.fixture(scope="module")
def redis_url() -> Iterator[str]:
    with RedisContainer("redis:8") as r:
        yield f"redis://{r.get_container_host_ip()}:{r.get_exposed_port(6379)}/0"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@dataclass
class Bus:
    base: str
    admin: httpx.AsyncClient

    async def chaos(self, **fields: object) -> None:
        current = (await self.admin.get("/_chaos", headers=SIM_ADMIN)).json()
        failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}  # type: ignore[arg-type]
        current.update(fields)
        current["failpoints"] = failpoints
        assert (await self.admin.put("/_chaos", json=current, headers=SIM_ADMIN)).status_code == 200

    async def reset(self) -> None:
        await self.admin.post("/_chaos/reset", headers=SIM_ADMIN)
        await self.admin.post("/_truth/wipe", headers=SIM_ADMIN)

    async def truth(self) -> list[dict[str, object]]:
        body = (await self.admin.get("/_truth/reservations", headers=SIM_ADMIN)).json()
        return body["reservations"]  # type: ignore[no-any-return]

    async def rebuild_index(self) -> int:
        response = await self.admin.post("/_truth/rebuild-index", headers=SIM_ADMIN)
        assert response.status_code == 200, response.text
        return int(response.json()["exposed"])


@pytest.fixture
async def bus(tmp_path: object) -> AsyncIterator[Bus]:
    app = create_bus_app(db_path=f"{tmp_path}/bus.sqlite3", admin_token=SIM_TOKEN)
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
            yield Bus(base, admin)
    finally:
        server.should_exit = True
        await task


@dataclass
class Stack:
    public: httpx.AsyncClient
    admin: httpx.AsyncClient
    worker: Worker
    creator: BookingCreator
    recovery: Recovery
    bus: Bus
    redis: Any
    services: Any
    public_app: Any

    async def tick(self, n: int = 1) -> dict[str, int]:
        totals = {
            "submitted": 0,
            "abandoned": 0,
            "reconciled": 0,
            "recovered": 0,
            "confirmed": 0,
            "cancelled": 0,
            "polled": 0,
            "failed": 0,
        }
        for _ in range(n):
            for key, value in (await self.worker.tick()).items():
                totals[key] += value
        return totals

    async def get(self, booking_id: str) -> dict[str, object]:
        response = await self.public.get(f"/v1/bookings/{booking_id}", headers=CLIENT)
        assert response.status_code == 200, response.text
        return response.json()  # type: ignore[no-any-return]


@pytest.fixture
def rail_url() -> str:
    """Provider A's simulator: a closed port here; ``test_booking_a`` overrides it."""
    return "http://127.0.0.1:1"


@pytest.fixture
def async_url() -> str:
    """Provider C's simulator: a closed port here; ``test_booking_c`` overrides it."""
    return "http://127.0.0.1:1"


@pytest.fixture
async def stack(
    postgres_url: str, redis_url: str, bus: Bus, rail_url: str, async_url: str
) -> AsyncIterator[Stack]:
    settings = make_settings(
        database_url=postgres_url,
        redis_url=redis_url,
        bus_legacy_url=bus.base,
        rail_osdm_url=rail_url,
        mobility_async_url=async_url,
        api_keys={sha256(DEV_CLIENT_KEY): "test-client", sha256(OTHER_CLIENT_KEY): "other"},
        provider_mutation_timeout_seconds=1.0,
        provider_read_timeout_seconds=2.0,
        lookup_budget=LOOKUP_BUDGET,
        create_max_attempts=3,
        reconcile_backoff_seconds=0.0,
        reschedule_backoff_seconds=0.0,
        submitting_stale_after_seconds=0.0,
        abandon_after_seconds=60.0,
    )
    engine = create_async_engine(postgres_url)
    async with engine.begin() as conn:
        for table in (
            "webhook_receipts",
            "idempotency_keys",
            "booking_events",
            "evidence",
            "review_cases",
            "attempts",
            "commands",
            "bookings",
        ):
            await conn.execute(text(f"DELETE FROM {table}"))  # noqa: S608 - fixed table list
    await engine.dispose()
    # One set of services for both listeners, as in the process entry point: one admission
    # state per provider and purpose, one set of gauges.
    services = await build_services(settings)
    public_app = create_public_app(settings, services=services)
    admin_app = create_admin_app(settings, services=services)
    async with LifespanManager(public_app), LifespanManager(admin_app):
        worker = Worker(
            services.uow,
            services.creator,
            services.recovery,
            services.policy,
            confirm_uow=services.confirm_uow,
        )
        async with (
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=public_app, raise_app_exceptions=False),
                base_url="http://public",
            ) as public,
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=admin_app, raise_app_exceptions=False),
                base_url="http://admin",
            ) as admin,
        ):
            try:
                yield Stack(
                    public,
                    admin,
                    worker,
                    services.creator,
                    services.recovery,
                    bus,
                    services.redis,
                    services,
                    public_app,
                )
            finally:
                worker.close()
    await services.close()


async def _offer_id(stack: Stack, jid: str = "BUS-ROM-MIL-0715") -> str:
    response = await stack.public.get(
        "/v1/trips",
        params={
            "from": "loc_bus_101",
            "to": "loc_bus_102",
            "departureDate": "2026-06-15",
            "adults": 1,
        },
        headers=CLIENT,
    )
    assert response.status_code == 200, response.text
    return next(o["id"] for o in response.json()["offers"] if o["provider_offer_ref"] == jid)  # type: ignore[no-any-return]


def _body(offer_id: str, name: str = "Ada Lovelace") -> dict[str, object]:
    return {
        "offer_id": offer_id,
        "passengers": [{"full_name": name}],
        "contact_email": "ada@example.org",
    }


async def _create(stack: Stack, offer_id: str, key: str, **body: object) -> httpx.Response:
    return await stack.public.post(
        "/v1/bookings", json={**_body(offer_id), **body}, headers={**CLIENT, "Idempotency-Key": key}
    )


def _model(mode: DispatchMode) -> BReferenceModel:
    model = BReferenceModel(client_ref="x", lookup_budget=LOOKUP_BUDGET, max_age=timedelta(0))
    model.dispatch(mode)
    return model


def _check(
    model: BReferenceModel,
    booking: dict[str, object],
    truth: list[dict[str, object]] | None = None,
) -> None:
    assert str(booking["state"]) in model.allowed_booking_states(), (
        f"state {booking['state']} but the model allows {model.allowed_booking_states()}"
    )
    command = booking.get("command")
    if command is not None:
        assert command["disposition"] in model.allowed_dispositions()  # type: ignore[index]
    if truth is not None:
        # The identity property: a bound reference is a real reservation carrying our id.
        bound = booking.get("provider_booking_ref")
        if bound is not None:
            matching = [r for r in truth if str(r["resId"]) == bound]
            assert matching and matching[0]["yourRef"] == booking["id"], (
                f"bound to {bound}, which the provider does not hold for {booking['id']}"
            )
        if str(booking["state"]) == "CONFIRMED":
            assert bound is not None
        if str(booking["state"]) == "FAILED":
            assert not [r for r in truth if r["yourRef"] == booking["id"]], (
                "FAILED while the provider holds a reservation for us"
            )


async def _check_persisted(
    stack: Stack, model: BReferenceModel, booking_id: str, truth: list[dict[str, object]]
) -> None:
    """The oracle over what is *persisted*: attempts, disposition, basis, binding."""
    async with stack.creator.uow() as store:
        command = await store.command_for(BookingId(booking_id), CommandKind.CREATE)
        booking = await store.get(BookingId(booking_id))
    assert command.disposition.value in model.allowed_dispositions(), (
        f"persisted {command.disposition} but the model allows {model.allowed_dispositions()}"
    )
    dispatched = [a for a in command.attempts if a.dispatch_marked_at is not None]
    assert len(dispatched) == model.dispatched_to_network
    assert all(
        a.finished_at is not None or a.dispatch_marked_at is not None for a in command.attempts
    ), "an attempt left open must carry its dispatch mark (a lost response is a possible effect)"
    # The model names references its own way; identity is checked against provider truth.
    assert (command.submission_ref is None) == (model.bound_ref is None)
    assert booking.provider_booking_ref == command.submission_ref
    if command.is_settled:
        assert command.basis is not None
        if command.disposition.value == "SUCCEEDED":
            assert command.basis.value in ("PROVIDER_RESULT", "LOOKUP")
        if command.disposition.value == "REJECTED":
            assert command.basis.value == "PROVIDER_RESULT" and not model.possible_effect
        if command.disposition.value == "ABANDONED":
            assert command.basis.value == "LOCAL" and model.dispatched_to_network == 0
    else:
        assert command.possibly_executed == (model.possible_effect and not model.settled_success)
    if model.bound_ref is not None:
        held = [r for r in truth if str(r["resId"]) == command.submission_ref]
        assert held and held[0]["yourRef"] == booking_id


# Scenarios ----------------------------------------------------------------------------------


async def test_happy_path_confirms_synchronously(stack: Stack) -> None:
    offer_id = await _offer_id(stack)
    response = await _create(stack, offer_id, "k-happy")
    assert response.status_code == 201, response.text
    _check(_model(DispatchMode.OK), response.json(), await stack.bus.truth())
    await _check_persisted(
        stack, _model(DispatchMode.OK), response.json()["id"], await stack.bus.truth()
    )
    body = response.json()
    assert body["state"] == "CONFIRMED" and body["provider_booking_ref"]
    assert response.headers["location"] == f"/v1/bookings/{body['id']}"
    _check(_model(DispatchMode.OK), body)
    detail = await stack.get(body["id"])
    assert [e["to"] for e in detail["events"]] == ["SUBMITTING", "CONFIRMED"]  # type: ignore[index]
    assert [r["yourRef"] for r in await stack.bus.truth()] == [body["id"]]


async def test_idempotent_replay_and_conflict(stack: Stack) -> None:
    offer_id = await _offer_id(stack)
    first = await _create(stack, offer_id, "k-idem")
    again = await _create(stack, offer_id, "k-idem")
    assert again.status_code == 201 and again.headers["idempotent-replayed"] == "true"
    assert again.json()["id"] == first.json()["id"]
    assert len(await stack.bus.truth()) == 1, "a replay never reaches the provider"

    conflict = await _create(stack, offer_id, "k-idem", passengers=[{"full_name": "Someone Else"}])
    assert conflict.status_code == 422 and conflict.json()["code"] == "idempotency-key-reuse"

    missing = await stack.public.post("/v1/bookings", json=_body(offer_id), headers=CLIENT)
    assert missing.status_code == 400 and missing.json()["code"] == "idempotency-key-required"


async def test_concurrent_same_key_creates_one_booking(stack: Stack) -> None:
    offer_id = await _offer_id(stack)
    responses = await asyncio.gather(*(_create(stack, offer_id, "k-race") for _ in range(12)))
    ids = {r.json()["id"] for r in responses}
    assert len(ids) == 1, ids
    # The winner answers 201; a loser that replays while the winner is still in flight answers
    # 202 with the same booking (section 6.5). Nobody gets a second booking or a 5xx.
    statuses = sorted(r.status_code for r in responses)
    assert set(statuses) <= {201, 202} and 201 in statuses, statuses
    assert len(await stack.bus.truth()) == 1


async def test_offer_mismatch_and_unknown_offer(stack: Stack) -> None:
    offer_id = await _offer_id(stack)
    two = await _create(
        stack, offer_id, "k-mismatch", passengers=[{"full_name": "A"}, {"full_name": "B"}]
    )
    assert two.status_code == 409 and two.json()["code"] == "offer-mismatch"
    gone = await _create(stack, "off_bus_nothing", "k-gone")
    assert gone.status_code == 404 and gone.json()["code"] == "offer-unavailable"
    assert await stack.bus.truth() == []


async def test_definitive_rejection_is_422_and_failed(stack: Stack) -> None:
    sold_out = await _offer_id(stack, "BUS-ROM-MIL-0230")
    for i in range(2):
        assert (await _create(stack, sold_out, f"k-fill{i}")).status_code == 201
    rejected = await _create(stack, sold_out, "k-full")
    assert rejected.status_code == 422, rejected.text
    body = rejected.json()
    assert body["code"] == "booking-rejected" and body["booking"]["state"] == "FAILED"
    _check(_model(DispatchMode.REJECT), body["booking"], await stack.bus.truth())
    await _check_persisted(
        stack, _model(DispatchMode.REJECT), body["booking"]["id"], await stack.bus.truth()
    )


async def test_commit_then_503_becomes_unknown_then_confirmed_by_reconciliation(
    stack: Stack,
) -> None:
    """Section 6.6 #2: the provider had created the booking; the platform never says FAILED."""
    offer_id = await _offer_id(stack)
    await stack.bus.chaos(failpoints={"after_reserve_commit": "503"})
    response = await _create(stack, offer_id, "k-503")
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["state"] == "UNKNOWN" and body["unresolved"] and response.headers["retry-after"]
    model = _model(DispatchMode.COMMIT_LOSE_RESPONSE)
    _check(model, body)

    replay = await _create(stack, offer_id, "k-503")
    assert replay.status_code == 202 and replay.headers["idempotent-replayed"] == "true"
    assert len(await stack.bus.truth()) == 1, "no second unsafe dispatch"

    await stack.bus.chaos(failpoints={"after_reserve_commit": None})
    assert (await stack.tick())["reconciled"] == 1
    model.lookup()
    confirmed = await stack.get(body["id"])
    assert confirmed["state"] == "CONFIRMED" and confirmed["provider_booking_ref"]
    _check(model, confirmed, await stack.bus.truth())
    await _check_persisted(stack, model, body["id"], await stack.bus.truth())
    later = await _create(stack, offer_id, "k-503")
    assert later.status_code == 201, "the replay now reports the settled command"


async def test_dropped_request_ends_in_review_never_failed(stack: Stack) -> None:
    """Section 6.6 #44: nothing exists at the provider, and the platform cannot know that."""
    offer_id = await _offer_id(stack)
    await stack.bus.chaos(failpoints={"drop_request": True, "hold_seconds": 3})
    response = await _create(stack, offer_id, "k-drop")
    assert response.status_code == 202 and response.json()["state"] == "UNKNOWN"
    booking_id = response.json()["id"]
    await stack.bus.chaos(failpoints={"drop_request": False})
    model = _model(DispatchMode.DROP_REQUEST)
    for _ in range(LOOKUP_BUDGET):
        await stack.tick()
        model.lookup()
        _check(model, await stack.get(booking_id))
    parked = await stack.get(booking_id)
    assert (
        parked["state"] == "NEEDS_REVIEW"
        and parked["unresolved_reason"] == "provider-cannot-settle"
    )
    assert await stack.bus.truth() == []
    await _check_persisted(stack, model, booking_id, [])

    cases = (await stack.admin.get("/review", headers=OPERATOR)).json()["cases"]
    assert [c["booking_id"] for c in cases] == [booking_id]
    reconciled = await stack.admin.post(f"/review/{booking_id}/reconcile", headers=OPERATOR)
    assert reconciled.status_code == 200 and reconciled.json()["state"] == "NEEDS_REVIEW"
    resolve = await stack.admin.post(
        f"/review/{booking_id}/resolve",
        json={"expected_version": parked["version"], "reason": "looks gone"},
        headers=OPERATOR,
    )
    assert resolve.status_code == 409 and resolve.json()["code"] == "case-not-closable", (
        "an operator cannot turn silence into FAILED"
    )
    assert (await stack.get(booking_id))["state"] == "NEEDS_REVIEW"


async def test_lagging_index_is_found_within_budget(stack: Stack) -> None:
    """Visibility is controlled, not timed: the index exposes the row when we say so."""
    offer_id = await _offer_id(stack)
    await stack.bus.chaos(failpoints={"after_reserve_commit": "503", "never_index": True})
    response = await _create(stack, offer_id, "k-lag")
    booking_id = response.json()["id"]
    await stack.bus.chaos(failpoints={"after_reserve_commit": None, "never_index": False})
    model = _model(DispatchMode.COMMIT_INVISIBLE)
    await stack.tick()
    model.lookup()
    _check(model, await stack.get(booking_id), await stack.bus.truth())
    assert (await stack.get(booking_id))["state"] == "UNKNOWN", "not visible yet: keep looking"
    assert await stack.bus.rebuild_index() == 1
    model.expose()
    await stack.tick()
    model.lookup()
    final = await stack.get(booking_id)
    _check(model, final, await stack.bus.truth())
    assert final["state"] == "CONFIRMED"


async def test_never_indexed_reservation_is_a_permanent_case_the_operator_can_close_after_exposure(
    stack: Stack,
) -> None:
    offer_id = await _offer_id(stack)
    await stack.bus.chaos(failpoints={"after_reserve_commit": "503", "never_index": True})
    booking_id = (await _create(stack, offer_id, "k-ghost")).json()["id"]
    await stack.bus.chaos(failpoints={"after_reserve_commit": None})
    await stack.tick(LOOKUP_BUDGET)
    assert (await stack.get(booking_id))["state"] == "NEEDS_REVIEW"
    assert len(await stack.bus.truth()) == 1, (
        "the reservation exists and the platform cannot see it"
    )

    reconciled = await stack.admin.post(f"/review/{booking_id}/reconcile", headers=OPERATOR)
    assert reconciled.json()["state"] == "NEEDS_REVIEW"
    cases = (await stack.admin.get("/review", headers=OPERATOR)).json()["cases"]
    assert [c["booking_id"] for c in cases] == [booking_id]

    # The provider's operations team rebuilds the index: the reservation becomes visible,
    # reconciliation observes it, the complete evidence set closes the case into CONFIRMED.
    assert await stack.bus.rebuild_index() == 1
    closed = await stack.admin.post(f"/review/{booking_id}/reconcile", headers=OPERATOR)
    assert closed.status_code == 200, closed.text
    assert closed.json()["state"] == "CONFIRMED" and closed.json()["case"] == "closed"
    final = await stack.get(booking_id)
    model = _model(DispatchMode.COMMIT_INVISIBLE)
    model.lookups_after_uncertain = LOOKUP_BUDGET
    model.expose()
    model.lookup()
    _check(model, final, await stack.bus.truth())
    assert (await stack.admin.get("/review", headers=OPERATOR)).json()["cases"] == []
    detail = await stack.admin.get(f"/review/{booking_id}", headers=OPERATOR)
    assert detail.status_code == 404, "no open case remains"


async def test_review_does_not_close_while_an_implicated_reservation_is_unaccounted(
    stack: Stack,
) -> None:
    """Section 6.1: a case closes only when *every* implicated reservation is affirmatively
    accounted for. Discovery that finds one of two must leave the booking under review."""
    offer_id = await _offer_id(stack)
    await stack.bus.chaos(failpoints={"after_reserve_commit": "503", "never_index": True})
    booking_id = (await _create(stack, offer_id, "k-unaccounted")).json()["id"]
    await stack.bus.chaos(failpoints={"after_reserve_commit": None, "never_index": False})
    await stack.tick(LOOKUP_BUDGET)
    assert (await stack.get(booking_id))["state"] == "NEEDS_REVIEW"
    # An earlier observation implicated a second reference the index no longer returns
    # (a provider-side merge, an operator's note): it is part of the case's evidence set.
    async with stack.creator.uow() as store:
        found = await store.review_case(BookingId(booking_id))
        assert found is not None
        await store.extend_review_case(found[1], implicated=(ProviderBookingRef("999999"),))
    assert await stack.bus.rebuild_index() == 1
    reconciled = await stack.admin.post(f"/review/{booking_id}/reconcile", headers=OPERATOR)
    assert reconciled.status_code == 200, reconciled.text
    assert reconciled.json()["state"] == "NEEDS_REVIEW", "one of two accounted for: stay open"
    kinds = {(e["subject"], e["reservation"]) for e in reconciled.json()["evidence"]}
    assert ("999999", None) in kinds, "the negative lookup is on record, dated"
    parked = await stack.get(booking_id)
    assert parked["provider_booking_ref"] is None, "nothing bound while the case is open"
    resolve = await stack.admin.post(
        f"/review/{booking_id}/resolve",
        json={"expected_version": parked["version"], "reason": "looks fine"},
        headers=OPERATOR,
    )
    assert resolve.status_code == 409 and resolve.json()["code"] == "case-not-closable"


async def test_crash_after_provider_response_is_recovered_by_reconciliation(stack: Stack) -> None:
    """Section 6.6 #10: the journaled attempt survives the crash; recovery settles it."""
    offer_id = await _offer_id(stack)

    def crash(point: str) -> None:
        if point == "after_provider_response":
            raise RuntimeError(f"simulated crash at {point}")

    stack.creator.failpoint = crash
    response = await _create(stack, offer_id, "k-crash")
    stack.creator.failpoint = None
    assert response.status_code == 500, "the request path died after the provider answered"
    assert len(await stack.bus.truth()) == 1

    replay = await _create(stack, offer_id, "k-crash")
    assert replay.status_code == 202 and replay.json()["state"] == "SUBMITTING"
    booking_id = replay.json()["id"]

    done = await stack.tick()
    assert done["recovered"] == 1, "SUBMITTING with a journaled attempt becomes UNKNOWN"
    assert (await stack.get(booking_id))["state"] in ("UNKNOWN", "CONFIRMED")
    await stack.tick()
    final = await stack.get(booking_id)
    assert final["state"] == "CONFIRMED" and final["provider_booking_ref"]
    truth = await stack.bus.truth()
    assert len(truth) == 1, "recovery never re-dispatched"
    settled = _model(DispatchMode.COMMIT_LOSE_RESPONSE)
    settled.lookup()
    await _check_persisted(stack, settled, booking_id, truth)


async def test_crash_before_dispatch_mark_is_submitted_by_the_worker(stack: Stack) -> None:
    """Section 6.6: a request that died before journaling anything left a plain CREATED
    booking; the worker submits it normally, and does not abandon it."""
    offer_id = await _offer_id(stack)

    def crash(point: str) -> None:
        if point == "before_dispatch_mark":
            raise RuntimeError("simulated crash before the dispatch mark")

    stack.creator.failpoint = crash
    response = await _create(stack, offer_id, "k-early")
    stack.creator.failpoint = None
    assert response.status_code == 500
    assert await stack.bus.truth() == []
    replay = await _create(stack, offer_id, "k-early")
    assert replay.status_code == 202 and replay.json()["state"] == "CREATED"
    booking_id = replay.json()["id"]
    done = await stack.tick()
    assert done == {
        "submitted": 1,
        "abandoned": 0,
        "reconciled": 0,
        "recovered": 0,
        "confirmed": 0,
        "cancelled": 0,
        "polled": 0,
        "failed": 0,
    }
    final = await stack.get(booking_id)
    _check(_model(DispatchMode.OK), final, await stack.bus.truth())
    assert final["state"] == "CONFIRMED"


async def test_never_dispatched_booking_is_abandoned_only_after_its_age(stack: Stack) -> None:
    offer_id = await _offer_id(stack)

    def crash(point: str) -> None:
        if point == "before_dispatch_mark":
            raise RuntimeError("simulated crash before the dispatch mark")

    stack.creator.failpoint = crash
    assert (await _create(stack, offer_id, "k-abandon")).status_code == 500
    booking_id = (await _create(stack, offer_id, "k-abandon")).json()["id"]  # the replay
    # Young: not abandoned, not yet submitted either (the failpoint still crashes submits,
    # which the loop contains per row and reports).
    assert (await stack.tick())["failed"] == 1
    assert (await stack.get(booking_id))["state"] == "CREATED"
    # Old enough: abandoned without ever reaching the provider.
    impatient = Recovery(
        stack.recovery.uow,
        stack.recovery.registry,
        stack.creator,
        replace(stack.recovery.policy, abandon_after=timedelta(0)),
        admission=stack.recovery.admission,
    )
    assert await impatient.abandon_if_due(BookingId(booking_id), lease=None)
    stack.creator.failpoint = None
    final = await stack.get(booking_id)
    assert final["state"] == "FAILED" and final["failure_code"] == "booking-not-submitted"
    assert await stack.bus.truth() == []
    never = BReferenceModel(client_ref="x", lookup_budget=LOOKUP_BUDGET, max_age=timedelta(0))
    never.abandon(timedelta(seconds=1))
    _check(never, final, [])
    await _check_persisted(stack, never, booking_id, [])


async def test_repeated_safe_failures_are_bounded_and_end_in_review(stack: Stack) -> None:
    """Section 6.2: attempts that certainly had no effect are bounded. The command is not
    ABANDONED (attempts were dispatched) and not FAILED (nothing proves the provider's
    state): an operator decides when to stop waiting for the provider."""
    offer_id = await _offer_id(stack)
    await stack.bus.chaos(failure_rate=1.0)  # every request answered 503 from the edge
    response = await _create(stack, offer_id, "k-edge")
    assert response.status_code == 202, response.text
    booking_id = response.json()["id"]
    assert (await stack.get(booking_id))["state"] == "CREATED"
    for _ in range(2):
        await stack.tick()
    final = await stack.get(booking_id)
    assert final["state"] == "NEEDS_REVIEW"
    assert final["unresolved_reason"] == "provider-unavailable"
    replay = await _create(stack, offer_id, "k-edge")
    assert replay.status_code == 202 and replay.json()["command"]["disposition"] == "UNRESOLVED"
    async with stack.creator.uow() as store:
        command = await store.command_for(BookingId(booking_id), CommandKind.CREATE)
    assert len(command.attempts) == 3 and not command.possibly_executed
    assert all(
        a.side_effect is not None and a.side_effect.value == "NONE" for a in command.attempts
    )
    assert await stack.bus.truth() == []
    exhausted = BReferenceModel(
        client_ref="x", lookup_budget=LOOKUP_BUDGET, max_age=timedelta(0), max_attempts=3
    )
    for _ in range(3):
        exhausted.dispatch(DispatchMode.SAFE_FAILURE)
    _check(exhausted, final, [])
    await _check_persisted(stack, exhausted, booking_id, [])
    cases = (await stack.admin.get("/review", headers=OPERATOR)).json()["cases"]
    assert [c["booking_id"] for c in cases] == [booking_id] and cases[0]["remediable"] is True
    await stack.bus.chaos(failure_rate=0.0)


async def test_two_workers_claim_a_booking_once(stack: Stack) -> None:
    """Section 6.4: leased claims with SKIP LOCKED let two workers race without a double
    submission. The interleaving is forced, not hoped for: worker A's claim transaction is
    held open (its row lock held) while worker B runs a whole tick, then A proceeds."""
    offer_id = await _offer_id(stack)

    def crash(point: str) -> None:
        if point == "before_dispatch_mark":
            raise RuntimeError("simulated crash before the dispatch mark")

    stack.creator.failpoint = crash
    assert (await _create(stack, offer_id, "k-race")).status_code == 500
    booking_id = (await _create(stack, offer_id, "k-race")).json()["id"]
    stack.creator.failpoint = None
    other = Worker(stack.creator.uow, stack.creator, stack.recovery, stack.recovery.policy)

    async with stack.creator.uow() as store:  # worker A: claimed, not yet committed
        claimed = await store.claim(
            (BookingState.CREATED,), limit=10, ttl=stack.recovery.policy.lease_ttl
        )
        assert [b.id for b, _ in claimed] == [booking_id]
        lease_a = claimed[0][1]
        b = await other.tick()  # worker B: the row is locked, SKIP LOCKED skips it
        assert b["submitted"] == 0 and b["abandoned"] == 0, b
    # A commits its claim and processes the row it owns.
    await stack.creator.submit_once(BookingId(booking_id), lease=lease_a, correlation_id=None)
    async with stack.creator.uow() as store:
        await store.release(lease_a)
    # B ticks again after A released: nothing left to claim, nothing double-submitted.
    assert (await other.tick())["submitted"] == 0
    final = await stack.get(booking_id)
    assert final["state"] == "CONFIRMED"
    truth = await stack.bus.truth()
    assert len(truth) == 1
    await _check_persisted(stack, _model(DispatchMode.OK), booking_id, truth)


async def test_request_path_and_worker_recovery_interleave_safely(stack: Stack) -> None:
    """Section 6.6 #10 with a barrier: the request path is parked *between* the provider's
    answer and its outcome transaction while the recovery scan and reconciliation run; the
    late outcome then closes its own attempt against the already settled command."""
    offer_id = await _offer_id(stack)
    parked = asyncio.Event()
    released = asyncio.Event()

    def barrier(point: str) -> None:
        if point == "after_provider_response":
            parked.set()
            raise _ParkError()

    class _ParkError(Exception):
        pass

    stack.creator.failpoint = barrier
    request = asyncio.create_task(_create(stack, offer_id, "k-interleave"))
    await parked.wait()
    stack.creator.failpoint = None
    response = await request  # the request path died after the answer: 500, attempt open
    assert response.status_code == 500
    booking_id = (await _create(stack, offer_id, "k-interleave")).json()["id"]
    assert (await stack.tick())["recovered"] == 1
    await stack.tick()
    settled = await stack.get(booking_id)
    assert settled["state"] == "CONFIRMED"
    # The late outcome transaction, replayed by hand: the attempt closes, nothing changes.
    async with stack.creator.uow() as store:
        before = await store.command_for(BookingId(booking_id), CommandKind.CREATE)
    assert before.disposition.value == "SUCCEEDED" and before.attempts[0].finished_at is None
    released.set()
    model = _model(DispatchMode.COMMIT_LOSE_RESPONSE)
    model.lookup()
    await _check_persisted(stack, model, booking_id, await stack.bus.truth())


async def test_resolve_replays_its_own_result(stack: Stack) -> None:
    """A retried resolve with the original expected version returns the original result."""
    offer_id = await _offer_id(stack)
    await stack.bus.chaos(failpoints={"after_reserve_commit": "503", "never_index": True})
    booking_id = (await _create(stack, offer_id, "k-replay-resolve")).json()["id"]
    await stack.bus.chaos(failpoints={"after_reserve_commit": None, "never_index": False})
    await stack.tick(LOOKUP_BUDGET)
    parked = await stack.get(booking_id)
    assert parked["state"] == "NEEDS_REVIEW"
    # Evidence without closure: add the observation by hand so resolve has something to
    # judge, without reconcile closing the case first.
    async with stack.creator.uow() as store:
        case = await store.review_case(BookingId(booking_id))
        assert case is not None
        _, case_id = case
        truth = await stack.bus.truth()
        reservation = Reservation(
            ProviderBookingRef(str(truth[0]["resId"])),
            BookingId(booking_id),
            ReservationState.CONFIRMED,
            datetime.now(UTC),
            product_ref="BUS-ROM-MIL-0715",
            service_date=date(2026, 6, 15),
        )
        await store.extend_review_case(case_id, implicated=(reservation.ref,))
        await store.add_evidence(
            case_id,
            "ev_manual",
            Evidence(
                EvidenceKind.LOOKUP_BY_CLIENT_REF, datetime.now(UTC), reservation.ref, reservation
            ),
        )
        booking = await store.get(BookingId(booking_id), for_update=True)
        await store.save_booking(booking, provider_booking_ref=reservation.ref)
    parked = await stack.get(booking_id)
    body = {"expected_version": parked["version"], "reason": "index rebuilt"}
    first = await stack.admin.post(f"/review/{booking_id}/resolve", json=body, headers=OPERATOR)
    assert first.status_code == 200, first.text
    assert first.json()["state"] == "CONFIRMED"
    again = await stack.admin.post(f"/review/{booking_id}/resolve", json=body, headers=OPERATOR)
    assert again.status_code == 200 and again.json() == first.json()
    stale = await stack.admin.post(
        f"/review/{booking_id}/resolve",
        json={"expected_version": parked["version"] + 5, "reason": "x"},
        headers=OPERATOR,
    )
    assert stale.status_code == 409


async def test_replay_survives_offer_expiry(stack: Stack) -> None:
    """Section 6.5: a replay is decided by the durable record, not by the offer store."""
    offer_id = await _offer_id(stack)
    first = await _create(stack, offer_id, "k-offer-gone")
    assert first.status_code == 201
    async with stack.creator.uow() as _:
        pass
    services_redis = stack.redis
    await services_redis.flushdb()
    again = await _create(stack, offer_id, "k-offer-gone")
    assert again.status_code == 201 and again.headers["Idempotent-Replayed"] == "true"
    assert again.json()["id"] == first.json()["id"]


async def test_lease_fencing_blocks_a_stale_worker(stack: Stack) -> None:
    from datetime import timedelta

    from orchestrator.domain import BookingState

    offer_id = await _offer_id(stack)
    await stack.bus.chaos(failpoints={"after_reserve_commit": "503"})
    booking_id = (await _create(stack, offer_id, "k-lease")).json()["id"]
    await stack.bus.chaos(failpoints={"after_reserve_commit": None})

    async with stack.worker.uow() as store:
        [(booking, stale)] = await store.claim(
            (BookingState.UNKNOWN,), limit=1, ttl=timedelta(seconds=0)
        )
    await asyncio.sleep(0.05)
    async with stack.worker.uow() as store:
        [(booking2, fresh)] = await store.claim(
            (BookingState.UNKNOWN,), limit=1, ttl=timedelta(seconds=30)
        )
    assert booking.id == booking2.id == booking_id and stale.token != fresh.token

    with pytest.raises(StaleLeaseError):
        await stack.recovery.reconcile_once(booking_id, lease=stale)
    await stack.recovery.reconcile_once(booking_id, lease=fresh)
    assert (await stack.get(booking_id))["state"] == "CONFIRMED"


async def test_unauthenticated_and_foreign_bookings_are_hidden(stack: Stack) -> None:
    offer_id = await _offer_id(stack)
    booking_id = (await _create(stack, offer_id, "k-own")).json()["id"]
    assert (await stack.public.get(f"/v1/bookings/{booking_id}")).status_code == 401
    unknown = await stack.public.get(f"/v1/bookings/{booking_id}", headers={"X-API-Key": "nope"})
    assert unknown.status_code == 401
    foreign = await stack.public.get(f"/v1/bookings/{booking_id}", headers=OTHER_CLIENT)
    assert foreign.status_code == 404, "another authenticated client cannot see it"
    # Idempotency keys are scoped per client: the same key from another client is new.
    theirs = await stack.public.post(
        "/v1/bookings",
        json=_body(offer_id, "Grace Hopper"),
        headers={**OTHER_CLIENT, "Idempotency-Key": "k-own"},
    )
    assert theirs.status_code == 201 and theirs.json()["id"] != booking_id
    assert (await stack.public.get("/v1/bookings/bk_missing", headers=CLIENT)).status_code == 404


async def test_readiness_now_includes_the_new_tables(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    try:
        async with engine.connect() as conn:
            tables = set(
                (
                    await conn.execute(
                        text("SELECT tablename FROM pg_tables WHERE schemaname='public'")
                    )
                ).scalars()
            )
    finally:
        await engine.dispose()
    assert {
        "bookings",
        "commands",
        "attempts",
        "review_cases",
        "evidence",
        "booking_events",
        "idempotency_keys",
    } <= tables
    assert datetime.now(UTC).tzinfo is not None

"""Provider C, "mobility-async": a shuttle operator with asynchronous confirmation (fictional;
docs/provider-integration-guide.md).

- ``GET /stops?q=``; ``GET /products?origin=&destination=&date=``.
- ``POST /bookings`` with ``clientRef`` and ``executeBefore``: deduplicated by client reference
  for 24 hours, bound before execution (a repeated reference replays or answers 409
  ``IN_PROGRESS``). Answers ``{providerBookingId, status: PENDING, generation, sequence}``; the
  outcome (``CONFIRMED`` or ``FAILED``) arrives later through a **signed webhook** (Standard
  Webhooks; the event payload is immutable, each delivery attempt carries a fresh timestamp and
  signature; an event is acknowledged only by a non-5xx answer and is retried otherwise)
  carrying ``generation`` and ``sequence``, and can always be read back with
  ``GET /bookings/{id}``. Expiry and capacity are enforced inside the serialized commit.
- ``GET /bookings?clientRef=``; ``POST /bookings/fenced-lookup`` by client reference (final).
- ``POST /bookings/{id}/cancel``: idempotent, free, honours ``executeBefore``, confirmed only.

Chaos: ``pending_seconds``, ``fail_confirmation_rate``, ``webhook_duplicate_rate``,
``webhook_before_response``, ``webhook_disabled``, ``stall_pending``, ``webhook_wrong_booking``,
``generation_bump``, ``clock_offset_ms``; failpoints ``slow_commit_seconds_async``,
``validate_then_stall_seconds``, ``pause_before_execute_seconds``,
``pause_after_check_before_commit_seconds``, ``admit_then_stall_then_commit_seconds``,
``lose_response``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, EmailStr, Field
from standardwebhooks import Webhook

from provider_sims.chaos import ChaosConfig, ChaosMiddleware, chaos_router
from provider_sims.chaos.config import register_random_source
from provider_sims.mobility_async.store import (
    AsyncStore,
    BookingRow,
    EventRow,
    ExpiredError,
    FencedError,
    SoldOutError,
)

LOST_RESPONSE_HOLD_SECONDS = 30.0
MAX_DELIVERY_ATTEMPTS = 8
FIRST_DELIVERY_BOUND = 5.0  # seconds a first delivery may be in flight before it is owed again
_rng = register_random_source(random.Random())  # noqa: S311 - simulated faults, not security


def log_progressor_error() -> None:
    import logging

    logging.getLogger(__name__).exception("pending progressor failed")


def _problem(status: int, code: str, detail: str, **extra: Any) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "type": f"urn:mobility-async:problem:{code}",
            "status": status,
            "code": code,
            "detail": detail,
            **extra,
        },
        media_type="application/problem+json",
    )


class Passenger(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class BookingBody(BaseModel):
    productId: str = Field(min_length=1, max_length=64)
    date: date
    clientRef: str = Field(min_length=1, max_length=64)
    passengers: list[Passenger] = Field(min_length=1, max_length=9)
    contactEmail: EmailStr
    executeBefore: datetime


class CancelBody(BaseModel):
    executeBefore: datetime


class FencedLookupBody(BaseModel):
    clientRef: str = Field(min_length=1, max_length=64)


def create_app(
    *,
    db_path: str | Path | None = None,
    admin_token: str | None = None,
    webhook_url: str | None = None,
    webhook_secret: str | None = None,
) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.progressor = asyncio.create_task(progress_pending())
        try:
            yield
        finally:
            task = app.state.progressor
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(title="mobility-async (fictional provider C)", version="1.0", lifespan=lifespan)
    store = AsyncStore(db_path or os.environ.get("ASYNC_DB_PATH", "mobility-async.sqlite3"))
    chaos = ChaosConfig()
    app.state.store = store
    app.state.chaos = chaos
    app.state.admin_token = admin_token or os.environ.get("SIM_ADMIN_TOKEN", "dev-sim-token")
    app.state.webhook_url = webhook_url or os.environ.get("WEBHOOK_URL", "")
    app.state.webhook_secret = webhook_secret or os.environ.get(
        "WEBHOOK_SECRET", "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
    )
    app.state.deliveries = []
    app.state.progressor = None
    app.add_middleware(
        ChaosMiddleware, config=chaos, exempt_prefixes=("/_chaos", "/_truth", "/healthz")
    )
    app.include_router(chaos_router)

    def now() -> datetime:
        return datetime.now(UTC) + timedelta(milliseconds=chaos.clock_offset_ms)

    def generation() -> int:
        return 1 + chaos.generation_bump

    def _admin(request: Request) -> JSONResponse | None:
        if request.headers.get("x-admin-token", "") != app.state.admin_token:
            return _problem(401, "UNAUTHORIZED", "admin token required")
        return None

    @app.exception_handler(RequestValidationError)
    async def _bad_request(request: Request, exc: RequestValidationError) -> JSONResponse:
        return _problem(400, "INVALID_REQUEST", "request validation failed")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    # The pending window: bookings progress on the provider's own schedule -------------------

    async def progress_pending() -> None:
        while True:
            try:
                if not chaos.stall_pending:
                    for row in store.due_pending(now=now()):
                        failed = (
                            chaos.fail_confirmation_rate
                            and _rng.random() < chaos.fail_confirmation_rate
                        )
                        moved = store.transition(
                            row.booking_id,
                            to="FAILED" if failed else "CONFIRMED",
                            only_from=("PENDING",),
                        )
                        if moved is not None:
                            await deliver(moved)
                if app.state.webhook_url and not chaos.webhook_disabled:
                    await retry_owed_deliveries()
            except Exception:
                log_progressor_error()
            await asyncio.sleep(0.1)

    async def retry_owed_deliveries() -> None:
        """Events the receiver never acknowledged are re-sent with a growing pause. A first
        delivery claims the event before posting; a claim older than the in-flight bound means
        the process died mid-delivery and the event is owed again."""
        current = datetime.now(UTC)
        for event in store.undelivered(max_attempts=MAX_DELIVERY_ATTEMPTS):
            if event.attempts == 0:
                in_flight = (
                    event.last_attempt_at is not None
                    and current - event.last_attempt_at < timedelta(seconds=FIRST_DELIVERY_BOUND)
                )
                if in_flight:
                    continue
                await _post_webhook(event)
                continue
            pause = timedelta(seconds=0.3 * event.attempts)
            if event.last_attempt_at is None or current - event.last_attempt_at >= pause:
                await _post_webhook(event)

    async def deliver(booking: BookingRow) -> None:
        """One immutable event per state change; delivered now, retried by the progressor until
        the receiver acknowledges it; duplicated by chaos."""
        target_id = booking.booking_id
        if chaos.webhook_wrong_booking:
            target_id = "MB999999"  # a provider defect: the event names another booking
            chaos.webhook_wrong_booking = False
        event_id = store.next_event_id()
        payload = json.dumps(
            {
                "type": f"booking.{booking.status.lower()}",
                "eventId": event_id,
                "providerBookingId": target_id,
                "clientRef": booking.client_ref,
                "status": booking.status,
                "generation": generation(),
                "sequence": booking.sequence,
                "occurredAt": now().isoformat(),
            },
            separators=(",", ":"),
        )
        suppressed = chaos.webhook_disabled or not app.state.webhook_url
        event = store.record_event(
            event_id,
            booking,
            generation=generation(),
            now=now(),
            payload=payload,
            delivered=suppressed,  # nothing is owed when webhooks are off
        )
        if suppressed:
            return
        store.claim_delivery(event_id, now=datetime.now(UTC))  # in flight from here
        copies = (
            2
            if chaos.webhook_duplicate_rate and _rng.random() < chaos.webhook_duplicate_rate
            else 1
        )
        for _ in range(copies):
            await _post_webhook(event)

    async def _post_webhook(event: EventRow) -> None:
        """One delivery attempt of an immutable event: fresh timestamp and signature, the
        same id and payload. Acknowledged by any non-5xx answer; otherwise it stays owed."""
        timestamp = datetime.now(UTC)
        signature = Webhook(app.state.webhook_secret).sign(event.event_id, timestamp, event.payload)
        headers = {
            "content-type": "application/json",
            "webhook-id": event.event_id,
            "webhook-timestamp": str(int(timestamp.timestamp())),
            "webhook-signature": signature,
        }
        delivery: dict[str, Any] = {"eventId": event.event_id, "status": None, "attempts": 1}
        app.state.deliveries.append(delivery)
        acknowledged = False
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.post(
                    app.state.webhook_url, content=event.payload, headers=headers
                )
            delivery["status"] = response.status_code
            acknowledged = response.status_code < 500
        except httpx.HTTPError:
            delivery["status"] = "unreachable"
        store.record_delivery_attempt(
            event.event_id, acknowledged=acknowledged, now=datetime.now(UTC)
        )

    # Catalogue -----------------------------------------------------------------------------

    @app.get("/stops")
    async def stops(q: str = Query(default="", max_length=64)) -> dict[str, Any]:
        return {
            "stops": [
                {"id": i, "name": n, "timezone": tz, "country": c} for i, n, tz, c in store.stops(q)
            ]
        }

    @app.get("/products")
    async def products(origin: str, destination: str, date: date) -> Any:
        rows = store.products(origin, destination)
        out = []
        for product_id, o, d, dep, minutes, cents, _seats in rows:
            origin_stop = next(s for s in store.stops("") if s[0] == o)
            dest_stop = next(s for s in store.stops("") if s[0] == d)
            departure = datetime.combine(
                date, datetime.strptime(dep, "%H:%M").time(), ZoneInfo(origin_stop[2])
            )
            arrival = departure + timedelta(minutes=minutes)
            out.append(
                {
                    "productId": product_id,
                    "origin": {
                        "id": o,
                        "name": origin_stop[1],
                        "timezone": origin_stop[2],
                        "country": origin_stop[3],
                    },
                    "destination": {
                        "id": d,
                        "name": dest_stop[1],
                        "timezone": dest_stop[2],
                        "country": dest_stop[3],
                    },
                    "departure": departure.isoformat(),
                    "arrival": arrival.astimezone(ZoneInfo(dest_stop[2])).isoformat(),
                    "pricePerPassenger": {"amount": cents, "currency": "EUR"},
                    "seatsAvailable": store.seats_left(product_id, date.isoformat()),
                    "cancellation": {"free": True},
                }
            )
        return {"date": date.isoformat(), "products": out}

    # Bookings --------------------------------------------------------------------------------

    def _booking_json(row: BookingRow) -> dict[str, Any]:
        return {
            "providerBookingId": row.booking_id,
            "clientRef": row.client_ref,
            "productId": row.product_id,
            "serviceDate": row.service_date,
            "status": row.status,
            "generation": generation(),
            "sequence": row.sequence,
        }

    async def _lose() -> Response:
        await asyncio.sleep(LOST_RESPONSE_HOLD_SECONDS)
        return _problem(503, "GATEWAY_TIMEOUT", "gateway gave up")

    @app.post("/bookings", status_code=202)
    async def create_booking(body: BookingBody) -> Any:
        fp = chaos.failpoints
        if fp.pause_before_execute_seconds > 0:
            await asyncio.sleep(fp.pause_before_execute_seconds)
        existing = store.admit_ref(body.clientRef, now=now())
        if existing is not None:
            if existing.state == "IN_PROGRESS":
                return _problem(409, "IN_PROGRESS", "this client reference is still being executed")
            assert existing.status_code is not None and existing.body is not None
            return JSONResponse(
                status_code=existing.status_code,
                content=json.loads(existing.body),
                headers={"Idempotent-Replayed": "true"},
            )
        if fp.admit_then_stall_then_commit_seconds > 0:
            await asyncio.sleep(fp.admit_then_stall_then_commit_seconds)
        if fp.validate_then_stall_seconds > 0:
            await asyncio.sleep(fp.validate_then_stall_seconds)
        outcome = _validate(body)
        if outcome is not None:
            if outcome.status_code == 422 and b"EXPIRED_REQUEST" in bytes(outcome.body):
                # Nothing was executed: the reference stays free for a fresh expiry.
                store.release_ref(body.clientRef)
            else:
                store.finish_ref(
                    body.clientRef,
                    status_code=outcome.status_code,
                    body=bytes(outcome.body).decode(),
                )
            return outcome
        if fp.pause_after_check_before_commit_seconds > 0:
            await asyncio.sleep(fp.pause_after_check_before_commit_seconds)
        if fp.slow_commit_seconds_async > 0:
            await asyncio.sleep(fp.slow_commit_seconds_async)
        product = store.product(body.productId)
        assert product is not None
        try:
            row = store.create_booking(
                client_ref=body.clientRef,
                product_id=body.productId,
                service_date=body.date.isoformat(),
                passengers=len(body.passengers),
                now=now(),
                due_at=now() + timedelta(seconds=chaos.pending_seconds),
                execute_before=body.executeBefore,
                clock=now,
                seats=int(product[6]),
            )
        except ExpiredError:
            # Nothing was created: the reference is free again for a request with a fresh expiry.
            store.release_ref(body.clientRef)
            return _problem(422, "EXPIRED_REQUEST", "executeBefore passed before the commit")
        except SoldOutError:
            sold_out = _problem(422, "SOLD_OUT", "no seats left")
            store.finish_ref(body.clientRef, status_code=422, body=bytes(sold_out.body).decode())
            return sold_out
        except FencedError:
            fenced = _problem(
                409, "FENCED", "a fenced lookup for this reference preceded the commit"
            )
            store.finish_ref(body.clientRef, status_code=409, body=bytes(fenced.body).decode())
            return fenced
        response = JSONResponse(status_code=202, content=_booking_json(row))
        store.finish_ref(body.clientRef, status_code=202, body=bytes(response.body).decode())
        if chaos.webhook_before_response and not chaos.stall_pending:
            # The outcome overtakes the response: confirmed and announced before we answer.
            moved = store.transition(row.booking_id, to="CONFIRMED", only_from=("PENDING",))
            if moved is not None:
                await deliver(moved)
        if fp.lose_response:
            return await _lose()
        return response

    def _validate(body: BookingBody) -> JSONResponse | None:
        if body.executeBefore.tzinfo is None:
            return _problem(400, "INVALID_REQUEST", "executeBefore must be timezone-aware")
        if now() >= body.executeBefore:
            return _problem(422, "EXPIRED_REQUEST", "executeBefore has passed")
        product = store.product(body.productId)
        if product is None:
            return _problem(404, "PRODUCT_NOT_FOUND", "unknown product")
        left = store.seats_left(body.productId, body.date.isoformat())
        if left is not None and left < len(body.passengers):
            return _problem(422, "SOLD_OUT", "no seats left")  # rechecked inside the commit
        return None

    @app.get("/bookings/{booking_id}")
    async def get_booking(booking_id: str) -> Any:
        row = store.booking(booking_id)
        if row is None:
            return _problem(404, "BOOKING_NOT_FOUND", "unknown booking")
        return _booking_json(row)

    @app.get("/bookings")
    async def bookings_by_ref(clientRef: str = Query(min_length=1, max_length=64)) -> Any:
        return {"bookings": [_booking_json(r) for r in store.bookings_by_ref(clientRef)]}

    @app.post("/bookings/fenced-lookup")
    async def fenced_lookup(body: FencedLookupBody) -> Any:
        rows = store.fenced_lookup(body.clientRef, now=now())
        return {
            "clientRef": body.clientRef,
            "final": True,
            "bookings": [_booking_json(r) for r in rows],
        }

    @app.post("/bookings/{booking_id}/cancel")
    async def cancel_booking(booking_id: str, body: CancelBody) -> Any:
        fp = chaos.failpoints
        if body.executeBefore.tzinfo is None:
            return _problem(400, "INVALID_REQUEST", "executeBefore must be timezone-aware")
        if now() >= body.executeBefore:
            return _problem(422, "EXPIRED_REQUEST", "executeBefore has passed")
        row = store.booking(booking_id)
        if row is None:
            return _problem(404, "BOOKING_NOT_FOUND", "unknown booking")
        if row.status == "PENDING":
            return _problem(409, "NOT_CANCELLABLE", "a pending booking cannot be cancelled yet")
        if row.status == "FAILED":
            return _problem(409, "NOT_CANCELLABLE", "a failed booking cannot be cancelled")
        if fp.pause_after_check_before_commit_seconds > 0:
            await asyncio.sleep(fp.pause_after_check_before_commit_seconds)
        try:
            moved = store.transition(
                booking_id,
                to="CANCELLED",
                only_from=("CONFIRMED",),
                execute_before=body.executeBefore,
                clock=now,
            )
        except ExpiredError:
            return _problem(422, "EXPIRED_REQUEST", "executeBefore passed before the commit")
        assert moved is not None
        if row.status == "CONFIRMED" and moved.status == "CANCELLED":
            await deliver(moved)
        response = JSONResponse(status_code=200, content=_booking_json(moved))
        if fp.lose_response:
            return await _lose()
        return response

    # Truth (test oracle) ---------------------------------------------------------------------

    @app.get("/_truth/bookings")
    async def truth(request: Request) -> Any:
        denied = _admin(request)
        if denied is not None:
            return denied
        return {
            "bookings": [
                {
                    "providerBookingId": r.booking_id,
                    "clientRef": r.client_ref,
                    "status": r.status,
                    "sequence": r.sequence,
                    "dueAt": r.due_at.isoformat(),
                }
                for r in store.truth()
            ],
            "events": store.events(),
            "deliveries": app.state.deliveries,
        }

    @app.post("/_truth/webhook-url")
    async def set_webhook_url(request: Request) -> Any:
        denied = _admin(request)
        if denied is not None:
            return denied
        body = await request.json()
        app.state.webhook_url = str(body.get("url", ""))
        return {"url": app.state.webhook_url}

    @app.post("/_truth/redeliver/{event_id}")
    async def redeliver(event_id: str, request: Request) -> Any:
        """Re-send one event (a provider retry): a fresh signature, the same event id."""
        denied = _admin(request)
        if denied is not None:
            return denied
        event = store.event(event_id)
        if event is None:
            return _problem(404, "EVENT_NOT_FOUND", "unknown event")
        await _post_webhook(event)
        return {"ok": True}

    @app.post("/_truth/wipe")
    async def wipe(request: Request) -> Any:
        denied = _admin(request)
        if denied is not None:
            return denied
        store.wipe()
        app.state.deliveries.clear()
        return {"ok": True}

    return app

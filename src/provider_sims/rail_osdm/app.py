"""Provider A, "rail-osdm": an OSDM-flavoured rail API
(fictional; docs/provider-integration-guide.md).

- ``GET /places?name=``; ``POST /offers`` (offers carry ``validUntil`` and cancellation
  conditions); ``POST /bookings`` with ``Idempotency-Key``, ``externalRef`` and
  ``executeBefore`` creates a ``PREBOOKED`` hold with ``confirmationTimeLimit``.
- **The key is bound before execution**: a repeated key replays the original outcome, or
  answers 409 ``IN_PROGRESS`` while the first execution is still running. Records survive a
  generation bump.
- ``PATCH /bookings/{id}`` confirms (idempotent, honours ``executeBefore``); ``GET /bookings/{id}``
  reports ``status``, ``generation``, ``version`` and refund offers by id;
  ``GET /bookings?externalRef=`` returns every match; ``POST /bookings/fenced-lookup`` fences a
  key and answers finally; ``POST /bookings/{id}/refund-offers`` quotes (one offer per quote,
  ``validUntil``); ``PATCH .../refund-offers/{id}`` accepts (idempotent, ``executeBefore``).
- Problems are ``application/problem+json`` with a stable ``code``. ``executeBefore`` is
  checked against the provider's clock (``clock_offset_ms``) right before execution.

Chaos: ``hold_expiry_seconds``, ``confirm_delay_ms``, ``generation_bump``, ``clock_offset_ms``;
failpoints ``after_prebook_commit``, ``after_confirm_commit``, ``after_refund_accept_commit``,
``slow_refund_accept_commit_seconds``, ``pause_before_execute_seconds``,
``pause_after_check_before_commit_seconds``, ``admit_then_stall_then_commit_seconds``,
``lose_response``.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, EmailStr, Field

from provider_sims.chaos import ChaosConfig, ChaosMiddleware, chaos_router
from provider_sims.rail_osdm.store import ExpiredError, FencedError, RailStore

IDEMPOTENCY_WINDOW = timedelta(hours=24)
OFFER_VALIDITY = timedelta(minutes=20)
REFUND_OFFER_VALIDITY = timedelta(minutes=2)
LOST_RESPONSE_HOLD_SECONDS = 30.0


def _problem(status: int, code: str, detail: str, **extra: Any) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "type": f"urn:rail-osdm:problem:{code}",
            "status": status,
            "code": code,
            "detail": detail,
            **extra,
        },
        media_type="application/problem+json",
    )


class Passenger(BaseModel):
    firstName: str = Field(min_length=1, max_length=60)
    lastName: str = Field(min_length=1, max_length=60)


class OffersBody(BaseModel):
    origin: str
    destination: str
    date: date
    adults: int = Field(default=1, ge=1, le=9)
    children: int = Field(default=0, ge=0, le=9)


class BookingBody(BaseModel):
    offerId: str = Field(min_length=1, max_length=40)
    externalRef: str = Field(min_length=1, max_length=64)
    passengers: list[Passenger] = Field(min_length=1, max_length=9)
    contactEmail: EmailStr
    executeBefore: datetime


class ConfirmBody(BaseModel):
    status: str = Field(pattern="^CONFIRMED$")
    executeBefore: datetime


class FencedLookupBody(BaseModel):
    idempotencyKey: str = Field(min_length=1, max_length=128)


class AcceptRefundBody(BaseModel):
    status: str = Field(pattern="^CONFIRMED$")
    executeBefore: datetime


def create_app(*, db_path: str | Path | None = None, admin_token: str | None = None) -> FastAPI:
    app = FastAPI(title="rail-osdm (fictional provider A)", version="1.0")
    store = RailStore(db_path or os.environ.get("RAIL_DB_PATH", "rail-osdm.sqlite3"))
    chaos = ChaosConfig()
    app.state.store = store
    app.state.chaos = chaos
    app.state.admin_token = admin_token or os.environ.get("SIM_ADMIN_TOKEN", "dev-sim-token")
    app.add_middleware(
        ChaosMiddleware, config=chaos, exempt_prefixes=("/_chaos", "/_truth", "/healthz")
    )
    app.include_router(chaos_router)

    def now() -> datetime:
        """The provider's clock: ours, skewed by the configured offset."""
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

    # Catalogue -----------------------------------------------------------------------------

    @app.get("/places")
    async def places(name: str = Query(default="", max_length=64)) -> dict[str, Any]:
        return {
            "places": [
                {"id": i, "name": n, "timezone": tz, "country": c}
                for i, n, tz, c in store.places(name)
            ]
        }

    @app.post("/offers")
    async def offers(body: OffersBody) -> Any:
        origin, destination = store.place(body.origin), store.place(body.destination)
        if origin is None or destination is None:
            return _problem(404, "PLACE_NOT_FOUND", "unknown place")
        valid_until = now() + OFFER_VALIDITY
        out = []
        for trip_id, o, d, dep, arr, base_cents, _seats in store.trips(
            body.origin, body.destination
        ):
            price = base_cents * body.adults + (base_cents // 2) * body.children
            refundable = "TGV" not in trip_id  # one non-refundable fare family in the set
            offer_id = store.create_offer(
                trip_id=trip_id,
                service_date=body.date.isoformat(),
                adults=body.adults,
                children=body.children,
                price_cents=price,
                refundable=refundable,
                fee_percent=20 if refundable else 100,
                valid_until=valid_until,
            )
            tz = ZoneInfo(origin[2])
            departure = datetime.combine(body.date, datetime.strptime(dep, "%H:%M").time(), tz)
            arrival = datetime.combine(
                body.date, datetime.strptime(arr, "%H:%M").time(), ZoneInfo(destination[2])
            )
            out.append(
                {
                    "offerId": offer_id,
                    "validUntil": valid_until.isoformat(),
                    "price": {"amount": price, "currency": "CHF"},
                    "cancellationConditions": {
                        "refundable": refundable,
                        "feePercent": 20 if refundable else 100,
                    },
                    "trip": {
                        "id": trip_id,
                        "legs": [
                            {
                                "origin": {"id": o, "name": origin[1], "timezone": origin[2]},
                                "destination": {
                                    "id": d,
                                    "name": destination[1],
                                    "timezone": destination[2],
                                },
                                "departure": departure.isoformat(),
                                "arrival": arrival.isoformat(),
                                "vehicle": trip_id,
                            }
                        ],
                    },
                    "seatsAvailable": store.seats_left(trip_id, body.date.isoformat()),
                }
            )
        return {"offers": out}

    # Bookings --------------------------------------------------------------------------------

    def _booking_json(row: Any) -> dict[str, Any]:
        return {
            "bookingId": row.booking_id,
            "externalRef": row.external_ref,
            "offerId": row.offer_id,
            "tripId": row.trip_id,
            "serviceDate": row.service_date,
            "status": row.status,
            "confirmationTimeLimit": row.confirmation_time_limit.isoformat(),
            "generation": generation(),
            "version": row.version,
            "refundOffers": [
                {
                    "id": r.refund_offer_id,
                    "status": r.status,
                    "validUntil": r.valid_until.isoformat(),
                }
                for r in store.refund_offers_for(row.booking_id, now=now())
            ],
        }

    async def _lose(response: JSONResponse) -> Response:
        """The work is done; the answer never arrives (the socket is held, then dropped)."""
        await asyncio.sleep(LOST_RESPONSE_HOLD_SECONDS)
        return _problem(503, "GATEWAY_TIMEOUT", "gateway gave up")

    @app.post("/bookings", status_code=201)
    async def create_booking(
        body: BookingBody,
        idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=128),
    ) -> Any:
        fp = chaos.failpoints
        if fp.pause_before_execute_seconds > 0:
            # The request sat in the provider's queue: the expiry check happens after.
            await asyncio.sleep(fp.pause_before_execute_seconds)
        # 1. The key is bound before anything executes.
        existing = store.admit_key(idempotency_key, now=now())
        if existing is not None:
            if existing.state == "IN_PROGRESS":
                return _problem(409, "IN_PROGRESS", "this key is still being executed")
            assert existing.status_code is not None and existing.body is not None
            return JSONResponse(
                status_code=existing.status_code,
                content=json.loads(existing.body),
                headers={"Idempotent-Replayed": "true"},
            )
        if fp.admit_then_stall_then_commit_seconds > 0:
            await asyncio.sleep(fp.admit_then_stall_then_commit_seconds)
        # 2. Validation and the expiry check, on the provider's clock.
        outcome = _validate_create(body, idempotency_key)
        if outcome is not None:
            if outcome.status_code == 422 and b"EXPIRED_REQUEST" in bytes(outcome.body):
                # Attempt-specific: a later request with a fresh expiry may execute.
                store.release_key(idempotency_key)
            else:
                store.finish_key(
                    idempotency_key,
                    status_code=outcome.status_code,
                    body=bytes(outcome.body).decode(),
                )
            return outcome
        if fp.pause_after_check_before_commit_seconds > 0:
            await asyncio.sleep(fp.pause_after_check_before_commit_seconds)
        offer = store.offer(body.offerId)
        assert offer is not None
        # 3. The commit, which checks the fence inside its own write transaction.
        try:
            row = store.create_booking(
                key=idempotency_key,
                external_ref=body.externalRef,
                offer_id=body.offerId,
                trip_id=str(offer["trip_id"]),
                service_date=str(offer["service_date"]),
                passengers=len(body.passengers),
                confirmation_time_limit=now() + timedelta(seconds=chaos.hold_expiry_seconds),
                now=now(),
                execute_before=body.executeBefore,
                clock=now,
            )
        except ExpiredError:
            store.release_key(idempotency_key)
            return _problem(422, "EXPIRED_REQUEST", "executeBefore passed before the commit")
        except FencedError:
            fenced = _problem(409, "FENCED", "a fenced lookup for this key preceded the commit")
            store.finish_key(idempotency_key, status_code=409, body=bytes(fenced.body).decode())
            return fenced
        response = JSONResponse(status_code=201, content=_booking_json(row))
        store.finish_key(idempotency_key, status_code=201, body=bytes(response.body).decode())
        if fp.after_prebook_commit == "drop" or fp.lose_response:
            return await _lose(response)
        if fp.after_prebook_commit == "503":
            return _problem(503, "INTERNAL", "backend error after commit")
        return response

    def _validate_create(body: BookingBody, key: str) -> JSONResponse | None:
        if body.executeBefore.tzinfo is None:
            return _problem(400, "INVALID_REQUEST", "executeBefore must be timezone-aware")
        if now() >= body.executeBefore:
            return _problem(
                422, "EXPIRED_REQUEST", "executeBefore has passed", executedAt=now().isoformat()
            )
        offer = store.offer(body.offerId)
        if offer is None:
            return _problem(404, "OFFER_NOT_FOUND", "unknown offer")
        if now() >= offer["valid_until"]:  # type: ignore[operator]
            return _problem(422, "OFFER_EXPIRED", "the offer is no longer valid")
        if len(body.passengers) != int(offer["adults"]) + int(offer["children"]):  # type: ignore[call-overload]
            return _problem(422, "PASSENGER_MISMATCH", "passenger count differs from the offer")
        left = store.seats_left(str(offer["trip_id"]), str(offer["service_date"]))
        if left is not None and left < len(body.passengers):
            return _problem(422, "SOLD_OUT", "no seats left")
        return None

    @app.get("/bookings/{booking_id}")
    async def get_booking(booking_id: str) -> Any:
        row = store.booking(booking_id, now=now())
        if row is None:
            return _problem(404, "BOOKING_NOT_FOUND", "unknown booking")
        return _booking_json(row)

    @app.get("/bookings")
    async def bookings_by_ref(externalRef: str = Query(min_length=1, max_length=64)) -> Any:
        rows = store.bookings_by_external_ref(externalRef, now=now())
        return {"bookings": [_booking_json(r) for r in rows]}

    @app.patch("/bookings/{booking_id}")
    async def confirm_booking(booking_id: str, body: ConfirmBody) -> Any:
        fp = chaos.failpoints
        if fp.pause_before_execute_seconds > 0:
            await asyncio.sleep(fp.pause_before_execute_seconds)
        if body.executeBefore.tzinfo is None:
            return _problem(400, "INVALID_REQUEST", "executeBefore must be timezone-aware")
        if now() >= body.executeBefore:
            return _problem(422, "EXPIRED_REQUEST", "executeBefore has passed")
        row = store.booking(booking_id, now=now())
        if row is None:
            return _problem(404, "BOOKING_NOT_FOUND", "unknown booking")
        if row.status == "EXPIRED":
            return _problem(410, "HOLD_EXPIRED", "the hold expired before confirmation")
        if row.status == "CANCELLED":
            return _problem(409, "BOOKING_CANCELLED", "the booking is cancelled")
        if chaos.confirm_delay_ms:
            await asyncio.sleep(chaos.confirm_delay_ms / 1000)
        if fp.pause_after_check_before_commit_seconds > 0:
            await asyncio.sleep(fp.pause_after_check_before_commit_seconds)
        try:
            confirmed = store.confirm(
                booking_id, now=now(), execute_before=body.executeBefore, clock=now
            )
        except ExpiredError:
            return _problem(422, "EXPIRED_REQUEST", "executeBefore passed before the commit")
        assert confirmed is not None
        if confirmed.status == "EXPIRED":
            return _problem(410, "HOLD_EXPIRED", "the hold expired before confirmation")
        response = JSONResponse(status_code=200, content=_booking_json(confirmed))
        if fp.after_confirm_commit == "drop" or fp.lose_response:
            return await _lose(response)
        if fp.after_confirm_commit == "503":
            return _problem(503, "INTERNAL", "backend error after commit")
        return response

    @app.post("/bookings/fenced-lookup")
    async def fenced_lookup(body: FencedLookupBody) -> Any:
        rows = store.fenced_lookup(body.idempotencyKey, now=now())
        return {
            "idempotencyKey": body.idempotencyKey,
            "final": True,
            "bookings": [_booking_json(r) for r in rows],
        }

    # Refund offers ---------------------------------------------------------------------------

    @app.post("/bookings/{booking_id}/refund-offers", status_code=201)
    async def quote_refund(booking_id: str) -> Any:
        row = store.booking(booking_id, now=now())
        if row is None:
            return _problem(404, "BOOKING_NOT_FOUND", "unknown booking")
        if row.status != "CONFIRMED":
            return _problem(409, "NOT_CANCELLABLE", f"a {row.status} booking cannot be cancelled")
        offer = store.offer(row.offer_id)
        assert offer is not None
        price = int(offer["price_cents"])  # type: ignore[call-overload]
        fee = price * int(offer["fee_percent"]) // 100  # type: ignore[call-overload]
        quote = store.create_refund_offer(
            booking_id,
            refund_cents=price - fee,
            fee_cents=fee,
            valid_until=now() + REFUND_OFFER_VALIDITY,
        )
        return {
            "refundOfferId": quote.refund_offer_id,
            "bookingId": booking_id,
            "status": quote.status,
            "validUntil": quote.valid_until.isoformat(),
            "refund": {"amount": quote.refund_cents, "currency": "CHF"},
            "fee": {"amount": quote.fee_cents, "currency": "CHF"},
        }

    @app.patch("/bookings/{booking_id}/refund-offers/{refund_id}")
    async def accept_refund(booking_id: str, refund_id: str, body: AcceptRefundBody) -> Any:
        fp = chaos.failpoints
        if body.executeBefore.tzinfo is None:
            return _problem(400, "INVALID_REQUEST", "executeBefore must be timezone-aware")
        if now() >= body.executeBefore:
            return _problem(422, "EXPIRED_REQUEST", "executeBefore has passed")
        offer = store.refund_offer(refund_id, now=now())
        if offer is None or offer.booking_id != booking_id:
            return _problem(404, "REFUND_OFFER_NOT_FOUND", "unknown refund offer")
        if offer.status == "EXPIRED":
            return _problem(410, "REFUND_OFFER_EXPIRED", "the refund offer expired")
        if offer.status == "REJECTED":
            return _problem(409, "REFUND_OFFER_REJECTED", "another offer was accepted")
        if fp.slow_refund_accept_commit_seconds > 0:
            await asyncio.sleep(fp.slow_refund_accept_commit_seconds)
        try:
            accepted = store.accept_refund_offer(
                refund_id, now=now(), execute_before=body.executeBefore, clock=now
            )
        except ExpiredError:
            return _problem(422, "EXPIRED_REQUEST", "executeBefore passed before the commit")
        assert accepted is not None
        if accepted.status == "EXPIRED":
            return _problem(410, "REFUND_OFFER_EXPIRED", "the refund offer expired")
        booking = store.booking(booking_id, now=now())
        assert booking is not None
        response = JSONResponse(
            status_code=200,
            content={
                "refundOfferId": accepted.refund_offer_id,
                "status": accepted.status,
                "booking": _booking_json(booking),
            },
        )
        if fp.after_refund_accept_commit == "drop" or fp.lose_response:
            return await _lose(response)
        if fp.after_refund_accept_commit == "503":
            return _problem(503, "INTERNAL", "backend error after commit")
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
                    "bookingId": r.booking_id,
                    "externalRef": r.external_ref,
                    "status": r.status,
                    "key": r.key,
                    "version": r.version,
                    "confirmationTimeLimit": r.confirmation_time_limit.isoformat(),
                    "fenced": store.is_fenced(r.key),
                }
                for r in store.truth()
            ]
        }

    @app.post("/_truth/wipe")
    async def wipe(request: Request) -> Any:
        denied = _admin(request)
        if denied is not None:
            return denied
        store.wipe()
        return {"ok": True}

    return app

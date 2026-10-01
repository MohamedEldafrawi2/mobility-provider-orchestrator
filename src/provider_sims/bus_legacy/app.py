"""Provider B, "bus-legacy": a proprietary REST API with the worst characteristics in the set.

- Naive local times (``HH:MM``) with a day offset for overnight arrivals, prices as strings of
  cents, numeric error codes in a ``{"err": n, "msg": "..."}`` body (also for malformed input:
  err 40).
- ``POST /api/v1/reserve`` confirms immediately and has no idempotency: the same ``yourRef``
  sent twice creates two reservations. Inventory is per journey per service date.
- ``GET /api/v1/reservations?yourRef=`` is served from an index that lags writes and can be
  configured never to expose a reservation.
- No cancellation. No execution expiry (an ``executeBefore`` field is ignored). No processing
  bound. 429 without ``Retry-After``. Edge responses (rate limit, unavailability, random
  failure) carry ``X-Bus-Edge: 1``; a 503 without it came from behind the handler.

Failpoints (``/_chaos``): ``drop_request``, ``after_reserve_commit`` (``drop`` or ``503``),
``slow_commit_seconds``, ``never_index``.
"""

from __future__ import annotations

import asyncio
import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from provider_sims.bus_legacy.store import BusStore
from provider_sims.chaos import ChaosConfig, ChaosMiddleware, chaos_router

_DATE = re.compile(r"\d{2}-\d{2}-\d{4}")


def _canonical_date(value: str) -> str | None:
    """Strictly parse DD-MM-YYYY and return it in the one form inventory is keyed by."""
    if not _DATE.fullmatch(value):
        return None
    try:
        return datetime.strptime(value, "%d-%m-%Y").strftime("%d-%m-%Y")
    except ValueError:
        return None


ERR_BAD_DATE = 12
ERR_UNKNOWN_JOURNEY = 17
ERR_SOLD_OUT = 21
ERR_BAD_REQUEST = 40


def _err(status: int, code: int, msg: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"err": code, "msg": msg})


class ReserveBody(BaseModel):
    jid: str
    date: str
    yourRef: str = Field(min_length=1, max_length=64)
    pax: int = Field(ge=1, le=9)
    name: str = Field(min_length=1, max_length=120)
    executeBefore: str | None = None  # accepted and ignored: this provider enforces no expiry


def create_app(*, db_path: str | Path | None = None, admin_token: str | None = None) -> FastAPI:
    app = FastAPI(title="bus-legacy (fictional provider B)", version="1.0")
    store = BusStore(db_path or os.environ.get("BUS_DB_PATH", "bus-legacy.sqlite3"))
    chaos = ChaosConfig()
    app.state.store = store
    app.state.chaos = chaos
    app.state.admin_token = admin_token or os.environ.get("SIM_ADMIN_TOKEN", "dev-sim-token")
    app.add_middleware(
        ChaosMiddleware, config=chaos, exempt_prefixes=("/_chaos", "/_truth", "/healthz")
    )
    app.include_router(chaos_router)

    @app.exception_handler(RequestValidationError)
    async def _bad_request(request: Request, exc: RequestValidationError) -> JSONResponse:
        # The legacy contract: malformed input is a definitive 400 with code 40, never a 422.
        return _err(400, ERR_BAD_REQUEST, "bad request")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/stops")
    async def stops(q: str = Query(default="", max_length=64)) -> dict[str, Any]:
        return {
            "stops": [
                {"id": i, "name": n, "tz": tz, "country": c} for i, n, tz, c in store.stops(q)
            ]
        }

    @app.get("/api/v1/journeys")
    async def journeys(src: int, dst: int, date: str) -> Any:
        canonical = _canonical_date(date)
        if canonical is None:
            return _err(400, ERR_BAD_DATE, "date must be a real DD-MM-YYYY date")
        date = canonical
        rows = store.journeys(src, dst)
        return {
            "date": date,
            "journeys": [
                {
                    "jid": jid,
                    "src": s,
                    "dst": d,
                    "dep": dep,
                    "arr": arr,
                    "arrDayOffset": off,
                    "priceCents": str(price),
                    "cur": "EUR",
                    "seats": store.seats_left(jid, date),
                }
                for jid, s, d, dep, arr, off, price, _ in rows
            ],
        }

    @app.post("/api/v1/reserve")
    async def reserve(body: ReserveBody) -> Any:
        fp = chaos.failpoints
        if fp.drop_request:
            # The request never reaches the provider. Nothing happens; the caller times out.
            await asyncio.sleep(fp.hold_seconds)
            return _err(503, ERR_BAD_REQUEST, "gateway gave up")

        service_date = _canonical_date(body.date)
        if service_date is None:
            return _err(400, ERR_BAD_DATE, "date must be a real DD-MM-YYYY date")
        if not store.journey_exists(body.jid):
            return _err(400, ERR_UNKNOWN_JOURNEY, "unknown journey")
        left = store.seats_left(body.jid, service_date)
        if left is not None and left < body.pax:
            return _err(400, ERR_SOLD_OUT, "sold out")

        if fp.slow_commit_seconds > 0:
            await asyncio.sleep(fp.slow_commit_seconds)

        row = store.reserve(
            body.yourRef,
            body.jid,
            service_date,
            body.pax,
            lag=timedelta(seconds=chaos.lookup_lag_seconds),
            never_index=fp.never_index,
        )
        if row is None:
            return _err(400, ERR_SOLD_OUT, "sold out")

        if fp.after_reserve_commit == "drop":
            await asyncio.sleep(fp.hold_seconds)
            return _err(503, ERR_BAD_REQUEST, "gateway gave up")
        if fp.after_reserve_commit == "503":
            return _err(503, ERR_BAD_REQUEST, "backend error")

        return {
            "resId": row.res_id,
            "state": "OK",
            "jid": row.jid,
            "date": row.service_date,
            "yourRef": row.your_ref,
        }

    @app.get("/api/v1/reservations")
    async def reservations(yourRef: str = Query(min_length=1, max_length=64)) -> dict[str, Any]:
        rows = store.by_your_ref(yourRef, now=datetime.now(UTC))
        return {
            "reservations": [
                {
                    "resId": r.res_id,
                    "yourRef": r.your_ref,
                    "state": "OK",
                    "jid": r.jid,
                    "date": r.service_date,
                    "pax": r.pax,
                }
                for r in rows
            ]
        }

    # Test-only oracle access: the truth regardless of index visibility. Admin token required.
    @app.get("/_truth/reservations")
    async def truth(request: Request) -> Any:
        if request.headers.get("x-admin-token", "") != app.state.admin_token:
            return _err(401, ERR_BAD_REQUEST, "admin token required")
        return {
            "reservations": [
                {
                    "resId": r.res_id,
                    "yourRef": r.your_ref,
                    "jid": r.jid,
                    "date": r.service_date,
                    "pax": r.pax,
                    "committedAt": r.committed_at.isoformat(),
                    "visibleAfter": r.visible_after.isoformat() if r.visible_after else None,
                }
                for r in store.truth()
            ]
        }

    @app.post("/_truth/rebuild-index")
    async def rebuild_index(request: Request) -> Any:
        """The provider's operations team rebuilt the index: everything is visible now."""
        if request.headers.get("x-admin-token", "") != app.state.admin_token:
            return _err(401, ERR_BAD_REQUEST, "admin token required")
        return {"exposed": store.rebuild_index(now=datetime.now(UTC))}

    @app.post("/_truth/wipe")
    async def wipe(request: Request) -> Any:
        if request.headers.get("x-admin-token", "") != app.state.admin_token:
            return _err(401, ERR_BAD_REQUEST, "admin token required")
        store.wipe_reservations()
        return {"ok": True}

    return app

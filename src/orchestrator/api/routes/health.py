"""Liveness and readiness.

Readiness reports each dependency separately. PostgreSQL is the system of record, so it is
required. Redis only holds offers and rate-limit buckets; when it is down the API stays up and
admission fails closed, so readiness reports it as degraded rather than failing the pod.
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

router = APIRouter(tags=["health"])

DependencyStatus = Literal["ok", "degraded", "down"]


async def _check_postgres(request: Request, budget_seconds: float) -> DependencyStatus:
    try:
        async with asyncio.timeout(budget_seconds):
            async with request.app.state.db_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        return "ok"
    except Exception:
        return "down"


async def _check_redis(request: Request, budget_seconds: float) -> DependencyStatus:
    try:
        async with asyncio.timeout(budget_seconds):
            await request.app.state.redis.ping()
        return "ok"
    except Exception:
        return "degraded"


@router.get("/healthz", summary="Liveness")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz", summary="Readiness", responses={503: {"description": "Not ready"}})
async def readyz(request: Request) -> JSONResponse:
    budget: float = request.app.state.settings.readiness_timeout_seconds
    postgres, redis = await asyncio.gather(
        _check_postgres(request, budget), _check_redis(request, budget)
    )
    body: dict[str, Any] = {
        "status": "ok" if postgres == "ok" else "not-ready",
        "postgres": postgres,
        "redis": redis,
    }
    return JSONResponse(status_code=200 if postgres == "ok" else 503, content=body)

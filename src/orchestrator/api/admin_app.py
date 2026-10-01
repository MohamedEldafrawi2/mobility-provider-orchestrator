"""The admin listener: operator-only routes, served on a port that is never published.

This is a separate FastAPI application with its own route registry, not a prefix on the public
application, so no misconfiguration of the public listener can expose review operations.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, FastAPI, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from orchestrator.api.app_factory import build_app
from orchestrator.api.auth import CurrentOperator
from orchestrator.api.problems import Problem
from orchestrator.api.routes.providers import operational_detail
from orchestrator.application.review import ReviewError
from orchestrator.application.wiring import Services
from orchestrator.config import Settings, get_settings
from orchestrator.domain import BookingId
from orchestrator.telemetry import prometheus_exposition

review = APIRouter(prefix="/review", tags=["review"])


def _services(request: Request) -> Services:
    return request.app.state.services  # type: ignore[no-any-return]


def _translate(exc: ReviewError) -> Problem:
    return Problem(exc.status, exc.code, exc.code.replace("-", " ").capitalize(), exc.detail)


@review.get("", summary="List open review cases")
async def list_review_cases(request: Request, operator: CurrentOperator) -> dict[str, Any]:
    return {"cases": await _services(request).review.list_open()}


@review.get("/{booking_id}", summary="Review case detail")
async def case_detail(
    booking_id: str, request: Request, operator: CurrentOperator
) -> dict[str, Any]:
    try:
        return await _services(request).review.detail(BookingId(booking_id))
    except ReviewError as exc:
        raise _translate(exc) from exc


@review.post("/{booking_id}/reconcile", summary="Run authoritative lookups now")
async def reconcile(booking_id: str, request: Request, operator: CurrentOperator) -> dict[str, Any]:
    try:
        return await _services(request).review.reconcile(BookingId(booking_id), actor=operator)
    except ReviewError as exc:
        raise _translate(exc) from exc


class ResolveBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=0)
    reason: str = Field(min_length=1, max_length=500)


@review.post("/{booking_id}/resolve", summary="Close the case into the state the evidence supports")
async def resolve(
    booking_id: str, body: ResolveBody, request: Request, operator: CurrentOperator
) -> dict[str, Any]:
    try:
        result = await _services(request).review.resolve(
            BookingId(booking_id),
            expected_version=body.expected_version,
            actor=operator,
            reason=body.reason,
        )
    except ReviewError as exc:
        raise _translate(exc) from exc
    return {"booking_id": result.booking_id, "state": result.state.value, "version": result.version}


ops = APIRouter(tags=["ops"])


@ops.get("/providers", summary="Providers' admission state in operational detail")
async def providers_detail(request: Request, operator: CurrentOperator) -> dict[str, Any]:
    return operational_detail(_services(request))


@ops.get("/metrics", summary="Prometheus exposition", include_in_schema=False)
async def metrics_endpoint(operator: CurrentOperator) -> Response:
    return Response(prometheus_exposition(), media_type="text/plain; version=0.0.4")


def create_admin_app(
    settings: Settings | None = None, *, services: Services | None = None
) -> FastAPI:
    app = build_app(
        title="Mobility Provider Orchestrator (admin)",
        settings=settings or get_settings(),
        seed_clients=False,
        services=services,
    )
    app.include_router(review)
    app.include_router(ops)
    return app

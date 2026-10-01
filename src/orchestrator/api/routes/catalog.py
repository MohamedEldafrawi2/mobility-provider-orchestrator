"""GET /v1/locations, GET /v1/trips and GET /v1/offers/{offerId} (docs/api.md).

Trip search answers 200 with whatever arrived in time plus a per-provider report; 503 with
``Retry-After`` only when no covering provider answered. An offer is retrievable for exactly
as long as the provider advertised it.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter, Query, Request

from orchestrator.api.auth import CurrentClient
from orchestrator.api.problems import Problem
from orchestrator.application.search import (
    LocationNotFoundError,
    ProviderReport,
    SearchUnavailableError,
)
from orchestrator.application.wiring import Services
from orchestrator.domain.offers import PassengerComposition
from orchestrator.persistence.codecs import location_to_json, offer_to_json

router = APIRouter(prefix="/v1", tags=["catalog"])


def _services(request: Request) -> Services:
    return request.app.state.services  # type: ignore[no-any-return]


def report_to_json(report: ProviderReport) -> dict[str, Any]:
    return {
        "code": report.code,
        "status": report.status,
        "latency_ms": report.latency_ms,
        "truncated": report.truncated,
        "warnings": list(report.warnings),
    }


@router.get("/locations", summary="Search locations")
async def locations(
    request: Request,
    client_id: CurrentClient,
    query: Annotated[str, Query(min_length=1, max_length=64)],
    limit: Annotated[int, Query(ge=1, le=20)] = 20,
) -> dict[str, Any]:
    found = await _services(request).search.locations(query, limit=limit)
    return {"locations": [location_to_json(loc) for loc in found]}


@router.get("/trips", summary="Search trips")
async def trips(
    request: Request,
    client_id: CurrentClient,
    from_: Annotated[str, Query(alias="from", min_length=1, max_length=64)],
    to: Annotated[str, Query(min_length=1, max_length=64)],
    departure_date: Annotated[date, Query(alias="departureDate")],
    adults: Annotated[int, Query(ge=1, le=9)] = 1,
    children: Annotated[int, Query(ge=0, le=9)] = 0,
) -> dict[str, Any]:
    services = _services(request)
    try:
        outcome = await services.search.search(
            from_, to, departure_date, PassengerComposition(adults, children)
        )
    except LocationNotFoundError as exc:
        raise Problem(404, "location-not-found", "Location not found", exc.location_id) from None
    except SearchUnavailableError as exc:
        raise Problem(
            503,
            "search-unavailable",
            "No provider answered",
            str(exc),
            retry_after=int(services.settings.search_deadline_seconds) + 2,
        ) from None
    return {
        "offers": [offer_to_json(o) for o in outcome.offers],
        "providers": [report_to_json(r) for r in outcome.providers],
        "complete": outcome.complete,
    }


@router.get("/offers/{offer_id}", summary="Retrieve an offer")
async def offer(request: Request, client_id: CurrentClient, offer_id: str) -> dict[str, Any]:
    found = await _services(request).offers.get(offer_id)
    if found is None:
        raise Problem(404, "offer-unavailable", "Offer unavailable", "Search again.")
    return offer_to_json(found)

"""The public listener: everything a client (a travel retailer) can call."""

from __future__ import annotations

from fastapi import APIRouter, FastAPI

from orchestrator.api.app_factory import build_app
from orchestrator.api.auth import CurrentClient
from orchestrator.api.routes import bookings, catalog, providers, webhooks
from orchestrator.application.wiring import Services
from orchestrator.config import Settings, get_settings

v1 = APIRouter(prefix="/v1", tags=["v1"])


@v1.get("/me", summary="Identify the calling client")
async def me(client_id: CurrentClient) -> dict[str, str]:
    return {"client_id": client_id}


def create_public_app(
    settings: Settings | None = None, *, services: Services | None = None
) -> FastAPI:
    app = build_app(
        title="Mobility Provider Orchestrator",
        settings=settings or get_settings(),
        seed_clients=True,
        services=services,
    )
    app.include_router(v1)
    app.include_router(catalog.router)
    app.include_router(bookings.router)
    app.include_router(providers.router)
    app.include_router(webhooks.router)
    return app

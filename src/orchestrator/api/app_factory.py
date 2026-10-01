"""Shared construction for the public and admin applications.

Both applications get the same lifespan (services on ``app.state``), the same correlation-id
middleware, and the same problem-details handlers. They differ only in their route registries:
the public application never sees review routes, and the admin application never sees client
routes.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from orchestrator.api.middleware import CorrelationIdMiddleware
from orchestrator.api.problems import install_problem_handlers
from orchestrator.api.routes import health
from orchestrator.application.wiring import Services, build_services
from orchestrator.config import Settings
from orchestrator.persistence.seed import seed_api_clients
from orchestrator.telemetry import (
    configure_logging,
    get_logger,
    instrument_app,
    instrument_engines,
)

log = get_logger(__name__)


def build_app(
    *,
    title: str,
    settings: Settings,
    seed_clients: bool,
    services: Services | None = None,
) -> FastAPI:
    """``services``: share one set (engines, clients, admission state) between the listeners
    of one process; the owner closes them. Without it the application builds and owns its own."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(json=settings.log_json, level=logging.INFO)
        owned = services is None
        shared: Services = services if services is not None else await build_services(settings)
        app.state.settings = settings
        app.state.problem_type_base = settings.problem_type_base
        app.state.services = shared
        app.state.db_engine = shared.engine
        app.state.redis = shared.redis
        instrument_engines([shared.engine, shared.confirm_engine], settings)
        if seed_clients:
            try:
                await seed_api_clients(shared.engine, settings)
            except Exception:
                log.exception("seed_api_clients_failed")
        log.info("app_started", title=title, environment=settings.environment)
        try:
            yield
        finally:
            if owned:
                await shared.close()
            log.info("app_stopped", title=title)

    app = FastAPI(
        title=title,
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        openapi_url="/openapi.json",
    )
    app.add_middleware(CorrelationIdMiddleware)
    install_problem_handlers(app)
    app.include_router(health.router)
    instrument_app(app, settings)
    return app

"""Worker process entry point.

The worker owns gauges no API process can report (bookings per state, open review cases,
expired leases) and makes provider attempts of its own, so it serves its own Prometheus
exposition: a small internal listener with ``/metrics`` (operator key) and ``/healthz``,
bound like the admin listener and never published outside the compose network.
"""

from __future__ import annotations

import asyncio
import logging

import uvicorn
from fastapi import FastAPI, Response

from orchestrator.api.auth import CurrentOperator
from orchestrator.api.problems import install_problem_handlers
from orchestrator.application.wiring import Services, build_services
from orchestrator.config import Settings, get_settings
from orchestrator.telemetry import (
    configure_logging,
    configure_tracing,
    get_logger,
    instrument_engines,
    prometheus_exposition,
)
from orchestrator.worker.loops import Worker

log = get_logger(__name__)


def create_worker_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="Mobility Provider Orchestrator (worker)", docs_url=None, openapi_url=None)
    app.state.settings = settings
    app.state.problem_type_base = settings.problem_type_base
    install_problem_handlers(app)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint(operator: CurrentOperator) -> Response:
        return Response(prometheus_exposition(), media_type="text/plain; version=0.0.4")

    return app


async def serve() -> None:
    settings = get_settings()
    configure_logging(json=settings.log_json, level=logging.INFO)
    configure_tracing(settings)
    services: Services = await build_services(settings)
    instrument_engines([services.engine, services.confirm_engine], settings)
    worker = Worker(
        services.uow,
        services.creator,
        services.recovery,
        services.policy,
        confirm_uow=services.confirm_uow,
        concurrency=settings.worker_concurrency,
        confirm_concurrency=settings.confirm_loop_concurrency,
    )
    listener = uvicorn.Server(
        uvicorn.Config(
            create_worker_app(settings),
            host=settings.host,
            port=settings.worker_metrics_port,
            log_config=None,
        )
    )
    log.info("worker_started", metrics_port=settings.worker_metrics_port)
    try:
        await asyncio.gather(
            worker.run_forever(interval=settings.worker_poll_interval_seconds),
            listener.serve(),
        )
    finally:
        worker.close()
        await services.close()


def run() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    run()

"""Process entry point: run the public and admin listeners in one process on two ports."""

from __future__ import annotations

import asyncio

import uvicorn

from orchestrator.api import create_admin_app, create_public_app
from orchestrator.application.wiring import build_services
from orchestrator.config import get_settings


async def serve() -> None:
    settings = get_settings()
    # One set of services for both listeners: one pool, one Redis client, one admission state
    # per provider and purpose, one set of gauges.
    services = await build_services(settings)
    try:
        public = uvicorn.Server(
            uvicorn.Config(
                create_public_app(settings, services=services),
                host=settings.host,
                port=settings.public_port,
                log_config=None,
            )
        )
        admin = uvicorn.Server(
            uvicorn.Config(
                create_admin_app(settings, services=services),
                host=settings.host,
                port=settings.admin_port,
                log_config=None,
            )
        )
        await asyncio.gather(public.serve(), admin.serve())
    finally:
        await services.close()


def run() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    run()

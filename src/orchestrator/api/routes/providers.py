"""GET /v1/providers: capabilities, breaker state per purpose, health (docs/api.md)."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Request

from orchestrator.api.auth import CurrentClient
from orchestrator.application.wiring import Services
from orchestrator.resilience import Purpose

router = APIRouter(prefix="/v1", tags=["providers"])


def _services(request: Request) -> Services:
    return request.app.state.services  # type: ignore[no-any-return]


def operational_detail(services: Services) -> dict[str, Any]:
    """Calls in the window and failure rates per purpose: operator information."""
    snapshot = services.admission.snapshot()
    return {
        "providers": [
            {
                "code": adapter.code,
                "purposes": {
                    purpose.value: (
                        {
                            "circuit": snap[purpose].state.value,
                            "calls_in_window": snap[purpose].calls,
                            "failure_rate": round(snap[purpose].failure_rate, 3),
                            "reopens_at_monotonic": snap[purpose].opens_at,
                        }
                        if purpose in snap
                        else {"circuit": "closed", "calls_in_window": 0, "failure_rate": 0.0}
                    )
                    for purpose in Purpose
                    for snap in (snapshot.get(adapter.code, {}),)
                },
            }
            for adapter in services.registry.all()
        ]
    }


@router.get("/providers", summary="Providers, their capabilities and admission state")
async def providers(request: Request, client_id: CurrentClient) -> dict[str, Any]:
    services = _services(request)
    snapshot = services.admission.snapshot()
    out = []
    for adapter in services.registry.all():
        breakers = snapshot.get(adapter.code, {})
        # Clients see the breaker state per purpose (the public contract, section 8) and
        # nothing about other clients' traffic; counts and rates are on the admin listener.
        purposes = {
            purpose.value: {
                "circuit": breakers[purpose].state.value if purpose in breakers else "closed"
            }
            for purpose in Purpose
        }
        caps = {
            k: (
                v.value
                if hasattr(v, "value")
                else (v.total_seconds() if hasattr(v, "total_seconds") else v)
            )
            for k, v in asdict(adapter.capabilities).items()
        }
        healthy = all(p["circuit"] != "open" for p in purposes.values())
        out.append(
            {
                "code": adapter.code,
                "capabilities": caps,
                "purposes": purposes,
                "health": "ok" if healthy else "degraded",
            }
        )
    return {"providers": out}

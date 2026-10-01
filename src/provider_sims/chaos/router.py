"""Admin endpoints to read and change chaos configuration. Guarded by a token; reachable only on
the compose network in the reference deployment."""

from __future__ import annotations

import hmac
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from provider_sims.chaos.config import ChaosConfig, reseed

chaos_router = APIRouter(prefix="/_chaos", tags=["chaos"])


def _require_token(request: Request, x_admin_token: Annotated[str | None, Header()] = None) -> None:
    expected: str = request.app.state.admin_token
    if not x_admin_token or not hmac.compare_digest(x_admin_token, expected):
        raise HTTPException(status_code=401, detail="admin token required")


@chaos_router.get("", dependencies=[Depends(_require_token)])
async def get_chaos(request: Request) -> ChaosConfig:
    return request.app.state.chaos  # type: ignore[no-any-return]


@chaos_router.put("", dependencies=[Depends(_require_token)])
async def put_chaos(request: Request, config: ChaosConfig) -> ChaosConfig:
    """Replace the live configuration in place (the middleware holds a reference to it)."""
    current: ChaosConfig = request.app.state.chaos
    for name in ChaosConfig.model_fields:
        setattr(current, name, getattr(config, name))
    if config.seed is not None:
        reseed(config.seed)
    return current


@chaos_router.post("/reset", dependencies=[Depends(_require_token)])
async def reset_chaos(request: Request) -> ChaosConfig:
    current: ChaosConfig = request.app.state.chaos
    fresh = ChaosConfig()
    for name in fresh.model_fields:
        setattr(current, name, getattr(fresh, name))
    return current

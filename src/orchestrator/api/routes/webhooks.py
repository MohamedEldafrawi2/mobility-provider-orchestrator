"""POST /v1/providers/{provider}/webhooks: inbound provider events (ADR 011)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from orchestrator.api.problems import Problem
from orchestrator.application.webhooks import WebhookRejectedError, parse_headers
from orchestrator.application.wiring import Services

router = APIRouter(prefix="/v1/providers", tags=["webhooks"])


def _services(request: Request) -> Services:
    return request.app.state.services  # type: ignore[no-any-return]


class _TooLargeError(Exception):
    pass


async def _bounded_body(request: Request, limit: int) -> bytes:
    """Read the raw body from the stream, stopping as soon as it exceeds ``limit``: a missing
    or dishonest Content-Length never makes the platform buffer an unbounded payload. The
    exact bytes read are what the signature is verified against."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise _TooLargeError
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise _TooLargeError
        chunks.append(chunk)
    return b"".join(chunks)


@router.post("/{provider}/webhooks", summary="Receive a provider's signed event")
async def receive_webhook(provider: str, request: Request) -> dict[str, Any]:
    services = _services(request)
    try:
        body = await _bounded_body(request, services.webhooks.max_body_bytes)
    except _TooLargeError:
        services.webhooks.count_rejection(provider, "payload-too-large")
        raise Problem(413, "payload-too-large", "Payload too large") from None
    try:
        result = await services.webhooks.receive(
            provider, body, parse_headers(dict(request.headers))
        )
    except WebhookRejectedError as exc:
        raise Problem(
            exc.status, exc.code, exc.code.replace("-", " ").capitalize(), exc.detail
        ) from exc
    return {
        "outcome": result.outcome.value,
        "event_id": result.event_id,
        "booking_id": result.booking_id,
    }

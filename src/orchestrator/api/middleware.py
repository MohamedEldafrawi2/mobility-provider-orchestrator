"""Correlation id propagation.

``X-Correlation-Id`` is accepted from the caller or generated, returned on every response, and
bound into the logging context for the life of the request. It is stored in a context variable so
problem handlers, dependencies, and later the booking event log can all read it.
"""

from __future__ import annotations

import re
import uuid
from contextvars import ContextVar

import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send

CORRELATION_HEADER = "x-correlation-id"
_VALID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def current_correlation_id() -> str:
    return _correlation_id.get()


class CorrelationIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = next(
            (v.decode("latin-1") for k, v in scope["headers"] if k == CORRELATION_HEADER.encode()),
            "",
        )
        correlation_id = incoming if _VALID.match(incoming) else uuid.uuid4().hex
        token = _correlation_id.set(correlation_id)
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

        async def send_with_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((CORRELATION_HEADER.encode(), correlation_id.encode()))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            structlog.contextvars.unbind_contextvars("correlation_id")
            _correlation_id.reset(token)

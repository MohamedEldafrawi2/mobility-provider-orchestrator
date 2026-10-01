"""RFC 9457 problem details for every error the API emits.

FastAPI does not produce ``application/problem+json`` on its own, so every error path is routed
through the handlers registered here: domain problems raised explicitly, request validation,
unmatched routes, and unexpected exceptions. Each response carries a stable ``type`` URI, a
``code`` extension member for clients that switch on it, and the request's correlation id.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from orchestrator.api.middleware import current_correlation_id
from orchestrator.telemetry import get_logger

PROBLEM_MEDIA_TYPE = "application/problem+json"

log = get_logger(__name__)


class Problem(Exception):
    """An error with a stable machine-readable code, rendered as a problem details document."""

    def __init__(
        self,
        status: int,
        code: str,
        title: str,
        detail: str | None = None,
        **extensions: Any,
    ) -> None:
        super().__init__(detail or title)
        self.status = status
        self.code = code
        self.title = title
        self.detail = detail
        self.extensions = extensions


def problem_response(request: Request, problem: Problem) -> JSONResponse:
    base: str = request.app.state.problem_type_base
    body: dict[str, Any] = {
        "type": f"{base}{problem.code}",
        "title": problem.title,
        "status": problem.status,
        "instance": str(request.url.path),
        "code": problem.code,
        "correlation_id": current_correlation_id(),
    }
    if problem.detail:
        body["detail"] = problem.detail
    body.update(problem.extensions)
    headers: dict[str, str] = {}
    if isinstance(retry_after := problem.extensions.get("retry_after"), int):
        headers["Retry-After"] = str(retry_after)  # the body field, also as the standard header
    return JSONResponse(
        status_code=problem.status, content=body, media_type=PROBLEM_MEDIA_TYPE, headers=headers
    )


_HTTP_STATUS_CODES = {
    404: ("not-found", "Not found"),
    405: ("method-not-allowed", "Method not allowed"),
    401: ("unauthorized", "Unauthorized"),
    403: ("forbidden", "Forbidden"),
    413: ("payload-too-large", "Payload too large"),
    429: ("rate-limited", "Too many requests"),
}


def install_problem_handlers(app: FastAPI) -> None:
    @app.exception_handler(Problem)
    async def _on_problem(request: Request, exc: Problem) -> JSONResponse:
        return problem_response(request, exc)

    @app.exception_handler(RequestValidationError)
    async def _on_validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {
                "location": ".".join(str(part) for part in err.get("loc", ())),
                "message": err.get("msg"),
            }
            for err in exc.errors()
        ]
        return problem_response(
            request,
            Problem(400, "validation-error", "Request validation failed", errors=errors),
        )

    @app.exception_handler(StarletteHTTPException)
    async def _on_http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code, title = _HTTP_STATUS_CODES.get(exc.status_code, ("http-error", "HTTP error"))
        detail = exc.detail if isinstance(exc.detail, str) and exc.detail != title else None
        response = problem_response(request, Problem(exc.status_code, code, title, detail))
        if exc.headers:
            response.headers.update(exc.headers)
        return response

    @app.exception_handler(Exception)
    async def _on_unexpected(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled_error", path=request.url.path)
        return problem_response(
            request,
            Problem(500, "internal-error", "Internal error", "An unexpected error occurred."),
        )

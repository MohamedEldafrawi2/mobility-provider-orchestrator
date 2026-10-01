"""Distributed tracing: one trace per request or worker pass, spans per provider call and SQL
statement, exported over OTLP/HTTP to a collector when ``MPO_OTEL_EXPORTER_OTLP_ENDPOINT`` is set.

Without an endpoint nothing is installed and the process runs exactly as before: tracing is an
operational profile, not a dependency of correctness. Trace and span ids are bound into the
structlog context so a log line can be joined to its trace. No request body, header or SQL
parameter is ever recorded: the engines hide parameters from their error messages, so an
exception recorded on a span carries the statement, never the values.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine

from orchestrator.config import Settings
from orchestrator.telemetry.logging import bind_context

log = logging.getLogger(__name__)
_configured: dict[str, bool] = {}


def configure_tracing(settings: Settings) -> bool:
    """Install the SDK with an OTLP exporter once per process. Returns whether tracing is on."""
    if not settings.otel_exporter_otlp_endpoint:
        return False
    if _configured.get("provider"):
        return True
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: settings.otel_service_name}))
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=f"{settings.otel_exporter_otlp_endpoint}/v1/traces")
        )
    )
    trace.set_tracer_provider(provider)
    _configured["provider"] = True
    _instrument_httpx()
    log.info("tracing configured", extra={"endpoint": settings.otel_exporter_otlp_endpoint})
    return True


def instrument_app(app: FastAPI, settings: Settings) -> None:
    """Server spans for one FastAPI application; health and metrics endpoints are excluded."""
    if not configure_tracing(settings):
        return
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(
        app, excluded_urls="healthz,readyz,metrics", server_request_hook=_bind_trace_ids
    )


def instrument_engines(engines: Sequence[AsyncEngine], settings: Settings) -> bool:
    """Client spans per SQL statement for every engine of the process, in one call (the
    instrumentor guards itself process-wide, so a second call would silently do nothing).
    Statements carry no parameters; the engines hide them from error messages too."""
    if not configure_tracing(settings):
        return False
    if _configured.get("engines"):
        return True
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    SQLAlchemyInstrumentor().instrument(
        engines=[engine.sync_engine for engine in engines], enable_commenter=False
    )
    _configured["engines"] = True
    return True


def _instrument_httpx() -> None:
    if _configured.get("httpx"):
        return
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    HTTPXClientInstrumentor().instrument()
    _configured["httpx"] = True


def _bind_trace_ids(span: Any, scope: Any) -> None:
    context = span.get_span_context() if span is not None else None
    if context is None or not getattr(context, "is_valid", False):
        return
    bind_context(trace_id=format(context.trace_id, "032x"), span_id=format(context.span_id, "016x"))

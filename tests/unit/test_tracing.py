"""Tracing is an operational profile: without an endpoint nothing is installed."""

from __future__ import annotations

from fastapi import FastAPI

from orchestrator.telemetry.tracing import configure_tracing, instrument_app
from tests.conftest import make_settings


def test_tracing_is_off_without_an_endpoint() -> None:
    settings = make_settings()
    assert settings.otel_exporter_otlp_endpoint is None
    assert configure_tracing(settings) is False
    app = FastAPI()
    middleware_before = list(app.user_middleware)
    instrument_app(app, settings)
    assert list(app.user_middleware) == middleware_before, "no instrumentation was added"


def test_engines_hide_bound_parameters_from_error_messages() -> None:
    from orchestrator.persistence.db import create_engine

    engine = create_engine(make_settings())
    assert engine.sync_engine.hide_parameters is True, "no PII in SQL errors, logs or spans"


def test_engine_instrumentation_is_off_without_an_endpoint() -> None:
    from orchestrator.persistence.db import create_engine
    from orchestrator.telemetry.tracing import instrument_engines

    settings = make_settings()
    assert instrument_engines([create_engine(settings)], settings) is False

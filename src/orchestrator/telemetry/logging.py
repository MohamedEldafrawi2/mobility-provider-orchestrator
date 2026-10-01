"""Structured logging.

Every log line carries the request's correlation id (and later the booking id, provider, and
trace ids) through structlog context variables, so a single booking can be followed across the
API and the worker without grepping for free text.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(*, json: bool = True, level: int = logging.INFO) -> None:
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer: Any = structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[*shared, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(level=level, stream=sys.stdout, format="%(message)s")


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]


def bind_context(**values: Any) -> None:
    structlog.contextvars.bind_contextvars(**values)

"""Metrics (ADR 014), on the OpenTelemetry API with a Prometheus reader.

Instruments are created once on the global meter and are no-ops until ``configure_metrics``
installs the SDK. Names follow docs/resilience-strategy.md; the Prometheus exporter appends the unit
and ``_total`` suffixes, so ``provider_attempts`` is scraped as ``provider_attempts_total`` and
``provider_attempt_duration`` (unit ``s``) as ``provider_attempt_duration_seconds``.

Gauges that describe live state (breaker state, quota tokens, bookings per state) are
*observable*: a source registers a callback and the reader pulls the value at scrape time, so
no code path has to remember to update a gauge it does not own. Sources are held weakly, so a
controller or worker that is dropped without ``close()`` stops reporting instead of leaking.
Two sources reporting the same label set (two controllers in one process) are aggregated
explicitly: the *maximum* wins, so an open breaker is never masked by a closed one.
"""

from __future__ import annotations

import threading
import weakref
from collections.abc import Callable, Iterable
from typing import Any

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

_meter = metrics.get_meter("mobility-provider-orchestrator")

provider_attempts = _meter.create_counter(
    "provider_attempts", unit="1", description="Provider attempts by outcome and side effect"
)
provider_attempt_duration = _meter.create_histogram(
    "provider_attempt_duration", unit="s", description="Provider latency per attempt"
)
provider_timeouts = _meter.create_counter(
    "provider_timeouts", unit="1", description="Attempts that ended in a timeout"
)
provider_retries = _meter.create_counter(
    "provider_retries", unit="1", description="Retries the attempt loop made"
)
provider_retry_tokens_exhausted = _meter.create_counter(
    "provider_retry_tokens_exhausted", unit="1", description="Retries suppressed for lack of tokens"
)
provider_not_dispatched = _meter.create_counter(
    "provider_not_dispatched", unit="1", description="Attempts refused locally, by reason"
)
provider_admission_wait = _meter.create_histogram(
    "provider_admission_wait", unit="s", description="Time waited for a bulkhead slot"
)
confirm_dispatch_latency = _meter.create_histogram(
    "confirm_dispatch_latency", unit="s", description="Hold to confirmation dispatch"
)
booking_commands = _meter.create_counter(
    "booking_commands", unit="1", description="Settled commands by disposition and basis"
)
worker_leases_expired = _meter.create_counter(
    "worker_leases_expired", unit="1", description="Writes fenced off by an expired lease"
)
search_requests = _meter.create_counter(
    "search_requests", unit="1", description="Trip searches by outcome"
)
search_provider_outcomes = _meter.create_counter(
    "search_provider_outcomes", unit="1", description="Search branches by provider and status"
)
search_duration = _meter.create_histogram(
    "search_duration", unit="s", description="Trip search wall time, both rounds"
)

GaugeSource = Callable[[], Iterable[Observation]]
_sources: dict[str, list[weakref.ref[Any]]] = {}
_gauges: dict[str, Any] = {}
_lock = threading.Lock()


def _live_sources(name: str) -> list[GaugeSource]:
    out: list[GaugeSource] = []
    with _lock:
        refs = _sources.get(name, [])
        for ref in list(refs):
            source = ref()
            if source is None:
                refs.remove(ref)
            else:
                out.append(source)
    return out


def _observe(name: str) -> Callable[[CallbackOptions], Iterable[Observation]]:
    def callback(_: CallbackOptions) -> Iterable[Observation]:
        merged: dict[tuple[tuple[str, Any], ...], Observation] = {}
        for source in _live_sources(name):
            for observation in source():
                key = tuple(sorted((observation.attributes or {}).items()))
                current = merged.get(key)
                if current is None or observation.value > current.value:
                    merged[key] = observation  # explicit aggregation: the maximum wins
        return list(merged.values())

    return callback


def register_gauge_source(
    name: str, source: GaugeSource, *, description: str = "", unit: str = ""
) -> None:
    """Add a live-state source for the observable gauge ``name`` (created on first use)."""
    ref: weakref.ref[Any]
    ref = weakref.WeakMethod(source) if hasattr(source, "__self__") else weakref.ref(source)
    with _lock:
        _sources.setdefault(name, []).append(ref)
        if name not in _gauges:
            _gauges[name] = _meter.create_observable_gauge(
                name, callbacks=[_observe(name)], description=description, unit=unit
            )


def unregister_gauge_source(name: str, source: GaugeSource) -> None:
    with _lock:
        refs = _sources.get(name, [])
        for ref in list(refs):
            target = ref()
            if target is None or target == source:
                refs.remove(ref)


_configured = False


def configure_metrics() -> bool:
    """Install the SDK with a Prometheus reader. Idempotent; returns whether it ran."""
    global _configured
    with _lock:
        if _configured:
            return False
        _configured = True
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View

    latency = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16)
    provider = MeterProvider(
        metric_readers=[PrometheusMetricReader()],
        views=[
            View(
                instrument_name="provider_attempt_duration",
                aggregation=ExplicitBucketHistogramAggregation(latency),
            ),
            View(
                instrument_name="provider_admission_wait",
                aggregation=ExplicitBucketHistogramAggregation(latency),
            ),
            View(
                instrument_name="confirm_dispatch_latency",
                aggregation=ExplicitBucketHistogramAggregation(latency),
            ),
            View(
                instrument_name="search_duration",
                aggregation=ExplicitBucketHistogramAggregation(latency),
            ),
        ],
    )
    metrics.set_meter_provider(provider)
    return True


def prometheus_exposition() -> bytes:
    from prometheus_client import REGISTRY, generate_latest

    return generate_latest(REGISTRY)

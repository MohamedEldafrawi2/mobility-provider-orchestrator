# ADR 014: OpenTelemetry metrics and traces, Prometheus and Grafana as a profile

Status: accepted

## Context

Following one booking through the API, the worker and three providers needs correlation, and
the design's safety claims need measurable counterparts.

## Decision

Structured logs with correlation and trace ids; OpenTelemetry metrics exposed for Prometheus
from both the API and the worker (the worker owns the state gauges); OpenTelemetry traces over
OTLP to a collector and Jaeger when an endpoint is configured; Grafana provisioned with one
dashboard and Prometheus with the alert rules the design commits to. All of it is a compose
override, not a dependency of correctness.

## Consequences

- Command metrics (by disposition and basis) are separate from state gauges, so settlement
  quality and backlog are read independently.
- Without the profile the platform runs identically, with logs and metrics only.

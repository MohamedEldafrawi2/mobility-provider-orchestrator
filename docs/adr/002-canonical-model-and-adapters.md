# ADR 002: Canonical domain model plus per-provider adapters

Status: accepted

## Context

Providers differ in protocol, vocabulary, multi-step flows and after-sales semantics.

## Decision

A canonical domain model (locations, offers, bookings, commands, attempts, reservations) and one
adapter per provider that translates to and from it. Multi-step flows (hold then confirm, quote
then accept) are separate port operations; the application owns the commits between steps and
persists every intermediate phase.

## Consequences

- Provider differences are absorbed in one place and verified by contract tests.
- Adapters classify every failure by side effect; this is their most important duty.
- Adapters never retry, sleep or cache: admission and recovery belong to the platform.

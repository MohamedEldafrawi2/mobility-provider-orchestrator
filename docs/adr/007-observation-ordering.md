# ADR 007: Observations ordered by provider generation then revision

Status: accepted

## Context

Webhooks and polls about the same reservation arrive in any order, duplicated, and sometimes
from a provider that reset its history.

## Decision

Every observation is checked against the bound reference, then ordered by the provider's
generation and revision. An older fact is stale; a newer generation is adopted only from an
authoritative read; an authoritative read behind what was already applied is quarantined for
review. An observation that confirms the current state still advances the watermark. Terminal
states reopen only on verified evidence of a reservation.

## Consequences

- Stale or duplicated deliveries cannot regress a booking.
- A provider's own contradiction becomes a review case rather than a silent flip.

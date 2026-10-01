# ADR 011: Webhooks: reference library, receipts in the applying transaction

Status: accepted

## Context

Inbound events are retried by the provider, may be duplicated, may arrive before the response
to the request that caused them, and must never be trusted on their own word.

## Decision

Signatures are verified with the Standard Webhooks reference library (bounded body, several keys
during rotation). The receipt and the state change happen in one transaction under the
booking's row lock: a duplicate is the receipts table's unique constraint firing. An early event
is bound only after an authoritative read agrees with it on both references; a disagreement is
contradictory evidence. Events about a booking under review are quarantined as case evidence.

## Consequences

- Any replica can receive any event; two receivers apply it once.
- A provider is acknowledged only for work that is committed, correctly addressed and current.

# ADR 012: Review cases close only on complete evidence, with no override

Status: accepted

## Context

Some situations cannot be settled automatically: duplicates, contradictions, providers
without finality. An operator must be able to act, but never to create a ghost or lose a
reservation.

## Decision

A review case lists every implicated reservation and dated, validity-bounded evidence. It closes
only when every implicated reservation is accounted for, at most one of ours is live and it is
the bound reference, verifiably the one the command asked for; a live reservation that verifiably
belongs to another client competes for nothing. A cancellation case is judged by the exact refund
offer. Operators reconcile and resolve under `expected_version`; there is no override path. Under
review nothing but the case's evidence moves the booking. Automatic disposal of a duplicate
(`CANCEL_EXTRA`) is modelled but not executed by the platform.

## Consequences

- Cases for a provider without finality may stay open indefinitely, visibly.
- Resolutions are idempotent and audited.

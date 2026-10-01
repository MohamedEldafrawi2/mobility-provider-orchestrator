# ADR 013: Refund inside cancellation under authorised terms, phases persisted

Status: accepted

## Context

A cancellation may cost money. A one-call cancel is honest only when the client authorised the
terms it ends up with.

## Decision

The client authorises a maximum fee. The platform obtains a quote, validates it against the
authorisation, persists it, and accepts it by its exact identity before its validity ends. Phases
(`NONE`, `QUOTED`, `ACCEPTING`) are persisted by the application; an acceptance attempt settles
only by the exact offer's status; a re-quote happens only once every acceptance attempt is
settled. Worse terms settle the command `TERMS_CHANGED` and the booking stays confirmed.

## Consequences

- The client is never charged a fee it did not authorise.
- A lost acceptance answer is settled by the offer's status, never by the reservation's state
  alone.

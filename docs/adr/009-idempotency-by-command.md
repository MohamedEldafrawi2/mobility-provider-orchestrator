# ADR 009: Idempotency keyed to commands, replays from one function

Status: accepted

## Context

Clients retry. A replay must never contradict the original response or the booking's reality.

## Decision

A client key maps immutably to one command. The initial response and every replay are produced
by one function over the command's disposition, which also carries the booking's current state.
`OPEN` and `UNRESOLVED` replay as 202; `SUCCEEDED` as the original success with the current
state; a definitive rejection as 422. Concurrent identical requests race for the unique
`(client, key)` row; the loser replays the winner.

## Consequences

- A CREATE stays `OPEN` through holds and pending states, so a replay while the outcome is
  unknown is honest (202) rather than a guess.
- Request bodies are strict: an unknown authorisation field is an error, never dropped.

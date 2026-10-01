# ADR 003: An explicit booking state machine, no workflow engine

Status: accepted

## Context

A workflow engine would hide the booking lifecycle inside its own state and retries.

## Decision

The booking state machine is a table in the domain layer: a total, pure function from state
and trigger to the next state, exhaustively tested. The application applies transitions under
the booking's row lock, in the same transaction as the command disposition that justifies them.

## Consequences

- The lifecycle is readable in one file and provable by model-based tests.
- Recovery is written in terms of states and command certainty, not of engine internals.

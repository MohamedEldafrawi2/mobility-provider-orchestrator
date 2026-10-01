# ADR 005: Commands with journaled attempts, expiries, and evidence-based settlement

Status: accepted

## Context

A provider call can time out after executing. The platform must never claim more than it
knows, and must bound how long it can remain uncertain.

## Decision

A command has one immutable intent, one provider key, one first-dispatch anchor and one absolute
execution cutoff. Each attempt is journaled before any network IO with a short expiry inside the
cutoff. Attempts settle individually by evidence; the command settles when every possible effect
is confirmed or excluded. Negative settlement after a possible effect needs a provider fenced
lookup; a plain lookup never settles negatively; `ABANDONED` needs proven zero dispatch. Elapsed
time alone never settles anything.

## Consequences

- Every disposition carries its basis and the domain refuses unjustified combinations.
- Providers with key binding and finality recover by resubmission then a fence; others by
  review. Permanent uncertainty is a supported outcome.
- A late answer closes its attempt's record but never reopens a settled question.

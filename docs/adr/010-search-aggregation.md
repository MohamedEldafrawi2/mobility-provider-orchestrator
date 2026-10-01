# ADR 010: Search: one deadline, per-provider budgets, partial results

Status: accepted

## Context

One slow or failing provider must not kill search for the others, and search must never
compete with completion for a provider's allowance.

## Decision

Search fans out under one request deadline with a smaller per-provider budget and no retries,
through the search purpose only. A catalogue round decides coverage; a trips round asks only the
covering providers; stragglers are cancelled; the response carries a per-provider report. The
answer is 503 only when no covering provider answered. No cross-provider deduplication, no
currency conversion, no offer cache.

## Consequences

- Partial results are the normal case and are labelled as such.
- A location is reported unknown only when every catalogue was read.

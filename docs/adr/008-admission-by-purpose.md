# ADR 008: Admission partitioned by purpose, fail-closed quota, a stated load envelope

Status: accepted

## Context

Search traffic is elastic and bursty; confirmations are deadline-bound. One shared limiter lets
the former starve the latter.

## Decision

Five purposes (search, create, confirm, cancel, lookup), each with its own bulkhead, sliding
window circuit breaker, share of the provider allowance (a two-bucket quota in Redis), retry
tokens and HTTP client. Search and create are capped; confirm, cancel and lookup have floors that
are never lent. The quota fails closed without Redis. The design states a load envelope and
measures it with a benchmark instead of claiming starvation is impossible. The breaker is written
in-repo because it is evaluated per attempt inside the same loop as the retry budget and the
quota, and because it is small enough to read.

## Consequences

- A search storm cannot delay a confirmation beyond the measured envelope.
- A Redis outage refuses attempts locally and reports `quota-outage`; nothing is presumed expired.
- Breaker state is per process; the named step-up is shared state in Redis.

# ADR 006: The bookings table as the work source, with leases and a reserved confirmation loop

Status: accepted

## Context

Recovery is scheduled scans over state the platform already stores. A broker would add a second
source of truth.

## Decision

Worker loops claim due booking rows with `FOR UPDATE SKIP LOCKED` under a lease token that fences
every write. Separate loops own submission, reconciliation, polling, cancellation and
confirmation; the confirmation loop runs on its own PostgreSQL pool slice with its own
concurrency and takes imminent deadlines first. Backoff is capped by any hold deadline.

## Consequences

- Every state and phase has an owner; a dead worker's rows are reclaimed when the lease expires.
- Confirmations have reserved resources that no other loop can borrow.
- The named step-up for latency is `LISTEN/NOTIFY` or a Postgres queue; for throughput,
  partitioning by provider.

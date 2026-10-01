# Scaling

The platform is a modular monolith with two process roles, the API and the worker, over
PostgreSQL and Redis. This document says what scales how, where the limits are, and which
step-up is named for each limit. Nothing here is speculative capacity: the numbers come from
the benchmarks in `benchmarks/`, and the limits are the ones the design accepted on purpose.

## What is stateless and what is not

| Component | State | Scaling |
|---|---|---|
| API process | none beyond connection pools and per-process admission state | horizontal; any replica serves any request |
| Worker process | leases on booking rows; per-process gauges | horizontal; rows are claimed with `FOR UPDATE SKIP LOCKED` and fenced by a lease token |
| PostgreSQL | bookings, commands, attempts, events, review cases, idempotency keys, webhook receipts | vertical first; the step-ups are below |
| Redis | offers with TTL; the per-provider two-bucket quota | small; a single instance with persistence is enough, and the quota fails closed without it |
| Provider simulators | one SQLite file each | not part of the platform |

## Admission is partitioned, so one kind of traffic cannot starve another

Every provider call goes through one of five **purposes** (search, create, confirm, cancel,
lookup). Each purpose has its own bulkhead, circuit breaker, share of the provider's allowance,
retry tokens and HTTP client. The allowance shares are a cap for search (45 %) and create (25 %)
and a floor that is never lent for confirm, cancel and lookup (10 % each). The platform side is
partitioned too: the confirmation loop runs on its own PostgreSQL pool slice with its own
concurrency.

The **load envelope** the design commits to: 200 concurrent holds per provider dispatched for
confirmation within 5 s at p99 while search runs at its cap. `docs/resilience-strategy.md`
records the measured numbers.

## Per-process admission state

Breaker windows and retry tokens live in each process. With `n` API replicas a provider sees up
to `n` times the per-process half-open probes and retry budget; the quota, which is the
provider-facing limit that matters, is shared in Redis and therefore exact across replicas.
Step-up when `n` grows past a handful: move breaker state to Redis (the admission controller
already isolates it behind one class) or run a sidecar proxy that owns the breaker.

## Database

The hot paths are indexed claims (`state`, `next_action_at`, `lease_expires_at`), row locks on
one booking, and appends to `booking_events`, `attempts` and `webhook_receipts`. Expected
headroom on one modest PostgreSQL instance is tens of bookings per second sustained, which is
far beyond the provider allowances this platform fronts.

Named step-ups, in order:

1. **Partition or archive events and receipts** by month: they are append-only and read by
   booking id.
2. **Replace the 1 s claim poll with `LISTEN/NOTIFY`** on booking changes, or adopt `pgqueuer`
   for the work table. The loops already drain in batches, so the poll interval is latency, not
   throughput.
3. **Read replicas** for the review API and the state gauges, which tolerate staleness.
4. **Shard by provider**: every booking belongs to exactly one provider, and nothing joins across
   providers.

## Provider allowances are the real ceiling

The platform never sends a provider more than its documented allowance per second, per purpose
share. A provider that allows 50 requests per second gives search at most 22.5, creates at most
12.5 and confirmations at least 5. Adding API replicas does not raise this; it only spreads the
same quota. Raising it is a commercial conversation with the provider, after which one number
changes in configuration.

## Search

Search fans out under one deadline with a per-provider budget and no retries, and is capped by
its share of each provider's allowance. The catalogue round is cached briefly per process. At
scale, the step-up is a shared catalogue cache (the location sets are seed data) and, if a
provider's search is slow, a lower budget for it; offers themselves are never cached, because a
stale offer that fails at booking time costs more than a slower search.

## Webhooks

Inbound events are verified, recorded and applied in one transaction under the booking's row
lock, so any replica can receive any event and duplicates are settled by the receipts table's
unique constraint. The step-up for a provider that sends bursts is a queue in front of the
handler, with the same receipt rule applied by the consumer.

## What does not scale by adding machines

- **Provider finality.** Negative settlement needs a provider fenced lookup. Without it a case
  can stay open indefinitely; more workers do not change that.
- **Hold deadlines.** A hold expires on the provider's clock. The confirmation loop's reserved
  resources exist so that platform load never spends that time; beyond the envelope the honest
  behaviour is the one designed: the hold expires, the provider says so, the booking is `FAILED`
  with that reason.
- **Review.** Cases need an operator. The queue is visible (`review_cases_open`), bounded by the
  reasons that create it, and alerts when a remediable case is older than five minutes.

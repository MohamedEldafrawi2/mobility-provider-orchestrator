# Resilience strategy

Resilience here means two things that are usually conflated: **never losing track of a
reservation** (correctness under failure) and **never letting one kind of traffic starve another**
(capacity under load). The first is the subject of the [state machine](booking-state-machine.md)
and the [integration guide](provider-integration-guide.md); this document covers the second, and
the measurements that back it.

## Admission by purpose

Every provider call is made for one of five purposes. Each purpose owns its resources end to end:

| Purpose | Traffic | Share of the provider allowance |
|---|---|---|
| search | trip and location search | at most 45 % |
| create | new creates, from handlers or the submission loop | at most 25 % |
| confirm | hold confirmations only | at least 10 %, never lent |
| cancel | quotes, acceptances, free cancellations | at least 10 %, never lent |
| lookup | polls, reconciliation, review, fenced lookups | at least 10 %, never lent |

```
attempt loop (deadline-aware, full-jitter backoff, Retry-After honoured)
  retry token of the purpose (attempts after the first; refunded when nothing was sent)
    -> bulkhead slot of the purpose (bounded wait, never past the deadline)
      -> circuit breaker of the purpose (sliding window, half-open probes)
        -> quota token: the purpose's bucket and the provider's shared bucket (Redis, Lua)
          -> deadline rechecked
            -> dispatch mark committed with the attempt's expiry
              -> deadline and expiry rechecked right before the IO
                -> the call, on the purpose's own HTTP client, under the operation's timeout
```

Each step refuses with a named reason (`bulkhead-full`, `circuit-open`, `quota-exhausted`,
`quota-outage`, `retry-tokens-exhausted`, `deadline-passed`) that is journaled as an attempt
that certainly left nothing at the provider, counted in `provider_not_dispatched_total`, and
scheduled for retry with the provider's own `Retry-After` when it gave one.

### Fail closed

The quota lives in Redis so that every replica draws from the same allowance. Without Redis the
platform **refuses** rather than guesses: attempts are not dispatched, bookings report
`unresolved_reason: quota-outage`, and holds are not presumed expired. A hold whose deadline
passes during an outage is settled later by the provider's own word, like any other.

### The breaker is per purpose

A provider whose search endpoint is failing has an open search breaker and a closed confirm
breaker. The breaker is a sliding window over a configurable number of buckets with a minimum
call count, a failure-rate threshold, an open period and a bounded number of half-open probes.
Breaker and retry-token state are per process; the quota is the shared limit that matters to the
provider.

## The platform side is partitioned too

Provider-side admission is not the only place a confirmation can starve. The worker runs a
**dedicated confirmation loop** with its own concurrency, its own slice of the PostgreSQL
connection pool (reserved, never lent to another loop) and its own lease claims, ordered by
confirmation deadline so imminent holds go first. Reconciliation, polling and cancellation cannot
consume its connections. Loops drain in batches and defer rows that fail, so one persistently
failing row cannot keep a loop spinning.

## The load envelope

The design does not claim starvation is impossible. It states an envelope and measures it:

> 200 concurrent holds per provider get their confirmation dispatched within 5 s at p99 while
> search runs at its allowance cap.

### Admission alone, in memory

`benchmarks/envelope.py` runs the real admission controller against the two-bucket quota
arithmetic in memory, 64 searchers saturating search, 200 holds arriving at once, a provider
answering in 50 ms, an allowance of 120 requests per second. It runs twice: partitioned by
purpose (the design) and with one undifferentiated first-come bucket (the naive alternative).

| configuration | dispatch p50 | dispatch p99 | max | searches completed meanwhile |
|---|---|---|---|---|
| partitioned by purpose | 0.42 s | 3.27 s | 3.32 s | 288 |
| one undifferentiated bucket | 1.72 s | 6.69 s | 8.77 s | 1085 |

Partitioned, every confirmation dispatches inside the envelope while search keeps its cap. The
naive configuration serves more searches and misses the envelope: that is the trade the design
makes on purpose.

### Through the real stack

`benchmarks/confirmation_path.py` runs the platform as deployed: PostgreSQL and Redis in
containers, the rail-osdm simulator on a socket, the public application, the confirmer on its
reserved pool slice, leases, journaling before IO and the quota in Redis. It creates bookings
concurrently against the hold-then-confirm provider with and without a search storm through the
same admission controller, and reads the platform's own `confirm_dispatch_latency` histogram
(hold to the confirm IO) from the admin `/metrics` endpoint.

Measured on a developer laptop with the default configuration (allowance 50 requests per
second per provider, so a confirm floor of 5 per second), the simulator answering in 20 ms,
40 bookings created at once (the benchmark trip has 40 seats), 24 searchers in the storm:

| scenario | confirmed in the request | handed to the loop | request p50 | request p99 | hold-to-dispatch p50 | hold-to-dispatch p99 |
|---|---|---|---|---|---|---|
| no storm | 25 | 15 | 3.6 s | 4.3 s | under 2 s | under 2 s |
| search storm (198 searches served, 624 refused by the search cap) | 27 | 13 | 6.7 s | 9.0 s | under 2 s | under 8 s |

Histogram quantiles are bucket upper bounds. Two things to read from this:

- The storm never touched the confirmations' allowance: every refusal it caused was a *search*
  refusal at the search cap, and every hold was confirmed. What the storm did cost is CPU and
  event-loop time in a single process on one machine, which is why request wall time rose.
- Forty simultaneous holds against a floor of five confirmations per second need about eight
  seconds of dispatch capacity, and that is what the tail shows. The envelope's five seconds for
  two hundred holds assumes the 120 requests per second allowance of the in-memory benchmark
  (a floor of twelve per second); the floor is a share of whatever the provider grants, and
  raising it is one configuration value, not a design change.

The request wall time includes the hold *and* its confirmation, two round trips to the provider
and four transactions. Run it yourself with `make bench`; the numbers above are indicative, not a
service level.

## What this does not protect against

- **Provider finality.** No amount of admission makes a provider without a fenced lookup able to
  say "it did not happen". That is the integration guide's problem, and the answer is an open
  review case.
- **Hold deadlines beyond the envelope.** Past the envelope confirmations queue and holds may
  expire. The provider reports the expiry; the booking is `FAILED` with `hold-expired`; nothing
  is guessed. Raising the envelope means raising the provider's allowance or the confirmation
  loop's resources, both of which are configuration.
- **Per-process breaker state.** Many replicas probe a half-open provider more often than one
  would. The named step-up is shared breaker state.

## Operating it

The alert rules in `observability/alerts.yml` are the envelope's operational form: a remediable
review case older than five minutes, an unresolved booking older than fifteen, a confirm breaker
open for two, hold-to-dispatch p99 over five seconds, and any refusal for `quota-outage`. The
dashboard shows each of them next to the raw signals (bookings by state, attempts by outcome and
side effect, refusals by reason, quota tokens, breaker states).

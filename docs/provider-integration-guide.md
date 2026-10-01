# Provider integration guide

How to add a provider, and what the platform will and will not promise on its behalf. The
platform reasons about **capabilities**, never about provider identities: an adapter declares
what its provider can do, contract tests prove every claim against the provider (here, its
simulator), and the orchestration layer derives its behaviour from the declaration.

## The port

An adapter implements `orchestrator.providers.port.ProviderAdapter`:

| Operation | Purpose | Notes |
|---|---|---|
| `search_locations(query, limit)` | search | the provider's catalogue, mapped to canonical `Location`s whose ids are minted by the adapter |
| `search_trips(query, limit)` | search | offers priced for a passenger composition, with cancellation conditions and an expiry |
| `create_booking(request, key, expiry)` | create | one reservation under `key`; `expiry` is the request's `executeBefore` where the provider supports it |
| `confirm_booking(ref, expiry)` | confirm | hold-then-confirm providers only; addresses one immutable hold, never creates another |
| `get_booking(ref)` | lookup | an authoritative read by provider reference |
| `find_bookings_by_client_ref(client_ref)` | lookup | every reservation carrying our reference (discovery) |
| `fenced_lookup(key)` | lookup | providers with finality only: what the key produced, with the guarantee that nothing later can commit for it |
| `quote_cancellation(ref)` | cancel | providers that quote refunds: one offer with amounts and validity |
| `cancel_booking(ref, quote, expiry)` | cancel | accept the exact quote (or cancel for free) before `expiry` |

An operation the provider does not support raises `CapabilityNotSupportedError`. The adapter
never retries, never sleeps and never caches: admission, retries and deadlines belong to the
resilience layer, which wraps every call by purpose.

## Classify every failure by its side effect

The single most important job of an adapter is honest **side-effect classification** of every
failure. `ProviderError` carries a `kind` and a `SideEffect`:

| Situation | `SideEffect` | Why |
|---|---|---|
| connection refused, DNS failure, connect timeout, pool timeout | `NONE` | nothing was sent |
| the provider's edge refused it (429, a gateway 503 that marks itself as such) | `NONE` | the request never reached a handler |
| timeout after the send, connection reset after the send, unparsable success body of a mutation, an unexpected status after the handler | `POSSIBLE` | the provider may have executed it |
| a definitive validation rejection, "request expired before execution", "fenced", "not found" | `NONE` and `definitive` | the provider says it did nothing |
| an unparsable body of a **read** | `NONE` | a read has no effect; the mutation's uncertainty, if any, stays where it was |

The rule for success bodies: semantic decoding of a **mutation's** answer that fails keeps the
mutation's `POSSIBLE` (something was accepted; what, the platform does not know). The rail and
shuttle adapters show the pattern (`_mapped`, `_mutation_result`). An adapter that reports
`NONE` for a lost answer is the one bug this platform cannot defend against.

Identity must be **echoed and checked**: a success body must name our client reference (and for
a create, the product and date we asked for); a body that names another booking is `MALFORMED`
with the mutation's side effect, never silently adopted.

## Declare the capabilities

```python
ProviderCapabilities(
    booking_flow=...,  # DIRECT or HOLD_THEN_CONFIRM
    confirmation=...,  # SYNC or ASYNC (the outcome arrives later)
    supports_webhooks=...,
    supports_status_lookup=...,  # get_booking by provider reference
    lookup_by_client_ref=...,  # NONE, IMMEDIATE or EVENTUAL (an index that lags)
    idempotent_create=...,  # NONE, KEY (a header) or CLIENT_REF (our reference)
    key_bound_before_execution=...,  # a repeated key replays or reports "in progress"
    idempotency_window=...,  # how long the provider remembers a key
    execution_expiry=...,  # the provider rejects requests received after executeBefore
    finality_lookup=...,  # a fenced lookup exists: negative settlement is possible
    max_clock_skew=...,  # declared; margins are derived from it
    unknown_resolution=...,  # RESUBMIT (same key, fresh expiry) or REVIEW
    confirm_is_idempotent=...,
    cancellation=...,  # NONE or CONFIRMED_ONLY
    cancel_is_idempotent=...,
    supports_refund=...,  # cancellation goes through a refund quote
    reports_refund_offer_status=...,
    revisioned=...,  # observations carry a revision
    reports_generation=...,  # observations carry a generation
)
```

The declaration is validated: `RESUBMIT` needs an idempotent create with the key bound before
execution; a finality lookup needs execution expiry; execution expiry needs a declared clock
skew. The three fictional providers:

| Capability | rail-osdm (A) | bus-legacy (B) | mobility-async (C) |
|---|---|---|---|
| booking flow, confirmation | HOLD_THEN_CONFIRM, SYNC | DIRECT, SYNC | DIRECT, ASYNC |
| webhooks | no | no | yes (Standard Webhooks) |
| lookup by client reference | IMMEDIATE | EVENTUAL (the index lags) | IMMEDIATE |
| idempotent create | KEY, 24 h, bound before execution | none | CLIENT_REF, 24 h, bound before execution |
| execution expiry, finality lookup | yes, yes | no, no | yes, yes |
| clock skew | 5 s | n/a | 5 s |
| unknown resolution | RESUBMIT | REVIEW | RESUBMIT |
| cancellation | by refund quote, confirmed only | none | free, idempotent, confirmed only |
| revision, generation | yes, yes | no, no | yes, yes |

## What each capability buys

- **Key bound before execution** lets the platform **resubmit** an attempt whose answer was lost:
  the provider replays the original outcome, reports "in progress" (uncertainty preserved) or
  creates exactly one reservation. Without it, a lost answer is one unsafe dispatch per command
  and the platform never sends that command again.
- **Execution expiry** gives every attempt a short expiry inside an absolute command cutoff, so
  no request can execute after the platform has stopped waiting for it. The cutoff is
  `first dispatch + min(idempotency window - skew - margin, max command lifetime)`.
- **A fenced lookup** is the only thing that lets the platform conclude "it did not happen"
  after a possible effect. The lookup must serialize with every in-flight mutation of the key
  and fence later ones; a clock check inside the provider's transaction is not enough, because
  a writer can pause after the check and commit later. Without finality, a possibly executed
  command that never becomes visible stays `UNRESOLVED` in an open review case. That is the
  honest answer, and the capability table tells clients to expect it.
- **A terminal reservation read for a key-bound provider with finality** is as final as a
  fence: the one reservation the key produced is terminal, and no other can carry the key.
- **Revisions and generations** let the platform order webhooks and polls: an older fact never
  overwrites a newer one, a newer generation is adopted only from an authoritative read, and an
  authoritative read behind what was already applied is quarantined for review.
- **Refund quotes** make cancellation a two-step command: the quote is validated against the
  terms the client authorised, persisted, and accepted by its exact identity; success is judged
  by that offer's status, never by the reservation's state alone.

## Webhooks

If the provider pushes events, the adapter exposes `reservation_from(payload)` mapping a verified
payload to a canonical `Reservation` (reference, our client reference, state, generation,
revision), and rejects undocumented event types or types inconsistent with the status they
carry. The platform verifies signatures with the Standard Webhooks library (several keys during
rotation), records a receipt and applies the observation in one transaction under the booking's
row lock, orders by generation then revision, binds an early event only after an authoritative
read agrees with it, and quarantines events about bookings under review as case evidence.

## Simulators and contract tests

Every provider here is a small FastAPI application with its own SQLite file and a `/_chaos`
endpoint behind an admin token: latency, jitter, random edge failures, rate limits, lookup lag,
and named failpoints that pause a handler between its checks and its commit, lose answers after
the commit, or drop requests before they arrive. A simulator enforces expiry and capacity inside
its serialized commit, so a paused writer never commits late and two writers never oversell.

A provider's contract test suite proves each declared capability against the simulator: the key
is bound before execution, an expired request is rejected, the fenced lookup is final under
`pause_after_check_before_commit`, the clock skew stays within the declared bound, refund offers
report their status, generations are reported, discovery by client reference works. Adding a
provider means adding its simulator, its adapter with capabilities, and its contract tests, then
registering the adapter in `orchestrator.application.wiring`. Nothing in the domain or the
application changes.

## Checklist

1. Classify every failure path by side effect, with a test per path.
2. Echo and check identity in every success body.
3. Declare capabilities you can prove; the contract suite is the proof.
4. Map times with their IANA zones; reject local times in a DST gap or fold rather than guess.
5. Mint canonical location and offer ids that encode the provider.
6. Keep the adapter free of retries, sleeps and caches.
7. If the provider cannot offer finality, say so in the capability table and expect review cases
   that stay open. Do not invent a timeout that pretends otherwise.

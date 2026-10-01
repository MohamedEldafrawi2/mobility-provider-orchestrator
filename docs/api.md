# API reference

Two listeners. The **public** listener (`:8000`) serves clients authenticated by an API key
(`X-API-Key`, stored hashed, resolving to a `client_id` that owns its bookings). The **admin**
listener (`:8001`) serves operators authenticated by a separate key (`X-Admin-Key`) and is never
published outside the deployment network. Both answer errors as RFC 9457 problem details
(`application/problem+json`) with a stable `code` member, and echo or generate
`X-Correlation-Id`. The interactive OpenAPI documentation is at `/docs` on each listener.

## Public endpoints

| Method and path | Purpose | Outcomes |
|---|---|---|
| `GET /healthz` | liveness | 200 |
| `GET /readyz` | readiness; PostgreSQL required, Redis loss degrades | 200, 503 |
| `GET /v1/locations?query=&limit=` | location search across providers | 200 |
| `GET /v1/trips?from=&to=&departureDate=&adults=&children=` | trip search; partial results with a per-provider report | 200, 404 `location-not-found`, 503 `search-unavailable` |
| `GET /v1/offers/{offerId}` | an offer, for as long as the provider advertised it valid | 200, 404 `offer-unavailable` |
| `POST /v1/bookings` | create a booking (`Idempotency-Key` required) | 201, 202, 400, 404, 409, 422 (see below) |
| `GET /v1/bookings/{bookingId}` | the booking, with `unresolved`, `unresolved_reason`, commands summary, refund info | 200, 404 `booking-not-found` |
| `POST /v1/bookings/{bookingId}/cancel` | cancel under authorised terms (`Idempotency-Key` required) | 200, 202, 409 (see below) |
| `POST /v1/providers/{provider}/webhooks` | inbound signed provider events | 200, 400, 401, 404, 413, 503 |
| `GET /v1/providers` | capabilities and breaker state per purpose | 200 |

### Creating a booking

```http
POST /v1/bookings
X-API-Key: ...
Idempotency-Key: 5f1c...
Content-Type: application/json

{"offer_id": "off_rail_...", "passengers": [{"full_name": "Ada Lovelace"}], "contact_email": "ada@example.org"}
```

| Status | When |
|---|---|
| 201 | the CREATE command is `SUCCEEDED`: the booking is `CONFIRMED` (or, on a replay, its current state) |
| 202 | the CREATE command is still `OPEN`: the booking is `HELD`, `PENDING_PROVIDER`, `UNKNOWN` or under review; `unresolved: true`, `unresolved_reason`, `Location`, `Retry-After` |
| 400 `idempotency-key-required` | no key, or longer than 128 characters |
| 400 `validation-error` | the body is malformed, or carries a field the API does not define |
| 404 `offer-unavailable` | the offer expired or was never stored; search again |
| 409 `offer-mismatch` | the passenger count differs from what the offer was priced for |
| 422 `idempotency-key-reuse` | the key was used with a different request |
| 422 `booking-rejected` | a replay of a command the provider definitively rejected |
| 422 `booking-not-submitted` | a replay of a command abandoned before any dispatch |

Replays carry `Idempotent-Replayed: true`. The status comes from the command's disposition,
never from the booking's current state, so the original response and its replays never disagree.

### Cancelling a booking

```http
POST /v1/bookings/{bookingId}/cancel
X-API-Key: ...
Idempotency-Key: 9ab2...
Content-Type: application/json

{"max_fee": {"amount_minor": 700, "currency": "CHF"}}
```

`max_fee` is the most the client authorises; omit it to accept only a free cancellation. The
provider's quote is accepted only inside these terms and by its exact identity.

| Status | When |
|---|---|
| 200 | the CANCEL command is `SUCCEEDED`: the booking is `CANCELLED`; `refund` carries the accepted quote |
| 202 | the cancellation is in progress (`CANCELLING`) or under review |
| 409 `booking-not-cancellable` | the booking is not `CONFIRMED`, the provider does not cancel, or the command was `REFUSED` (for example after its cutoff) |
| 409 `cancellation-terms-changed` | the quote is worse than the authorised terms; the quote is in the problem |
| 422 `idempotency-key-reuse` | the key was used with a different request or another booking |
| 400 `validation-error` | an unknown field such as an obsolete flag; authorisation fields are never silently dropped |

### Webhooks

The body is read under a size bound and verified with Standard Webhooks (`webhook-id`,
`webhook-timestamp`, `webhook-signature`; several keys during rotation). Any verification failure
is 401 `invalid-signature`. A verified event is answered 200 with its `outcome`: `APPLIED`,
`DUPLICATE`, `STALE`, `SUPERSEDED_GENERATION`, `NEWER_GENERATION`, `UNMATCHED`,
`CONTRADICTORY`, `NO_CHANGE` or `QUARANTINED`. A 503 `lookup-failed` asks the provider to retry
an early event whose authoritative read could not be made.

## Admin endpoints

| Method and path | Purpose | Outcomes |
|---|---|---|
| `GET /review` | open cases: age, reason, remediable, outstanding command, implicated reservations | 200 |
| `GET /review/{bookingId}` | commands, attempts, evidence with validity, events | 200, 404 `no-open-case` |
| `POST /review/{bookingId}/reconcile` | authoritative lookups now, stored as dated evidence; may settle the booking | 200, 404, 503 `lookup-failed`, 503 `lookup-not-admitted` |
| `POST /review/{bookingId}/resolve` | close the case into the state the evidence supports (`expected_version`, `reason`) | 200, 404, 409 `version-mismatch`, 409 `lookup-required`, 409 `case-not-closable` |
| `GET /providers` | admission state in operational detail (calls in window, failure rates) | 200 |
| `GET /metrics` | Prometheus exposition | 200 |

There is no override: `resolve` closes a case only when its complete evidence set allows it, and
a retried resolve against the version it was issued for replays its original result.

## Problem details

```json
{
  "type": "urn:mpo:problem:offer-mismatch",
  "title": "Offer mismatch",
  "status": 409,
  "detail": "the offer is priced for 1 passengers",
  "instance": "/v1/bookings",
  "code": "offer-mismatch",
  "correlation_id": "9362d02f4aa449a1a00258d7e69b38e1",
  "expected": 1
}
```

`type` is `MPO_PROBLEM_TYPE_BASE` plus the code. Generic failures use `not-found`,
`method-not-allowed`, `unauthorized`, `forbidden`, `payload-too-large`, `rate-limited`,
`validation-error` and `internal-error`; an unexpected exception never leaks its message.
Problems that carry `retry_after` also send it as the `Retry-After` header.

## Booking representation

```json
{
  "id": "bk_...",
  "state": "HELD",
  "provider": "rail-osdm",
  "provider_booking_ref": "RB000012",
  "provider_generation": 1,
  "unresolved": true,
  "unresolved_reason": null,
  "failure_code": null,
  "confirmation_deadline": "2026-06-15T07:14:00+00:00",
  "refund": null,
  "offer": {"id": "off_rail_...", "provider": "rail-osdm", "total_price": {"amount_minor": 3400, "currency": "CHF"}},
  "passengers": [{"full_name": "Ada Lovelace"}],
  "contact_email": "ada@example.org",
  "version": 4,
  "created_at": "2026-06-15T07:03:51+00:00",
  "command": {"kind": "CREATE", "disposition": "OPEN"}
}
```

`unresolved` is true in every state that is not `CONFIRMED`, `FAILED` or `CANCELLED`;
`unresolved_reason` names why (`provider-outcome-unknown`, `confirmation-outcome-unknown`,
`quota-outage`, `pending-overdue`, `observation-from-a-newer-generation`, ...). `command` is
present on the responses to `POST /v1/bookings` and `POST .../cancel` and names the command the
response reports on. A booking in `NEEDS_REVIEW` stays visible as unresolved for as long as its
case is open.

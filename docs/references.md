# Public references used in the design

Everything in this repository is built from public standards, public specifications, and fictional
providers. No proprietary API, schema, or documentation was used. This page records the public
sources consulted while designing the platform, so reviewers can check the claims made in the
architecture documents. Versions checked in September 2026.

## Transport distribution standards

| Source | What was taken from it |
|---|---|
| [OSDM specification](https://osdm.io/spec/) (UIC, Apache-2.0, [GitHub](https://github.com/UnionInternationalCheminsdeFer/OSDM), online API v3.9.0) | Concept vocabulary only: places, trips, offers with `validFrom`/`validUntil`, bookings made of booked offers, `BookingPartStatus` (`PREBOOKED`, `ON_HOLD`, `CONFIRMED`, `FULFILLED`, `CANCELLED`, `RELEASED`, `REFUNDED`, `ERROR`), `confirmationTimeLimit`, two-step refund (`refund-offers` proposed then confirmed), `release-offers` for unfulfilled bookings, `externalRef` echoed back to the caller, an `Idempotency-Key` header, a `traceparent` header, 202 "fulfillment initiated" responses, and a webhook `BookingEvent` with `BookingChangeType` values such as `BOOKING_TRIP_CONFIRMED` and `FULFILLMENT_REFUNDED`. The fictional rail provider mimics these shapes loosely; it is not an OSDM implementation. |
| [OSDM business capabilities](https://osdm.io/spec/business-capabilities/) | The lifecycle vocabulary pre-booked, booked, fulfilled, and the after-sales set refund, exchange, release. |

## HTTP and API standards

| Source | What was taken from it |
|---|---|
| [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457.html) | Error response format for the unified API (`application/problem+json`, `type`, `title`, `status`, `detail`, `instance`, plus extension members). |
| [Idempotency-Key header draft-07](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-idempotency-key-header-07) (IETF HTTPAPI WG, expired 2026-04, still the de-facto reference) | Semantics for `POST /bookings`: same key and same payload returns the original result, same key and different payload returns 422, an in-flight duplicate returns 409, a missing key returns 400. |
| [Standard Webhooks](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md) | Webhook signing for the fictional async provider: `webhook-id`, `webhook-timestamp`, `webhook-signature` headers, HMAC-SHA256 over `id.timestamp.body`, `v1,` prefix, timestamp tolerance window, using `webhook-id` as the consumer-side idempotency key, and exponential retry with jitter on the sender side. |
| [W3C Trace Context](https://www.w3.org/TR/trace-context/) | `traceparent` propagation to providers, matching what OSDM also specifies. |

## Observability

| Source | What was taken from it |
|---|---|
| [OpenTelemetry HTTP metric semantic conventions](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/) | Stable names `http.server.request.duration` and `http.client.request.duration` (histogram, seconds) and the recommended bucket boundaries, reused for provider latency metrics. |
| [OpenTelemetry Python contrib](https://github.com/open-telemetry/opentelemetry-python-contrib) | Auto-instrumentation for FastAPI, httpx, SQLAlchemy, redis. |

## Libraries and runtime versions (PyPI, 2026-09-16)

| Package | Version | Note |
|---|---|---|
| Python | 3.13 | 3.14 is supported by every dependency below; 3.13 chosen for tooling maturity |
| fastapi | 0.141.1 | |
| pydantic / pydantic-settings | 2.13.5 / 2.15.0 | |
| sqlalchemy (async) + asyncpg | 2.0.54 / 0.31.0 | |
| alembic | 1.20.0 | |
| httpx + respx | 0.28.1 / 0.23.1 | provider HTTP client and test mocking |
| standardwebhooks | 1.1.0 | reference verifier for Standard Webhooks signatures |
| tenacity | 9.1.4 | retry with backoff and jitter, async-native |
| structlog | 26.1.0 | |
| opentelemetry-sdk / instrumentation | 1.44.0 / 0.65b0 | |
| redis | 8.1.0 | |
| pytest-asyncio / hypothesis / testcontainers | 1.4.0 / 6.168.0 / 4.15.0 | |
| uv / ruff / mypy | 0.12.15 / 0.16.8 / 2.3.1 | |
| PostgreSQL / Redis images | 18 / 8 | |
| Jaeger / Prometheus / Grafana | 2.x / 3.x / 13.x | |

Rejected or noted alternatives:

- Circuit breaker libraries: `aiobreaker` (last release 2021) is unmaintained; `purgatory` 3.0.1 (2024-11) is a viable, maintained asyncio breaker with in-memory or Redis state. The breaker is written in-repo for a different reason: it is evaluated per attempt inside the same loop as the retry budget and rate limiter and emits one metric set, and it is deliberately small enough to read. See [ADR 008](adr/008-admission-by-purpose.md).
- Postgres job queues `pgqueuer` 1.4.0 (active, asyncpg) and `procrastinate` 3.9.0 (psycopg only) were considered; the background work here is scheduled scans over the bookings table, not a general job queue, so `FOR UPDATE SKIP LOCKED` is used directly. `pgqueuer` is the named step-up in the scaling document. See [ADR 006](adr/006-work-table-with-leases.md).
- Temporal (used by a similar public project, [hotel-booking-orchestrator](https://github.com/AnuragChauhan1120/hotel-booking-orchestrator)) was rejected because a workflow engine hides the booking state machine, which is the thing this project is meant to make explicit. See [ADR 003](adr/003-explicit-state-machine.md).

## Sources for the design decisions

Cross-checked directly against the published documentation.

| Decision | Source | What it shows |
|---|---|---|
| Hold then confirm as a provider flow | [OSDM processes](https://osdm.io/spec/processes/), [OSDM models](https://osdm.io/spec/models/) | Pre-booking and confirmation are distinct steps bounded by `confirmationTimeLimit`; on-hold is a separate extension |
| Hold then confirm | [Duffel: holding orders and paying later](https://duffel.com/docs/guides/holding-orders-and-paying-later) | Held orders are exposed with a payment deadline because a payment step exists |
| Hold then confirm | [Stripe PaymentIntents](https://docs.stripe.com/payments/payment-intents), [PaymentIntent lifecycle](https://docs.stripe.com/payments/paymentintents/lifecycle) | `requires_confirmation` exists as a state; most integrations confirm at creation |
| Hold then confirm | [Amadeus Self-Service booking FAQ](https://admin.developers.amadeus.com/self-service/apis-docs/guides/developer-guides/faq/) | Order creation and ticket issuance are separated; payment is outside the API |
| Refund quotes inside cancellation | [OSDM after-sales processes](https://osdm.io/spec/after-sales-processes/) | Refund offers carry amounts, fees, and validity; accepting an offer performs the refund |
| Refund quotes | [Duffel order cancellations](https://duffel.com/docs/api/order-cancellations) | Create a pending cancellation with refund amount and expiry, then confirm; only the latest quote is confirmable |
| In-repo circuit breaker | [purgatory on PyPI](https://pypi.org/project/purgatory/), [changelog](https://mardiros.github.io/purgatory/user/changelog.html), [model](https://mardiros.github.io/purgatory/develop/domain/model.html), [listeners](https://mardiros.github.io/purgatory/develop/service/circuitbreaker.html) | 3.0.1 (2024-11-02); async context manager per call; event listeners; consecutive-failure model, not a sliding-window ratio |
| Circuit breaker | [Polly and HttpClientFactory](https://github.com/App-vNext/Polly/wiki/Polly-and-HttpClientFactory), [Resilience4j circuit breaker](https://resilience4j.readme.io/docs/circuitbreaker), [Resilience4j aspect order](https://resilience4j.readme.io/docs/getting-started-3), [Hystrix: how it works](https://github.com/Netflix/Hystrix/wiki/How-it-Works) | Retry composed outside the breaker so every attempt re-checks circuit state; breaker and concurrency limiting are separate |
| Review tooling | [Stripe: responding to disputes](https://docs.stripe.com/disputes/responding), [Stripe user roles](https://docs.stripe.com/get-started/account/teams/roles) | Evidence-based case handling with role-restricted permissions |
| Review tooling | [Travelport ticketing and queues](https://developer.travelport.com/docs/flights/guides/ticketing-guide) | Queue management distributes booking work needing action |
| Review tooling | [Compensating transaction pattern (Microsoft)](https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction) | Preserve progress, make steps resumable and idempotent, alert when manual intervention is needed |
| Unknown outcomes | [AWS Builders' Library: making retries safe with idempotent APIs](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/), [Timeouts, retries, and backoff with jitter](https://d1.awsstatic.com/builderslibrary/pdfs/timeouts-retries-and-backoff-with-jitter.pdf) | Idempotency lifetimes; retry budgets; side-effecting retries |
| Outbox and sagas | [Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html), [Saga](https://microservices.io/patterns/data/saga.html) | Durable delivery tracking; saga isolation limits |
| Object-level authorization | [OWASP API1:2023](https://api-security.owasp.org/editions/2023/en/0xa1-broken-object-level-authorization/) | Booking IDs are not authorization |
| Local time policy | [PEP 495](https://peps.python.org/pep-0495/) | DST fold and gap disambiguation |

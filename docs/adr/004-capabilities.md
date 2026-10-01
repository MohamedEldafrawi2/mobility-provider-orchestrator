# ADR 004: A capability model verified by contract tests

Status: accepted

## Context

The platform must know what it may conclude about a provider: whether a lost answer can be
resubmitted, whether a negative conclusion is ever possible, how observations are ordered.

## Decision

Each adapter declares `ProviderCapabilities` (booking flow, idempotency and key binding,
execution expiry, finality lookup, clock skew, cancellation and refund semantics, revisions and
generations). The declaration is validated for consistency and every claim is proven against the
provider's simulator by a contract test. Orchestration reasons about capabilities only.

## Consequences

- Adding a provider changes nothing in the domain or the application.
- A provider that cannot offer finality is handled honestly: permanent uncertainty is a visible
  outcome, not a timeout dressed as a fact.

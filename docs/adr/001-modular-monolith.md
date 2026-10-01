# ADR 001: Modular monolith with API and worker roles

Status: accepted

## Context

The platform is one bounded context: bookings over providers. The real boundary is on the
provider side, where each provider has its own protocol, storage and failure modes.

## Decision

One Python package with two process roles, the API and the worker, sharing one code base, one
database and one set of services. Providers are separate simulators with their own storage.
Python 3.13 and FastAPI, for community reach and because asyncio fits fan-out IO.

## Consequences

- One deployment unit to reason about; one transaction boundary around booking state.
- Horizontal scaling by adding API or worker processes; the work table is claimed with
  `SKIP LOCKED` and leases.
- Layering is enforced by an import-linter contract rather than by process boundaries.

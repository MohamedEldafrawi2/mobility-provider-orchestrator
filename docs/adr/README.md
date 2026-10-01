# Architecture decision records


| ADR | Decision |
|---|---|
| [001](001-modular-monolith.md) | Modular monolith with API and worker roles |
| [002](002-canonical-model-and-adapters.md) | Canonical domain model plus per-provider adapters |
| [003](003-explicit-state-machine.md) | An explicit booking state machine, no workflow engine |
| [004](004-capabilities.md) | A capability model verified by contract tests |
| [005](005-commands-attempts-and-settlement.md) | Commands with journaled attempts, expiries, and evidence-based settlement |
| [006](006-work-table-with-leases.md) | The bookings table as the work source, with leases and a reserved confirmation loop |
| [007](007-observation-ordering.md) | Observations ordered by provider generation then revision |
| [008](008-admission-by-purpose.md) | Admission partitioned by purpose, fail-closed quota, a stated load envelope |
| [009](009-idempotency-by-command.md) | Idempotency keyed to commands, replays from one function |
| [010](010-search-aggregation.md) | Search: one deadline, per-provider budgets, partial results |
| [011](011-webhooks.md) | Webhooks: reference library, receipts in the applying transaction |
| [012](012-review-cases.md) | Review cases close only on complete evidence, with no override |
| [013](013-cancellation-under-authorised-terms.md) | Refund inside cancellation under authorised terms, phases persisted |
| [014](014-observability.md) | OpenTelemetry metrics and traces, Prometheus and Grafana as a profile |

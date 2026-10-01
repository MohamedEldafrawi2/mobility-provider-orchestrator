"""Recovery policy values, derived from settings once and passed explicitly."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from orchestrator.config import Settings
from orchestrator.domain.cutoffs import ExpiryPolicy


@dataclass(frozen=True, slots=True)
class RecoveryPolicy:
    lookup_budget: int
    max_attempts: int
    reconcile_backoff: timedelta
    reschedule_backoff: timedelta
    abandon_after: timedelta
    submitting_stale_after: timedelta
    mutation_timeout: timedelta
    lease_ttl: timedelta
    expiry: ExpiryPolicy
    confirm_budget: int
    pending_poll: timedelta
    pending_max_age: timedelta

    @classmethod
    def from_settings(cls, s: Settings) -> RecoveryPolicy:
        return cls(
            lookup_budget=s.lookup_budget,
            max_attempts=s.create_max_attempts,
            reconcile_backoff=timedelta(seconds=s.reconcile_backoff_seconds),
            reschedule_backoff=timedelta(seconds=s.reschedule_backoff_seconds),
            abandon_after=timedelta(seconds=s.abandon_after_seconds),
            submitting_stale_after=timedelta(seconds=s.submitting_stale_after_seconds),
            mutation_timeout=timedelta(seconds=s.provider_mutation_timeout_seconds),
            lease_ttl=timedelta(seconds=s.worker_lease_seconds),
            expiry=ExpiryPolicy(
                attempt_ttl=timedelta(seconds=s.attempt_ttl_seconds),
                acceptance_ttl=timedelta(seconds=s.acceptance_ttl_seconds),
                max_command_lifetime=timedelta(seconds=s.max_command_lifetime_seconds),
                margin=timedelta(seconds=s.expiry_margin_seconds),
            ),
            confirm_budget=s.confirm_budget,
            pending_poll=timedelta(seconds=s.pending_poll_seconds),
            pending_max_age=timedelta(seconds=s.pending_max_age_seconds),
        )

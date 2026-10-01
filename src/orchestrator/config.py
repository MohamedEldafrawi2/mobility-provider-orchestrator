"""Application settings, loaded from the environment with the ``MPO_`` prefix.

Secrets never appear in plain text here: client API keys and the operator key are stored as
SHA-256 hashes. The plain keys live only with the callers that use them.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ClientId = str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MPO_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    environment: Literal["dev", "test", "prod"] = "dev"

    database_url: str = "postgresql+asyncpg://mpo:mpo@localhost:5432/mpo"
    redis_url: str = "redis://localhost:6379/0"

    host: str = "0.0.0.0"  # noqa: S104 - container-facing bind address; publishing is compose's decision
    public_port: int = 8000
    admin_port: int = 8001

    # NoDecode: the raw env string reaches the validator instead of being JSON-decoded first.
    api_keys: Annotated[dict[str, ClientId], NoDecode] = Field(
        default_factory=dict,
        description='Map of SHA-256(api key) -> client_id, parsed from "client_id=hexhash,...".',
    )
    admin_key_hash: str = ""

    problem_type_base: str = "urn:mpo:problem:"
    log_json: bool = True
    # Tracing is an operational profile: without an endpoint nothing is installed.
    otel_exporter_otlp_endpoint: str | None = None  # e.g. http://otel-collector:4318
    otel_service_name: str = "mobility-provider-orchestrator"

    readiness_timeout_seconds: float = 2.0

    # Providers
    bus_legacy_url: str = "http://localhost:9002"
    rail_osdm_url: str = "http://localhost:9001"
    mobility_async_url: str = "http://localhost:9003"
    # Standard Webhooks secrets for Provider C, comma separated (several during rotation).
    mobility_async_webhook_secrets: str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
    webhook_max_body_bytes: int = 65_536
    provider_mutation_timeout_seconds: float = 8.0
    provider_read_timeout_seconds: float = 2.0
    provider_connect_timeout_seconds: float = 2.0

    # Admission (docs/resilience-strategy.md): every purpose has its own bulkhead, breaker, quota
    # share and
    # retry tokens. The allowance is what each (fictional) provider documents per second.
    provider_allowance_per_second: float = 50.0
    quota_burst_seconds: float = 1.0
    quota_timeout_seconds: float = 0.25
    bulkhead_search: int = 32
    # Search (ADR 010): one request deadline, a smaller budget per provider, no retries.
    search_deadline_seconds: float = 3.0
    search_provider_budget_seconds: float = 2.0
    search_max_offers_per_provider: int = 50
    search_catalogue_ttl_seconds: float = 60.0
    bulkhead_create: int = 16
    bulkhead_confirm: int = 16
    bulkhead_cancel: int = 8
    bulkhead_lookup: int = 8
    admission_wait_seconds_search: float = 0.2
    admission_wait_seconds: float = 2.0
    breaker_window_seconds: float = 30.0
    breaker_buckets: int = 10
    breaker_minimum_calls: int = 10
    breaker_failure_rate_threshold: float = 0.5
    breaker_open_seconds: float = 10.0
    breaker_half_open_calls: int = 2
    retry_max_attempts: int = 4
    retry_base_seconds: float = 0.2
    retry_cap_seconds: float = 5.0
    retry_token_capacity: int = 50

    # Worker (ADR 006): the confirmation loop's reserved pool slice and concurrency.
    confirm_pool_size: int = 8  # sized to the confirmation loop's concurrency
    worker_concurrency: int = 4
    confirm_loop_concurrency: int = 8

    # Recovery policy (docs/booking-state-machine.md): how hard the platform tries before honest
    # escalation.
    lookup_budget: int = 3
    create_max_attempts: int = 8  # attempts that certainly had no effect before giving up
    # Execution expiry (docs/architecture.md): attempt expiries and the command cutoff.
    attempt_ttl_seconds: float = 30.0
    acceptance_ttl_seconds: float = 60.0
    max_command_lifetime_seconds: float = 600.0
    expiry_margin_seconds: float = 5.0
    confirm_budget: int = 3  # replacement CONFIRM commands per booking
    pending_poll_seconds: float = 5.0  # PENDING_PROVIDER: poll cadence between webhooks
    pending_max_age_seconds: float = 900.0  # PENDING_PROVIDER: overdue, review
    reconcile_backoff_seconds: float = 5.0
    reschedule_backoff_seconds: float = 5.0
    abandon_after_seconds: float = 300.0
    submitting_stale_after_seconds: float = 30.0
    worker_lease_seconds: float = 30.0
    worker_poll_interval_seconds: float = 1.0
    worker_metrics_port: int = 8002  # internal: /metrics with the operator key, /healthz

    @field_validator("api_keys", mode="before")
    @classmethod
    def _parse_api_keys(cls, value: object) -> dict[str, ClientId]:
        """Accept the compact env form ``client_id=hexhash,client_id=hexhash``."""
        if isinstance(value, dict):
            return {str(k): str(v) for k, v in value.items()}
        if not isinstance(value, str):
            raise TypeError("api_keys must be a mapping or a comma-separated string")
        parsed: dict[str, ClientId] = {}
        for entry in filter(None, (part.strip() for part in value.split(","))):
            client_id, sep, key_hash = entry.partition("=")
            if not sep or not client_id or len(key_hash) != 64:
                raise ValueError(f"malformed api key entry for client {client_id!r}")
            parsed[key_hash.lower()] = client_id
        return parsed


@lru_cache
def get_settings() -> Settings:
    return Settings()

"""Construct the services both applications and the worker share."""

from __future__ import annotations

from dataclasses import dataclass

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncEngine

from orchestrator.application.booking_create import BookingCreator
from orchestrator.application.cancellation import Canceller
from orchestrator.application.confirmation import Confirmer
from orchestrator.application.offers import OfferStore
from orchestrator.application.policy import RecoveryPolicy
from orchestrator.application.recovery import Recovery
from orchestrator.application.review import ReviewService
from orchestrator.application.search import SearchService
from orchestrator.application.webhooks import WebhookService
from orchestrator.config import Settings
from orchestrator.persistence.db import create_engine, dispose_engine
from orchestrator.persistence.uow import UnitOfWorkFactory
from orchestrator.providers.bus_legacy import BUS_LEGACY, BusLegacyAdapter
from orchestrator.providers.mobility_async import MOBILITY_ASYNC, MobilityAsyncAdapter
from orchestrator.providers.rail_osdm import RAIL_OSDM, RailOsdmAdapter
from orchestrator.providers.registry import ProviderRegistry
from orchestrator.providers.transport import PurposeClients
from orchestrator.resilience import (
    DEFAULT_SHARES,
    AdmissionController,
    BreakerConfig,
    Purpose,
    PurposeConfig,
    QuotaPolicy,
    RedisQuota,
    RetryPolicy,
)
from orchestrator.telemetry import configure_metrics


@dataclass
class Services:
    settings: Settings
    engine: AsyncEngine
    confirm_engine: AsyncEngine
    redis: aioredis.Redis
    clients: list[PurposeClients]
    registry: ProviderRegistry
    admission: AdmissionController
    uow: UnitOfWorkFactory
    confirm_uow: UnitOfWorkFactory
    policy: RecoveryPolicy
    offers: OfferStore
    search: SearchService
    creator: BookingCreator
    confirmer: Confirmer
    canceller: Canceller
    recovery: Recovery
    review: ReviewService
    webhooks: WebhookService

    async def close(self) -> None:
        self.admission.close()
        for clients in self.clients:
            await clients.aclose()
        await self.redis.aclose()
        await dispose_engine(self.confirm_engine)
        await dispose_engine(self.engine)


def purpose_configs(settings: Settings) -> dict[Purpose, PurposeConfig]:
    breaker = BreakerConfig(
        window_seconds=settings.breaker_window_seconds,
        buckets=settings.breaker_buckets,
        minimum_calls=settings.breaker_minimum_calls,
        failure_rate_threshold=settings.breaker_failure_rate_threshold,
        open_seconds=settings.breaker_open_seconds,
        half_open_max_calls=settings.breaker_half_open_calls,
    )
    limits = {
        Purpose.SEARCH: settings.bulkhead_search,
        Purpose.CREATE: settings.bulkhead_create,
        Purpose.CONFIRM: settings.bulkhead_confirm,
        Purpose.CANCEL: settings.bulkhead_cancel,
        Purpose.LOOKUP: settings.bulkhead_lookup,
    }
    return {
        purpose: PurposeConfig(
            bulkhead_limit=limit,
            bulkhead_max_wait=(
                settings.admission_wait_seconds_search
                if purpose is Purpose.SEARCH
                else settings.admission_wait_seconds
            ),
            breaker=breaker,
            retry_token_capacity=settings.retry_token_capacity,
        )
        for purpose, limit in limits.items()
    }


async def build_services(settings: Settings, *, engine: AsyncEngine | None = None) -> Services:
    configure_metrics()
    engine = engine or create_engine(settings)
    # The confirmation loop's reserved slice: its own pool, never lent to another loop.
    confirm_engine = create_engine(settings, pool_size=settings.confirm_pool_size, max_overflow=0)
    redis_client: aioredis.Redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    bus_clients = PurposeClients.build(
        settings.bus_legacy_url,
        read_timeout=settings.provider_read_timeout_seconds,
        mutation_timeout=settings.provider_mutation_timeout_seconds,
        connect_timeout=settings.provider_connect_timeout_seconds,
        pool_limits={
            Purpose.SEARCH: settings.bulkhead_search,
            Purpose.CREATE: settings.bulkhead_create,
            Purpose.CONFIRM: settings.bulkhead_confirm,
            Purpose.CANCEL: settings.bulkhead_cancel,
            Purpose.LOOKUP: settings.bulkhead_lookup,
        },
    )
    rail_clients = PurposeClients.build(
        settings.rail_osdm_url,
        read_timeout=settings.provider_read_timeout_seconds,
        mutation_timeout=settings.provider_mutation_timeout_seconds,
        connect_timeout=settings.provider_connect_timeout_seconds,
        pool_limits={
            Purpose.SEARCH: settings.bulkhead_search,
            Purpose.CREATE: settings.bulkhead_create,
            Purpose.CONFIRM: settings.bulkhead_confirm,
            Purpose.CANCEL: settings.bulkhead_cancel,
            Purpose.LOOKUP: settings.bulkhead_lookup,
        },
    )
    async_clients = PurposeClients.build(
        settings.mobility_async_url,
        read_timeout=settings.provider_read_timeout_seconds,
        mutation_timeout=settings.provider_mutation_timeout_seconds,
        connect_timeout=settings.provider_connect_timeout_seconds,
        pool_limits={
            Purpose.SEARCH: settings.bulkhead_search,
            Purpose.CREATE: settings.bulkhead_create,
            Purpose.CONFIRM: settings.bulkhead_confirm,
            Purpose.CANCEL: settings.bulkhead_cancel,
            Purpose.LOOKUP: settings.bulkhead_lookup,
        },
    )
    registry = ProviderRegistry(
        [
            BusLegacyAdapter(bus_clients),
            RailOsdmAdapter(rail_clients),
            MobilityAsyncAdapter(async_clients),
        ]
    )
    quota = RedisQuota(
        redis_client,
        {
            BUS_LEGACY: QuotaPolicy(
                settings.provider_allowance_per_second,
                DEFAULT_SHARES,
                burst_seconds=settings.quota_burst_seconds,
            ),
            RAIL_OSDM: QuotaPolicy(
                settings.provider_allowance_per_second,
                DEFAULT_SHARES,
                burst_seconds=settings.quota_burst_seconds,
            ),
            MOBILITY_ASYNC: QuotaPolicy(
                settings.provider_allowance_per_second,
                DEFAULT_SHARES,
                burst_seconds=settings.quota_burst_seconds,
            ),
        },
        timeout_seconds=settings.quota_timeout_seconds,
    )
    admission = AdmissionController(quota, purposes=purpose_configs(settings))
    retry = RetryPolicy(
        max_attempts=settings.retry_max_attempts,
        base_seconds=settings.retry_base_seconds,
        cap_seconds=settings.retry_cap_seconds,
    )
    uow = UnitOfWorkFactory(engine)
    confirm_uow = UnitOfWorkFactory(confirm_engine)
    policy = RecoveryPolicy.from_settings(settings)
    offers = OfferStore(redis_client)
    creator = BookingCreator(uow, registry, policy, admission=admission, retry=retry)
    # The confirmer's database work rides the reserved pool slice, like its loop (5.4).
    confirmer = Confirmer(
        confirm_uow, registry, creator, policy, admission=admission, retry=retry, request_uow=uow
    )
    creator.confirmer = confirmer
    canceller = Canceller(uow, registry, creator, policy, admission=admission)
    recovery = Recovery(
        uow,
        registry,
        creator,
        policy,
        admission=admission,
        retry=retry,
        confirmer=confirmer,
        canceller=canceller,
    )
    return Services(
        settings=settings,
        engine=engine,
        confirm_engine=confirm_engine,
        redis=redis_client,
        clients=[bus_clients, rail_clients, async_clients],
        registry=registry,
        admission=admission,
        uow=uow,
        confirm_uow=confirm_uow,
        policy=policy,
        offers=offers,
        search=SearchService(
            registry,
            offers,
            admission=admission,
            deadline_seconds=settings.search_deadline_seconds,
            provider_budget_seconds=settings.search_provider_budget_seconds,
            max_offers_per_provider=settings.search_max_offers_per_provider,
            catalogue_ttl_seconds=settings.search_catalogue_ttl_seconds,
        ),
        creator=creator,
        confirmer=confirmer,
        canceller=canceller,
        recovery=recovery,
        review=ReviewService(
            uow, registry, creator, admission=admission, canceller=canceller, policy=policy
        ),
        webhooks=WebhookService(
            uow,
            registry,
            creator,
            secrets={
                MOBILITY_ASYNC: [
                    s.strip()
                    for s in settings.mobility_async_webhook_secrets.split(",")
                    if s.strip()
                ]
            },
            max_body_bytes=settings.webhook_max_body_bytes,
            admission=admission,
        ),
    )

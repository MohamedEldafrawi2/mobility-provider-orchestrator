"""The resilience wrapper against real infrastructure (edge cases 48, 50).

- quota fails closed: with the quota service unavailable nothing is dispatched, nothing is
  presumed, and the booking waits with reason ``quota-outage`` until the service is back;
- the confirmation loop runs on its reserved pool slice while the main pool is exhausted;
- the Redis quota agrees with the in-memory arithmetic the benchmark uses;
- the admin listener exposes the metrics and the public listener the providers' state.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncConnection

from orchestrator.domain import BookingId, CommandKind, ProviderCode
from orchestrator.providers.bus_legacy import BUS_LEGACY
from orchestrator.resilience import (
    DEFAULT_SHARES,
    REASON_DEADLINE,
    REASON_QUOTA_OUTAGE,
    LocalQuota,
    Purpose,
    QuotaPolicy,
    QuotaResult,
    QuotaUnavailableError,
    RedisQuota,
)
from tests.integration.test_booking_b import (  # noqa: F401 - fixtures
    CLIENT,
    LOOKUP_BUDGET,
    OPERATOR,
    Stack,
    _check_persisted,
    _create,
    _model,
    _offer_id,
    async_url,
    bus,
    postgres_url,
    rail_url,
    redis_url,
    stack,
)
from tests.model.b_reference_model import BReferenceModel, DispatchMode

pytestmark = pytest.mark.integration


class _QuotaDown:
    """The quota service is unreachable: every take raises, nothing is granted."""

    def policy_for(self, provider: ProviderCode) -> QuotaPolicy:
        return QuotaPolicy(1.0, DEFAULT_SHARES)

    async def take(self, provider: ProviderCode, purpose: Purpose) -> QuotaResult:
        raise QuotaUnavailableError("connection refused")


async def test_quota_outage_no_presumed_expiry(stack: Stack) -> None:
    """6.6 #48: all purposes fail closed; the booking reports the outage and waits."""
    offer_id = await _offer_id(stack)
    admission = stack.services.admission
    live_quota = admission.quota
    admission.quota = _QuotaDown()
    try:
        response = await _create(stack, offer_id, "k-outage")
        assert response.status_code == 202, response.text
        body = response.json()
        assert body["state"] == "CREATED" and body["unresolved_reason"] == REASON_QUOTA_OUTAGE
        booking_id = body["id"]
        assert await stack.bus.truth() == [], "nothing was dispatched"
        # The worker keeps refusing, and keeps the booking, while the outage lasts.
        await stack.tick()
        waiting = await stack.get(booking_id)
        assert waiting["state"] == "CREATED" and waiting["unresolved_reason"] == REASON_QUOTA_OUTAGE
        model = BReferenceModel(client_ref="x", lookup_budget=LOOKUP_BUDGET, max_age=timedelta(0))
        model.dispatch(DispatchMode.LOCAL_DENIAL)
        model.dispatch(DispatchMode.LOCAL_DENIAL)
        await _check_persisted(stack, model, booking_id, [])
        async with stack.creator.uow() as store:
            command = await store.command_for(BookingId(booking_id), CommandKind.CREATE)
        assert all(a.dispatch_marked_at is None for a in command.attempts), "journaled refusals"
        assert len(command.attempts) >= 2
    finally:
        admission.quota = live_quota
    # The service is back: the next pass submits normally.
    await stack.tick()
    final = await stack.get(booking_id)
    assert final["state"] == "CONFIRMED" and final["unresolved_reason"] is None
    ok = BReferenceModel(client_ref="x", lookup_budget=LOOKUP_BUDGET, max_age=timedelta(0))
    ok.dispatch(DispatchMode.LOCAL_DENIAL)
    ok.dispatch(DispatchMode.LOCAL_DENIAL)
    ok.dispatch(DispatchMode.OK)
    await _check_persisted(stack, ok, booking_id, await stack.bus.truth())


async def test_confirm_loop_isolated(stack: Stack) -> None:
    """6.6 #14, section 10: the confirmation loop's pool slice is its own. With every
    connection of the main pool checked out, the confirm loop still ticks; the submit loop
    cannot."""
    services = stack.services
    held: list[AsyncConnection] = []
    try:
        for _ in range(15):  # pool_size 10 + max_overflow 5
            held.append(await services.engine.connect())
        async with asyncio.timeout(5):
            assert await stack.worker.confirm.tick() == {}
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(1.5):
                await stack.worker.submit.tick()
    finally:
        for conn in held:
            await conn.close()
    assert await stack.worker.submit.tick() == {}


async def test_redis_quota_agrees_with_the_local_arithmetic(redis_url: str) -> None:
    policy = QuotaPolicy(40.0, DEFAULT_SHARES, burst_seconds=1.0)
    provider = ProviderCode("agree")
    client: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)
    try:
        await client.flushdb()
        remote = RedisQuota(client, {provider: policy}, key_prefix="test:quota")
        local = LocalQuota({provider: policy})
        for purpose in (Purpose.SEARCH, Purpose.CONFIRM, Purpose.CREATE, Purpose.LOOKUP):
            remote_granted = local_granted = 0
            while (await remote.take(provider, purpose)).allowed:
                remote_granted += 1
            while (await local.take(provider, purpose)).allowed:
                local_granted += 1
            assert abs(remote_granted - local_granted) <= 1, (
                purpose,
                remote_granted,
                local_granted,
            )
        refused = await remote.take(provider, Purpose.SEARCH)
        assert not refused.allowed and refused.retry_after is not None and refused.retry_after > 0
    finally:
        await client.aclose()


async def test_redis_quota_fails_closed_when_redis_is_unreachable() -> None:
    client: aioredis.Redis = aioredis.from_url("redis://127.0.0.1:1/0", decode_responses=True)
    try:
        quota = RedisQuota(client, {BUS_LEGACY: QuotaPolicy(10.0, DEFAULT_SHARES)})
        with pytest.raises(QuotaUnavailableError):
            await quota.take(BUS_LEGACY, Purpose.CREATE)
    finally:
        await client.aclose()


async def test_metrics_and_provider_state_are_exposed(stack: Stack) -> None:
    offer_id = await _offer_id(stack)
    assert (await _create(stack, offer_id, "k-metrics")).status_code == 201
    scrape = await stack.admin.get("/metrics", headers=OPERATOR)
    assert scrape.status_code == 200
    text = scrape.text
    for name in (
        "provider_attempts_total",
        "provider_attempt_duration_seconds",
        "provider_admission_wait_seconds",
        "provider_circuit_state",
        "booking_commands_total",
    ):
        assert name in text, name
    assert 'purpose="create"' in text and 'provider="bus-legacy"' in text
    assert (await stack.admin.get("/metrics")).status_code == 401, "operator only"

    providers = await stack.public.get("/v1/providers", headers=CLIENT)
    assert providers.status_code == 200, providers.text
    bus_legacy = next(p for p in providers.json()["providers"] if p["code"] == "bus-legacy")
    assert bus_legacy["code"] == "bus-legacy" and bus_legacy["health"] == "ok"
    assert bus_legacy["purposes"]["create"] == {"circuit": "closed"}, "coarse state only"
    assert bus_legacy["capabilities"]["supports_status_lookup"] is False
    # Counts and rates are operator information, on the admin listener.
    detail = await stack.admin.get("/providers", headers=OPERATOR)
    assert detail.status_code == 200
    ops = next(p for p in detail.json()["providers"] if p["code"] == "bus-legacy")
    assert ops["purposes"]["create"]["calls_in_window"] >= 1
    assert (await stack.admin.get("/providers")).status_code == 401


async def test_deadline_reached_after_the_dispatch_mark_never_sends(stack: Stack) -> None:
    """Closure item 2: the transaction that writes the dispatch mark may use up the time that
    was left; the attempt is then finished as never sent, right before the network IO. The
    request path's deadline is one mutation timeout (1 s in this stack)."""
    import time

    offer_id = await _offer_id(stack)

    def stall(point: str) -> None:
        if point == "after_dispatch_mark":
            time.sleep(1.2)  # past the request path's deadline, on purpose

    stack.creator.failpoint = stall
    response = await _create(stack, offer_id, "k-late")
    stack.creator.failpoint = None
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["state"] == "CREATED" and body["unresolved_reason"] == REASON_DEADLINE
    booking_id = body["id"]
    assert await stack.bus.truth() == [], "nothing was sent"
    async with stack.creator.uow() as store:
        command = await store.command_for(BookingId(booking_id), CommandKind.CREATE)
    marked = [a for a in command.attempts if a.dispatch_marked_at is not None]
    assert marked and all(
        a.outcome is not None and a.outcome.value == "NOT_DISPATCHED" for a in marked
    )
    assert all(a.error == REASON_DEADLINE for a in marked)
    assert not command.possibly_executed
    # The next pass submits normally.
    assert (await stack.tick())["submitted"] == 1
    assert (await stack.get(booking_id))["state"] == "CONFIRMED"

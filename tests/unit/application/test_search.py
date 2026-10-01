"""Search aggregation in memory (ADR 010): one deadline, per-provider budgets,
coverage, partial results, deterministic ordering, and isolation between branches."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from time import monotonic
from typing import cast

import pytest

from orchestrator.application.search import (
    STATUS_CATALOGUE_UNAVAILABLE,
    STATUS_DEADLINE,
    STATUS_NOT_COVERED,
    STATUS_OK,
    STATUS_TIMEOUT,
    WARNING_OFFERS_NOT_STORED,
    LocationNotFoundError,
    SearchService,
    SearchUnavailableError,
)
from orchestrator.domain import ProviderCode, SideEffect
from orchestrator.domain.money import Money
from orchestrator.domain.offers import (
    FareConditions,
    Location,
    LocationKind,
    Offer,
    PassengerComposition,
    Segment,
    TransportMode,
    Trip,
)
from orchestrator.providers import ProviderAdapter, ProviderError, SearchResult, TripQuery
from orchestrator.providers.errors import ErrorKind
from orchestrator.providers.registry import ProviderRegistry
from orchestrator.resilience import (
    DEFAULT_SHARES,
    AdmissionController,
    BreakerConfig,
    LocalQuota,
    Purpose,
    PurposeConfig,
    QuotaPolicy,
    QuotaResult,
    QuotaUnavailableError,
)

A = ProviderCode("alpha")
B = ProviderCode("beta")
DAY = date(2026, 6, 15)
PAX = PassengerComposition(1)


def loc(provider: ProviderCode, ref: str) -> Location:
    return Location(
        id=f"loc_{provider}_{ref}",
        name=ref,
        country="IT",
        timezone="Europe/Rome",
        kind=LocationKind.STATION,
        provider_ref=ref,
    )


def offer(provider: ProviderCode, ident: str, departure_hour: int, query: TripQuery) -> Offer:
    departure = datetime(2026, 6, 15, departure_hour, tzinfo=UTC)
    segment = Segment(
        query.origin,
        query.destination,
        departure,
        departure + timedelta(hours=2),
        TransportMode.RAIL,
        "carrier",
        "V1",
    )
    return Offer(
        id=f"{provider}:{ident}",
        provider=provider,
        trip=Trip(f"trip-{ident}", provider, (segment,)),
        passengers=query.passengers,
        total_price=Money(1000, "EUR"),
        conditions=FareConditions(refundable=False, max_cancellation_fee=None),
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
        provider_offer_ref=ident,
    )


class FakeAdapter:
    """A provider that answers from a script: locations it knows, and a trips behaviour."""

    def __init__(
        self,
        code: ProviderCode,
        locations: Iterable[Location],
        *,
        trips: list[tuple[str, int]] | None = None,
        delay: float = 0.0,
        error: Exception | None = None,
        catalogue_error: Exception | None = None,
        truncated: bool = False,
    ) -> None:
        self.code = code
        self._locations = list(locations)
        self._trips = trips or []
        self.delay = delay
        self.error = error
        self.catalogue_error = catalogue_error
        self.truncated = truncated
        self.trip_calls = 0
        self.cancelled = False

    async def search_locations(self, query: str, *, limit: int) -> list[Location]:
        if self.catalogue_error is not None:
            raise self.catalogue_error
        return [loc for loc in self._locations if query.lower() in loc.name.lower()][:limit]

    async def search_trips(self, query: TripQuery, *, limit: int) -> SearchResult:
        self.trip_calls += 1
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if self.error is not None:
            raise self.error
        offers = tuple(offer(self.code, ident, hour, query) for ident, hour in self._trips)
        return SearchResult(offers[:limit], truncated=self.truncated)


class FakeOffers:
    def __init__(self, *, fail: bool = False) -> None:
        self.stored: dict[str, Offer] = {}
        self.fail = fail

    async def put_many(self, offers: Iterable[Offer]) -> bool:
        if self.fail:
            return False
        for o in offers:
            self.stored[o.id] = o
        return True


class BrokenQuota:
    def policy_for(self, provider: ProviderCode) -> QuotaPolicy:
        return QuotaPolicy(100.0, DEFAULT_SHARES)

    def peek(self) -> list[tuple[ProviderCode, str, float]]:
        return []

    async def take(self, provider: ProviderCode, purpose: Purpose) -> QuotaResult:
        raise QuotaUnavailableError("connection refused")


def admission(quota: object | None = None) -> AdmissionController:
    breaker = BreakerConfig(
        window_seconds=10, buckets=5, minimum_calls=4, failure_rate_threshold=0.6, open_seconds=5
    )
    cfg = PurposeConfig(
        bulkhead_limit=8, bulkhead_max_wait=0.05, breaker=breaker, retry_token_capacity=10
    )
    policies = {code: QuotaPolicy(1000.0, DEFAULT_SHARES) for code in (A, B, ProviderCode("gamma"))}
    return AdmissionController(
        quota or LocalQuota(policies),  # type: ignore[arg-type]
        purposes=dict.fromkeys(Purpose, cfg),
    )


def service(
    *adapters: FakeAdapter,
    deadline: float = 1.0,
    budget: float = 0.5,
    offers: FakeOffers | None = None,
    quota: object | None = None,
    catalogue_ttl: float = 0.0,
) -> tuple[SearchService, FakeOffers]:
    store = offers or FakeOffers()
    svc = SearchService(
        ProviderRegistry([cast(ProviderAdapter, a) for a in adapters]),
        store,  # type: ignore[arg-type]
        admission=admission(quota),
        deadline_seconds=deadline,
        provider_budget_seconds=budget,
        max_offers_per_provider=50,
        catalogue_ttl_seconds=catalogue_ttl,
    )
    return svc, store


SHARED = [loc(A, "ROM"), loc(A, "MIL")]  # ids are provider-minted; B has its own below
B_LOCS = [loc(B, "ROM"), loc(B, "MIL")]


async def test_partial_results_when_one_provider_exceeds_its_budget() -> None:
    fast = FakeAdapter(A, SHARED, trips=[("x", 9), ("y", 7)])
    slow = FakeAdapter(B, B_LOCS + SHARED, trips=[("z", 8)], delay=5.0)
    svc, store = service(fast, slow, deadline=2.0, budget=0.2)
    started = monotonic()
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    elapsed = monotonic() - started
    assert elapsed < 1.0, "the slow provider's budget, not the deadline, bounded the wait"
    assert [o.id for o in outcome.offers] == ["alpha:y", "alpha:x"], "sorted by departure"
    by_code = {r.code: r for r in outcome.providers}
    assert by_code[A].status == STATUS_OK and by_code[B].status == STATUS_TIMEOUT
    assert not outcome.complete
    assert set(store.stored) == {"alpha:x", "alpha:y"}
    await asyncio.sleep(0)
    assert slow.cancelled, "the straggler's call was cancelled, not left running"


async def test_request_deadline_cancels_stragglers_before_their_budget() -> None:
    fast = FakeAdapter(A, SHARED, trips=[("x", 9)])
    slow = FakeAdapter(B, B_LOCS + SHARED, trips=[("z", 8)], delay=5.0)
    svc, _ = service(fast, slow, deadline=0.3, budget=2.0)
    started = monotonic()
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    assert monotonic() - started < 1.0
    by_code = {r.code: r for r in outcome.providers}
    assert by_code[B].status == STATUS_DEADLINE
    assert [o.id for o in outcome.offers] == ["alpha:x"]
    await asyncio.sleep(0)
    assert slow.cancelled


async def test_one_branch_raising_never_touches_its_siblings() -> None:
    good = FakeAdapter(A, SHARED, trips=[("x", 9)])
    broken = FakeAdapter(B, SHARED, error=RuntimeError("adapter bug"))
    failing = FakeAdapter(
        ProviderCode("gamma"),
        SHARED,
        error=ProviderError(ErrorKind.TRANSIENT, SideEffect.NONE, "503"),
    )
    svc, _ = service(good, broken, failing)
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    by_code = {r.code: r for r in outcome.providers}
    assert by_code[A].status == STATUS_OK
    assert by_code[B].status == "error:unexpected"
    assert by_code["gamma"].status.startswith("error:")
    assert [o.id for o in outcome.offers] == ["alpha:x"]


async def test_all_covering_providers_failing_is_unavailable_with_reports() -> None:
    a = FakeAdapter(A, SHARED, delay=5.0)
    b = FakeAdapter(B, SHARED, error=RuntimeError("down"))
    svc, _ = service(a, b, deadline=0.3, budget=0.2)
    with pytest.raises(SearchUnavailableError):
        await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)


async def test_local_refusal_is_reported_and_nothing_is_sent() -> None:
    a = FakeAdapter(A, SHARED, trips=[("x", 9)])
    svc, _ = service(a, quota=BrokenQuota(), deadline=0.5)
    with pytest.raises(SearchUnavailableError):
        await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    assert a.trip_calls == 0, "quota outage fails closed: no request left the platform"


async def test_coverage_filters_providers_that_do_not_know_both_locations() -> None:
    a = FakeAdapter(A, SHARED, trips=[("x", 9)])
    b = FakeAdapter(B, B_LOCS, trips=[("z", 8)])  # knows only its own references
    svc, _ = service(a, b)
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    by_code = {r.code: r for r in outcome.providers}
    assert by_code[B].status == STATUS_NOT_COVERED
    assert b.trip_calls == 0
    assert outcome.complete, "not covered is not a failure"
    assert [o.id for o in outcome.offers] == ["alpha:x"]


async def test_no_covering_provider_is_an_empty_complete_answer() -> None:
    a = FakeAdapter(A, [loc(A, "ROM")], trips=[("x", 9)])
    b = FakeAdapter(B, [loc(A, "MIL")], trips=[("z", 8)])
    svc, _ = service(a, b)
    outcome = await svc.search(loc(A, "ROM").id, loc(A, "MIL").id, DAY, PAX)
    assert outcome.offers == ()
    assert all(r.status == STATUS_NOT_COVERED for r in outcome.providers)
    assert a.trip_calls == b.trip_calls == 0


async def test_unknown_location_is_404_only_with_every_catalogue_read() -> None:
    a = FakeAdapter(A, SHARED)
    b = FakeAdapter(B, B_LOCS)
    svc, _ = service(a, b)
    with pytest.raises(LocationNotFoundError) as info:
        await svc.search("loc_nowhere", SHARED[1].id, DAY, PAX)
    assert info.value.location_id == "loc_nowhere"

    b_down = FakeAdapter(B, B_LOCS, catalogue_error=RuntimeError("down"))
    svc, _ = service(a, b_down)
    with pytest.raises(SearchUnavailableError):
        await svc.search("loc_nowhere", SHARED[1].id, DAY, PAX)


async def test_catalogue_failure_of_a_non_covering_provider_still_answers() -> None:
    a = FakeAdapter(A, SHARED, trips=[("x", 9)])
    b_down = FakeAdapter(B, B_LOCS, catalogue_error=RuntimeError("down"))
    svc, _ = service(a, b_down)
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    by_code = {r.code: r for r in outcome.providers}
    assert by_code[A].status == STATUS_OK
    assert by_code[B].status == STATUS_CATALOGUE_UNAVAILABLE
    assert not outcome.complete


async def test_ordering_dedupe_truncation_and_foreign_offers() -> None:
    a = FakeAdapter(A, SHARED, trips=[("b", 9), ("a", 9), ("b", 9), ("c", 7)])
    b = FakeAdapter(B, SHARED, trips=[("q", 9)], truncated=True)
    svc, _ = service(a, b)
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    assert [o.id for o in outcome.offers] == ["alpha:c", "alpha:a", "alpha:b", "beta:q"]
    by_code = {r.code: r for r in outcome.providers}
    assert by_code[A].truncated, "a duplicate within the provider is dropped and reported"
    assert by_code[B].truncated


async def test_offers_that_could_not_be_stored_are_flagged() -> None:
    a = FakeAdapter(A, SHARED, trips=[("x", 9)])
    svc, _ = service(a, offers=FakeOffers(fail=True))
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    assert outcome.providers[0].warnings == (WARNING_OFFERS_NOT_STORED,)
    assert [o.id for o in outcome.offers] == ["alpha:x"], "the search still answers"


async def test_catalogue_cache_spares_the_provider_within_its_ttl() -> None:
    a = FakeAdapter(A, SHARED, trips=[("x", 9)])
    svc, _ = service(a, catalogue_ttl=60.0)
    await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    a.catalogue_error = RuntimeError("catalogue down")
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    assert outcome.providers[0].status == STATUS_OK


async def test_locations_merge_across_providers_under_the_deadline() -> None:
    a = FakeAdapter(A, [loc(A, "Roma Termini"), loc(A, "Milano")])
    b = FakeAdapter(B, [loc(B, "Roma Tiburtina")])
    svc, _ = service(a, b)
    found = await svc.locations("rom", limit=10)
    assert [f.name for f in found] == ["Roma Termini", "Roma Tiburtina"]


async def test_cancelling_the_request_cancels_every_branch() -> None:
    a = FakeAdapter(A, SHARED, trips=[("x", 9)], delay=5.0)
    b = FakeAdapter(B, SHARED, trips=[("z", 9)], delay=5.0)
    svc, _ = service(a, b, deadline=10.0, budget=10.0)
    task = asyncio.create_task(svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert a.cancelled and b.cancelled


class SlowOffers(FakeOffers):
    async def put_many(self, offers: Iterable[Offer]) -> bool:
        await asyncio.sleep(5.0)
        return True


async def test_a_slow_offer_store_cannot_hold_the_answer_past_the_deadline() -> None:
    a = FakeAdapter(A, SHARED, trips=[("x", 9)])
    svc, _ = service(a, offers=SlowOffers(), deadline=0.4, budget=0.3)
    started = monotonic()
    outcome = await svc.search(SHARED[0].id, SHARED[1].id, DAY, PAX)
    assert monotonic() - started < 1.0, "the store got what was left of the deadline, no more"
    assert outcome.providers[0].warnings == (WARNING_OFFERS_NOT_STORED,)
    assert [o.id for o in outcome.offers] == ["alpha:x"]

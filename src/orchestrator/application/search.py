"""Search aggregation (docs/architecture.md, search; ADR 010).

One request fans out to every provider that covers both locations, under a single deadline and
a per-provider budget, through the search purpose only. Results that arrive in time are kept,
stragglers are cancelled, and one branch failing never touches its siblings. The response is
partial by default and carries a per-provider report; it is a failure only when every covering
provider failed.

Two rounds share the deadline. The first reads each provider's location catalogue (cached for
a short while, it is seed data) and decides coverage: a provider is asked for trips only when it
knows both locations under its own reference. The second asks the covering providers for trips.
A location is reported as unknown only when every catalogue was read; when one catalogue could
not be read and no other knows the location, the search is unavailable rather than wrong.

There is no search cache and no retry: a stale offer that ends in ``offer-expired`` costs more
than a slower search, and a retried search would compete with completion for the provider's
allowance.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date
from time import monotonic

from orchestrator.application.offers import OfferStore
from orchestrator.domain import ProviderCode
from orchestrator.domain.offers import Location, Offer, PassengerComposition
from orchestrator.providers import ProviderAdapter, ProviderError, SearchResult, TripQuery
from orchestrator.providers.registry import ProviderRegistry
from orchestrator.resilience import AdmissionController, NotDispatchedError, Purpose
from orchestrator.telemetry import metrics

log = logging.getLogger(__name__)

STATUS_OK = "ok"
STATUS_TIMEOUT = "timeout"  # the provider's own budget ran out
STATUS_DEADLINE = "deadline"  # the request deadline arrived first; the branch was cancelled
STATUS_NOT_COVERED = "not-covered"  # the provider does not know both locations
STATUS_CATALOGUE_UNAVAILABLE = "catalogue-unavailable"  # coverage could not be decided
WARNING_OFFERS_NOT_STORED = "offers-not-stored: booking these offers will fail; search again"


@dataclass(frozen=True, slots=True)
class ProviderReport:
    code: str
    status: str
    latency_ms: int
    truncated: bool = False
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.status == STATUS_OK


@dataclass(frozen=True, slots=True)
class SearchOutcome:
    offers: tuple[Offer, ...]
    providers: tuple[ProviderReport, ...]

    @property
    def complete(self) -> bool:
        """Every provider that was asked answered."""
        return all(r.ok or r.status == STATUS_NOT_COVERED for r in self.providers)


class SearchUnavailableError(Exception):
    """No covering provider answered within the deadline."""


class LocationNotFoundError(Exception):
    def __init__(self, location_id: str) -> None:
        super().__init__(location_id)
        self.location_id = location_id


@dataclass(slots=True)
class _Catalogue:
    expires_at: float
    locations: dict[str, Location]


@dataclass(slots=True)
class _Branch:
    """What one provider's branch produced: a report, and offers or a catalogue."""

    report: ProviderReport
    offers: tuple[Offer, ...] = ()
    catalogue: dict[str, Location] = field(default_factory=dict)


class SearchService:
    def __init__(
        self,
        registry: ProviderRegistry,
        offers: OfferStore,
        *,
        admission: AdmissionController,
        deadline_seconds: float = 3.0,
        provider_budget_seconds: float = 2.0,
        max_offers_per_provider: int = 50,
        catalogue_ttl_seconds: float = 60.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if deadline_seconds <= 0 or provider_budget_seconds <= 0:
            raise ValueError("the deadline and the provider budget must be positive")
        self.registry = registry
        self.offers = offers
        self.admission = admission
        self.deadline_seconds = deadline_seconds
        self.provider_budget_seconds = provider_budget_seconds
        self.max_offers_per_provider = max_offers_per_provider
        self.catalogue_ttl_seconds = catalogue_ttl_seconds
        self._clock = clock
        self._catalogues: dict[ProviderCode, _Catalogue] = {}

    # ------------------------------------------------------------------ public

    async def search(
        self,
        origin_id: str,
        destination_id: str,
        departure_date: date,
        passengers: PassengerComposition,
    ) -> SearchOutcome:
        """Trips from ``origin_id`` to ``destination_id`` across every covering provider.

        Raises ``LocationNotFoundError`` when every catalogue was read and none knows a
        location, and ``SearchUnavailableError`` when coverage could not be decided for an
        unknown location or when no covering provider answered.
        """
        started = self._clock()
        try:
            outcome = await self._search(origin_id, destination_id, departure_date, passengers)
        except SearchUnavailableError:
            metrics.search_requests.add(1, {"outcome": "unavailable"})
            raise
        except LocationNotFoundError:
            metrics.search_requests.add(1, {"outcome": "location-not-found"})
            raise
        finally:
            metrics.search_duration.record(self._clock() - started)
        metrics.search_requests.add(
            1,
            {
                "outcome": "complete"
                if outcome.complete
                else ("partial" if outcome.offers else "empty")
            },
        )
        return outcome

    async def locations(self, query: str, *, limit: int = 20) -> list[Location]:
        """Locations matching ``query`` from every provider that answers in time."""
        deadline = self._clock() + self.deadline_seconds
        branches = await self._fan_out(
            {
                adapter.code: self._locations_branch(adapter, query, limit, deadline)
                for adapter in self.registry.all()
            },
            deadline,
        )
        found = [loc for branch in branches.values() for loc in branch.catalogue.values()]
        found.sort(key=lambda loc: (loc.name, loc.id))
        return found[:limit]

    # ----------------------------------------------------------------- phases

    async def _search(
        self,
        origin_id: str,
        destination_id: str,
        departure_date: date,
        passengers: PassengerComposition,
    ) -> SearchOutcome:
        deadline = self._clock() + self.deadline_seconds
        catalogues = await self._fan_out(
            {
                adapter.code: self._catalogue_branch(adapter, deadline)
                for adapter in self.registry.all()
            },
            deadline,
        )
        reports: dict[ProviderCode, ProviderReport] = {}
        covering: dict[ProviderCode, TripQuery] = {}
        known = {origin_id: False, destination_id: False}
        every_catalogue_read = True
        for adapter in self.registry.all():
            branch = catalogues[adapter.code]
            if not branch.report.ok:
                every_catalogue_read = False
                reports[adapter.code] = ProviderReport(
                    adapter.code, STATUS_CATALOGUE_UNAVAILABLE, branch.report.latency_ms
                )
                continue
            origin = branch.catalogue.get(origin_id)
            destination = branch.catalogue.get(destination_id)
            known[origin_id] |= origin is not None
            known[destination_id] |= destination is not None
            if origin is None or destination is None:
                reports[adapter.code] = ProviderReport(
                    adapter.code, STATUS_NOT_COVERED, branch.report.latency_ms
                )
                continue
            covering[adapter.code] = TripQuery(origin, destination, departure_date, passengers)
        for location_id, seen in known.items():
            if not seen:
                if every_catalogue_read:
                    raise LocationNotFoundError(location_id)
                raise SearchUnavailableError("a catalogue is unavailable and the rest do not know")
        if not covering:
            if not every_catalogue_read:
                raise SearchUnavailableError("no catalogue that was read covers both locations")
            return SearchOutcome((), tuple(reports[a.code] for a in self.registry.all()))

        branches = await self._fan_out(
            {
                code: self._trips_branch(self.registry.get(code), query, deadline)
                for code, query in covering.items()
            },
            deadline,
        )
        offers: list[Offer] = [o for branch in branches.values() for o in branch.offers]
        stored = False
        if offers:
            # The store is part of the request: it gets exactly what is left of the deadline,
            # and when nothing is left the offers are flagged instead of delaying the answer.
            remaining = deadline - self._clock()
            if remaining > 0:
                try:
                    async with asyncio.timeout(remaining):
                        stored = await self.offers.put_many(offers)
                except TimeoutError:
                    stored = False
        for code, branch in branches.items():
            report = branch.report
            if not stored and report.ok:
                report = ProviderReport(
                    report.code,
                    report.status,
                    report.latency_ms,
                    report.truncated,
                    (*report.warnings, WARNING_OFFERS_NOT_STORED),
                )
            reports[code] = report
        ordered = tuple(reports[a.code] for a in self.registry.all())
        if not any(r.ok for r in ordered):
            raise SearchUnavailableError("no covering provider answered")
        offers.sort(key=lambda o: (o.trip.departure, o.provider, o.id))
        return SearchOutcome(tuple(offers), ordered)

    async def _fan_out(
        self,
        branches: dict[ProviderCode, Awaitable[_Branch]],
        deadline: float,
    ) -> dict[ProviderCode, _Branch]:
        """Run every branch concurrently until the deadline; cancel what has not finished.

        A branch never raises (it reports); if one does, its report says so and the others
        are unaffected. Cancellation of the caller propagates to every branch.
        """
        tasks = {asyncio.ensure_future(coro): code for code, coro in branches.items()}
        if not tasks:
            return {}
        try:
            _done, pending = await asyncio.wait(
                tasks, timeout=max(deadline - self._clock(), 0.0), return_when=asyncio.ALL_COMPLETED
            )
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        out: dict[ProviderCode, _Branch] = {}
        for task, code in tasks.items():
            if task in pending or task.cancelled():
                out[code] = _Branch(ProviderReport(code, STATUS_DEADLINE, self._ms(deadline)))
                metrics.search_provider_outcomes.add(
                    1, {"provider": code, "status": STATUS_DEADLINE}
                )
            elif (exc := task.exception()) is not None:
                log.exception("search branch failed", exc_info=exc, extra={"provider": code})
                out[code] = _Branch(ProviderReport(code, "error:unexpected", 0))
                metrics.search_provider_outcomes.add(1, {"provider": code, "status": "error"})
            else:
                out[code] = task.result()
        return out

    # --------------------------------------------------------------- branches

    async def _catalogue_branch(self, adapter: ProviderAdapter, deadline: float) -> _Branch:
        cached = self._catalogues.get(adapter.code)
        if cached is not None and cached.expires_at > self._clock():
            return _Branch(ProviderReport(adapter.code, STATUS_OK, 0), catalogue=cached.locations)
        branch = await self._locations_branch(adapter, "", 10_000, deadline)
        if branch.report.ok and self.catalogue_ttl_seconds > 0:
            self._catalogues[adapter.code] = _Catalogue(
                self._clock() + self.catalogue_ttl_seconds, branch.catalogue
            )
        return branch

    async def _locations_branch(
        self, adapter: ProviderAdapter, query: str, limit: int, deadline: float
    ) -> _Branch:
        async def call() -> list[Location]:
            return await adapter.search_locations(query, limit=limit)

        found, report = await self._call(adapter, "search_locations", call, deadline)
        if found is None:
            return _Branch(report)
        return _Branch(report, catalogue={loc.id: loc for loc in found})

    async def _trips_branch(
        self, adapter: ProviderAdapter, query: TripQuery, deadline: float
    ) -> _Branch:
        async def call() -> SearchResult:
            return await adapter.search_trips(query, limit=self.max_offers_per_provider)

        result, report = await self._call(adapter, "search_trips", call, deadline)
        if result is None:
            return _Branch(report)
        unique: dict[str, Offer] = {}
        for offer in result.offers:
            if offer.provider != adapter.code:
                log.warning(
                    "offer attributed to another provider dropped",
                    extra={"provider": adapter.code, "offer": offer.id},
                )
                continue
            unique.setdefault(offer.id, offer)  # dedupe within the provider, first wins
        return _Branch(
            ProviderReport(
                adapter.code,
                STATUS_OK,
                report.latency_ms,
                truncated=result.truncated or len(result.offers) > len(unique),
                warnings=tuple(f"{w.code}: {w.detail}" for w in result.warnings),
            ),
            offers=tuple(unique.values()),
        )

    async def _call[T](
        self,
        adapter: ProviderAdapter,
        operation: str,
        call: Callable[[], Awaitable[T]],
        deadline: float,
    ) -> tuple[T | None, ProviderReport]:
        """One search-purpose call under ``min(provider budget, request deadline)``, never
        retried. Returns the value and an ``ok`` report, or ``None`` and the failure report."""
        started = self._clock()
        budget = min(deadline, started + self.provider_budget_seconds)
        clipped = deadline < started + self.provider_budget_seconds  # the request ends first
        try:
            async with self.admission.admit(
                adapter.code, Purpose.SEARCH, operation=operation, deadline=budget
            ) as ticket:
                ticket.require_time()
                try:
                    async with asyncio.timeout(max(budget - self._clock(), 0.0)):
                        value = await call()
                except ProviderError as exc:
                    ticket.record_provider_error(exc)
                    raise
                except TimeoutError:
                    ticket.record(
                        outcome="timeout", side_effect="NONE", healthy=False, timeout=True
                    )
                    raise
                ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
        except NotDispatchedError as exc:
            return None, self._report(adapter, f"not-dispatched:{exc.reason}", started)
        except TimeoutError:
            return None, self._report(
                adapter, STATUS_DEADLINE if clipped else STATUS_TIMEOUT, started
            )
        except ProviderError as exc:
            return None, self._report(adapter, f"error:{exc.kind}", started)
        return value, self._report(adapter, STATUS_OK, started)

    def _report(self, adapter: ProviderAdapter, status: str, started: float) -> ProviderReport:
        metrics.search_provider_outcomes.add(
            1, {"provider": adapter.code, "status": status.split(":", 1)[0]}
        )
        return ProviderReport(adapter.code, status, self._ms(started))

    def _ms(self, started: float) -> int:
        return max(int((self._clock() - started) * 1000), 0)

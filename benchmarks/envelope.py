"""The load envelope benchmark (docs/resilience-strategy.md).

Claim under test: with search running at its allowance cap, 200 concurrent holds per provider
get their confirmation *dispatched* within 5 s at p99. Dispatch latency is what admission
controls; the provider's own latency is outside the platform and is held constant here.

The benchmark runs the real admission controller in one process against an in-memory quota
with the real two-bucket arithmetic and a fake provider that answers after a fixed latency.
It runs twice: partitioned by purpose (the design) and with one undifferentiated first-come
bucket (the naive alternative), so the number that matters is the difference.

    python -m benchmarks.envelope --allowance 120 --holds 200 --searchers 64

The measured numbers for the reference configuration are recorded in
docs/resilience-strategy.md.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
from dataclasses import dataclass
from time import monotonic

from orchestrator.domain import ProviderCode
from orchestrator.resilience import (
    DEFAULT_SHARES,
    AdmissionController,
    AttemptContext,
    LocalQuota,
    NotDispatchedError,
    Purpose,
    PurposeConfig,
    QuotaPolicy,
    RetryPolicy,
    Share,
    run_attempts,
)

PROVIDER = ProviderCode("bench")


@dataclass(frozen=True, slots=True)
class Result:
    partitioned: bool
    holds: int
    confirm_dispatch_p50_ms: float
    confirm_dispatch_p99_ms: float
    confirm_dispatch_max_ms: float
    searches_completed: int
    searches_refused: int
    searches_during_confirmations: int
    confirm_phase_s: float
    search_cap_per_s: float
    duration_s: float

    def as_dict(self) -> dict[str, object]:
        return {k: getattr(self, k) for k in self.__slots__}


def _controller(*, allowance: float, partitioned: bool) -> AdmissionController:
    if partitioned:
        shares = DEFAULT_SHARES
        limits = {
            Purpose.SEARCH: PurposeConfig(bulkhead_limit=64, bulkhead_max_wait=0.2),
            Purpose.CREATE: PurposeConfig(bulkhead_limit=32, bulkhead_max_wait=2.0),
            Purpose.CONFIRM: PurposeConfig(bulkhead_limit=32, bulkhead_max_wait=2.0),
            Purpose.CANCEL: PurposeConfig(bulkhead_limit=16, bulkhead_max_wait=2.0),
            Purpose.LOOKUP: PurposeConfig(bulkhead_limit=16, bulkhead_max_wait=2.0),
        }
    else:
        # The naive alternative: every purpose is "search", one bucket, one bulkhead.
        shares = {
            p: Share(0.999 if p is Purpose.SEARCH else 0.0002, reserved=False) for p in Purpose
        }
        limits = dict.fromkeys(Purpose, PurposeConfig(bulkhead_limit=128, bulkhead_max_wait=2.0))
    quota = LocalQuota({PROVIDER: QuotaPolicy(allowance, shares, burst_seconds=1.0)})
    return AdmissionController(quota, purposes=limits)


async def run(
    *,
    allowance: float,
    holds: int,
    searchers: int,
    provider_latency: float,
    partitioned: bool,
    duration: float,
    seed: int = 1,
) -> Result:
    rng = random.Random(seed)  # noqa: S311 - reproducible jitter, not security
    admission = _controller(allowance=allowance, partitioned=partitioned)
    search_purpose = Purpose.SEARCH
    confirm_purpose = Purpose.CONFIRM if partitioned else Purpose.SEARCH
    stop = asyncio.Event()
    searches = {"ok": 0, "refused": 0, "during": 0}
    confirming = asyncio.Event()

    async def searcher() -> None:
        while not stop.is_set():
            try:
                async with admission.admit(
                    PROVIDER, search_purpose, operation="search_trips", deadline=monotonic() + 2
                ) as ticket:
                    await asyncio.sleep(provider_latency)
                    ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
                searches["ok"] += 1
                if confirming.is_set():
                    searches["during"] += 1
            except NotDispatchedError as exc:
                searches["refused"] += 1
                await asyncio.sleep(min(exc.retry_after or 0.01, 0.05))

    latencies: list[float] = []

    async def hold_confirmation() -> None:
        arrived = monotonic()

        async def attempt(context: AttemptContext) -> None:
            async with admission.admit(
                PROVIDER,
                confirm_purpose,
                operation="confirm_booking",
                attempt_n=context.n,
                after_timeout=context.after_timeout,
                charged=context.charged,
                deadline=arrived + 30,
            ) as ticket:
                latencies.append(monotonic() - arrived)
                await asyncio.sleep(provider_latency)
                ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)

        try:
            await run_attempts(
                attempt,
                policy=RetryPolicy(max_attempts=10_000, base_seconds=0.05, cap_seconds=0.5),
                deadline=arrived + 30,
                labels={"provider": PROVIDER, "purpose": confirm_purpose.value},
                rng=rng,
            )
        except NotDispatchedError:
            latencies.append(30.0)  # never dispatched inside the deadline: a miss is a miss

    started = monotonic()
    storm = [asyncio.create_task(searcher()) for _ in range(searchers)]
    await asyncio.sleep(1.0)  # let search saturate its share first
    confirming.set()
    phase_started = monotonic()
    await asyncio.gather(*[hold_confirmation() for _ in range(holds)])
    confirm_phase = monotonic() - phase_started
    confirming.clear()
    stop.set()
    await asyncio.gather(*storm)
    admission.close()
    ms = sorted(x * 1000 for x in latencies)
    return Result(
        partitioned=partitioned,
        holds=holds,
        confirm_dispatch_p50_ms=round(statistics.median(ms), 1),
        confirm_dispatch_p99_ms=round(ms[min(len(ms) - 1, int(len(ms) * 0.99))], 1),
        confirm_dispatch_max_ms=round(ms[-1], 1),
        searches_completed=searches["ok"],
        searches_refused=searches["refused"],
        searches_during_confirmations=searches["during"],
        confirm_phase_s=round(confirm_phase, 2),
        search_cap_per_s=round(allowance * (0.45 if partitioned else 0.999), 1),
        duration_s=round(monotonic() - started, 2),
    )


async def main(argv: list[str] | None = None) -> list[Result]:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--allowance", type=float, default=120.0, help="provider allowance, req/s")
    parser.add_argument("--holds", type=int, default=200)
    parser.add_argument("--searchers", type=int, default=64)
    parser.add_argument("--provider-latency", type=float, default=0.05)
    parser.add_argument("--duration", type=float, default=20.0)
    args = parser.parse_args(argv)
    results = []
    for partitioned in (True, False):
        results.append(
            await run(
                allowance=args.allowance,
                holds=args.holds,
                searchers=args.searchers,
                provider_latency=args.provider_latency,
                partitioned=partitioned,
                duration=args.duration,
            )
        )
    print(json.dumps([r.as_dict() for r in results], indent=2))
    return results


if __name__ == "__main__":
    asyncio.run(main())

"""The hold-to-confirmation path through the real stack (docs/resilience-strategy.md).

``benchmarks.envelope`` measures admission alone, in memory. This benchmark runs the platform as
deployed: PostgreSQL and Redis in containers, the rail-osdm simulator on a socket, the public
application, the confirmer on its reserved pool slice, leases, journaling before IO, the
two-bucket quota in Redis. ``N`` bookings are created concurrently against the hold-then-confirm
provider while a search storm runs against the same provider through the same admission
controller. Measured:

- the wall time of ``POST /v1/bookings`` (hold, then confirmation on the request path);
- how many bookings were confirmed inside the request (201) and how many were handed to the
  confirmation loop (202);
- the platform's own ``confirm_dispatch_latency`` histogram (hold to the confirm IO), read from
  the admin ``/metrics`` endpoint.

Run twice, with and without the storm, so the number that matters is the difference:

    python -m benchmarks.confirmation_path --holds 60 --searchers 24

Docker is required (testcontainers), or point ``MPO_BENCH_DATABASE_URL`` and
``MPO_BENCH_REDIS_URL`` at running services.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import socket
import statistics
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any

import httpx
import uvicorn
from alembic import command
from alembic.config import Config
from asgi_lifespan import LifespanManager

from orchestrator.api import create_admin_app, create_public_app
from orchestrator.application.wiring import build_services
from orchestrator.config import Settings
from orchestrator.worker.loops import Worker

CLIENT_KEY = "bench-client-key"
ADMIN_KEY = "bench-admin-key"
SIM_TOKEN = "bench"  # noqa: S105 - a simulator admin token, not a credential


@dataclass(frozen=True, slots=True)
class Result:
    storm: bool
    holds: int
    confirmed_in_request: int
    handed_to_loop: int
    failed: int
    request_p50_ms: float
    request_p99_ms: float
    request_max_ms: float
    hold_to_dispatch_p50_ms: float | None
    hold_to_dispatch_p99_ms: float | None
    searches_completed: int
    searches_refused: int
    duration_s: float

    def as_dict(self) -> dict[str, object]:
        return {k: getattr(self, k) for k in self.__slots__}


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@asynccontextmanager
async def _infrastructure() -> AsyncIterator[tuple[str, str]]:
    database_url = os.environ.get("MPO_BENCH_DATABASE_URL")
    redis_url = os.environ.get("MPO_BENCH_REDIS_URL")
    if database_url and redis_url:
        yield database_url, redis_url
        return
    from testcontainers.community.postgres import PostgresContainer
    from testcontainers.community.redis import RedisContainer

    with PostgresContainer("postgres:18", driver="asyncpg") as pg, RedisContainer("redis:8") as r:
        yield (
            pg.get_connection_url(),
            f"redis://{r.get_container_host_ip()}:{r.get_exposed_port(6379)}/0",
        )


@asynccontextmanager
async def _rail_simulator(tmp: Path) -> AsyncIterator[str]:
    from provider_sims.rail_osdm.app import create_app

    app = create_app(db_path=str(tmp / "rail.sqlite3"), admin_token=SIM_TOKEN)
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="off")
    )
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - uvicorn exposes a flag, not an Event
        await asyncio.sleep(0.01)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


def _buckets(metrics_text: str, name: str) -> dict[float, float]:
    out: dict[float, float] = {}
    for line in metrics_text.splitlines():
        if not line.startswith(name) or "_bucket{" not in line:
            continue
        match = re.search(r'le="([^"]+)"', line)
        if match is None:
            continue
        bound = float("inf") if match.group(1) == "+Inf" else float(match.group(1))
        out[bound] = out.get(bound, 0.0) + float(line.rsplit(" ", 1)[1])
    return out


def _histogram_quantiles(
    metrics_text: str, name: str, *, baseline: str = ""
) -> tuple[float | None, float | None]:
    """p50 and p99 upper bounds (ms) of a Prometheus histogram, as a delta against a baseline
    exposition (metrics are process-global and cumulative; each scenario is judged alone)."""
    before = _buckets(baseline, name) if baseline else {}
    buckets = sorted(
        (bound, cumulative - before.get(bound, 0.0))
        for bound, cumulative in _buckets(metrics_text, name).items()
    )
    if not buckets:
        return None, None
    total = buckets[-1][1]
    if total == 0:
        return None, None

    def at(q: float) -> float:
        target = q * total
        for bound, cumulative in buckets:
            if cumulative >= target:
                return bound * 1000 if bound != float("inf") else float("inf")
        return float("inf")

    return round(at(0.5), 1), round(at(0.99), 1)


async def run(*, holds: int, searchers: int, storm: bool, provider_latency_ms: int) -> Result:
    started = monotonic()
    async with _infrastructure() as (database_url, redis_url):
        cfg = Config("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", database_url)
        command.upgrade(cfg, "head")
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:  # SQLite on Windows
            async with _rail_simulator(Path(tmp)) as rail_url:
                return await _measure(
                    database_url,
                    redis_url,
                    rail_url,
                    holds=holds,
                    searchers=searchers,
                    storm=storm,
                    provider_latency_ms=provider_latency_ms,
                    started=started,
                )


async def _measure(
    database_url: str,
    redis_url: str,
    rail_url: str,
    *,
    holds: int,
    searchers: int,
    storm: bool,
    provider_latency_ms: int,
    started: float,
) -> Result:
    settings = Settings(
        _env_file=None,
        environment="test",
        api_keys={_sha(CLIENT_KEY): "bench"},
        admin_key_hash=_sha(ADMIN_KEY),
        log_json=False,
        database_url=database_url,
        redis_url=redis_url,
        rail_osdm_url=rail_url,
        bus_legacy_url="http://127.0.0.1:1",
        mobility_async_url="http://127.0.0.1:1",
        provider_mutation_timeout_seconds=5.0,
        reconcile_backoff_seconds=0.0,
        reschedule_backoff_seconds=0.0,
    )
    services = await build_services(settings)
    public_app = create_public_app(settings, services=services)
    admin_app = create_admin_app(settings, services=services)
    client_headers = {"X-API-Key": CLIENT_KEY}
    async with (
        LifespanManager(public_app),
        LifespanManager(admin_app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=public_app), base_url="http://public", timeout=60
        ) as public,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=admin_app), base_url="http://admin", timeout=60
        ) as admin,
        httpx.AsyncClient(base_url=rail_url, timeout=10) as sim_admin,
    ):
        chaos = (await sim_admin.get("/_chaos", headers={"X-Admin-Token": SIM_TOKEN})).json()
        chaos["latency_ms"] = provider_latency_ms
        await sim_admin.put("/_chaos", json=chaos, headers={"X-Admin-Token": SIM_TOKEN})
        trips: dict[str, str] = {
            "from": "loc_rail_8500010",
            "to": "loc_rail_8503000",
            "departureDate": "2026-06-15",
            "adults": "1",
        }
        found = await public.get("/v1/trips", params=trips, headers=client_headers)
        found.raise_for_status()
        offer_id = next(
            o["id"]
            for o in found.json()["offers"]
            if o["trip"]["segments"][0]["vehicle_ref"] == "IC-BS-ZH-0704"
        )
        worker = Worker(
            services.uow,
            services.creator,
            services.recovery,
            services.policy,
            confirm_uow=services.confirm_uow,
        )
        stop = asyncio.Event()
        searches = {"ok": 0, "refused": 0}

        async def searcher() -> None:
            while not stop.is_set():
                response = await public.get("/v1/trips", params=trips, headers=client_headers)
                if response.status_code == 200 and any(
                    r["status"] == "ok" for r in response.json()["providers"]
                ):
                    searches["ok"] += 1
                else:
                    searches["refused"] += 1

        async def ticker() -> None:
            while not stop.is_set():
                await worker.tick()
                await asyncio.sleep(0.2)

        latencies: list[float] = []
        statuses: list[int] = []

        async def book(i: int) -> None:
            t0 = monotonic()
            try:
                response = await public.post(
                    "/v1/bookings",
                    json={
                        "offer_id": offer_id,
                        "passengers": [{"full_name": f"Passenger {i}"}],
                        "contact_email": "bench@example.org",
                    },
                    headers={**client_headers, "Idempotency-Key": f"bench-{storm}-{i}"},
                )
                statuses.append(response.status_code)
            except Exception:
                statuses.append(0)
            latencies.append(monotonic() - t0)

        baseline = (await admin.get("/metrics", headers={"X-Admin-Key": ADMIN_KEY})).text
        tasks = [asyncio.create_task(searcher()) for _ in range(searchers if storm else 0)]
        tasks.append(asyncio.create_task(ticker()))
        try:
            await asyncio.sleep(0.5)
            await asyncio.gather(*[book(i) for i in range(holds)])
            # Let the loop finish whatever the request path handed over.
            for _ in range(50):
                await asyncio.sleep(0.2)
                async with services.uow() as store:
                    counts = await store.count_by_state()
                if not any(state in ("HELD", "CONFIRMING", "UNKNOWN") for state, _ in counts):
                    break
        finally:
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        metrics = await admin.get("/metrics", headers={"X-Admin-Key": ADMIN_KEY})
        p50, p99 = _histogram_quantiles(metrics.text, "confirm_dispatch_latency", baseline=baseline)
        worker.close()
    await services.close()
    ms = sorted(x * 1000 for x in latencies)
    return Result(
        storm=storm,
        holds=holds,
        confirmed_in_request=sum(1 for s in statuses if s == 201),
        handed_to_loop=sum(1 for s in statuses if s == 202),
        failed=sum(1 for s in statuses if s not in (201, 202)),
        request_p50_ms=round(statistics.median(ms), 1),
        request_p99_ms=round(ms[min(len(ms) - 1, int(len(ms) * 0.99))], 1),
        request_max_ms=round(ms[-1], 1),
        hold_to_dispatch_p50_ms=p50,
        hold_to_dispatch_p99_ms=p99,
        searches_completed=searches["ok"],
        searches_refused=searches["refused"],
        duration_s=round(monotonic() - started, 2),
    )


async def main(argv: list[str] | None = None) -> list[Result]:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--holds", type=int, default=40)  # the benchmark trip seats 40
    parser.add_argument("--searchers", type=int, default=24)
    parser.add_argument("--provider-latency-ms", type=int, default=20)
    args = parser.parse_args(argv)
    results: list[Result] = []
    for storm in (False, True):
        results.append(
            await run(
                holds=args.holds,
                searchers=args.searchers,
                storm=storm,
                provider_latency_ms=args.provider_latency_ms,
            )
        )
    print(json.dumps([r.as_dict() for r in results], indent=2))
    return results


def _entry() -> Any:
    return asyncio.run(main())


if __name__ == "__main__":
    _entry()

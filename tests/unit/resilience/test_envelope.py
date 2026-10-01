"""Proposal 6.6 #50, ``test_search_cannot_starve_confirm``: the envelope, scaled down.

The full benchmark (``benchmarks/envelope.py``) runs 200 holds against a 120 req/s allowance
and takes tens of seconds; this test keeps the shape (search saturating its share, holds
arriving at once) at a size that finishes in a few seconds, and asserts the property rather
than a number: partitioned, confirmation dispatch stays inside the 5 s envelope; unpartitioned,
the same load misses it.
"""

from __future__ import annotations

import pytest
from benchmarks.envelope import run


@pytest.mark.slow
async def test_search_cannot_starve_confirm() -> None:
    partitioned = await run(
        allowance=60,
        holds=120,
        searchers=32,
        provider_latency=0.02,
        partitioned=True,
        duration=5,
        seed=7,
    )
    assert partitioned.confirm_dispatch_p99_ms < 5000, partitioned
    # Search kept running near its cap *while* confirmations were dispatched: the measured
    # throughput during the confirmation phase is at least half the cap (the provider latency
    # and the bulkhead take their share; the assertion is about isolation, not peak rate).
    rate = partitioned.searches_during_confirmations / partitioned.confirm_phase_s
    assert rate >= 0.5 * partitioned.search_cap_per_s, partitioned


@pytest.mark.slow
async def test_without_purposes_the_same_load_starves_confirmations() -> None:
    """The same load through one undifferentiated bucket misses the five-second envelope."""
    naive = await run(
        allowance=60,
        holds=120,
        searchers=32,
        provider_latency=0.02,
        partitioned=False,
        duration=5,
        seed=7,
    )
    assert naive.confirm_dispatch_p99_ms > 5000, naive

"""Admission purposes (docs/resilience-strategy.md, ADR 008).

Every provider call is made *for a purpose*, and every purpose has its own bulkhead, circuit
breaker, quota share, retry tokens and transport client. Capped purposes (search, create) may
never exceed their share; reserved purposes (confirm, cancel, lookup) always keep theirs and may
borrow what the capped purposes leave unused. That asymmetry is the whole point: a search storm
can saturate search, and only search.

The enum lives with the providers because adapters name the purpose of each call they make;
the resilience layer above builds the gates around it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Purpose(StrEnum):
    SEARCH = "search"
    CREATE = "create"
    CONFIRM = "confirm"
    CANCEL = "cancel"
    LOOKUP = "lookup"


@dataclass(frozen=True, slots=True)
class Share:
    fraction: float
    reserved: bool  # reserved: a floor that is never lent; capped: a ceiling

    def __post_init__(self) -> None:
        if not 0 < self.fraction <= 1:
            raise ValueError("a share is a fraction in (0, 1]")


DEFAULT_SHARES: dict[Purpose, Share] = {
    Purpose.SEARCH: Share(0.45, reserved=False),
    Purpose.CREATE: Share(0.25, reserved=False),
    Purpose.CONFIRM: Share(0.10, reserved=True),
    Purpose.CANCEL: Share(0.10, reserved=True),
    Purpose.LOOKUP: Share(0.10, reserved=True),
}


def validate_shares(shares: dict[Purpose, Share]) -> None:
    if set(shares) != set(Purpose):
        raise ValueError("every purpose needs a share")
    reserved = sum(s.fraction for s in shares.values() if s.reserved)
    if reserved >= 1:
        raise ValueError("reserved shares must leave capacity for the capped purposes")

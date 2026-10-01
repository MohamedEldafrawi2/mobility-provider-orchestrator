"""Money as integer minor units plus an ISO 4217 currency. No floats, no conversion."""

from __future__ import annotations

import re
from dataclasses import dataclass

_CURRENCY = re.compile(r"^[A-Z]{3}$")


@dataclass(frozen=True, slots=True)
class Money:
    amount_minor: int
    currency: str

    def __post_init__(self) -> None:
        if not isinstance(self.amount_minor, int) or isinstance(self.amount_minor, bool):
            raise TypeError("amount_minor must be an int of minor units")
        if not _CURRENCY.match(self.currency):
            raise ValueError(f"currency must be an ISO 4217 code, got {self.currency!r}")

    def __add__(self, other: Money) -> Money:
        self._same_currency(other)
        return Money(self.amount_minor + other.amount_minor, self.currency)

    def __sub__(self, other: Money) -> Money:
        self._same_currency(other)
        return Money(self.amount_minor - other.amount_minor, self.currency)

    def _same_currency(self, other: Money) -> None:
        if other.currency != self.currency:
            raise ValueError(f"currency mismatch: {self.currency} vs {other.currency}")

    @classmethod
    def zero(cls, currency: str) -> Money:
        return cls(0, currency)

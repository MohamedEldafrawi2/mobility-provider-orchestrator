"""Refund quotes and refund offer identity (ADR 013).

A cancellation at a provider with refunds is two steps: a **quote** (the provider's refund
offer, with an identity, an amount, a fee and a validity bound) and an **acceptance** of that
exact offer. The platform persists the quote it authorised and settles an acceptance attempt
only by the status of *that* offer, never by the booking's state alone (6.2, CANCEL at A).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from orchestrator.domain.money import Money
from orchestrator.domain.time import require_aware


class RefundOfferState(StrEnum):
    PROPOSED = "PROPOSED"
    CONFIRMED = "CONFIRMED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class RefundOfferStatus:
    """A provider's report of one refund offer's identity and status."""

    offer_id: str
    state: RefundOfferState
    valid_until: datetime | None = None

    def __post_init__(self) -> None:
        require_aware(self.valid_until, "RefundOfferStatus.valid_until")


@dataclass(frozen=True, slots=True)
class RefundQuote:
    """One refund offer as quoted: the terms an acceptance is bound to."""

    offer_id: str
    refund: Money
    fee: Money
    valid_until: datetime

    def __post_init__(self) -> None:
        require_aware(self.valid_until, "RefundQuote.valid_until")
        if self.refund.currency != self.fee.currency:
            raise ValueError("refund and fee must share a currency")

    def within(self, max_fee: Money | None) -> bool:
        """Whether the quoted fee stays inside the terms the client authorised."""
        if max_fee is None:
            return False
        return (
            self.fee.currency == max_fee.currency and self.fee.amount_minor <= max_fee.amount_minor
        )

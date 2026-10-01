"""The single error type adapters may raise (docs/provider-integration-guide.md).

Two independent facts travel with every failure: what kind it is, and whether a side effect
may have happened. The orchestration layer never sees an HTTP status code.
"""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum

from orchestrator.domain import SideEffect


class ErrorKind(StrEnum):
    TRANSIENT = "transient"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    REJECTED = "rejected"
    OFFER_EXPIRED = "offer_expired"
    NOT_FOUND = "not_found"
    MALFORMED = "malformed"
    EXPIRED_REQUEST = "expired_request"
    FENCED = "fenced"
    HOLD_EXPIRED = "hold_expired"


class ProviderError(Exception):
    def __init__(
        self,
        kind: ErrorKind,
        side_effect: SideEffect,
        detail: str,
        *,
        retry_after: timedelta | None = None,
    ) -> None:
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.side_effect = side_effect
        self.detail = detail
        self.retry_after = retry_after

    @property
    def definitive(self) -> bool:
        """A rejection the provider made final: nothing was created by this attempt."""
        return (
            self.kind
            in (
                ErrorKind.REJECTED,
                ErrorKind.OFFER_EXPIRED,
                ErrorKind.NOT_FOUND,
                ErrorKind.EXPIRED_REQUEST,
                ErrorKind.FENCED,
                ErrorKind.HOLD_EXPIRED,
            )
            and self.side_effect is SideEffect.NONE
        )


class CapabilityNotSupportedError(Exception):
    """The adapter was asked for an operation its capabilities do not declare."""

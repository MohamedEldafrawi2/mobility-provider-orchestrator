"""Identifier types. Distinct ``NewType``s so a provider reference can never be passed where a
booking id is expected. Generation lives at the application boundary (it needs randomness);
the domain only names the types."""

from __future__ import annotations

from typing import NewType

BookingId = NewType("BookingId", str)
ClientId = NewType("ClientId", str)
CommandId = NewType("CommandId", str)
AttemptId = NewType("AttemptId", str)
ProviderCode = NewType("ProviderCode", str)
ProviderBookingRef = NewType("ProviderBookingRef", str)

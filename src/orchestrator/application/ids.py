"""Identifier generation. Random, so it sits outside the pure domain."""

from __future__ import annotations

import secrets

from orchestrator.domain import AttemptId, BookingId, CommandId


def _token(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(12)}"


def new_booking_id() -> BookingId:
    return BookingId(_token("bk"))


def new_command_id() -> CommandId:
    return CommandId(_token("cmd"))


def new_attempt_id() -> AttemptId:
    return AttemptId(_token("att"))

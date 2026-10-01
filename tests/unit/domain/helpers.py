from __future__ import annotations

from datetime import UTC, datetime, timedelta

from orchestrator.domain import (
    Attempt,
    AttemptId,
    AttemptOutcome,
    BookingId,
    Command,
    CommandId,
    CommandKind,
    CreateIntent,
    ProviderRequest,
    SideEffect,
)
from orchestrator.providers.bus_legacy.capabilities import BUS_LEGACY_CAPABILITIES

T0 = datetime(2026, 1, 1, tzinfo=UTC)
DEFAULT_BOOKING = BookingId("bk_1")
DEFAULT_COMMAND = CommandId("cmd_1")
PROVIDER_B = BUS_LEGACY_CAPABILITIES
INTENT = CreateIntent(offer_id="off_1", passenger_names=("Ada Lovelace",), contact_email="a@x.io")
REQUEST = ProviderRequest(payload=(("jid", "BUS-1"), ("yourRef", "bk_1")))


def new_command(
    kind: CommandKind = CommandKind.CREATE,
    booking_id: BookingId = DEFAULT_BOOKING,
    command_id: CommandId = DEFAULT_COMMAND,
) -> Command:
    return Command(
        id=command_id,
        booking_id=booking_id,
        kind=kind,
        intent=INTENT,
        provider_key=str(booking_id),
        created_at=T0,
    )


def attempt_id(n: int) -> AttemptId:
    return AttemptId(f"att_{n}")


def attempt(
    n: int,
    outcome: AttemptOutcome | None,
    side_effect: SideEffect | None,
    *,
    marked: bool = True,
    finished: bool = True,
) -> Attempt:
    start = T0 + timedelta(seconds=n)
    return Attempt(
        id=attempt_id(n),
        command_id=DEFAULT_COMMAND,
        n=n,
        request=REQUEST,
        dispatch_marked_at=start if marked else None,
        finished_at=start + timedelta(seconds=1) if finished else None,
        outcome=outcome,
        side_effect=side_effect,
    )


def open_attempt(n: int) -> Attempt:
    """The dispatch journal entry: marked, not finished, outcome unknown."""
    return attempt(n, None, None, finished=False)

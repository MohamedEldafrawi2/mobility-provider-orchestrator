"""The single function that maps a command's disposition to an HTTP outcome (section 6.5).

Both the initial response and every idempotent replay come from here, so they cannot
disagree. The booking's current state and unresolved reason are carried alongside, never used
to pick the status.
"""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.domain.commands import CommandKind, Disposition
from orchestrator.domain.states import BookingState


@dataclass(frozen=True, slots=True)
class Replay:
    status: int
    command_kind: CommandKind
    disposition: Disposition
    booking_state: BookingState
    problem_code: str | None = None
    unresolved_reason: str | None = None

    @property
    def unresolved(self) -> bool:
        return self.status == 202


_CREATE: dict[Disposition, tuple[int, str | None]] = {
    Disposition.OPEN: (202, None),
    Disposition.UNRESOLVED: (202, None),
    Disposition.SUCCEEDED: (201, None),
    Disposition.REJECTED: (422, "booking-rejected"),
    Disposition.ABANDONED: (422, "booking-not-submitted"),
}

_CANCEL: dict[Disposition, tuple[int, str | None]] = {
    Disposition.OPEN: (202, None),
    Disposition.UNRESOLVED: (202, None),
    Disposition.SUCCEEDED: (200, None),
    Disposition.REFUSED: (409, "booking-not-cancellable"),
    Disposition.TERMS_CHANGED: (409, "cancellation-terms-changed"),
}


def replay_for(
    kind: CommandKind,
    disposition: Disposition,
    *,
    booking_state: BookingState,
    unresolved_reason: str | None = None,
) -> Replay:
    table = {CommandKind.CREATE: _CREATE, CommandKind.CANCEL: _CANCEL}.get(kind)
    if table is None:
        raise NotImplementedError(f"{kind} replay is not implemented")
    try:
        status, code = table[disposition]
    except KeyError as exc:
        raise ValueError(f"{kind} cannot have disposition {disposition}") from exc
    if status != 202 and unresolved_reason is not None:
        raise ValueError("only an unresolved replay carries an unresolved reason")
    return Replay(
        status=status,
        command_kind=kind,
        disposition=disposition,
        booking_state=booking_state,
        problem_code=code,
        unresolved_reason=unresolved_reason,
    )

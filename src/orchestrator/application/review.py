"""Operator review of bookings the platform could not settle (docs/api.md).

Resolution is evidence-only: ``reconcile`` runs authoritative lookups (by every implicated
reference the provider can address, and always by our client reference) and stores the results
as dated evidence; ``resolve`` closes the case only into the state that evidence supports, under
an expected version, with an audit event naming the operator. There is no override.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from orchestrator.application.booking_create import BookingCreator
from orchestrator.application.ids import new_command_id
from orchestrator.domain import (
    BookingId,
    BookingState,
    Command,
    CommandId,
    CommandKind,
    Disposition,
    DispositionBasis,
    Evidence,
    EvidenceKind,
    NotClosable,
    Reservation,
    Trigger,
    bind,
    closable_into,
    decide_create_after_lookup,
    note_lookup,
    settle,
    transition,
)
from orchestrator.domain.cancellation import Cancelled, decide_cancel_after_lookup
from orchestrator.domain.refunds import RefundQuote
from orchestrator.domain.settlement import Bind, Escalate
from orchestrator.persistence.bookings import NotFoundError
from orchestrator.persistence.codecs import quote_to_json
from orchestrator.persistence.uow import UnitOfWorkFactory
from orchestrator.providers import CapabilityNotSupportedError, ProviderError
from orchestrator.providers.registry import ProviderRegistry
from orchestrator.resilience import AdmissionController, NotDispatchedError, Purpose


class ReviewError(Exception):
    def __init__(self, code: str, detail: str, status: int = 409) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status = status


@dataclass(frozen=True, slots=True)
class Resolution:
    booking_id: BookingId
    state: BookingState
    version: int


class ReviewService:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        registry: ProviderRegistry,
        creator: BookingCreator,
        *,
        admission: AdmissionController,
        canceller: Any = None,
        policy: Any = None,
    ) -> None:
        self.uow = uow
        self.registry = registry
        self.creator = creator
        self.admission = admission
        self.canceller = canceller
        self.policy = policy

    async def list_open(self) -> list[dict[str, Any]]:
        async with self.uow() as store:
            return await store.list_open_cases()

    async def detail(self, booking_id: BookingId) -> dict[str, Any]:
        async with self.uow() as store:
            try:
                booking = await store.get(booking_id)
            except NotFoundError as exc:
                raise ReviewError("not-found", f"no booking {booking_id}", 404) from exc
            found = await store.review_case(booking_id)
            if found is None:
                raise ReviewError("no-open-case", f"booking {booking_id} has no open case", 404)
            case, case_id = found
            command = await store.command_for(booking_id, CommandKind.CREATE)
            events = await store.events(booking_id)
        return {
            "case_id": case_id,
            "booking_id": booking_id,
            "state": booking.state.value,
            "version": booking.version,
            "bound_reference": booking.provider_booking_ref,
            "reason": case.reason,
            "remediable": case.remediable,
            "implicated": list(case.implicated),
            "command": {
                "id": command.id,
                "disposition": command.disposition.value,
                "basis": command.basis.value if command.basis else None,
                "possibly_executed": command.possibly_executed,
                "attempts": [
                    {
                        "n": a.n,
                        "dispatch_marked_at": a.dispatch_marked_at.isoformat()
                        if a.dispatch_marked_at
                        else None,
                        "outcome": a.outcome.value if a.outcome else None,
                        "side_effect": a.effective_side_effect.value,
                    }
                    for a in command.attempts
                ],
            },
            "evidence": [
                {
                    "kind": e.kind.value,
                    "initiated_at": e.initiated_at.isoformat(),
                    "subject": e.subject_ref,
                    "reservation": e.reservation.state.value if e.reservation else None,
                    "superseded": e.superseded,
                }
                for e in case.evidence
            ],
            "events": events,
        }

    async def reconcile(self, booking_id: BookingId, *, actor: str) -> dict[str, Any]:
        """Run authoritative lookups now and store them as evidence. May settle the booking."""
        async with self.uow() as store:
            booking = await store.get(booking_id)
            found_case = await store.review_case(booking_id)
        if found_case is None:
            raise ReviewError("no-open-case", f"booking {booking_id} has no open case", 404)
        case, case_id = found_case
        adapter = self.registry.get(booking.provider)
        initiated = datetime.now(UTC)
        try:
            async with self.admission.admit(
                adapter.code, Purpose.LOOKUP, operation="find_bookings_by_client_ref"
            ) as ticket:
                try:
                    by_client_ref = tuple(await adapter.find_bookings_by_client_ref(booking_id))
                except ProviderError as exc:
                    ticket.record_provider_error(exc)
                    raise
                ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
        except NotDispatchedError as exc:
            raise ReviewError("lookup-not-admitted", f"lookup refused: {exc.reason}", 503) from exc
        except ProviderError as exc:
            raise ReviewError("lookup-failed", f"provider lookup failed: {exc.kind}", 503) from exc

        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            seen = {r.ref for r in by_client_ref}
            for reservation in by_client_ref:
                await store.add_evidence(
                    case_id,
                    new_command_id().replace("cmd_", "ev_"),
                    Evidence(
                        EvidenceKind.LOOKUP_BY_CLIENT_REF, initiated, reservation.ref, reservation
                    ),
                )
            for ref in case.implicated:
                if ref in seen:
                    continue
                # Not returned by our reference; try by id where the provider can, else record
                # the negative client-reference lookup, which proves nothing but is dated.
                by_ref: Reservation | None = None
                try:
                    async with self.admission.admit(
                        adapter.code, Purpose.LOOKUP, operation="get_booking"
                    ) as ticket:
                        by_ref = await adapter.get_booking(ref)
                        ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
                except (CapabilityNotSupportedError, ProviderError, NotDispatchedError):
                    by_ref = None
                await store.add_evidence(
                    case_id,
                    new_command_id().replace("cmd_", "ev_"),
                    Evidence(
                        EvidenceKind.LOOKUP_BY_REF if by_ref else EvidenceKind.LOOKUP_BY_CLIENT_REF,
                        initiated,
                        ref,
                        by_ref,
                    ),
                )
            row = await store.open_case_row(booking_id)
            outstanding = (
                await store.command_by_id(CommandId(row.outstanding_command_id))
                if row is not None and row.outstanding_command_id
                else None
            )
            if outstanding is not None and outstanding.kind in (
                CommandKind.CANCEL,
                CommandKind.CANCEL_EXTRA,
            ):
                # A cancellation case is judged by the cancellation's own rules (the exact
                # refund offer, or the reservation's state for a free cancel), never through
                # the CREATE command, whose success would say nothing about the cancellation.
                await self._reconcile_cancellation(
                    store, booking, case_id, outstanding, by_client_ref, actor=actor
                )
            else:
                # The same decision the reconciler would make. A Bind leaves review only if the
                # case's complete evidence set closes it (the creator checks that); an Escalate
                # merges newly discovered references into the case.
                command = note_lookup(await store.command_for(booking_id, CommandKind.CREATE))
                decision = decide_create_after_lookup(
                    adapter.capabilities, command, by_client_ref, lookup_budget=10**9
                )
                if isinstance(decision, Bind | Escalate):
                    await self.creator.apply(
                        store,
                        booking,
                        command,
                        decision,
                        lease=None,
                        correlation_id=None,
                        source="OPERATOR",
                        actor=actor,
                    )
        return await self.detail_or_summary(booking_id)  # read after the commit

    async def _reconcile_cancellation(
        self,
        store: Any,
        booking: Any,
        case_id: str,
        command: Command,
        found: tuple[Reservation, ...],
        *,
        actor: str,
    ) -> None:
        """Close a cancellation case only when the bound reservation is affirmatively cancelled
        through the exact offer the client accepted (or, for a free cancel, is cancelled), and
        the case's complete evidence set agrees. Anything else stays with the operator."""
        if self.canceller is None or self.policy is None or booking.provider_booking_ref is None:
            return
        bound = next((r for r in found if r.ref == booking.provider_booking_ref), None)
        if bound is None:
            return
        quote = command.quote if isinstance(command.quote, RefundQuote) else None
        now = datetime.now(UTC)
        decision = decide_cancel_after_lookup(
            self.registry.get(booking.provider).capabilities,
            command,
            bound,
            bound=str(booking.provider_booking_ref),
            quote=quote,
            now=now,
            policy=self.policy.expiry,
        )
        if not isinstance(decision, Cancelled):
            return
        refreshed = await store.review_case(booking.id)
        if refreshed is None:
            return
        case, _ = refreshed
        target = closable_into(case, booking.provider_booking_ref, now=now, command=command)
        if target is not BookingState.CANCELLED:
            return
        # Under review the canceller settles the command only; the booking's transition and the
        # case's closure are this service's, in the same transaction as the evidence.
        await self.canceller.apply(
            store, booking, command, decision, lease=None, correlation_id=None, source="OPERATOR"
        )
        await store.save_booking(
            booking,
            state=BookingState.CANCELLED,
            trigger=Trigger.PROVIDER_CANCELLED,
            source="OPERATOR",
            actor=actor,
            refund={"quote": quote_to_json(command.quote), "status": "CONFIRMED"},
            provider_generation=bound.generation,
            last_revision=bound.revision,
            clear_next_action=True,
            clear_unresolved=True,
        )
        await store.close_review_case(case_id, resolution=BookingState.CANCELLED.value, actor=actor)

    async def resolve(
        self, booking_id: BookingId, *, expected_version: int, actor: str, reason: str
    ) -> Resolution:
        async with self.uow() as store:
            booking = await store.get(booking_id, for_update=True)
            if booking.version != expected_version:
                # A retried resolve is answered with its original result, not a conflict.
                done = await store.closed_case_at_version(
                    booking_id, closed_version=expected_version
                )
                if done is not None:
                    _, resolution, _ = done
                    return Resolution(booking_id, BookingState(resolution), expected_version + 1)
                raise ReviewError(
                    "version-mismatch", f"booking is at version {booking.version}", 409
                )
            found = await store.review_case(booking_id)
            if found is None:
                raise ReviewError("no-open-case", f"booking {booking_id} has no open case", 404)
            case, case_id = found
            if case.implicated and not case.evidence:
                raise ReviewError("lookup-required", "run reconcile before resolving")
            # The case's own outstanding command decides (a CANCEL case is judged by its quote).
            row = await store.open_case_row(booking_id)
            command = (
                await store.command_by_id(CommandId(row.outstanding_command_id))
                if row is not None and row.outstanding_command_id
                else await store.command_for(booking_id, CommandKind.CREATE)
            )
            target = closable_into(
                case, booking.provider_booking_ref, now=datetime.now(UTC), command=command
            )
            if isinstance(target, NotClosable):
                raise ReviewError("case-not-closable", target.reason)
            trigger = _trigger_into(booking.state, target)
            caps = self.registry.get(booking.provider).capabilities
            create = await store.command_for(booking_id, CommandKind.CREATE)
            if target is BookingState.CONFIRMED and create.disposition is not Disposition.SUCCEEDED:
                assert booking.provider_booking_ref is not None  # closable_into requires it
                create = bind(create, booking.provider_booking_ref)
                create = settle(create, Disposition.SUCCEEDED, DispositionBasis.LOOKUP, caps=caps)
                await store.save_command(create)
            resumes = target is BookingState.CANCELLING
            if resumes and command.kind is CommandKind.CANCEL:
                # The outstanding cancellation resumes: open again, scheduled now (6.1).
                await store.save_command(replace(command, disposition=Disposition.OPEN, basis=None))
            if (
                target is BookingState.CANCELLED
                and command.kind is CommandKind.CANCEL
                and command.disposition is not Disposition.SUCCEEDED
            ):
                await store.save_command(
                    settle(command, Disposition.SUCCEEDED, DispositionBasis.LOOKUP, caps=caps)
                )
            updated = await store.save_booking(
                booking,
                state=target,
                trigger=trigger,
                source="OPERATOR",
                actor=actor,
                payload={"reason": reason, "case_id": case_id},
                clear_unresolved=True,
                clear_next_action=not resumes,
                next_action_at=datetime.now(UTC) if resumes else None,
            )
            await store.close_review_case(
                case_id, resolution=target.value, actor=actor, closed_version=expected_version
            )
        return Resolution(booking_id, updated.state, updated.version)

    async def detail_or_summary(self, booking_id: BookingId) -> dict[str, Any]:
        try:
            return await self.detail(booking_id)
        except ReviewError:
            async with self.uow() as store:
                booking = await store.get(booking_id)
            return {
                "booking_id": booking_id,
                "state": booking.state.value,
                "version": booking.version,
                "bound_reference": booking.provider_booking_ref,
                "case": "closed",
            }


def _trigger_into(state: BookingState, target: BookingState) -> Trigger:
    for trigger in (
        Trigger.PROVIDER_CONFIRMED,
        Trigger.PROVIDER_HELD,
        Trigger.PROVIDER_PENDING,
        Trigger.PROVIDER_CANCELLED,
        Trigger.PROVIDER_REJECTED,
        Trigger.CANCEL_RESUMED,
    ):
        if transition(state, trigger) is target:
            return trigger
    raise ReviewError("case-not-closable", f"no transition from {state} into {target}")

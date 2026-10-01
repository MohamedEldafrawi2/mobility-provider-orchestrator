"""The booking store: rows in, domain values out, and the two concurrency primitives the design
depends on.

- ``lock`` takes ``SELECT ... FOR UPDATE`` on the booking row for the duration of a unit of work.
- ``claim`` is the worker's leased claim: ``FOR UPDATE SKIP LOCKED`` to pick rows, a lease token
  and expiry written in the same statement, and every later write by that worker fenced with
  ``WHERE lease_token = :token`` (``save_booking`` with ``lease``).

Provider calls never happen while a row is locked; callers open one unit of work to journal
the attempt, close it, call the provider, and open another to record the outcome.
"""

from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.domain import (
    Attempt,
    AttemptId,
    AttemptOutcome,
    BookingId,
    BookingState,
    CancelPhase,
    ClientId,
    Command,
    CommandId,
    CommandKind,
    Disposition,
    DispositionBasis,
    Evidence,
    EvidenceKind,
    ProviderBookingRef,
    ProviderCode,
    ReviewCase,
    SideEffect,
    Trigger,
)
from orchestrator.domain.offers import Offer
from orchestrator.domain.states import ACTIVE_STATES
from orchestrator.persistence.codecs import (
    intent_from_json,
    intent_to_json,
    offer_from_json,
    offer_to_json,
    quote_from_json,
    quote_to_json,
    request_from_json,
    request_to_json,
    reservation_from_json,
    reservation_to_json,
)
from orchestrator.persistence.models import (
    AttemptRow,
    BookingEventRow,
    BookingRow,
    CommandRow,
    EvidenceRow,
    IdempotencyRow,
    ReviewCaseRow,
    WebhookReceiptRow,
)


class StaleLeaseError(Exception):
    """A fenced write found the lease gone: another worker owns the booking now."""


class NotFoundError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Booking:
    """The booking aggregate as the application sees it."""

    id: BookingId
    client_id: ClientId
    provider: ProviderCode
    state: BookingState
    offer: Offer
    passenger_names: tuple[str, ...]
    contact_email: str
    provider_booking_ref: ProviderBookingRef | None
    unresolved_reason: str | None
    failure_code: str | None
    version: int
    next_action_at: datetime | None
    created_at: datetime
    updated_at: datetime
    lease_expires_at: datetime | None
    confirmation_deadline: datetime | None = None
    provider_generation: int | None = None
    last_revision: int | None = None
    confirm_budget_remaining: int | None = None
    refund: dict[str, Any] | None = None

    def lease_live(self, now: datetime) -> bool:
        return self.lease_expires_at is not None and self.lease_expires_at > now


@dataclass(frozen=True, slots=True)
class Lease:
    booking_id: BookingId
    token: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    client_id: ClientId
    key: str
    fingerprint: str
    command_id: CommandId


def _booking_from_row(row: BookingRow) -> Booking:
    return Booking(
        id=BookingId(row.id),
        client_id=ClientId(row.client_id),
        provider=ProviderCode(row.provider),
        state=BookingState(row.state),
        offer=offer_from_json(row.offer_snapshot),
        passenger_names=tuple(row.passengers["names"]),
        contact_email=row.contact_email,
        provider_booking_ref=ProviderBookingRef(row.provider_booking_ref)
        if row.provider_booking_ref
        else None,
        unresolved_reason=row.unresolved_reason,
        failure_code=row.failure_code,
        version=row.version,
        next_action_at=row.next_action_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
        lease_expires_at=row.lease_expires_at,
        confirmation_deadline=row.confirmation_deadline,
        provider_generation=row.provider_generation,
        last_revision=row.last_revision,
        confirm_budget_remaining=row.confirm_budget_remaining,
        refund=row.refund,
    )


def _command_from_rows(row: CommandRow, attempts: list[AttemptRow]) -> Command:
    return Command(
        id=CommandId(row.id),
        booking_id=BookingId(row.booking_id),
        kind=CommandKind(row.kind),
        intent=intent_from_json(row.intent),
        provider_key=row.provider_key,
        created_at=row.created_at,
        first_dispatch_at=row.first_dispatch_at,
        execution_cutoff=row.execution_cutoff,
        disposition=Disposition(row.disposition),
        basis=DispositionBasis(row.basis) if row.basis else None,
        submission_ref=ProviderBookingRef(row.submission_ref) if row.submission_ref else None,
        phase=CancelPhase(row.phase) if row.phase else None,
        quote=quote_from_json(row.quote),
        target_ref=ProviderBookingRef(row.target_ref) if row.target_ref else None,
        attempts=tuple(
            Attempt(
                id=AttemptId(a.id),
                command_id=CommandId(row.id),
                n=a.n,
                request=request_from_json(a.request),
                dispatch_marked_at=a.dispatch_marked_at,
                finished_at=a.finished_at,
                outcome=AttemptOutcome(a.outcome) if a.outcome else None,
                side_effect=SideEffect(a.side_effect) if a.side_effect else None,
                error=a.error,
                excluded_at=a.excluded_at,
            )
            for a in sorted(attempts, key=lambda a: a.n)
        ),
        lookups_performed=row.lookups_performed,
    )


class BookingStore:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session
        self.after_commit_callbacks: list[Callable[[], None]] = []

    def after_commit(self, callback: Callable[[], None]) -> None:
        """Run ``callback`` once this unit of work has committed, never otherwise."""
        self.after_commit_callbacks.append(callback)

    # Creation --------------------------------------------------------------------------------

    async def insert_new(
        self,
        booking: Booking,
        command: Command,
        idempotency: IdempotencyRecord,
        *,
        next_action_at: datetime,
    ) -> None:
        self.s.add(
            BookingRow(
                id=booking.id,
                client_id=booking.client_id,
                provider=booking.provider,
                state=booking.state.value,
                offer_snapshot=offer_to_json(booking.offer),
                passengers={"names": list(booking.passenger_names)},
                contact_email=booking.contact_email,
                version=0,
                next_action_at=next_action_at,
                created_at=booking.created_at,
                updated_at=booking.created_at,
            )
        )
        self.s.add(
            CommandRow(
                id=command.id,
                booking_id=command.booking_id,
                kind=command.kind.value,
                intent=intent_to_json(command.intent),
                provider_key=command.provider_key,
                disposition=command.disposition.value,
                created_at=command.created_at,
            )
        )
        self.s.add(
            IdempotencyRow(
                client_id=idempotency.client_id,
                key=idempotency.key,
                request_fingerprint=idempotency.fingerprint,
                command_id=idempotency.command_id,
            )
        )
        await self.s.flush()

    async def find_idempotency(self, client_id: ClientId, key: str) -> IdempotencyRecord | None:
        row = await self.s.get(IdempotencyRow, (client_id, key))
        if row is None:
            return None
        return IdempotencyRecord(
            ClientId(row.client_id), row.key, row.request_fingerprint, CommandId(row.command_id)
        )

    # Reads -------------------------------------------------------------------------------------

    async def get(
        self, booking_id: BookingId, *, for_update: bool = False, for_share: bool = False
    ) -> Booking:
        """``for_share`` keeps writers out until this transaction ends, so a booking and its
        command read in the same transaction come from one consistent state (ADR 007)."""
        stmt = select(BookingRow).where(BookingRow.id == booking_id)
        if for_update:
            stmt = stmt.with_for_update()
        elif for_share:
            stmt = stmt.with_for_update(read=True)
        row = (await self.s.execute(stmt)).scalar_one_or_none()
        if row is None:
            raise NotFoundError(booking_id)
        return _booking_from_row(row)

    async def get_owned(self, booking_id: BookingId, client_id: ClientId) -> Booking | None:
        row = await self.s.get(BookingRow, booking_id)
        if row is None or row.client_id != client_id:
            return None
        return _booking_from_row(row)

    async def command_for(self, booking_id: BookingId, kind: CommandKind) -> Command:
        """The latest command of ``kind`` for the booking (replacement commands are newer)."""
        row = (
            await self.s.execute(
                select(CommandRow)
                .where(CommandRow.booking_id == booking_id, CommandRow.kind == kind.value)
                .order_by(CommandRow.created_at.desc(), CommandRow.id.desc())
                .limit(1)
            )
        ).scalar_one()
        attempts = list(
            (await self.s.execute(select(AttemptRow).where(AttemptRow.command_id == row.id)))
            .scalars()
            .all()
        )
        return _command_from_rows(row, attempts)

    async def commands_for(self, booking_id: BookingId, kind: CommandKind) -> list[Command]:
        rows = (
            await self.s.execute(
                select(CommandRow)
                .where(CommandRow.booking_id == booking_id, CommandRow.kind == kind.value)
                .order_by(CommandRow.created_at, CommandRow.id)
            )
        ).scalars()
        out = []
        for row in rows:
            attempts = list(
                (await self.s.execute(select(AttemptRow).where(AttemptRow.command_id == row.id)))
                .scalars()
                .all()
            )
            out.append(_command_from_rows(row, attempts))
        return out

    async def open_command(self, booking_id: BookingId, kind: CommandKind) -> Command | None:
        """The latest command of ``kind`` if it is still OPEN, else None."""
        found = await self.commands_for(booking_id, kind)
        if not found:
            return None
        latest = found[-1]
        return latest if latest.disposition is Disposition.OPEN else None

    async def add_command(
        self, command: Command, *, idempotency: IdempotencyRecord | None = None
    ) -> None:
        self.s.add(
            CommandRow(
                id=command.id,
                booking_id=command.booking_id,
                kind=command.kind.value,
                intent=intent_to_json(command.intent),
                provider_key=command.provider_key,
                disposition=command.disposition.value,
                submission_ref=command.submission_ref,
                phase=command.phase.value if command.phase else None,
                quote=quote_to_json(command.quote),
                target_ref=command.target_ref,
                created_at=command.created_at,
            )
        )
        if idempotency is not None:
            self.s.add(
                IdempotencyRow(
                    client_id=idempotency.client_id,
                    key=idempotency.key,
                    request_fingerprint=idempotency.fingerprint,
                    command_id=idempotency.command_id,
                )
            )
        await self.s.flush()

    async def command_by_id(self, command_id: CommandId) -> Command:
        row = await self.s.get(CommandRow, command_id)
        if row is None:
            raise NotFoundError(command_id)
        attempts = list(
            (await self.s.execute(select(AttemptRow).where(AttemptRow.command_id == row.id)))
            .scalars()
            .all()
        )
        return _command_from_rows(row, attempts)

    # Writes ------------------------------------------------------------------------------------

    async def save_command(self, command: Command) -> None:
        await self.s.execute(
            update(CommandRow)
            .where(CommandRow.id == command.id)
            .values(
                first_dispatch_at=command.first_dispatch_at,
                execution_cutoff=command.execution_cutoff,
                disposition=command.disposition.value,
                basis=command.basis.value if command.basis else None,
                submission_ref=command.submission_ref,
                lookups_performed=command.lookups_performed,
                phase=command.phase.value if command.phase else None,
                quote=quote_to_json(command.quote),
                target_ref=command.target_ref,
            )
        )
        for attempt in command.attempts:
            existing = await self.s.get(AttemptRow, attempt.id)
            if existing is None:
                self.s.add(
                    AttemptRow(
                        id=attempt.id,
                        command_id=command.id,
                        n=attempt.n,
                        request=request_to_json(attempt.request),
                        dispatch_marked_at=attempt.dispatch_marked_at,
                        finished_at=attempt.finished_at,
                        outcome=attempt.outcome.value if attempt.outcome else None,
                        side_effect=attempt.side_effect.value if attempt.side_effect else None,
                        error=attempt.error,
                        excluded_at=attempt.excluded_at,
                    )
                )
            else:
                existing.finished_at = attempt.finished_at
                existing.error = attempt.error
                existing.excluded_at = attempt.excluded_at
                existing.outcome = attempt.outcome.value if attempt.outcome else None
                existing.side_effect = attempt.side_effect.value if attempt.side_effect else None
        await self.s.flush()

    async def save_booking(
        self,
        booking: Booking,
        *,
        state: BookingState | None = None,
        trigger: Trigger | None = None,
        source: str = "PLATFORM",
        actor: str | None = None,
        payload: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        next_action_at: datetime | None = None,
        clear_next_action: bool = False,
        provider_booking_ref: ProviderBookingRef | None = None,
        unresolved_reason: str | None = None,
        clear_unresolved: bool = False,
        failure_code: str | None = None,
        lease: Lease | None = None,
        confirmation_deadline: datetime | None = None,
        provider_generation: int | None = None,
        last_revision: int | None = None,
        confirm_budget_remaining: int | None = None,
        refund: dict[str, Any] | None = None,
    ) -> Booking:
        """Persist a state change (and its audit event) with optional lease fencing."""
        values: dict[str, Any] = {"version": booking.version + 1, "updated_at": _now()}
        new_state = booking.state
        if state is not None and state is not booking.state:
            values["state"] = state.value
            new_state = state
        if next_action_at is not None:
            values["next_action_at"] = next_action_at
        if clear_next_action:
            values["next_action_at"] = None
        if provider_booking_ref is not None:
            values["provider_booking_ref"] = provider_booking_ref
        if unresolved_reason is not None:
            values["unresolved_reason"] = unresolved_reason
        if clear_unresolved:
            values["unresolved_reason"] = None
        if failure_code is not None:
            values["failure_code"] = failure_code
        if confirmation_deadline is not None:
            values["confirmation_deadline"] = confirmation_deadline
        if provider_generation is not None:
            values["provider_generation"] = provider_generation
        if last_revision is not None:
            values["last_revision"] = last_revision
        if confirm_budget_remaining is not None:
            values["confirm_budget_remaining"] = confirm_budget_remaining
        if refund is not None:
            values["refund"] = refund
        stmt = update(BookingRow).where(
            BookingRow.id == booking.id, BookingRow.version == booking.version
        )
        if lease is not None:
            stmt = stmt.where(
                BookingRow.lease_token == lease.token, BookingRow.lease_expires_at > _now()
            )
        result = await self.s.execute(stmt.values(**values))
        if getattr(result, "rowcount", 0) != 1:
            raise StaleLeaseError(booking.id)
        if trigger is not None or state is not None:
            await self._append_event(
                booking.id,
                from_state=booking.state,
                to_state=new_state,
                trigger=trigger.value if trigger else "NOTE",
                source=source,
                actor=actor,
                payload=payload or {},
                correlation_id=correlation_id,
            )
        return replace(
            booking,
            state=new_state,
            version=booking.version + 1,
            provider_booking_ref=provider_booking_ref or booking.provider_booking_ref,
            unresolved_reason=None
            if clear_unresolved
            else (unresolved_reason or booking.unresolved_reason),
            failure_code=failure_code or booking.failure_code,
            next_action_at=None
            if clear_next_action
            else (next_action_at or booking.next_action_at),
        )

    async def _append_event(
        self,
        booking_id: BookingId,
        *,
        from_state: BookingState | None,
        to_state: BookingState,
        trigger: str,
        source: str,
        actor: str | None,
        payload: dict[str, Any],
        correlation_id: str | None,
    ) -> None:
        seq = (
            await self.s.execute(
                select(func.coalesce(func.max(BookingEventRow.seq), 0)).where(
                    BookingEventRow.booking_id == booking_id
                )
            )
        ).scalar_one() + 1
        self.s.add(
            BookingEventRow(
                booking_id=booking_id,
                seq=seq,
                from_state=from_state.value if from_state else None,
                to_state=to_state.value,
                trigger=trigger,
                source=source,
                actor=actor,
                payload=payload,
                correlation_id=correlation_id,
                occurred_at=_now(),
            )
        )
        await self.s.flush()

    async def events(self, booking_id: BookingId) -> list[dict[str, Any]]:
        rows = (
            await self.s.execute(
                select(BookingEventRow)
                .where(BookingEventRow.booking_id == booking_id)
                .order_by(BookingEventRow.seq)
            )
        ).scalars()
        return [
            {
                "seq": r.seq,
                "from": r.from_state,
                "to": r.to_state,
                "trigger": r.trigger,
                "source": r.source,
                "actor": r.actor,
                "at": r.occurred_at.isoformat(),
            }
            for r in rows
        ]

    # Worker claims -----------------------------------------------------------------------------

    async def claim(
        self, states: tuple[BookingState, ...], *, limit: int, ttl: timedelta
    ) -> list[tuple[Booking, Lease]]:
        """Lease up to ``limit`` due bookings in ``states``. One statement, SKIP LOCKED."""
        token = secrets.token_hex(16)
        now = _now()
        expires = now + ttl
        result = await self.s.execute(
            text(
                """
                UPDATE bookings SET lease_token = :token, lease_expires_at = :expires
                WHERE id IN (
                    SELECT id FROM bookings
                    WHERE state = ANY(:states)
                      AND next_action_at IS NOT NULL AND next_action_at <= :now
                      AND (lease_expires_at IS NULL OR lease_expires_at < :now)
                    ORDER BY confirmation_deadline NULLS LAST, next_action_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                )
                RETURNING id
                """
            ),
            {
                "token": token,
                "expires": expires,
                "now": now,
                "states": [s.value for s in states],
                "limit": limit,
            },
        )
        ids = [BookingId(r[0]) for r in result.fetchall()]
        claimed: list[tuple[Booking, Lease]] = []
        for booking_id in ids:
            claimed.append((await self.get(booking_id), Lease(booking_id, token, expires)))
        return claimed

    async def claim_one(self, booking_id: BookingId, *, ttl: timedelta) -> Lease | None:
        """Lease one specific row now (the request path taking ownership of a hold or a
        cancellation it just created), or None if a live lease exists."""
        token = secrets.token_hex(16)
        now = _now()
        expires = now + ttl
        result = await self.s.execute(
            update(BookingRow)
            .where(
                BookingRow.id == booking_id,
                (BookingRow.lease_expires_at.is_(None)) | (BookingRow.lease_expires_at < now),
            )
            .values(lease_token=token, lease_expires_at=expires)
        )
        if getattr(result, "rowcount", 0) != 1:
            return None
        return Lease(booking_id, token, expires)

    async def renew(self, lease: Lease, *, ttl: timedelta) -> Lease:
        """Extend a lease that is still ours and still live; otherwise it is stale."""
        now = _now()
        expires = now + ttl
        result = await self.s.execute(
            update(BookingRow)
            .where(
                BookingRow.id == lease.booking_id,
                BookingRow.lease_token == lease.token,
                BookingRow.lease_expires_at > now,
            )
            .values(lease_expires_at=expires)
        )
        if getattr(result, "rowcount", 0) != 1:
            raise StaleLeaseError(lease.booking_id)
        return Lease(lease.booking_id, lease.token, expires)

    async def defer(self, lease: Lease, *, until: datetime) -> None:
        """Push a row's next action back without touching its state or version: the loop's
        handler failed on it and must not spin on it (fenced by the lease)."""
        await self.s.execute(
            update(BookingRow)
            .where(BookingRow.id == lease.booking_id, BookingRow.lease_token == lease.token)
            .values(next_action_at=until)
        )

    async def release(self, lease: Lease) -> None:
        await self.s.execute(
            update(BookingRow)
            .where(BookingRow.id == lease.booking_id, BookingRow.lease_token == lease.token)
            .values(lease_token=None, lease_expires_at=None)
        )

    async def expired_submitting(self, *, stale_before: datetime) -> list[Booking]:
        """Bookings whose request-path submission died: SUBMITTING, untouched since
        ``stale_before`` and with no live lease. The staleness policy is the caller's."""
        rows = (
            await self.s.execute(
                select(BookingRow).where(
                    BookingRow.state.in_(
                        (BookingState.SUBMITTING.value, BookingState.CONFIRMING.value)
                    ),
                    (BookingRow.lease_expires_at.is_(None))
                    | (BookingRow.lease_expires_at < stale_before),
                    BookingRow.updated_at < stale_before,
                )
            )
        ).scalars()
        return [_booking_from_row(r) for r in rows]

    # Review cases ------------------------------------------------------------------------------

    async def open_review_case(
        self,
        booking_id: BookingId,
        *,
        case_id: str,
        reason: str,
        remediable: bool,
        outstanding_command_id: CommandId | None,
        implicated: tuple[ProviderBookingRef, ...],
    ) -> None:
        existing = await self.open_case_row(booking_id)
        if existing is not None:
            merged = list(dict.fromkeys([*existing.implicated["refs"], *implicated]))
            existing.implicated = {"refs": merged}
            existing.reason = reason
            existing.remediable = remediable
            await self.s.flush()
            return
        self.s.add(
            ReviewCaseRow(
                id=case_id,
                booking_id=booking_id,
                reason=reason,
                remediable=remediable,
                outstanding_command_id=outstanding_command_id,
                implicated={"refs": list(implicated)},
                opened_at=_now(),
            )
        )
        await self.s.flush()

    async def open_case_row(self, booking_id: BookingId) -> ReviewCaseRow | None:
        return (
            await self.s.execute(
                select(ReviewCaseRow).where(
                    ReviewCaseRow.booking_id == booking_id, ReviewCaseRow.closed_at.is_(None)
                )
            )
        ).scalar_one_or_none()

    async def review_case(self, booking_id: BookingId) -> tuple[ReviewCase, str] | None:
        row = await self.open_case_row(booking_id)
        if row is None:
            return None
        evidence_rows = (
            await self.s.execute(select(EvidenceRow).where(EvidenceRow.review_case_id == row.id))
        ).scalars()
        outstanding = None
        if row.outstanding_command_id:
            outstanding = CommandKind(
                (await self.command_by_id(CommandId(row.outstanding_command_id))).kind
            )
        case = ReviewCase(
            reason=row.reason,
            remediable=row.remediable,
            outstanding_command=outstanding,
            implicated=tuple(ProviderBookingRef(r) for r in row.implicated["refs"]),
            evidence=tuple(
                Evidence(
                    kind=EvidenceKind(e.kind),
                    initiated_at=e.initiated_at,
                    subject_ref=ProviderBookingRef(e.subject_ref),
                    reservation=reservation_from_json(e.reservation) if e.reservation else None,
                    valid_until=e.valid_until,
                    superseded=e.superseded,
                )
                for e in evidence_rows
            ),
        )
        return case, row.id

    async def add_evidence(self, case_id: str, evidence_id: str, evidence: Evidence) -> None:
        self.s.add(
            EvidenceRow(
                id=evidence_id,
                review_case_id=case_id,
                kind=evidence.kind.value,
                initiated_at=evidence.initiated_at,
                subject_ref=evidence.subject_ref,
                reservation=reservation_to_json(evidence.reservation)
                if evidence.reservation
                else None,
                valid_until=evidence.valid_until,
                superseded=evidence.superseded,
            )
        )
        await self.s.flush()

    async def close_review_case(
        self, case_id: str, *, resolution: str, actor: str, closed_version: int | None = None
    ) -> None:
        """``closed_version``: the booking version an operator's resolve was issued against,
        so a retried resolve can be recognised and replayed (docs/api.md, idempotency)."""
        row = await self.s.get(ReviewCaseRow, case_id)
        if row is None:
            raise NotFoundError(case_id)
        row.closed_at = _now()
        row.resolution = resolution
        row.resolved_by = actor
        row.closed_version = closed_version
        await self.s.flush()

    async def extend_review_case(
        self, case_id: str, *, implicated: tuple[ProviderBookingRef, ...]
    ) -> None:
        row = await self.s.get(ReviewCaseRow, case_id)
        if row is None:
            raise NotFoundError(case_id)
        row.implicated = {"refs": list(dict.fromkeys([*row.implicated["refs"], *implicated]))}
        await self.s.flush()

    async def closed_case_at_version(
        self, booking_id: BookingId, *, closed_version: int
    ) -> tuple[str, str, str] | None:
        """(case id, resolution, actor) of the case an operator closed at that version."""
        row = (
            await self.s.execute(
                select(ReviewCaseRow).where(
                    ReviewCaseRow.booking_id == booking_id,
                    ReviewCaseRow.closed_version == closed_version,
                )
            )
        ).scalar_one_or_none()
        if row is None or row.resolution is None or row.resolved_by is None:
            return None
        return row.id, row.resolution, row.resolved_by

    async def add_webhook_receipt(
        self, provider: str, event_id: str, outcome: str, booking_id: str | None
    ) -> None:
        """Inside a savepoint, so a duplicate (IntegrityError) leaves the transaction usable."""
        async with self.s.begin_nested():
            self.s.add(
                WebhookReceiptRow(
                    provider=provider, event_id=event_id, outcome=outcome, booking_id=booking_id
                )
            )
            await self.s.flush()

    async def rollback_to_savepoint(self) -> None:
        """The nested transaction already rolled back on error; nothing else to do."""
        return None

    def savepoint(self) -> Any:
        """A nested transaction: a constraint violation inside it leaves the outer
        transaction usable (``async with store.savepoint(): ...``)."""
        return self.s.begin_nested()

    async def count_by_state(self) -> dict[tuple[str, str], int]:
        """(state, provider) -> count, for the worker-owned gauge."""
        rows = await self.s.execute(
            select(BookingRow.state, BookingRow.provider, func.count()).group_by(
                BookingRow.state, BookingRow.provider
            )
        )
        return {(str(state), str(provider)): int(n) for state, provider, n in rows.all()}

    async def oldest_unresolved_age(self, *, now: datetime) -> dict[str, float]:
        """state -> age in seconds of the oldest booking in that state, for every state still
        in flight (the worker-owned staleness gauge): neither settled nor under review."""
        active = [s.value for s in ACTIVE_STATES]
        rows = await self.s.execute(
            select(BookingRow.state, func.min(BookingRow.created_at))
            .where(BookingRow.state.in_(active))
            .group_by(BookingRow.state)
        )
        return {
            str(state): max((now - oldest).total_seconds(), 0.0)
            for state, oldest in rows.all()
            if oldest is not None
        }

    async def count_open_cases(self) -> dict[tuple[str, str], int]:
        """(provider, remediable) -> open review cases."""
        rows = await self.s.execute(
            select(BookingRow.provider, ReviewCaseRow.remediable, func.count())
            .join(BookingRow, BookingRow.id == ReviewCaseRow.booking_id)
            .where(ReviewCaseRow.closed_at.is_(None))
            .group_by(BookingRow.provider, ReviewCaseRow.remediable)
        )
        return {
            (str(provider), "true" if remediable else "false"): int(n)
            for provider, remediable, n in rows.all()
        }

    async def list_open_cases(self) -> list[dict[str, Any]]:
        rows = (
            await self.s.execute(
                select(ReviewCaseRow)
                .where(ReviewCaseRow.closed_at.is_(None))
                .order_by(ReviewCaseRow.opened_at)
            )
        ).scalars()
        return [
            {
                "case_id": r.id,
                "booking_id": r.booking_id,
                "reason": r.reason,
                "remediable": r.remediable,
                "implicated": r.implicated["refs"],
                "opened_at": r.opened_at.isoformat(),
            }
            for r in rows
        ]


def _now() -> datetime:
    return datetime.now(UTC)

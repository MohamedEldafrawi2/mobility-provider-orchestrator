"""Inbound provider events (docs/architecture.md, observations; ADR 011).

The handler verifies the signature (Standard Webhooks, a tolerance window, several keys during
rotation), maps the event through the provider's adapter to a canonical observation, and, in
**one transaction**, inserts the receipt and applies the observation under the booking's row
lock: a duplicate is the receipt's unique constraint firing (``DUPLICATE``); an event for a
reference the platform does not hold is ``UNMATCHED``; ordering is generation then revision;
a legal transition applies, an illegal one is contradictory evidence and opens a review case.
Any failure after verification is a 5xx, so the provider retries and the receipt decides.

An event that overtakes the create response (the booking has no bound reference yet) is never
applied on its own word: an authoritative read of the reservation it names must agree with it
on both references, or the disagreement is contradictory evidence for review. An event about a
booking under review is quarantined: it joins the case's evidence and the case decides.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.exc import IntegrityError
from standardwebhooks import Webhook

from orchestrator.application.booking_create import BookingCreator
from orchestrator.application.ids import new_command_id
from orchestrator.domain import (
    Bind,
    BookingState,
    CommandKind,
    Disposition,
    DispositionBasis,
    Escalate,
    ProviderCode,
    Reservation,
    ReservationState,
    Trigger,
    bind,
    decide_create_after_lookup,
    settle,
)
from orchestrator.domain.observations import ObservationOutcome, order_observation
from orchestrator.domain.review import Evidence, EvidenceKind
from orchestrator.domain.settlement import REASON_IDENTITY
from orchestrator.persistence.bookings import Booking, BookingStore, NotFoundError
from orchestrator.persistence.uow import UnitOfWorkFactory
from orchestrator.providers import ProviderError
from orchestrator.providers.registry import ProviderRegistry, UnknownProviderError
from orchestrator.resilience import AdmissionController, NotDispatchedError, Purpose
from orchestrator.telemetry import get_logger, metrics

log = get_logger(__name__)

REASON_GENERATION_AHEAD = "observation-from-a-newer-generation"
REASON_CONTRADICTORY = "observation-contradicts-state"
_UNBOUND_STATES = (BookingState.SUBMITTING, BookingState.UNKNOWN, BookingState.CREATED)


@dataclass(frozen=True, slots=True)
class EarlyMismatch:
    """The authoritative read disagrees with an early event about the booking's identity."""

    event: Reservation
    read: Reservation


class WebhookRejectedError(Exception):
    def __init__(self, status: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.code = code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class WebhookResult:
    outcome: ObservationOutcome
    booking_id: str | None
    event_id: str


class WebhookService:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        registry: ProviderRegistry,
        creator: BookingCreator,
        *,
        secrets: dict[ProviderCode, list[str]],
        max_body_bytes: int = 64 * 1024,
        admission: AdmissionController | None = None,
    ) -> None:
        self.uow = uow
        self.registry = registry
        self.creator = creator
        self.secrets = secrets
        self.max_body_bytes = max_body_bytes
        self.admission = admission

    async def receive(self, provider: str, body: bytes, headers: dict[str, str]) -> WebhookResult:
        try:
            return await self._receive(provider, body, headers)
        except WebhookRejectedError as exc:
            self.count_rejection(provider, exc.code)
            raise

    def count_rejection(self, provider: str, code: str) -> None:
        """Rejected traffic is counted too (bounded labels: the rejection codes are fixed and
        an unknown provider is one value)."""
        known = provider if provider in {a.code for a in self.registry.all()} else "unknown"
        webhook_events.add(1, {"provider": known, "outcome": f"rejected:{code}"})

    async def _receive(self, provider: str, body: bytes, headers: dict[str, str]) -> WebhookResult:
        if len(body) > self.max_body_bytes:
            raise WebhookRejectedError(413, "payload-too-large", "webhook body too large")
        try:
            adapter = self.registry.get(ProviderCode(provider))
        except UnknownProviderError as exc:
            raise WebhookRejectedError(404, "unknown-provider", provider) from exc
        keys = self.secrets.get(adapter.code, [])
        if not keys or not adapter.capabilities.supports_webhooks:
            raise WebhookRejectedError(404, "webhooks-not-supported", provider)
        payload = self._verify(body, headers, keys)
        event_id = str(headers.get("webhook-id") or payload.get("eventId") or "")
        if not event_id:
            raise WebhookRejectedError(400, "event-id-required", "webhook-id header required")
        mapper = getattr(adapter, "event_from", None)
        try:
            observation: Reservation | None = mapper(payload) if mapper is not None else None
        except ProviderError:
            observation = None  # undocumented type, inconsistent type, unparsable: UNMATCHED
        verified: Reservation | EarlyMismatch | None = observation
        if observation is not None:
            verified = await self._verify_early(adapter, observation)
        async with self.uow() as store:
            outcome, booking_id = await self._apply(store, adapter.code, event_id, verified)
        metrics_labels = {"provider": adapter.code, "outcome": outcome.value}
        webhook_events.add(1, metrics_labels)
        return WebhookResult(outcome, booking_id, event_id)

    async def _verify_early(
        self, adapter: Any, observation: Reservation
    ) -> Reservation | EarlyMismatch:
        """An event that overtook the create response names a booking with no bound reference
        yet. It is replaced by an authoritative read of the reservation it names (through the
        lookup purpose), outside any transaction: the read, not the event, is what binds. A
        read that disagrees on either reference is a mismatch; a read that cannot be made tells
        the provider to retry (5xx)."""
        async with self.uow() as store:
            try:
                booking = await store.get(observation.client_ref)
            except NotFoundError:
                return observation
        if booking.provider_booking_ref is not None or booking.state not in _UNBOUND_STATES:
            return observation
        try:
            if self.admission is not None:
                async with self.admission.admit(
                    adapter.code, Purpose.LOOKUP, operation="get_booking"
                ) as ticket:
                    try:
                        read: Reservation = await adapter.get_booking(observation.ref)
                    except ProviderError as exc:
                        ticket.record_provider_error(exc)
                        raise
                    ticket.record(outcome="SUCCESS", side_effect="NONE", healthy=True)
            else:
                read = await adapter.get_booking(observation.ref)
        except (ProviderError, NotDispatchedError) as exc:
            raise WebhookRejectedError(
                503, "lookup-failed", f"authoritative read failed: {exc}"
            ) from exc
        if read.ref != observation.ref or read.client_ref != observation.client_ref:
            return EarlyMismatch(observation, read)
        return read

    def _verify(self, body: bytes, headers: dict[str, str], keys: list[str]) -> dict[str, Any]:
        last: Exception | None = None
        for key in keys:  # rotation: any current key verifies
            try:
                payload = Webhook(key).verify(body, headers)
                if not isinstance(payload, dict):
                    raise WebhookRejectedError(400, "invalid-payload", "an object was expected")
                return payload
            except Exception as exc:
                last = exc
        raise WebhookRejectedError(401, "invalid-signature", str(last or "no key")) from None

    async def _apply(
        self,
        store: BookingStore,
        provider: ProviderCode,
        event_id: str,
        verified: Reservation | EarlyMismatch | None,
    ) -> tuple[ObservationOutcome, str | None]:
        if verified is None:
            outcome = await self._receipt(
                store, provider, event_id, ObservationOutcome.UNMATCHED, None
            )
            return outcome, None
        mismatch = verified if isinstance(verified, EarlyMismatch) else None
        observation = mismatch.event if mismatch is not None else verified
        assert isinstance(observation, Reservation)
        try:
            booking = await store.get(observation.client_ref, for_update=True)
        except NotFoundError:
            return await self._receipt(
                store, provider, event_id, ObservationOutcome.UNMATCHED, None
            ), None
        if booking.provider != provider:
            return await self._receipt(
                store, provider, event_id, ObservationOutcome.UNMATCHED, booking.id
            ), booking.id
        if booking.state is BookingState.NEEDS_REVIEW:
            # Under review nothing moves on an event's word: it becomes evidence of the open
            # case, and the case closes only when its complete evidence set allows (6.1).
            outcome = await self._receipt(
                store, provider, event_id, ObservationOutcome.QUARANTINED, booking.id
            )
            if outcome is not ObservationOutcome.DUPLICATE:
                await self._quarantine(store, booking, observation)
            return outcome, booking.id
        if mismatch is not None:
            # The event and the authoritative read disagree about who this reservation is for.
            outcome = await self._receipt(
                store, provider, event_id, ObservationOutcome.CONTRADICTORY, booking.id
            )
            if outcome is ObservationOutcome.DUPLICATE:
                return outcome, booking.id
            command = await store.command_for(booking.id, CommandKind.CREATE)
            implicated = tuple(dict.fromkeys((mismatch.event.ref, mismatch.read.ref)))
            await self.creator.apply(
                store,
                booking,
                command,
                Escalate(REASON_IDENTITY, implicated=implicated, remediable=True),
                lease=None,
                correlation_id=None,
                source="WEBHOOK",
            )
            return outcome, booking.id
        if booking.provider_booking_ref is None and booking.state in _UNBOUND_STATES:
            # 6.6 #22: the event overtook the create response. The reservation carries our
            # reference; settlement binds it like a discovery (identity checked), and the late
            # response will close its own attempt against the settled command.
            command = await store.command_for(booking.id, CommandKind.CREATE)
            decision = decide_create_after_lookup(
                self.registry.get(booking.provider).capabilities,
                command,
                (observation,),
                lookup_budget=10**9,
            )
            early = (
                ObservationOutcome.APPLIED
                if isinstance(decision, Bind)
                else ObservationOutcome.CONTRADICTORY
            )
            outcome = await self._receipt(store, provider, event_id, early, booking.id)
            if outcome is ObservationOutcome.DUPLICATE:
                return outcome, booking.id
            if isinstance(decision, Bind | Escalate):
                await self.creator.apply(
                    store,
                    booking,
                    command,
                    decision,
                    lease=None,
                    correlation_id=None,
                    source="WEBHOOK",
                )
            return outcome, booking.id
        ordering = order_observation(
            observation,
            bound_ref=booking.provider_booking_ref,
            state=booking.state,
            generation=booking.provider_generation,
            last_revision=booking.last_revision,
            authoritative=False,
        )
        outcome = await self._receipt(store, provider, event_id, ordering.outcome, booking.id)
        if outcome is ObservationOutcome.DUPLICATE:
            return outcome, booking.id
        match ordering.outcome:
            case ObservationOutcome.APPLIED:
                assert ordering.trigger is not None and ordering.next_state is not None
                await self._transition(
                    store, booking, observation, ordering.trigger, ordering.next_state
                )
            case ObservationOutcome.NO_CHANGE if (
                observation.generation != booking.provider_generation
                or observation.revision != booking.last_revision
            ):
                # The same state, a newer fact: the watermarks move (6.4).
                await store.save_booking(
                    booking,
                    provider_generation=observation.generation,
                    last_revision=observation.revision,
                )
            case ObservationOutcome.NEWER_GENERATION | ObservationOutcome.CONTRADICTORY:
                reason = (
                    REASON_GENERATION_AHEAD
                    if ordering.outcome is ObservationOutcome.NEWER_GENERATION
                    else REASON_CONTRADICTORY
                )
                command = await store.command_for(booking.id, CommandKind.CREATE)
                await self.creator.apply(
                    store,
                    booking,
                    command,
                    Escalate(reason, implicated=(observation.ref,), remediable=True),
                    lease=None,
                    correlation_id=None,
                    source="WEBHOOK",
                )
            case _:
                pass  # STALE, SUPERSEDED_GENERATION, NO_CHANGE, UNMATCHED: recorded only
        return ordering.outcome, booking.id

    async def _quarantine(
        self, store: BookingStore, booking: Booking, observation: Reservation
    ) -> None:
        found = await store.review_case(booking.id)
        if found is None:
            return
        _, case_id = found
        await store.add_evidence(
            case_id,
            new_command_id().replace("cmd_", "ev_"),
            # A pushed event: it joins the case as information, it never closes it by itself.
            Evidence(EvidenceKind.WEBHOOK, datetime.now(UTC), observation.ref, observation),
        )
        await store.extend_review_case(case_id, implicated=(observation.ref,))

    async def _receipt(
        self,
        store: BookingStore,
        provider: ProviderCode,
        event_id: str,
        outcome: ObservationOutcome,
        booking_id: str | None,
    ) -> ObservationOutcome:
        """Insert the receipt; the unique constraint turns a redelivery into DUPLICATE."""
        try:
            await store.add_webhook_receipt(provider, event_id, outcome.value, booking_id)
        except IntegrityError:
            await store.rollback_to_savepoint()
            return ObservationOutcome.DUPLICATE
        return outcome

    async def _transition(
        self,
        store: BookingStore,
        booking: Booking,
        observation: Reservation,
        trigger: Trigger,
        next_state: BookingState,
    ) -> None:
        caps = self.registry.get(booking.provider).capabilities
        create = await store.command_for(booking.id, CommandKind.CREATE)
        if (
            create.disposition is Disposition.OPEN
            and observation.state is ReservationState.CONFIRMED
        ):
            create = settle(
                bind(create, observation.ref),
                Disposition.SUCCEEDED,
                DispositionBasis.PROVIDER_RESULT,
                caps=caps,
            )
            await store.save_command(create)
            self.creator._count(store, booking, create)
        elif (
            create.disposition is Disposition.OPEN and observation.state is ReservationState.FAILED
        ):
            create = settle(
                create, Disposition.REJECTED, DispositionBasis.PROVIDER_RESULT, caps=caps
            )
            await store.save_command(create)
            self.creator._count(store, booking, create)
        cancel = await store.open_command(booking.id, CommandKind.CANCEL)
        if cancel is not None and observation.state is ReservationState.CANCELLED:
            cancel = settle(
                cancel, Disposition.SUCCEEDED, DispositionBasis.PROVIDER_RESULT, caps=caps
            )
            await store.save_command(cancel)
            self.creator._count(store, booking, cancel)
        await store.save_booking(
            booking,
            state=next_state,
            trigger=trigger,
            source="WEBHOOK",
            provider_generation=observation.generation,
            last_revision=observation.revision,
            failure_code="booking-failed" if next_state is BookingState.FAILED else None,
            clear_next_action=next_state
            in (BookingState.CONFIRMED, BookingState.FAILED, BookingState.CANCELLED),
            clear_unresolved=True,
            payload={"event": "webhook", "observed_at": datetime.now(UTC).isoformat()},
        )


webhook_events = metrics._meter.create_counter(
    "webhook_events", unit="1", description="Inbound provider events by outcome"
)


def parse_headers(raw: dict[str, str]) -> dict[str, str]:
    return {k.lower(): v for k, v in raw.items()}


__all__ = ["WebhookRejectedError", "WebhookResult", "WebhookService", "json", "parse_headers"]

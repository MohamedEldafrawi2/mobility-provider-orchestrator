"""POST /v1/bookings and GET /v1/bookings/{id} (docs/api.md).

Status codes come from the command's disposition through one function, so the initial
response and every replay agree. The offer must exist and match the passenger composition
before any provider is called.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, EmailStr, Field

from orchestrator.api.auth import CurrentClient
from orchestrator.api.middleware import current_correlation_id
from orchestrator.api.problems import Problem
from orchestrator.application.booking_create import (
    CreateRequest,
    IdempotencyConflictError,
    Outcome,
    fingerprint_for,
)
from orchestrator.application.cancellation import (
    CancelConflictError,
    CancelRequest,
    NotCancellableError,
)
from orchestrator.application.wiring import Services
from orchestrator.domain import BookingId, ClientId, CommandKind, Money
from orchestrator.domain.cancellation import CancelIntent
from orchestrator.persistence.bookings import Booking
from orchestrator.persistence.codecs import offer_to_json, quote_to_json

router = APIRouter(prefix="/v1/bookings", tags=["bookings"])


class PassengerBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    full_name: str = Field(min_length=1, max_length=120)


class CreateBookingBody(BaseModel):
    model_config = ConfigDict(extra="forbid")  # an unknown field is a client error, not noise

    offer_id: str = Field(min_length=1, max_length=200)
    passengers: list[PassengerBody] = Field(min_length=1, max_length=9)
    contact_email: EmailStr


def _services(request: Request) -> Services:
    return request.app.state.services  # type: ignore[no-any-return]


def _representation(booking: Booking, outcome: Outcome | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": booking.id,
        "state": booking.state.value,
        "provider": booking.provider,
        "provider_booking_ref": booking.provider_booking_ref,
        "unresolved": booking.state.value not in ("CONFIRMED", "FAILED", "CANCELLED"),
        "unresolved_reason": booking.unresolved_reason,
        "failure_code": booking.failure_code,
        "offer": offer_to_json(booking.offer),
        "passengers": [{"full_name": n} for n in booking.passenger_names],
        "contact_email": booking.contact_email,
        "version": booking.version,
        "created_at": booking.created_at.isoformat(),
        "confirmation_deadline": (
            booking.confirmation_deadline.isoformat() if booking.confirmation_deadline else None
        ),
        "provider_generation": booking.provider_generation,
        "refund": booking.refund,
    }
    if outcome is not None:
        body["command"] = {
            "kind": outcome.command.kind.value,
            "disposition": outcome.command.disposition.value,
        }
    return body


@router.post("", status_code=201, summary="Create a booking")
async def create_booking(
    body: CreateBookingBody,
    request: Request,
    client_id: CurrentClient,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> Response:
    if not idempotency_key or len(idempotency_key) > 128:
        raise Problem(400, "idempotency-key-required", "Idempotency-Key required")
    services = _services(request)
    passenger_names = tuple(p.full_name for p in body.passengers)
    contact_email = str(body.contact_email)
    # Replays are decided by the durable idempotency record alone: an offer that has since
    # expired, or an offer store that is down, must not turn a retry into a 404 (6.5).
    try:
        replayed = await services.creator.find_replay(
            ClientId(client_id),
            idempotency_key,
            fingerprint_for(body.offer_id, passenger_names, contact_email),
        )
    except IdempotencyConflictError as exc:
        raise Problem(
            422, "idempotency-key-reuse", "Idempotency key reused with a different request"
        ) from exc
    if replayed is not None:
        return _respond(request, replayed)
    offer = await services.offers.get(body.offer_id)
    if offer is None:
        raise Problem(404, "offer-unavailable", "Offer unavailable", "Search again.")
    if len(body.passengers) != offer.passengers.total:
        raise Problem(
            409,
            "offer-mismatch",
            "Offer mismatch",
            f"the offer is priced for {offer.passengers.total} passengers",
            expected=offer.passengers.total,
        )
    create = CreateRequest(
        client_id=ClientId(client_id),
        idempotency_key=idempotency_key,
        offer=offer,
        passenger_names=passenger_names,
        contact_email=contact_email,
        correlation_id=current_correlation_id(),
    )
    try:
        outcome = await services.creator.create(create)
    except IdempotencyConflictError as exc:
        raise Problem(
            422, "idempotency-key-reuse", "Idempotency key reused with a different request"
        ) from exc
    return _respond(request, outcome)


def _respond(request: Request, outcome: Outcome) -> Response:
    replay = outcome.replay
    headers = {}
    if outcome.replayed:
        headers["Idempotent-Replayed"] = "true"
    location = f"/v1/bookings/{outcome.booking.id}"
    if replay.problem_code is not None:
        problem = Problem(
            replay.status,
            replay.problem_code,
            "Booking rejected" if replay.status == 422 else "Booking problem",
            outcome.booking.failure_code,
            booking=_representation(outcome.booking, outcome),
        )
        response = JSONResponse(
            status_code=replay.status,
            content={
                "type": f"{request.app.state.problem_type_base}{replay.problem_code}",
                "title": problem.title,
                "status": replay.status,
                "instance": location,
                "code": replay.problem_code,
                "correlation_id": current_correlation_id(),
                "booking": _representation(outcome.booking, outcome),
                **(
                    {"quote": quote_to_json(outcome.command.quote)}
                    if outcome.command.kind is CommandKind.CANCEL and outcome.command.quote
                    else {}
                ),
            },
            media_type="application/problem+json",
            headers=headers,
        )
        return response
    headers["Location"] = location
    if replay.unresolved:
        headers["Retry-After"] = "5"
    return JSONResponse(
        status_code=replay.status,
        content=_representation(outcome.booking, outcome),
        headers=headers,
    )


@router.get("/{booking_id}", summary="Get a booking")
async def get_booking(
    booking_id: str, request: Request, client_id: CurrentClient
) -> dict[str, Any]:
    services = _services(request)
    async with services.uow() as store:
        booking = await store.get_owned(BookingId(booking_id), ClientId(client_id))
        if booking is None:
            raise Problem(404, "not-found", "Not found")
        events = await store.events(booking.id)
    body = _representation(booking)
    body["events"] = events
    return body


class MoneyBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)


class CancelBody(BaseModel):
    """The terms the client authorises: at most this fee (omit it to accept only free
    cancellation). The quote the provider gives is accepted only inside these terms. An
    unknown field (an obsolete flag, a typo in ``max_fee``) is rejected rather than ignored:
    silently dropping an authorisation field would change what the client consented to."""

    model_config = ConfigDict(extra="forbid")

    max_fee: MoneyBody | None = None


@router.post("/{booking_id}/cancel", summary="Cancel a booking under authorised terms")
async def cancel_booking(
    booking_id: str,
    body: CancelBody,
    request: Request,
    client_id: CurrentClient,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> Response:
    if not idempotency_key or len(idempotency_key) > 128:
        raise Problem(400, "idempotency-key-required", "Idempotency-Key required")
    services = _services(request)
    async with services.uow() as store:
        owned = await store.get_owned(BookingId(booking_id), ClientId(client_id))
    if owned is None:
        raise Problem(404, "booking-not-found", "Booking not found", booking_id)
    intent = CancelIntent(
        max_fee=Money(body.max_fee.amount_minor, body.max_fee.currency) if body.max_fee else None,
    )
    cancel = CancelRequest(
        BookingId(booking_id),
        ClientId(client_id),
        idempotency_key,
        intent,
        correlation_id=current_correlation_id(),
    )
    try:
        outcome = await services.canceller.request(cancel)
    except CancelConflictError as exc:
        raise Problem(
            422, "idempotency-key-reuse", "Idempotency key reused with a different request"
        ) from exc
    except NotCancellableError as exc:
        raise Problem(
            409, "booking-not-cancellable", "Booking not cancellable", exc.detail
        ) from exc
    return _respond(request, outcome)

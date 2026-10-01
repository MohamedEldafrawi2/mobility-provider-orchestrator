"""JSON codecs between domain values and the JSON columns that store them."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from orchestrator.domain import (
    BookingId,
    CreateIntent,
    Money,
    ProviderBookingRef,
    ProviderCode,
    ProviderRequest,
    Reservation,
    ReservationState,
)
from orchestrator.domain.cancellation import CancelIntent
from orchestrator.domain.commands import ConfirmIntent
from orchestrator.domain.offers import (
    FareConditions,
    Location,
    LocationKind,
    Offer,
    PassengerComposition,
    Segment,
    TransportMode,
    Trip,
)
from orchestrator.domain.refunds import RefundOfferState, RefundOfferStatus, RefundQuote


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def money_to_json(m: Money) -> dict[str, Any]:
    return {"amount_minor": m.amount_minor, "currency": m.currency}


def money_from_json(d: dict[str, Any]) -> Money:
    return Money(int(d["amount_minor"]), str(d["currency"]))


def location_to_json(loc: Location) -> dict[str, Any]:
    return {
        "id": loc.id,
        "name": loc.name,
        "country": loc.country,
        "timezone": loc.timezone,
        "kind": loc.kind.value,
        "provider_ref": loc.provider_ref,
    }


def location_from_json(d: dict[str, Any]) -> Location:
    return Location(
        d["id"], d["name"], d["country"], d["timezone"], LocationKind(d["kind"]), d["provider_ref"]
    )


def offer_to_json(offer: Offer) -> dict[str, Any]:
    return {
        "id": offer.id,
        "provider": offer.provider,
        "trip": {
            "id": offer.trip.id,
            "provider": offer.trip.provider,
            "segments": [
                {
                    "origin": location_to_json(s.origin),
                    "destination": location_to_json(s.destination),
                    "departure": _dt(s.departure),
                    "arrival": _dt(s.arrival),
                    "mode": s.mode.value,
                    "carrier": s.carrier,
                    "vehicle_ref": s.vehicle_ref,
                }
                for s in offer.trip.segments
            ],
        },
        "passengers": {"adults": offer.passengers.adults, "children": offer.passengers.children},
        "total_price": money_to_json(offer.total_price),
        "conditions": {
            "refundable": offer.conditions.refundable,
            "max_cancellation_fee": money_to_json(offer.conditions.max_cancellation_fee)
            if offer.conditions.max_cancellation_fee
            else None,
        },
        "expires_at": _dt(offer.expires_at),
        "provider_offer_ref": offer.provider_offer_ref,
    }


def offer_from_json(d: dict[str, Any]) -> Offer:
    trip = d["trip"]
    segments = tuple(
        Segment(
            origin=location_from_json(s["origin"]),
            destination=location_from_json(s["destination"]),
            departure=datetime.fromisoformat(s["departure"]),
            arrival=datetime.fromisoformat(s["arrival"]),
            mode=TransportMode(s["mode"]),
            carrier=s["carrier"],
            vehicle_ref=s["vehicle_ref"],
        )
        for s in trip["segments"]
    )
    fee = d["conditions"].get("max_cancellation_fee")
    return Offer(
        id=d["id"],
        provider=ProviderCode(d["provider"]),
        trip=Trip(trip["id"], ProviderCode(trip["provider"]), segments),
        passengers=PassengerComposition(d["passengers"]["adults"], d["passengers"]["children"]),
        total_price=money_from_json(d["total_price"]),
        conditions=FareConditions(
            refundable=bool(d["conditions"]["refundable"]),
            max_cancellation_fee=money_from_json(fee) if fee else None,
        ),
        expires_at=datetime.fromisoformat(d["expires_at"]),
        provider_offer_ref=d["provider_offer_ref"],
    )


def intent_to_json(intent: object) -> dict[str, Any]:
    """Intents are persisted with their kind: a create, a confirm, or a cancel."""
    if isinstance(intent, CreateIntent):
        return {
            "kind": "CREATE",
            "offer_id": intent.offer_id,
            "passenger_names": list(intent.passenger_names),
            "contact_email": intent.contact_email,
            "product_ref": intent.product_ref,
            "service_date": intent.service_date.isoformat() if intent.service_date else None,
        }
    if isinstance(intent, ConfirmIntent):
        return {"kind": "CONFIRM", "reservation_ref": intent.reservation_ref}
    if isinstance(intent, CancelIntent):
        return {
            "kind": "CANCEL",
            "max_fee": money_to_json(intent.max_fee) if intent.max_fee else None,
        }
    raise TypeError(f"unknown intent {type(intent).__name__}")


def intent_from_json(d: dict[str, Any]) -> CreateIntent | ConfirmIntent | CancelIntent:
    kind = d.get("kind", "CREATE")
    if kind == "CONFIRM":
        return ConfirmIntent(ProviderBookingRef(d["reservation_ref"]))
    if kind == "CANCEL":
        return CancelIntent(max_fee=money_from_json(d["max_fee"]) if d.get("max_fee") else None)
    return CreateIntent(
        d["offer_id"],
        tuple(d["passenger_names"]),
        d["contact_email"],
        product_ref=d.get("product_ref"),
        service_date=date.fromisoformat(d["service_date"]) if d.get("service_date") else None,
    )


def quote_to_json(q: RefundQuote | None) -> dict[str, Any] | None:
    if q is None:
        return None
    return {
        "offer_id": q.offer_id,
        "refund": money_to_json(q.refund),
        "fee": money_to_json(q.fee),
        "valid_until": _dt(q.valid_until),
    }


def quote_from_json(d: dict[str, Any] | None) -> RefundQuote | None:
    if not d:
        return None
    valid_until = _parse_dt(d["valid_until"])
    assert valid_until is not None
    return RefundQuote(
        d["offer_id"], money_from_json(d["refund"]), money_from_json(d["fee"]), valid_until
    )


def request_to_json(req: ProviderRequest) -> dict[str, Any]:
    return {"payload": [list(pair) for pair in req.payload], "expiry": _dt(req.expiry)}


def request_from_json(d: dict[str, Any]) -> ProviderRequest:
    return ProviderRequest(
        payload=tuple((str(k), str(v)) for k, v in d["payload"]), expiry=_parse_dt(d.get("expiry"))
    )


def reservation_to_json(r: Reservation) -> dict[str, Any]:
    return {
        "ref": r.ref,
        "client_ref": r.client_ref,
        "state": r.state.value,
        "observed_at": _dt(r.observed_at),
        "product_ref": r.product_ref,
        "service_date": r.service_date.isoformat() if r.service_date else None,
        "generation": r.generation,
        "revision": r.revision,
        "valid_until": _dt(r.valid_until),
        "refund_offers": [
            {"offer_id": o.offer_id, "state": o.state.value, "valid_until": _dt(o.valid_until)}
            for o in r.refund_offers
        ],
    }


def reservation_from_json(d: dict[str, Any]) -> Reservation:
    return Reservation(
        ProviderBookingRef(d["ref"]),
        BookingId(d["client_ref"]),
        ReservationState(d["state"]),
        datetime.fromisoformat(d["observed_at"]),
        product_ref=d.get("product_ref"),
        service_date=date.fromisoformat(d["service_date"]) if d.get("service_date") else None,
        generation=d.get("generation"),
        revision=d.get("revision"),
        valid_until=_parse_dt(d.get("valid_until")),
        refund_offers=tuple(
            RefundOfferStatus(
                o["offer_id"], RefundOfferState(o["state"]), _parse_dt(o.get("valid_until"))
            )
            for o in d.get("refund_offers", [])
        ),
    )

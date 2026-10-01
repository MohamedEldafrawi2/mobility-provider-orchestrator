"""A guided tour of the platform against a running compose stack.

    make up-dev
    uv run python scripts/demo.py

It searches across the three fictional providers, books at each of them (direct, hold then
confirm, asynchronous with a signed webhook), then breaks things on purpose: a lost answer at
the hold-then-confirm provider is recovered by resubmission under the same key, and a webhook
from a newer generation is quarantined into a review case that an operator reconciles. It ends
with the metrics that show what happened. Every step prints the request it made and what came
back, so the tour doubles as an API walkthrough.

Development keys only (see .env.example): nothing here applies to another environment.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from standardwebhooks import Webhook

PUBLIC = "http://localhost:8000"
ADMIN = "http://127.0.0.1:8001"
RAIL = "http://127.0.0.1:9001"
CLIENT = {"X-API-Key": "dev-client-key"}
OPERATOR = {"X-Admin-Key": "dev-admin-key"}
SIM = {"X-Admin-Token": "dev-sim-token"}
WEBHOOK_SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"  # noqa: S105 - development default
BOOKING_KEYS = ("id", "state", "provider_booking_ref")


def say(title: str) -> None:
    print(f"\n== {title}")


def show(label: str, response: httpx.Response, *keys: str) -> dict[str, Any]:
    body: dict[str, Any] = response.json() if response.content else {}
    picked = {k: body.get(k) for k in keys} if keys else body
    print(f"{label}: {response.status_code} {json.dumps(picked, default=str)[:600]}")
    return body


def wait_for(client: httpx.Client, booking_id: str, *states: str, seconds: float = 15) -> Any:
    deadline = time.monotonic() + seconds
    while True:
        booking = client.get(f"{PUBLIC}/v1/bookings/{booking_id}", headers=CLIENT).json()
        if booking["state"] in states or time.monotonic() > deadline:
            return booking
        time.sleep(0.3)


def chaos(client: httpx.Client, base: str, **fields: Any) -> None:
    current = client.get(f"{base}/_chaos", headers=SIM).json()
    failpoints = {**current["failpoints"], **fields.pop("failpoints", {})}
    current.update(fields)
    current["failpoints"] = failpoints
    client.put(f"{base}/_chaos", json=current, headers=SIM).raise_for_status()


def book(client: httpx.Client, offer_id: str, key: str) -> httpx.Response:
    body = {
        "offer_id": offer_id,
        "passengers": [{"full_name": "Ada Lovelace"}],
        "contact_email": "ada@example.org",
    }
    return client.post(
        f"{PUBLIC}/v1/bookings", json=body, headers={**CLIENT, "Idempotency-Key": key}
    )


def offer(client: httpx.Client, origin: str, destination: str, ref: str) -> str:
    params = {"from": origin, "to": destination, "departureDate": "2026-06-15", "adults": 1}
    response = client.get(f"{PUBLIC}/v1/trips", params=params, headers=CLIENT)
    body = show(f"GET /v1/trips {origin} -> {destination}", response, "providers", "complete")
    for o in body["offers"]:
        if ref in (o["provider_offer_ref"], o["trip"]["segments"][0]["vehicle_ref"]):
            return str(o["id"])
    raise SystemExit(f"offer {ref} not found")


def signed_event(event_id: str, ref: str, booking_id: str) -> tuple[str, dict[str, str]]:
    payload = json.dumps(
        {
            "type": "booking.failed",
            "eventId": event_id,
            "providerBookingId": ref,
            "clientRef": booking_id,
            "status": "FAILED",
            "generation": 7,
            "sequence": 9,
            "occurredAt": datetime.now(UTC).isoformat(),
        }
    )
    ts = datetime.now(UTC)
    headers = {
        "webhook-id": event_id,
        "webhook-timestamp": str(int(ts.timestamp())),
        "webhook-signature": Webhook(WEBHOOK_SECRET).sign(event_id, ts, payload),
        "content-type": "application/json",
    }
    return payload, headers


def happy_paths(client: httpx.Client, run: str) -> dict[str, Any]:
    say("Readiness: PostgreSQL required, Redis degrades")
    show("GET /readyz", client.get(f"{PUBLIC}/readyz"))

    say("Providers: capabilities and admission state per purpose")
    providers = show("GET /v1/providers", client.get(f"{PUBLIC}/v1/providers", headers=CLIENT))
    for p in providers.get("providers", []):
        caps = p["capabilities"]
        print(
            f"  {p['code']:16} flow={caps['booking_flow']:18} "
            f"confirmation={caps['confirmation']:6} finality={caps['finality_lookup']!s:5} "
            f"webhooks={caps['supports_webhooks']!s:5} health={p['health']}"
        )

    say("Search fans out under one deadline; coverage decides who is asked")
    bus_offer = offer(client, "loc_bus_101", "loc_bus_102", "BUS-ROM-MIL-0715")
    rail_offer = offer(client, "loc_rail_8500010", "loc_rail_8503000", "IC-BS-ZH-0704")
    shuttle_offer = offer(client, "loc_mob_MOB-BER", "loc_mob_MOB-BER-AIR", "SHUTTLE-BER-AIR-0630")

    say("Provider B (bus-legacy): a direct booking, confirmed in the response")
    show("POST /v1/bookings", book(client, bus_offer, f"demo-{run}-bus"), *BOOKING_KEYS)
    replay = book(client, bus_offer, f"demo-{run}-bus")
    replayed = replay.headers.get("idempotent-replayed")
    print(f"  same key again: {replay.status_code}, Idempotent-Replayed={replayed}")

    say("Provider A (rail-osdm): a hold, confirmed inside its deadline on the request path")
    show(
        "POST /v1/bookings",
        book(client, rail_offer, f"demo-{run}-rail"),
        *BOOKING_KEYS,
        "confirmation_deadline",
    )

    say("Provider C (mobility-async): accepted now, confirmed later by a signed webhook")
    shuttle = show(
        "POST /v1/bookings", book(client, shuttle_offer, f"demo-{run}-shuttle"), *BOOKING_KEYS
    )
    final = wait_for(client, shuttle["id"], "CONFIRMED", "FAILED", "NEEDS_REVIEW")
    generation = final.get("provider_generation")
    print(f"  after the provider's webhook: {final['state']} (generation {generation})")
    return {"rail_offer": rail_offer, "shuttle": final}


def faults(client: httpx.Client, run: str, rail_offer: str, shuttle: dict[str, Any]) -> None:
    say("Fault 1: the hold-then-confirm provider loses the answer to a create")
    chaos(client, RAIL, failpoints={"lose_response": True})
    lost = show(
        "POST /v1/bookings (answer lost)",
        book(client, rail_offer, f"demo-{run}-lost"),
        "id",
        "state",
        "unresolved",
        "unresolved_reason",
    )
    chaos(client, RAIL, failpoints={"lose_response": False})
    recovered = wait_for(client, lost["id"], "CONFIRMED", "FAILED", "NEEDS_REVIEW", seconds=40)
    print(
        "  the worker resubmitted under the same key (bound before execution) and read the "
        f"fenced answer: {recovered['state']} ref={recovered.get('provider_booking_ref')}"
    )

    say("Fault 2: a webhook from a newer generation is quarantined, review decides")
    event_id = f"evt_demo_{run}"
    payload, headers = signed_event(event_id, shuttle["provider_booking_ref"], shuttle["id"])
    hook = f"{PUBLIC}/v1/providers/mobility-async/webhooks"
    show("POST .../mobility-async/webhooks", client.post(hook, content=payload, headers=headers))
    under_review = wait_for(client, shuttle["id"], "NEEDS_REVIEW")
    print(f"  booking: {under_review['state']} ({under_review['unresolved_reason']})")
    show("GET /review (admin)", client.get(f"{ADMIN}/review", headers=OPERATOR), "cases")
    reconcile = client.post(f"{ADMIN}/review/{shuttle['id']}/reconcile", headers=OPERATOR)
    show("POST /review/{id}/reconcile (admin)", reconcile, "state", "reason", "evidence")
    detail = client.get(f"{ADMIN}/review/{shuttle['id']}", headers=OPERATOR).json()
    if detail.get("state") == "NEEDS_REVIEW":
        body = {
            "expected_version": detail["version"],
            "reason": "demo: the authoritative read shows the booking confirmed",
        }
        resolve = client.post(
            f"{ADMIN}/review/{shuttle['id']}/resolve", json=body, headers=OPERATOR
        )
        show("POST /review/{id}/resolve (admin)", resolve)
    now = client.get(f"{PUBLIC}/v1/bookings/{shuttle['id']}", headers=CLIENT).json()
    print(f"  booking now: {now['state']}")

    say("What the platform measured")
    metrics = client.get(f"{ADMIN}/metrics", headers=OPERATOR).text
    interesting = ("booking_commands_total", "webhook_events_total", "provider_attempts_total")
    for line in metrics.splitlines():
        if line.startswith(interesting) and "{" in line:
            print("  " + line)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--skip-chaos", action="store_true", help="only the happy paths")
    args = parser.parse_args()
    run = uuid.uuid4().hex[:8]
    with httpx.Client(timeout=30) as client:
        context = happy_paths(client, run)
        if not args.skip_chaos:
            faults(client, run, context["rail_offer"], context["shuttle"])
    return 0


if __name__ == "__main__":
    sys.exit(main())

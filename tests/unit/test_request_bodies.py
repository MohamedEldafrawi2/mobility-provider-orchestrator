"""Request bodies are strict: an unknown field is a client error, never silently dropped.
An authorisation field a client believes it sent (an obsolete flag, a misspelt ``max_fee``)
must not vanish on the way in."""

from __future__ import annotations

from httpx import AsyncClient

from tests.conftest import DEV_CLIENT_KEY

CLIENT = {"X-API-Key": DEV_CLIENT_KEY, "Idempotency-Key": "k-strict-1"}


async def test_unknown_field_on_create_is_rejected(public_client: AsyncClient) -> None:
    response = await public_client.post(
        "/v1/bookings",
        json={
            "offer_id": "off_1",
            "passengers": [{"full_name": "Ada Lovelace"}],
            "contact_email": "ada@example.org",
            "seat_preference": "window",
        },
        headers=CLIENT,
    )
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["code"] == "validation-error"
    assert any("seat_preference" in e["location"] for e in body["errors"])


async def test_obsolete_authorisation_flag_on_cancel_is_rejected(
    public_client: AsyncClient,
) -> None:
    response = await public_client.post(
        "/v1/bookings/bk_1/cancel",
        json={"accept_quote": False},
        headers=CLIENT,
    )
    assert response.status_code == 400, response.text
    assert any("accept_quote" in e["location"] for e in response.json()["errors"])

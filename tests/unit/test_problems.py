from __future__ import annotations

from fastapi import FastAPI
from httpx import AsyncClient

from orchestrator.api.problems import PROBLEM_MEDIA_TYPE, Problem
from tests.conftest import DEV_CLIENT_KEY


async def test_unknown_route_is_a_problem(public_client: AsyncClient) -> None:
    response = await public_client.get("/v1/does-not-exist")
    assert response.status_code == 404
    assert response.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
    body = response.json()
    assert body["type"] == "urn:mpo:problem:not-found"
    assert body["code"] == "not-found"
    assert body["status"] == 404
    assert body["instance"] == "/v1/does-not-exist"
    assert body["correlation_id"] == response.headers["x-correlation-id"]


async def test_validation_error_is_400_with_locations(public_client: AsyncClient) -> None:
    app: FastAPI = public_client._transport.app  # type: ignore[attr-defined]

    @app.get("/v1/_echo")
    async def _echo(n: int) -> dict[str, int]:
        return {"n": n}

    response = await public_client.get("/v1/_echo?n=notanumber")
    assert response.status_code == 400
    body = response.json()
    assert body["code"] == "validation-error"
    assert body["errors"][0]["location"] == "query.n"


async def test_explicit_problem_carries_extensions(public_client: AsyncClient) -> None:
    app: FastAPI = public_client._transport.app  # type: ignore[attr-defined]

    @app.get("/v1/_conflict")
    async def _conflict() -> None:
        raise Problem(409, "offer-mismatch", "Offer mismatch", "Passengers differ.", expected=2)

    response = await public_client.get("/v1/_conflict")
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "offer-mismatch"
    assert body["detail"] == "Passengers differ."
    assert body["expected"] == 2


async def test_unexpected_exception_does_not_leak(public_client: AsyncClient) -> None:
    app: FastAPI = public_client._transport.app  # type: ignore[attr-defined]

    @app.get("/v1/_boom")
    async def _boom() -> None:
        raise RuntimeError("secret internals: password=hunter2")

    response = await public_client.get("/v1/_boom", headers={"X-API-Key": DEV_CLIENT_KEY})
    assert response.status_code == 500
    body = response.json()
    assert body["code"] == "internal-error"
    assert "hunter2" not in response.text


async def test_correlation_id_is_echoed_or_generated(public_client: AsyncClient) -> None:
    echoed = await public_client.get("/healthz", headers={"X-Correlation-Id": "req-42"})
    assert echoed.headers["x-correlation-id"] == "req-42"

    generated = await public_client.get("/healthz")
    assert len(generated.headers["x-correlation-id"]) == 32

    rejected = await public_client.get("/healthz", headers={"X-Correlation-Id": "bad value!"})
    assert rejected.headers["x-correlation-id"] != "bad value!"

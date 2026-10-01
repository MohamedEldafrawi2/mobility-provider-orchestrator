from __future__ import annotations

import pytest
from httpx import AsyncClient

from orchestrator.api.auth import is_admin, resolve_client
from orchestrator.config import Settings
from tests.conftest import DEV_ADMIN_KEY, DEV_CLIENT_KEY, make_settings, sha256


async def test_missing_key_is_401_problem(public_client: AsyncClient) -> None:
    response = await public_client.get("/v1/me")
    assert response.status_code == 401
    assert response.json()["code"] == "unauthorized"


async def test_wrong_key_is_401(public_client: AsyncClient) -> None:
    response = await public_client.get("/v1/me", headers={"X-API-Key": "nope"})
    assert response.status_code == 401


async def test_valid_key_resolves_client_id(public_client: AsyncClient) -> None:
    response = await public_client.get("/v1/me", headers={"X-API-Key": DEV_CLIENT_KEY})
    assert response.status_code == 200
    assert response.json() == {"client_id": "test-client"}


async def test_admin_route_requires_admin_key(admin_client: AsyncClient) -> None:
    denied = await admin_client.get("/review", headers={"X-API-Key": DEV_CLIENT_KEY})
    assert denied.status_code == 401

    allowed = await admin_client.get("/review", headers={"X-Admin-Key": DEV_ADMIN_KEY})
    assert allowed.status_code != 401, "authenticated; the database is not reachable in unit tests"


async def test_public_app_has_no_review_routes(public_client: AsyncClient) -> None:
    response = await public_client.get("/review", headers={"X-Admin-Key": DEV_ADMIN_KEY})
    assert response.status_code == 404


def test_resolve_client_and_admin_are_hash_based() -> None:
    settings = make_settings()
    assert resolve_client(settings, DEV_CLIENT_KEY) == "test-client"
    assert resolve_client(settings, sha256(DEV_CLIENT_KEY)) is None, "the hash itself is not a key"
    assert resolve_client(settings, None) is None
    assert is_admin(settings, DEV_ADMIN_KEY)
    assert not is_admin(settings, DEV_CLIENT_KEY)
    assert not is_admin(make_settings(admin_key_hash=""), DEV_ADMIN_KEY)


def test_api_keys_env_format_is_parsed() -> None:
    h = sha256("k1")
    settings = Settings(_env_file=None, api_keys=f"alpha={h}, beta={sha256('k2')}")  # type: ignore[call-arg]
    assert settings.api_keys[h] == "alpha"
    assert len(settings.api_keys) == 2

    with pytest.raises(ValueError, match="malformed"):
        Settings(_env_file=None, api_keys="alpha=tooshort")  # type: ignore[call-arg]


def test_api_keys_are_parsed_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """The compact env format must survive pydantic-settings' own decoding of dict fields."""
    monkeypatch.setenv("MPO_API_KEYS", f"env-client={sha256('k')}")
    monkeypatch.setenv("MPO_ADMIN_KEY_HASH", sha256("a"))
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.api_keys == {sha256("k"): "env-client"}
    assert resolve_client(settings, "k") == "env-client"

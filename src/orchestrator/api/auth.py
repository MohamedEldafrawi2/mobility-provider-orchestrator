"""Authentication for the public and admin listeners.

Clients present ``X-API-Key``; the key is hashed and matched against the configured hashes in
constant time and resolves to a stable ``client_id`` that owns everything the client creates.
Operators present ``X-Admin-Key`` on the admin listener only. Neither key is ever logged.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Annotated

from fastapi import Depends, Request

from orchestrator.api.problems import Problem
from orchestrator.config import ClientId, Settings

API_KEY_HEADER = "X-API-Key"
ADMIN_KEY_HEADER = "X-Admin-Key"


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def resolve_client(settings: Settings, presented_key: str | None) -> ClientId | None:
    """Return the client id for a presented key, or None. Constant-time over the configured set."""
    if not presented_key:
        return None
    presented_hash = _sha256(presented_key)
    matched: ClientId | None = None
    for key_hash, client_id in settings.api_keys.items():
        if hmac.compare_digest(presented_hash, key_hash):
            matched = client_id
    return matched


def is_admin(settings: Settings, presented_key: str | None) -> bool:
    if not presented_key or not settings.admin_key_hash:
        return False
    return hmac.compare_digest(_sha256(presented_key), settings.admin_key_hash.lower())


def _settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore[no-any-return]


def require_client(request: Request, settings: Annotated[Settings, Depends(_settings)]) -> ClientId:
    client_id = resolve_client(settings, request.headers.get(API_KEY_HEADER))
    if client_id is None:
        raise Problem(401, "unauthorized", "Unauthorized", f"A valid {API_KEY_HEADER} is required.")
    return client_id


def require_admin(request: Request, settings: Annotated[Settings, Depends(_settings)]) -> str:
    if not is_admin(settings, request.headers.get(ADMIN_KEY_HEADER)):
        raise Problem(
            401, "unauthorized", "Unauthorized", f"A valid {ADMIN_KEY_HEADER} is required."
        )
    return "operator"


CurrentClient = Annotated[ClientId, Depends(require_client)]
CurrentOperator = Annotated[str, Depends(require_admin)]

"""The provider registry: code to adapter. The only place that knows which adapters exist."""

from __future__ import annotations

from collections.abc import Iterable

from orchestrator.domain import ProviderCode
from orchestrator.providers.port import ProviderAdapter


class UnknownProviderError(KeyError):
    pass


class ProviderRegistry:
    def __init__(self, adapters: Iterable[ProviderAdapter]) -> None:
        self._adapters: dict[ProviderCode, ProviderAdapter] = {a.code: a for a in adapters}

    def get(self, code: ProviderCode) -> ProviderAdapter:
        try:
            return self._adapters[code]
        except KeyError as exc:
            raise UnknownProviderError(code) from exc

    def all(self) -> list[ProviderAdapter]:
        return list(self._adapters.values())

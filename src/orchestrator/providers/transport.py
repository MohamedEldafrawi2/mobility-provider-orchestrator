"""One HTTP client per provider *per purpose* (docs/resilience-strategy.md).

Connection pools are admission resources too: if search could fill the pool, confirmations
would queue behind it in the transport even after winning every other gate. Each purpose gets
its own client with its own pool and its own timeouts; the adapters pick the client for the
purpose of the operation they perform.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

import httpx

from orchestrator.providers.purpose import Purpose


class PurposeClients:
    def __init__(self, clients: Mapping[Purpose, httpx.AsyncClient]) -> None:
        if set(clients) != set(Purpose):
            raise ValueError("a client is needed for every purpose")
        self._clients = dict(clients)

    @classmethod
    def shared(cls, client: httpx.AsyncClient) -> PurposeClients:
        """One client for every purpose: tests and scripts, never the reference deployment."""
        return cls(dict.fromkeys(Purpose, client))

    @classmethod
    def build(
        cls,
        base_url: str,
        *,
        read_timeout: float,
        mutation_timeout: float,
        connect_timeout: float,
        pool_limits: Mapping[Purpose, int],
    ) -> PurposeClients:
        def client(purpose: Purpose) -> httpx.AsyncClient:
            timeout = (
                read_timeout if purpose in (Purpose.SEARCH, Purpose.LOOKUP) else mutation_timeout
            )
            limit = pool_limits[purpose]
            return httpx.AsyncClient(
                base_url=base_url,
                timeout=httpx.Timeout(timeout, connect=connect_timeout, pool=connect_timeout),
                limits=httpx.Limits(max_connections=limit, max_keepalive_connections=limit),
                headers={"x-mpo-purpose": purpose.value},
            )

        return cls({purpose: client(purpose) for purpose in Purpose})

    def for_purpose(self, purpose: Purpose) -> httpx.AsyncClient:
        return self._clients[purpose]

    def __iter__(self) -> Iterator[httpx.AsyncClient]:
        seen: set[int] = set()
        for client in self._clients.values():
            if id(client) not in seen:
                seen.add(id(client))
                yield client

    async def aclose(self) -> None:
        for client in self:
            await client.aclose()

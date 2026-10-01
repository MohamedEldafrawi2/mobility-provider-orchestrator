"""The offer store: offers live in Redis for exactly as long as they are advertised valid."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from datetime import UTC, datetime

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from orchestrator.domain.offers import Offer
from orchestrator.persistence.codecs import offer_from_json, offer_to_json

log = logging.getLogger(__name__)


class OfferStore:
    def __init__(self, client: aioredis.Redis) -> None:
        self._r = client

    async def put(self, offer: Offer) -> None:
        ttl = int((offer.expires_at - datetime.now(UTC)).total_seconds())
        if ttl <= 0:
            return
        await self._r.set(f"offer:{offer.id}", json.dumps(offer_to_json(offer)), ex=ttl)

    async def put_many(self, offers: Iterable[Offer]) -> bool:
        """Store every offer in one round trip. Returns False (and logs) when the store is
        unreachable: the search still answers, the caller marks the offers as not bookable."""
        now = datetime.now(UTC)
        async with self._r.pipeline(transaction=False) as pipe:
            queued = 0
            for offer in offers:
                ttl = int((offer.expires_at - now).total_seconds())
                if ttl <= 0:
                    continue
                pipe.set(f"offer:{offer.id}", json.dumps(offer_to_json(offer)), ex=ttl)
                queued += 1
            if not queued:
                return True
            try:
                await pipe.execute()
            except (RedisError, OSError, TimeoutError) as exc:
                log.warning("offers not stored", extra={"error": str(exc), "count": queued})
                return False
        return True

    async def get(self, offer_id: str) -> Offer | None:
        raw = await self._r.get(f"offer:{offer_id}")
        if raw is None:
            return None
        offer = offer_from_json(json.loads(raw))
        if offer.expires_at <= datetime.now(UTC):
            return None
        return offer

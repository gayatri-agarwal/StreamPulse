"""Redis key-value storage for the latest tumbling and sliding metrics."""

import json
import os
from typing import Any, Optional

from redis.asyncio import Redis

from app.processing.metrics import Metrics


class RedisStorageError(RuntimeError):
    """Redis connection or metrics storage error without credential details."""


class RedisMetricsStore:
    """Save latest summaries at `streampulse:metrics:{window_type}` keys."""

    VALID_WINDOW_TYPES = {"tumbling", "sliding"}

    def __init__(self, redis_url: Optional[str] = None, *, client: Any = None) -> None:
        url = (redis_url if redis_url is not None else os.getenv("REDIS_URL", "")).strip()
        if client is None and not url:
            raise ValueError("REDIS_URL must be configured")
        self._client = client if client is not None else Redis.from_url(url, decode_responses=True)

    @staticmethod
    def key_for(window_type: str) -> str:
        if window_type not in RedisMetricsStore.VALID_WINDOW_TYPES:
            raise ValueError("window_type must be 'tumbling' or 'sliding'")
        return f"streampulse:metrics:{window_type}"

    async def save_metrics(self, window_type: str, metrics: Metrics) -> None:
        key = self.key_for(window_type)
        if not isinstance(metrics, Metrics):
            raise TypeError("metrics must be a Metrics instance")
        payload = json.dumps({
            "count": metrics.count,
            "total": metrics.total,
            "average": metrics.average,
            "minimum": metrics.minimum,
            "maximum": metrics.maximum,
        }, separators=(",", ":"), allow_nan=False)
        try:
            await self._client.set(key, payload)
        except Exception:
            raise RedisStorageError("Could not save metrics to Redis") from None

    async def get_metrics(self, window_type: str) -> Optional[Metrics]:
        key = self.key_for(window_type)
        try:
            payload = await self._client.get(key)
        except Exception:
            raise RedisStorageError("Could not retrieve metrics from Redis") from None
        if payload is None:
            return None
        try:
            data = json.loads(payload)
            return Metrics(
                count=int(data["count"]),
                total=float(data["total"]),
                average=None if data["average"] is None else float(data["average"]),
                minimum=None if data["minimum"] is None else float(data["minimum"]),
                maximum=None if data["maximum"] is None else float(data["maximum"]),
            )
        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
            raise RedisStorageError("Stored Redis metrics are invalid") from None

    async def close(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            raise RedisStorageError("Could not close Redis connection") from None

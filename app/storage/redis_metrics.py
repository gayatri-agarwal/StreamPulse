"""Redis key-value storage for the latest tumbling and sliding metrics."""

import json
import os
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

from redis.asyncio import Redis
from redis.exceptions import WatchError

from app.processing.metrics import Metrics
from app.processing.window_config import WindowConfiguration


class RedisStorageError(RuntimeError):
    """Redis connection or metrics storage error without credential details."""


class RedisMonitoringError(RedisStorageError):
    """Monitoring storage failure, distinct from aggregation-summary failures."""


class RedisMonitoringDataError(RedisMonitoringError):
    """Monitoring snapshot was retrieved but is invalid or incomplete JSON data."""


class RedisConfigurationError(RedisStorageError):
    """Window configuration persistence failed."""


class RedisConfigurationDataError(RedisConfigurationError):
    """A stored window configuration is malformed or invalid."""


MONITORING_HEARTBEAT_TTL_SECONDS = 30


@dataclass(frozen=True)
class ConsumerMonitoringSnapshot:
    """Monitoring data for one consumer process; unavailable values remain null."""

    consumer_state: Optional[str] = None
    consumer_instance_id: Optional[str] = None
    last_heartbeat_at: Optional[str] = None
    processed_events_since_start: Optional[int] = None
    processing_failures_since_start: Optional[int] = None
    consumer_redis_status: Optional[str] = None
    last_successful_aggregation_write_at: Optional[str] = None
    tumbling_window_size: Optional[int] = None
    tumbling_window_progress: Optional[int] = None
    sliding_window_size: Optional[int] = None
    sliding_window_progress: Optional[int] = None
    sliding_interval: Optional[Union[int, str]] = None
    last_completed_tumbling_aggregation_at: Optional[str] = None

    TIMESTAMP_FIELDS = (
        "last_heartbeat_at",
        "last_successful_aggregation_write_at",
        "last_completed_tumbling_aggregation_at",
    )
    COUNTER_FIELDS = (
        "processed_events_since_start",
        "processing_failures_since_start",
        "tumbling_window_progress",
        "sliding_window_progress",
    )
    SIZE_FIELDS = ("tumbling_window_size", "sliding_window_size")

    def __post_init__(self) -> None:
        if self.consumer_state not in (None, "running", "stopped"):
            raise ValueError("consumer_state must be 'running', 'stopped', or null")
        for field_name in self.TIMESTAMP_FIELDS:
            value = getattr(self, field_name)
            if value is None:
                continue
            if not isinstance(value, str):
                raise TypeError(f"{field_name} must be an ISO 8601 string or null")
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(f"{field_name} must be a valid ISO 8601 timestamp") from exc
            if parsed.utcoffset() != timedelta(0):
                raise ValueError(f"{field_name} must be in UTC")
        for field_name in self.COUNTER_FIELDS:
            value = getattr(self, field_name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ValueError(f"{field_name} must be a non-negative integer or null")
        for field_name in self.SIZE_FIELDS:
            value = getattr(self, field_name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
                raise ValueError(f"{field_name} must be a positive integer or null")
        if self.sliding_interval is not None and self.sliding_interval != "per_event" and (
            isinstance(self.sliding_interval, bool)
            or not isinstance(self.sliding_interval, int)
            or self.sliding_interval <= 0
        ):
            raise ValueError("sliding_interval must be a positive integer, 'per_event', or null")

    @classmethod
    def from_mapping(cls, data: Any) -> "ConsumerMonitoringSnapshot":
        """Decode stored JSON while allowing fields added in future versions."""
        if not isinstance(data, dict):
            raise ValueError("monitoring snapshot must be a JSON object")
        accepted_fields = {field.name for field in fields(cls) if field.init}
        return cls(**{key: value for key, value in data.items() if key in accepted_fields})


class RedisMetricsStore:
    """Save latest summaries at `streampulse:metrics:{window_type}` keys."""

    VALID_WINDOW_TYPES = {"tumbling", "sliding"}
    MONITORING_HEARTBEAT_KEY = "streampulse:monitoring:consumer:heartbeat"
    MONITORING_STATE_KEY = "streampulse:monitoring:consumer:state"
    ACTIVE_CONFIGURATION_KEY = "streampulse:config:active"
    PENDING_CONFIGURATION_KEY = "streampulse:config:pending"

    def __init__(self, redis_url: Optional[str] = None, *, client: Any = None) -> None:
        url = (redis_url if redis_url is not None else os.getenv("REDIS_URL", "")).strip()
        if client is None and not url:
            raise ValueError("REDIS_URL must be configured")
        self._client = client if client is not None else Redis.from_url(url, decode_responses=True)
        self.aggregation_write_status: Optional[str] = None
        self.last_successful_aggregation_write_at: Optional[str] = None

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
            self.aggregation_write_status = "unavailable"
            raise RedisStorageError("Could not save metrics to Redis") from None
        self.aggregation_write_status = "available"
        self.last_successful_aggregation_write_at = datetime.now(timezone.utc).isoformat()

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

    async def write_monitoring_state(self, snapshot: ConsumerMonitoringSnapshot) -> None:
        """Atomically save a running snapshot and refresh its short-lived heartbeat."""
        if not isinstance(snapshot, ConsumerMonitoringSnapshot):
            raise TypeError("snapshot must be a ConsumerMonitoringSnapshot")
        if snapshot.consumer_state != "running":
            raise ValueError("write_monitoring_state requires a running consumer snapshot")
        payload = json.dumps(asdict(snapshot), separators=(",", ":"), allow_nan=False)
        try:
            async with self._client.pipeline(transaction=True) as pipeline:
                pipeline.set(self.MONITORING_STATE_KEY, payload)
                pipeline.set(
                    self.MONITORING_HEARTBEAT_KEY,
                    snapshot.last_heartbeat_at or "1",
                    ex=MONITORING_HEARTBEAT_TTL_SECONDS,
                )
                await pipeline.execute()
        except Exception:
            raise RedisMonitoringError("Could not write consumer monitoring state") from None

    async def refresh_monitoring_heartbeat(self, heartbeat_at: str) -> bool:
        """Refresh a running snapshot and its heartbeat together; return false if uninitialized/stopped."""
        self._validate_utc_timestamp("heartbeat_at", heartbeat_at)
        snapshot = await self.read_monitoring_state()
        if snapshot is None or snapshot.consumer_state != "running":
            return False
        await self.write_monitoring_state(replace(snapshot, last_heartbeat_at=heartbeat_at))
        return True

    async def read_monitoring_state(self) -> Optional[ConsumerMonitoringSnapshot]:
        """Return the latest persistent snapshot, or null if none was written."""
        try:
            payload = await self._client.get(self.MONITORING_STATE_KEY)
        except Exception:
            raise RedisMonitoringError("Could not read consumer monitoring state") from None
        if payload is None:
            return None
        try:
            return ConsumerMonitoringSnapshot.from_mapping(json.loads(payload))
        except (TypeError, ValueError, json.JSONDecodeError):
            raise RedisMonitoringDataError("Stored consumer monitoring state is invalid") from None

    async def is_monitoring_heartbeat_fresh(self) -> Optional[bool]:
        """Return None before monitoring is initialized, else whether its TTL key exists."""
        try:
            snapshot_exists = await self._client.exists(self.MONITORING_STATE_KEY)
            if not snapshot_exists:
                return None
            heartbeat_exists = await self._client.exists(self.MONITORING_HEARTBEAT_KEY)
            return bool(heartbeat_exists)
        except Exception:
            raise RedisMonitoringError("Could not check consumer monitoring heartbeat") from None

    async def record_consumer_shutdown(self, snapshot: ConsumerMonitoringSnapshot) -> None:
        """Persist a graceful stopped state and atomically remove the liveness marker."""
        if not isinstance(snapshot, ConsumerMonitoringSnapshot):
            raise TypeError("snapshot must be a ConsumerMonitoringSnapshot")
        if snapshot.consumer_state != "stopped":
            raise ValueError("record_consumer_shutdown requires a stopped consumer snapshot")
        payload = json.dumps(asdict(snapshot), separators=(",", ":"), allow_nan=False)
        try:
            async with self._client.pipeline(transaction=True) as pipeline:
                pipeline.set(self.MONITORING_STATE_KEY, payload)
                pipeline.delete(self.MONITORING_HEARTBEAT_KEY)
                await pipeline.execute()
        except Exception:
            raise RedisMonitoringError("Could not record consumer shutdown state") from None

    async def read_active_configuration(self) -> Optional[WindowConfiguration]:
        """Read the active persisted window configuration, if initialized."""
        return await self._read_configuration(self.ACTIVE_CONFIGURATION_KEY)

    async def read_pending_configuration(self) -> Optional[WindowConfiguration]:
        """Read the pending persisted window configuration, if one exists."""
        return await self._read_configuration(self.PENDING_CONFIGURATION_KEY)

    async def initialize_active_configuration(
        self, configuration: Optional[WindowConfiguration] = None
    ) -> WindowConfiguration:
        """Initialize active config once, preserving any existing persisted value."""
        if configuration is None:
            configuration = WindowConfiguration()
        self._validate_configuration_instance(configuration)
        payload = self._serialize_configuration(configuration)
        try:
            while True:
                created = await self._client.set(
                    self.ACTIVE_CONFIGURATION_KEY, payload, nx=True
                )
                if created:
                    return configuration
                existing = await self._read_configuration(self.ACTIVE_CONFIGURATION_KEY)
                if existing is not None:
                    return existing
                # The key may have been concurrently removed after SET NX failed.
        except RedisConfigurationError:
            raise
        except Exception:
            raise RedisConfigurationError("Could not initialize active window configuration") from None

    async def save_pending_configuration(self, configuration: WindowConfiguration) -> None:
        """Persist a validated pending configuration without an expiry."""
        self._validate_configuration_instance(configuration)
        payload = self._serialize_configuration(configuration)
        try:
            await self._client.set(self.PENDING_CONFIGURATION_KEY, payload)
        except Exception:
            raise RedisConfigurationError("Could not save pending window configuration") from None

    async def promote_pending_configuration(self) -> Optional[WindowConfiguration]:
        """Atomically promote pending config; return None when no pending value exists."""
        try:
            while True:
                async with self._client.pipeline(transaction=True) as pipeline:
                    await pipeline.watch(self.PENDING_CONFIGURATION_KEY)
                    payload = await pipeline.get(self.PENDING_CONFIGURATION_KEY)
                    if payload is None:
                        await pipeline.unwatch()
                        return None
                    configuration = self._deserialize_configuration(payload)
                    pipeline.multi()
                    pipeline.set(self.ACTIVE_CONFIGURATION_KEY, payload)
                    pipeline.delete(self.PENDING_CONFIGURATION_KEY)
                    try:
                        await pipeline.execute()
                        return configuration
                    except WatchError:
                        # A concurrent writer changed pending; validate and retry its value.
                        continue
        except RedisConfigurationDataError:
            raise
        except RedisConfigurationError:
            raise
        except Exception:
            raise RedisConfigurationError("Could not promote pending window configuration") from None

    async def clear_pending_configuration(self) -> bool:
        """Remove pending configuration, returning whether a key was removed."""
        try:
            return bool(await self._client.delete(self.PENDING_CONFIGURATION_KEY))
        except Exception:
            raise RedisConfigurationError("Could not clear pending window configuration") from None

    async def _read_configuration(self, key: str) -> Optional[WindowConfiguration]:
        try:
            payload = await self._client.get(key)
        except Exception:
            raise RedisConfigurationError("Could not read window configuration") from None
        if payload is None:
            return None
        return self._deserialize_configuration(payload)

    @staticmethod
    def _validate_configuration_instance(configuration: WindowConfiguration) -> None:
        if not isinstance(configuration, WindowConfiguration):
            raise TypeError("configuration must be a WindowConfiguration instance")

    @staticmethod
    def _serialize_configuration(configuration: WindowConfiguration) -> str:
        return json.dumps(asdict(configuration), separators=(",", ":"), allow_nan=False)

    @staticmethod
    def _deserialize_configuration(payload: Any) -> WindowConfiguration:
        try:
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            data = json.loads(payload)
            expected_fields = {"tumbling_size", "sliding_size", "sliding_interval"}
            if not isinstance(data, dict) or set(data) != expected_fields:
                raise ValueError("configuration must contain all window fields")
            return WindowConfiguration(**data)
        except (UnicodeDecodeError, TypeError, ValueError, json.JSONDecodeError):
            raise RedisConfigurationDataError("Stored window configuration is invalid") from None

    @staticmethod
    def _validate_utc_timestamp(field_name: str, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError(f"{field_name} must be an ISO 8601 string")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid ISO 8601 timestamp") from exc
        if parsed.utcoffset() != timedelta(0):
            raise ValueError(f"{field_name} must be in UTC")

    async def close(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            raise RedisStorageError("Could not close Redis connection") from None

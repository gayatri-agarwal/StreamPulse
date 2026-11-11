"""Redis key-value storage for the latest tumbling and sliding metrics."""

import asyncio
import hashlib
import json
import os
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import WatchError

from app.processing.metrics import Metrics
from app.processing.window_config import WindowConfiguration
from app.processing.measurements import (
    MeasurementSummary, average_latency, failure_rate, percentile_95,
)


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
MEASUREMENT_RETENTION_SECONDS = 7 * 24 * 60 * 60
MEASUREMENT_SAMPLE_LIMIT = 10_000
MEASUREMENT_STORAGE_OPERATION_TIMEOUT_SECONDS = 2
MEASUREMENT_COUNTERS = (
    "publish_attempts", "publish_successes", "publish_failures",
    "consumer_attempts", "consumer_successes", "consumer_unique_successes", "consumer_failures",
)
MEASUREMENT_LATENCIES = (
    "publish_latency_ms", "publish_to_delivery_latency_ms", "decode_validation_latency_ms",
    "consumer_processing_latency_ms", "redis_persistence_latency_ms", "end_to_end_latency_ms",
)


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

    @staticmethod
    def _measurement_samples(samples: dict) -> dict:
        valid = {}
        for name, value in samples.items():
            if name not in MEASUREMENT_LATENCIES or value is None:
                continue
            numeric = float(value)
            if numeric < 0 or numeric != numeric or numeric in (float("inf"), float("-inf")):
                continue
            valid[name] = numeric
        return valid

    @staticmethod
    def measurement_prefix(run_id: Optional[str] = None) -> str:
        """Separate ordinary traffic from a canonical UUID experiment run."""
        if run_id is None:
            return "streampulse:measurements:overall"
        try:
            run_id = str(UUID(run_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("run_id must be a valid UUID") from exc
        return f"streampulse:measurements:run:{run_id}"

    async def record_measurements(
        self,
        *,
        run_id: Optional[str],
        counters: dict,
        samples: dict,
        success_timestamp: Optional[float] = None,
    ) -> None:
        """Atomically update bounded, expiring stage counters and latency samples."""
        prefixes = [self.measurement_prefix()]
        if run_id is not None:
            prefixes.append(self.measurement_prefix(run_id))
        normalized_samples = self._measurement_samples(samples)
        try:
            async with self._client.pipeline(transaction=True) as pipeline:
                for prefix in prefixes:
                    counter_key = f"{prefix}:counts"
                    keys = [counter_key]
                    for name, increment in counters.items():
                        if name in MEASUREMENT_COUNTERS and increment:
                            pipeline.hincrby(counter_key, name, int(increment))
                    for name, value in normalized_samples.items():
                        key = f"{prefix}:samples:{name}"
                        pipeline.rpush(key, repr(value))
                        pipeline.ltrim(key, -MEASUREMENT_SAMPLE_LIMIT, -1)
                        keys.append(key)
                    if success_timestamp is not None:
                        key = f"{prefix}:success_timestamps"
                        pipeline.rpush(key, repr(float(success_timestamp)))
                        pipeline.ltrim(key, -MEASUREMENT_SAMPLE_LIMIT, -1)
                        keys.append(key)
                    for key in keys:
                        pipeline.expire(key, MEASUREMENT_RETENTION_SECONDS)
                await pipeline.execute()
        except Exception:
            raise RedisStorageError("Could not record measurements in Redis") from None

    async def record_consumer_delivery(
        self,
        *,
        run_id: Optional[str],
        event_id: str,
        succeeded: bool,
        samples: dict,
        success_timestamp: Optional[float] = None,
    ) -> bool:
        """Record one delivery attempt and idempotently record its first success.

        WATCH/MULTI/EXEC makes the deduplication decision and its unique-success
        counters/samples one atomic Redis transaction. Attempt counters still
        advance on every delivery, including a redelivery.
        """
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("event_id must be a non-empty string")
        prefixes = [self.measurement_prefix()]
        if run_id is not None:
            prefixes.append(self.measurement_prefix(run_id))
        normalized_samples = self._measurement_samples(samples)
        event_digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        dedupe_keys = [f"{prefix}:dedupe:{event_digest}" for prefix in prefixes]

        for _ in range(10):
            try:
                async with self._client.pipeline(transaction=True) as pipeline:
                    await pipeline.watch(*dedupe_keys)
                    existing = await pipeline.mget(*dedupe_keys)
                    new_scopes = [index for index, value in enumerate(existing) if value is None]
                    pipeline.multi()
                    for index, prefix in enumerate(prefixes):
                        counter_key = f"{prefix}:counts"
                        pipeline.hincrby(counter_key, "consumer_attempts", 1)
                        outcome = "consumer_successes" if succeeded else "consumer_failures"
                        pipeline.hincrby(counter_key, outcome, 1)
                        keys = [counter_key]
                        if succeeded and index in new_scopes:
                            pipeline.set(
                                dedupe_keys[index], "1",
                                ex=MEASUREMENT_RETENTION_SECONDS,
                                nx=True,
                            )
                            pipeline.hincrby(counter_key, "consumer_unique_successes", 1)
                            if success_timestamp is not None:
                                timestamp_key = f"{prefix}:success_timestamps"
                                pipeline.rpush(timestamp_key, repr(float(success_timestamp)))
                                pipeline.ltrim(timestamp_key, -MEASUREMENT_SAMPLE_LIMIT, -1)
                                keys.append(timestamp_key)
                            for name, value in normalized_samples.items():
                                sample_key = f"{prefix}:samples:{name}"
                                pipeline.rpush(sample_key, repr(value))
                                pipeline.ltrim(sample_key, -MEASUREMENT_SAMPLE_LIMIT, -1)
                                keys.append(sample_key)
                            keys.append(dedupe_keys[index])
                        elif not succeeded:
                            # Failed latency samples describe delivery attempts,
                            # so each failed attempt contributes its own samples.
                            for name, value in normalized_samples.items():
                                sample_key = f"{prefix}:samples:{name}"
                                pipeline.rpush(sample_key, repr(value))
                                pipeline.ltrim(sample_key, -MEASUREMENT_SAMPLE_LIMIT, -1)
                                keys.append(sample_key)
                        for key in keys:
                            pipeline.expire(key, MEASUREMENT_RETENTION_SECONDS)
                    await pipeline.execute()
                    return bool(new_scopes) if succeeded else False
            except WatchError:
                continue
            except Exception:
                raise RedisStorageError("Could not record consumer delivery measurements") from None
        raise RedisStorageError("Could not record consumer delivery measurements after concurrent updates")

    async def get_measurement_summary(self, run_id: Optional[str] = None) -> MeasurementSummary:
        """Summarize recent bounded samples; rates use stage-specific attempts."""
        prefix = self.measurement_prefix(run_id)
        try:
            counts, latency_samples, timestamps = await asyncio.gather(
                self._client.hgetall(f"{prefix}:counts"),
                asyncio.gather(*(
                    self._client.lrange(f"{prefix}:samples:{name}", 0, -1)
                    for name in MEASUREMENT_LATENCIES
                )),
                self._client.lrange(f"{prefix}:success_timestamps", 0, -1),
            )
        except Exception:
            raise RedisStorageError("Could not retrieve measurements from Redis") from None
        counts = {key.decode() if isinstance(key, bytes) else key: int(value) for key, value in counts.items()}
        decoded = {}
        for name, values in zip(MEASUREMENT_LATENCIES, latency_samples):
            decoded[name] = [float(value) for value in values]
        times = sorted(float(value) for value in timestamps)
        throughput = None
        if len(times) >= 2 and times[-1] > times[0]:
            throughput = (len(times) - 1) / (times[-1] - times[0])
        return MeasurementSummary(
            publish_attempts=counts.get("publish_attempts", 0),
            publish_successes=counts.get("publish_successes", 0),
            publish_failures=counts.get("publish_failures", 0),
            publish_failure_rate=failure_rate(counts.get("publish_failures", 0), counts.get("publish_attempts", 0)),
            consumer_attempts=counts.get("consumer_attempts", 0),
            consumer_successes=counts.get("consumer_successes", 0),
            consumer_failures=counts.get("consumer_failures", 0),
            consumer_failure_rate=failure_rate(counts.get("consumer_failures", 0), counts.get("consumer_attempts", 0)),
            throughput_events_per_second=throughput,
            sample_count={key: len(value) for key, value in decoded.items()},
            average_latency_ms={key: average_latency(value) for key, value in decoded.items()},
            p95_latency_ms={key: percentile_95(value) for key, value in decoded.items()},
            consumer_unique_successes=counts.get("consumer_unique_successes", 0),
            inter_arrival_rate_events_per_second=throughput,
            measurements_present=bool(counts) or any(decoded.values()) or bool(timestamps),
        )

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

    @staticmethod
    def run_key_for(window_type: str, run_id: str) -> str:
        """Build a summary key isolated to one canonical experiment run UUID."""
        if window_type not in RedisMetricsStore.VALID_WINDOW_TYPES:
            raise ValueError("window_type must be 'tumbling' or 'sliding'")
        try:
            canonical_run_id = str(UUID(run_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("run_id must be a valid UUID") from exc
        return f"streampulse:experiments:runs:{canonical_run_id}:metrics:{window_type}"

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

    async def save_run_metrics(self, run_id: str, window_type: str, metrics: Metrics) -> None:
        """Save the latest summary under a run-specific key without a TTL."""
        key = self.run_key_for(window_type, run_id)
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
            raise RedisStorageError("Could not save run metrics to Redis") from None
        self.aggregation_write_status = "available"
        self.last_successful_aggregation_write_at = datetime.now(timezone.utc).isoformat()

    async def get_run_metrics(self, run_id: str, window_type: str) -> Optional[Metrics]:
        """Read a run-specific summary, returning None when it has not been written."""
        key = self.run_key_for(window_type, run_id)
        try:
            payload = await self._client.get(key)
        except Exception:
            raise RedisStorageError("Could not retrieve run metrics from Redis") from None
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
            raise RedisStorageError("Stored run Redis metrics are invalid") from None

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

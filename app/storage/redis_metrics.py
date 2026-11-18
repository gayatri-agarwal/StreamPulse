"""Redis key-value storage for the latest tumbling and sliding metrics."""

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from redis.asyncio import Redis
from redis.exceptions import WatchError

from app.processing.metrics import Metrics
from app.processing.window_config import WindowConfiguration
from app.processing.measurements import (
    MeasurementSummary, average_latency, failure_rate, percentile_95,
)
from app.safe_diagnostics import safe_exception_details


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
CONSUMER_MONITORING_STOPPED_TTL_SECONDS = 300
CONSUMER_AGGREGATION_WRITE_EVIDENCE_TTL_SECONDS = 300
MEASUREMENT_RETENTION_SECONDS = 7 * 24 * 60 * 60
MEASUREMENT_SAMPLE_LIMIT = 10_000
MEASUREMENT_STORAGE_OPERATION_TIMEOUT_SECONDS = 2
MEASUREMENT_COUNTERS = (
    "publish_attempts", "publish_successes", "publish_failures",
    "consumer_attempts", "consumer_successes", "consumer_unique_successes", "consumer_failures",
)
AGGREGATION_WATCH_RETRIES = 3

logger = logging.getLogger(__name__)
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
    CONSUMER_AGGREGATION_WRITE_EVIDENCE_KEY = "streampulse:monitoring:consumer:aggregation-write-evidence"
    MONITORING_STATE_KEY = "streampulse:monitoring:consumer:state"
    MONITORING_INSTANCE_PREFIX = "streampulse:monitoring:consumer:instances"
    ACTIVE_CONSUMER_KEY = "streampulse:monitoring:consumer:active-instance"
    ACTIVE_CONFIGURATION_KEY = "streampulse:config:active"
    PENDING_CONFIGURATION_KEY = "streampulse:config:pending"
    SIMULATION_CONFIGURATION_PREFIX = "streampulse:simulations:"

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
    def measurement_prefix(run_id: Optional[str] = None, simulation_id: Optional[str] = None) -> str:
        """Separate ordinary, run-scoped, and simulation-scoped measurements."""
        if run_id is not None and simulation_id is not None:
            raise ValueError("run_id and simulation_id are mutually exclusive")
        if simulation_id is not None:
            try:
                simulation_id = str(UUID(simulation_id))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError("simulation_id must be a valid UUID") from exc
            return f"streampulse:measurements:simulation:{simulation_id}"
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
        simulation_id: Optional[str] = None,
        counters: dict,
        samples: dict,
        success_timestamp: Optional[float] = None,
    ) -> None:
        """Atomically update bounded, expiring stage counters and latency samples."""
        prefixes = [self.measurement_prefix()]
        if run_id is not None or simulation_id is not None:
            prefixes.append(self.measurement_prefix(run_id, simulation_id))
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
        simulation_id: Optional[str] = None,
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
        if run_id is not None or simulation_id is not None:
            prefixes.append(self.measurement_prefix(run_id, simulation_id))
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

    async def get_measurement_summary(self, run_id: Optional[str] = None,
                                      simulation_id: Optional[str] = None) -> MeasurementSummary:
        """Summarize recent bounded samples; rates use stage-specific attempts."""
        prefix = self.measurement_prefix(run_id, simulation_id)
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
            minimum_latency_ms={key: min(value) if value else None for key, value in decoded.items()},
            maximum_latency_ms={key: max(value) if value else None for key, value in decoded.items()},
            consumer_unique_successes=counts.get("consumer_unique_successes", 0),
            inter_arrival_rate_events_per_second=throughput,
            measurements_present=bool(counts) or any(decoded.values()) or bool(timestamps),
        )

    def __init__(self, redis_url: Optional[str] = None, *, client: Any = None) -> None:
        url = (redis_url if redis_url is not None else os.getenv("REDIS_URL", "")).strip()
        if client is None and not url:
            raise ValueError("REDIS_URL must be configured")
        self._client = client if client is not None else Redis.from_url(url, decode_responses=True)
        self.consumer_instance_id = str(uuid4())
        self.connection_fingerprint = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12] if url else None
        if url:
            parsed = urlsplit(url)
            host = parsed.hostname or "unknown"
            port = parsed.port or (6380 if parsed.scheme == "rediss" else 6379)
            database = parsed.path.lstrip("/") or "0"
            self.connection_identity = f"{host}:{port}/db{database}"
        else:
            self.connection_identity = None
        self.aggregation_write_status: Optional[str] = None
        self.last_successful_aggregation_write_at: Optional[str] = None

    @classmethod
    def instance_state_key(cls, instance_id: str) -> str:
        return f"{cls.MONITORING_INSTANCE_PREFIX}:{instance_id}:state"

    @classmethod
    def instance_heartbeat_key(cls, instance_id: str) -> str:
        return f"{cls.MONITORING_INSTANCE_PREFIX}:{instance_id}:heartbeat"

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

    @staticmethod
    def simulation_key_for(window_type: str, simulation_id: str) -> str:
        if window_type not in RedisMetricsStore.VALID_WINDOW_TYPES:
            raise ValueError("window_type must be 'tumbling' or 'sliding'")
        try:
            simulation_id = str(UUID(simulation_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("simulation_id must be a valid UUID") from exc
        return f"streampulse:simulations:{simulation_id}:metrics:{window_type}"

    @staticmethod
    def processing_scope_prefix(
        run_id: Optional[str] = None, simulation_id: Optional[str] = None,
    ) -> str:
        if run_id is not None and simulation_id is not None:
            raise ValueError("run_id and simulation_id are mutually exclusive")
        if simulation_id is not None:
            simulation_id = str(UUID(simulation_id))
            return f"streampulse:simulations:{simulation_id}:processing"
        if run_id is not None:
            run_id = str(UUID(run_id))
            return f"streampulse:experiments:runs:{run_id}:processing"
        return "streampulse:processing:global"

    @classmethod
    def processing_state_key(
        cls, run_id: Optional[str] = None, simulation_id: Optional[str] = None,
    ) -> str:
        return f"{cls.processing_scope_prefix(run_id, simulation_id)}:window_state"

    @classmethod
    def processing_receipt_key(
        cls, event_id: str, run_id: Optional[str] = None,
        simulation_id: Optional[str] = None,
    ) -> str:
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("event_id must be a non-empty string")
        digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        return f"{cls.processing_scope_prefix(run_id, simulation_id)}:receipt:{digest}"

    async def load_processing_checkpoint(
        self, *, run_id: Optional[str] = None,
        simulation_id: Optional[str] = None,
    ) -> dict:
        """Load durable count-window state shared by primary and standby consumers."""
        key = self.processing_state_key(run_id, simulation_id)
        try:
            payload = await self._client.get(key)
            if payload is None:
                return {
                    "version": 0, "tumbling_size": None, "sliding_size": None,
                    "sliding_interval": None, "tumbling": [],
                    "sliding": {"events": [], "events_since_update": 0},
                }
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            state = json.loads(payload)
            if (not isinstance(state, dict) or set(state) != {
                    "version", "tumbling_size", "sliding_size", "sliding_interval",
                    "tumbling", "sliding"}
                    or type(state["version"]) is not int or state["version"] < 1):
                raise ValueError("invalid processing checkpoint")
            return state
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            raise RedisStorageError("Stored processing checkpoint is invalid") from None
        except Exception:
            raise RedisStorageError("Could not load processing checkpoint from Redis") from None

    async def commit_processing_checkpoint(
        self, *, event_id: str, state: dict, expected_version: int,
        summaries: dict[str, Metrics], run_id: Optional[str] = None,
        simulation_id: Optional[str] = None,
    ) -> bool:
        """Atomically commit deduplication, window state, summaries, and evidence.

        Returns False when this event already has a durable receipt in its scope.
        """
        if not isinstance(summaries, dict):
            raise ValueError("summaries must be a mapping")
        if type(expected_version) is not int or expected_version < 0:
            raise ValueError("expected_version must be a non-negative integer")
        state_payload = json.dumps(state, separators=(",", ":"), allow_nan=False)
        written_at = datetime.now(timezone.utc).isoformat()
        state_key = self.processing_state_key(run_id, simulation_id)
        receipt_key = self.processing_receipt_key(event_id, run_id, simulation_id)
        scope_ttl = MEASUREMENT_RETENTION_SECONDS if simulation_id is not None else None
        summary_entries = []
        for window_type, metrics in summaries.items():
            if not isinstance(metrics, Metrics):
                raise TypeError("metrics must be a Metrics instance")
            key = (self.simulation_key_for(window_type, simulation_id)
                   if simulation_id is not None else
                   self.run_key_for(window_type, run_id) if run_id is not None else
                   self.key_for(window_type))
            payload = json.dumps({
                "count": metrics.count, "total": metrics.total,
                "average": metrics.average, "minimum": metrics.minimum,
                "maximum": metrics.maximum,
            }, separators=(",", ":"), allow_nan=False)
            summary_entries.append((key, payload, scope_ttl if simulation_id else None))
        evidence = json.dumps({
            "consumer_instance_id": self.consumer_instance_id,
            "written_at": written_at,
        }, separators=(",", ":"), allow_nan=False)
        try:
            for attempt in range(AGGREGATION_WATCH_RETRIES):
                try:
                    async with self._client.pipeline(transaction=True) as pipeline:
                        await pipeline.watch(
                            self.ACTIVE_CONSUMER_KEY, state_key, receipt_key,
                        )
                        owner = await pipeline.get(self.ACTIVE_CONSUMER_KEY)
                        if isinstance(owner, bytes):
                            owner = owner.decode("utf-8")
                        if owner != self.consumer_instance_id:
                            raise RedisStorageError(
                                "Consumer instance has no current delivery ownership"
                            )
                        if await pipeline.get(receipt_key) is not None:
                            return False
                        current = await pipeline.get(state_key)
                        if isinstance(current, bytes):
                            current = current.decode("utf-8")
                        current_version = 0 if current is None else json.loads(current)["version"]
                        if current_version != expected_version:
                            raise RedisStorageError(
                                "Processing checkpoint changed; refusing stale window state"
                            )
                        pipeline.multi()
                        # Checkpoints and receipts must outlive broker redelivery;
                        # summaries retain their existing scope-specific TTL.
                        pipeline.set(state_key, state_payload)
                        pipeline.set(receipt_key, "1")
                        for key, payload, ttl in summary_entries:
                            pipeline.set(key, payload, ex=ttl)
                        pipeline.set(
                            self.CONSUMER_AGGREGATION_WRITE_EVIDENCE_KEY,
                            evidence, ex=CONSUMER_AGGREGATION_WRITE_EVIDENCE_TTL_SECONDS,
                        )
                        await pipeline.execute()
                    self.aggregation_write_status = "available"
                    self.last_successful_aggregation_write_at = written_at
                    return True
                except WatchError:
                    if attempt + 1 >= AGGREGATION_WATCH_RETRIES:
                        raise
            raise RedisStorageError("Could not commit processing checkpoint after concurrent updates")
        except Exception as exc:
            self.aggregation_write_status = "unavailable"
            details, formatted_traceback = safe_exception_details(exc)
            logger.error(
                "redis_aggregation_write_failed stage=redis.processing_checkpoint "
                "exception_class=%s exception_details=%s traceback=%s",
                type(exc).__name__, details, formatted_traceback,
            )
            if isinstance(exc, RedisStorageError):
                raise
            raise RedisStorageError("Could not commit processing checkpoint to Redis") from None

    @classmethod
    def simulation_activity_key(cls, simulation_id: str) -> str:
        try:
            simulation_id = str(UUID(simulation_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("simulation_id must be a valid UUID") from exc
        return f"{cls.SIMULATION_CONFIGURATION_PREFIX}{simulation_id}:activity"

    async def record_simulation_activity(self, simulation_id: str, event_id: str,
                                         event_time: str, published_at: Optional[str],
                                         processed_at: Optional[str], status: Optional[str] = None,
                                         processing_error: Optional[str] = None) -> None:
        self._validate_utc_timestamp("event_time", event_time)
        if processed_at is not None:
            self._validate_utc_timestamp("processed_at", processed_at)
        if published_at is not None:
            self._validate_utc_timestamp("published_at", published_at)
        if status is None:
            status = "processed" if processed_at is not None else "processing_failed"
        if status not in {"processed", "processing_failed"}:
            raise ValueError("simulation activity status is invalid")
        if processing_error is not None and (
            not isinstance(processing_error, str)
            or not processing_error.isidentifier()
            or len(processing_error) > 80
        ):
            raise ValueError("processing_error must be a safe exception class name")
        item_data = {
            "event_id": event_id,
            "event_time": event_time,
            "published_at": published_at,
            "processed_at": processed_at,
            "status": status,
        }
        if processing_error is not None:
            item_data["processing_error"] = processing_error
        item = json.dumps(item_data, separators=(",", ":"), allow_nan=False)
        key = self.simulation_activity_key(simulation_id)
        try:
            async with self._client.pipeline(transaction=True) as pipeline:
                pipeline.lpush(key, item)
                pipeline.ltrim(key, 0, 99)
                pipeline.expire(key, MEASUREMENT_RETENTION_SECONDS)
                await pipeline.execute()
        except Exception:
            raise RedisStorageError("Could not record simulation activity") from None

    async def get_simulation_activity(self, simulation_id: str) -> list[dict]:
        try:
            values = await self._client.lrange(self.simulation_activity_key(simulation_id), 0, 99)
            result = []
            for value in values:
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                item = json.loads(value)
                if not isinstance(item, dict):
                    raise ValueError("activity item must be an object")
                result.append(item)
            return result
        except Exception:
            raise RedisStorageError("Could not retrieve simulation activity") from None

    async def save_metrics(self, window_type: str, metrics: Metrics) -> None:
        await self.save_metrics_batch({window_type: metrics})

    async def save_metrics_batch(
        self,
        summaries: dict[str, Metrics],
        *,
        run_id: Optional[str] = None,
        simulation_id: Optional[str] = None,
    ) -> None:
        """Atomically persist all summaries produced by one event."""
        if run_id is not None and simulation_id is not None:
            raise ValueError("run_id and simulation_id are mutually exclusive")
        if not isinstance(summaries, dict) or not summaries:
            raise ValueError("summaries must be a non-empty mapping")

        entries = []
        for window_type, metrics in summaries.items():
            if not isinstance(metrics, Metrics):
                raise TypeError("metrics must be a Metrics instance")
            if simulation_id is not None:
                key = self.simulation_key_for(window_type, simulation_id)
                ttl = MEASUREMENT_RETENTION_SECONDS
            elif run_id is not None:
                key = self.run_key_for(window_type, run_id)
                ttl = None
            else:
                key = self.key_for(window_type)
                ttl = None
            payload = json.dumps({
                "count": metrics.count,
                "total": metrics.total,
                "average": metrics.average,
                "minimum": metrics.minimum,
                "maximum": metrics.maximum,
            }, separators=(",", ":"), allow_nan=False)
            entries.append((key, payload, ttl))

        written_at = datetime.now(timezone.utc).isoformat()
        try:
            await self._save_aggregation_batch_with_evidence(entries, written_at)
        except Exception as exc:
            self.aggregation_write_status = "unavailable"
            details, formatted_traceback = safe_exception_details(exc)
            logger.error(
                "redis_aggregation_write_failed stage=redis.summary_transaction "
                "exception_class=%s exception_details=%s traceback=%s",
                type(exc).__name__, details, formatted_traceback,
            )
            raise RedisStorageError("Could not save metrics to Redis") from None
        self.aggregation_write_status = "available"
        self.last_successful_aggregation_write_at = written_at

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
        await self.save_metrics_batch({window_type: metrics}, run_id=run_id)

    async def _save_aggregation_batch_with_evidence(
        self, entries: list[tuple[str, str, Optional[int]]], written_at: str,
    ) -> None:
        """Atomically write event summaries only while this delivery owner is current."""
        evidence = json.dumps({
            "consumer_instance_id": self.consumer_instance_id,
            "written_at": written_at,
        }, separators=(",", ":"), allow_nan=False)
        for attempt in range(AGGREGATION_WATCH_RETRIES):
            try:
                async with self._client.pipeline(transaction=True) as pipeline:
                    await pipeline.watch(self.ACTIVE_CONSUMER_KEY)
                    owner = await pipeline.get(self.ACTIVE_CONSUMER_KEY)
                    if isinstance(owner, bytes):
                        owner = owner.decode("utf-8")
                    if owner != self.consumer_instance_id:
                        raise RedisStorageError(
                            "Consumer instance has no current delivery ownership"
                        )
                    pipeline.multi()
                    for key, payload, ttl in entries:
                        if ttl is None:
                            pipeline.set(key, payload)
                        else:
                            pipeline.set(key, payload, ex=ttl)
                    pipeline.set(
                        self.CONSUMER_AGGREGATION_WRITE_EVIDENCE_KEY,
                        evidence,
                        ex=CONSUMER_AGGREGATION_WRITE_EVIDENCE_TTL_SECONDS,
                    )
                    await pipeline.execute()
                return
            except WatchError:
                if attempt + 1 >= AGGREGATION_WATCH_RETRIES:
                    raise

    async def read_consumer_aggregation_write_evidence(self) -> Optional[dict]:
        """Read expiring proof tied to the consumer instance that wrote a summary."""
        try:
            value = await self._client.get(self.CONSUMER_AGGREGATION_WRITE_EVIDENCE_KEY)
        except Exception:
            raise RedisMonitoringError("Could not read aggregation write evidence") from None
        if value is None:
            return None
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except UnicodeDecodeError:
                raise RedisMonitoringDataError("Aggregation write evidence is invalid") from None
        try:
            evidence = json.loads(value)
            if not isinstance(evidence, dict):
                raise ValueError("evidence must be an object")
            instance_id = evidence.get("consumer_instance_id")
            if not isinstance(instance_id, str):
                raise ValueError("consumer instance is missing")
            UUID(instance_id)
            written_at = evidence.get("written_at")
            self._validate_utc_timestamp("aggregation_write_evidence", written_at)
        except (TypeError, ValueError, json.JSONDecodeError):
            raise RedisMonitoringDataError("Aggregation write evidence is invalid") from None
        return {"consumer_instance_id": instance_id, "written_at": written_at}

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

    async def save_simulation_metrics(self, simulation_id: str, window_type: str, metrics: Metrics) -> None:
        await self.save_metrics_batch({window_type: metrics}, simulation_id=simulation_id)

    async def get_simulation_metrics(self, simulation_id: str, window_type: str) -> Optional[Metrics]:
        key = self.simulation_key_for(window_type, simulation_id)
        try:
            payload = await self._client.get(key)
        except Exception:
            raise RedisStorageError("Could not retrieve simulation metrics from Redis") from None
        if payload is None:
            return None
        try:
            data = json.loads(payload)
            return Metrics(int(data["count"]), float(data["total"]),
                           None if data["average"] is None else float(data["average"]),
                           None if data["minimum"] is None else float(data["minimum"]),
                           None if data["maximum"] is None else float(data["maximum"]))
        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
            raise RedisStorageError("Stored simulation Redis metrics are invalid") from None

    @classmethod
    def simulation_configuration_key(cls, simulation_id: str) -> str:
        try:
            simulation_id = str(UUID(simulation_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("simulation_id must be a valid UUID") from exc
        return f"{cls.SIMULATION_CONFIGURATION_PREFIX}{simulation_id}:configuration"

    async def save_simulation_configuration(self, simulation_id: str,
                                            configuration: WindowConfiguration) -> None:
        self._validate_configuration_instance(configuration)
        payload = self._serialize_configuration(configuration)
        try:
            await self._client.set(self.simulation_configuration_key(simulation_id), payload)
        except Exception:
            raise RedisConfigurationError("Could not save simulation window configuration") from None

    async def read_simulation_configuration(self, simulation_id: str) -> Optional[WindowConfiguration]:
        try:
            payload = await self._client.get(self.simulation_configuration_key(simulation_id))
        except Exception:
            raise RedisConfigurationError("Could not read simulation window configuration") from None
        if payload is None:
            return None
        try:
            return self._deserialize_configuration(payload)
        except RedisConfigurationDataError:
            raise
        except Exception:
            raise RedisConfigurationDataError("Stored simulation window configuration is invalid") from None

    async def write_monitoring_state(self, snapshot: ConsumerMonitoringSnapshot) -> None:
        """Persist one instance's heartbeat without letting a standby take ownership."""
        if not isinstance(snapshot, ConsumerMonitoringSnapshot):
            raise TypeError("snapshot must be a ConsumerMonitoringSnapshot")
        if snapshot.consumer_state != "running":
            raise ValueError("write_monitoring_state requires a running consumer snapshot")
        try:
            instance_id = str(UUID(snapshot.consumer_instance_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("running snapshots require a UUID consumer_instance_id") from exc
        payload = json.dumps(asdict(snapshot), separators=(",", ":"), allow_nan=False)
        try:
            async with self._client.pipeline(transaction=True) as pipeline:
                pipeline.set(
                    self.instance_state_key(instance_id), payload,
                    ex=MONITORING_HEARTBEAT_TTL_SECONDS,
                )
                pipeline.set(
                    self.instance_heartbeat_key(instance_id),
                    snapshot.last_heartbeat_at or "1",
                    ex=MONITORING_HEARTBEAT_TTL_SECONDS,
                )
                await pipeline.execute()
            await self._renew_active_consumer_lease(instance_id)
        except Exception:
            raise RedisMonitoringError("Could not write consumer monitoring state") from None

    async def _renew_active_consumer_lease(self, instance_id: str) -> bool:
        """Refresh ownership only when this instance previously received a delivery."""
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end"
        )
        try:
            return bool(await self._client.eval(
                script, 1, self.ACTIVE_CONSUMER_KEY,
                instance_id, MONITORING_HEARTBEAT_TTL_SECONDS,
            ))
        except Exception:
            raise RedisMonitoringError("Could not refresh active consumer ownership") from None

    async def record_active_consumer_delivery(self, instance_id: str) -> None:
        """Set active ownership from actual broker delivery, never from a heartbeat."""
        try:
            canonical_id = str(UUID(instance_id))
            await self._client.set(
                self.ACTIVE_CONSUMER_KEY, canonical_id,
                ex=MONITORING_HEARTBEAT_TTL_SECONDS,
            )
        except Exception:
            raise RedisMonitoringError("Could not record active consumer delivery") from None

    async def read_consumer_instances(self) -> list[dict]:
        """Return currently retained per-instance snapshots and heartbeat liveness."""
        prefix = f"{self.MONITORING_INSTANCE_PREFIX}:"
        try:
            keys = [key async for key in self._client.scan_iter(match=f"{prefix}*:state")]
            owner = await self._client.get(self.ACTIVE_CONSUMER_KEY)
            if isinstance(owner, bytes):
                owner = owner.decode("utf-8")
            instances = []
            for key in keys:
                payload = await self._client.get(key)
                if payload is None:
                    continue
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8")
                snapshot = ConsumerMonitoringSnapshot.from_mapping(json.loads(payload))
                instance_id = snapshot.consumer_instance_id
                if not instance_id or key != self.instance_state_key(instance_id):
                    continue
                UUID(instance_id)
                live = bool(await self._client.exists(self.instance_heartbeat_key(instance_id)))
                instances.append({
                    "consumer_instance_id": instance_id,
                    "consumer_state": snapshot.consumer_state,
                    "last_heartbeat_at": snapshot.last_heartbeat_at,
                    "heartbeat_fresh": live,
                    "delivery_owner": owner == instance_id,
                    "active": live and owner == instance_id,
                })
            return instances
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            raise RedisMonitoringDataError("Stored consumer instance monitoring is invalid") from None
        except RedisMonitoringError:
            raise
        except Exception:
            raise RedisMonitoringError("Could not read consumer instance monitoring") from None

    async def read_active_consumer_monitoring(
        self,
    ) -> tuple[Optional[ConsumerMonitoringSnapshot], list[dict]]:
        """Select only the live snapshot owned by an instance with delivery evidence."""
        try:
            owner = await self._client.get(self.ACTIVE_CONSUMER_KEY)
            if isinstance(owner, bytes):
                owner = owner.decode("utf-8")
            instances = await self.read_consumer_instances()
            if not isinstance(owner, str):
                return None, instances
            UUID(owner)
            payload = await self._client.get(self.instance_state_key(owner))
            heartbeat_fresh = await self._client.exists(self.instance_heartbeat_key(owner))
            if payload is None or not heartbeat_fresh:
                return None, instances
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            snapshot = ConsumerMonitoringSnapshot.from_mapping(json.loads(payload))
            if snapshot.consumer_state != "running" or snapshot.consumer_instance_id != owner:
                return None, instances
            current_owner = await self._client.get(self.ACTIVE_CONSUMER_KEY)
            if isinstance(current_owner, bytes):
                current_owner = current_owner.decode("utf-8")
            if current_owner != owner:
                return None, await self.read_consumer_instances()
            return snapshot, instances
        except RedisMonitoringDataError:
            raise
        except RedisMonitoringError:
            raise
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            raise RedisMonitoringDataError("Stored active consumer monitoring is invalid") from None
        except Exception:
            raise RedisMonitoringError("Could not read active consumer monitoring") from None

    async def refresh_monitoring_heartbeat(self, heartbeat_at: str) -> bool:
        """Refresh only the active owner using its existing per-instance snapshot."""
        self._validate_utc_timestamp("heartbeat_at", heartbeat_at)
        try:
            owner = await self._client.get(self.ACTIVE_CONSUMER_KEY)
            if isinstance(owner, bytes):
                owner = owner.decode("utf-8")
            if not isinstance(owner, str):
                return False
            payload = await self._client.get(self.instance_state_key(owner))
            if payload is None:
                return False
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            snapshot = ConsumerMonitoringSnapshot.from_mapping(json.loads(payload))
            if snapshot.consumer_instance_id != owner or snapshot.consumer_state != "running":
                return False
            refreshed = replace(snapshot, last_heartbeat_at=heartbeat_at)
            await self.write_monitoring_state(refreshed)
            return True
        except Exception:
            raise RedisMonitoringError("Could not refresh monitoring heartbeat") from None

    async def read_monitoring_state(self) -> Optional[ConsumerMonitoringSnapshot]:
        """Return the delivery-evidenced active instance, excluding standby snapshots."""
        snapshot, _instances = await self.read_active_consumer_monitoring()
        return snapshot

    async def is_monitoring_heartbeat_fresh(
        self, expected_instance_id: Optional[str] = None,
    ) -> Optional[bool]:
        """Report liveness only for an owner established by actual delivery evidence."""
        try:
            owner = await self._client.get(self.ACTIVE_CONSUMER_KEY)
            if isinstance(owner, bytes):
                owner = owner.decode("utf-8")
            if not isinstance(owner, str):
                return None
            if expected_instance_id is not None and owner != expected_instance_id:
                return False
            return bool(await self._client.exists(self.instance_heartbeat_key(owner)))
        except Exception:
            raise RedisMonitoringError("Could not check consumer monitoring heartbeat") from None

    async def record_consumer_shutdown(self, snapshot: ConsumerMonitoringSnapshot) -> None:
        """Persist a graceful stopped state and atomically remove the liveness marker."""
        if not isinstance(snapshot, ConsumerMonitoringSnapshot):
            raise TypeError("snapshot must be a ConsumerMonitoringSnapshot")
        if snapshot.consumer_state != "stopped":
            raise ValueError("record_consumer_shutdown requires a stopped consumer snapshot")
        try:
            instance_id = str(UUID(snapshot.consumer_instance_id))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("stopped snapshots require a UUID consumer_instance_id") from exc
        payload = json.dumps(asdict(snapshot), separators=(",", ":"), allow_nan=False)
        try:
            active_owner_release = (
                "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                "return redis.call('DEL', KEYS[1]) else return 0 end"
            )
            async with self._client.pipeline(transaction=True) as pipeline:
                pipeline.set(
                    self.instance_state_key(instance_id), payload,
                    ex=CONSUMER_MONITORING_STOPPED_TTL_SECONDS,
                )
                pipeline.delete(self.instance_heartbeat_key(instance_id))
                await pipeline.execute()
            await self._client.eval(
                active_owner_release, 1, self.ACTIVE_CONSUMER_KEY, instance_id,
            )
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

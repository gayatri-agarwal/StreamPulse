"""FastAPI application for publishing library events and reading metrics."""

from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import asyncio
import inspect
import math
import random
import time
from typing import Any, Deque, Dict, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict

from app.messaging.rabbitmq import RabbitMQError, RabbitMQPublisher
from app.processing.window_config import WindowConfiguration
from app.schemas import EventType, LibraryEvent
from app.storage.redis_metrics import (
    RedisMetricsStore,
    RedisConfigurationDataError,
    RedisConfigurationError,
    RedisMonitoringDataError,
    RedisMonitoringError,
    RedisStorageError,
)


class EventSubmission(BaseModel):
    """Event input; numeric latency is assigned by the consumer."""

    model_config = ConfigDict(extra="forbid", strict=True)

    event_type: str
    student_id: str
    book_id: str
    value: Optional[float] = None  # Accepted for older clients; never used as the metric value.


class SampleBatchSubmission(BaseModel):
    """A finite request for a supported sample batch size."""

    model_config = ConfigDict(extra="forbid", strict=True)

    batch_size: int = 8


BATCH_EVENT_INTERVAL_SECONDS = 0.1


def _monotonic_time() -> float:
    return time.monotonic()


async def _wait_for_batch_interval(delay: float) -> None:
    await asyncio.sleep(delay)


def _random_library_event() -> LibraryEvent:
    return LibraryEvent.from_mapping({
        "event_id": str(uuid4()),
        "event_type": random.choice(tuple(EventType)).value,
        "student_id": f"STU-{random.randint(1000, 9999)}",
        "book_id": f"BK-{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc),
    })



def _event_data(event: LibraryEvent) -> Dict[str, Any]:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "student_id": event.student_id,
        "book_id": event.book_id,
        "timestamp": event.timestamp.isoformat(),
        **({"value": event.value} if event.value is not None else {}),
    }


def _metrics_data(metrics: Any) -> Optional[Dict[str, Any]]:
    if metrics is None:
        return None
    return {
        "count": metrics.count,
        "sum": metrics.total,
        "average": metrics.average,
        "minimum": metrics.minimum,
        "maximum": metrics.maximum,
    }


def create_app(*, publisher: Any = None, metrics_store: Any = None,
               recent_event_limit: int = 100) -> FastAPI:
    """Create an app; dependencies can be injected for tests without live services."""
    if isinstance(recent_event_limit, bool) or not isinstance(recent_event_limit, int) or recent_event_limit <= 0:
        raise ValueError("recent_event_limit must be a positive integer")

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        if application.state.publisher is None:
            try:
                application.state.publisher = RabbitMQPublisher()
                application.state.rabbitmq_status = "not_verified"
            except ValueError:
                application.state.rabbitmq_status = "not_configured"
        if application.state.metrics_store is None:
            try:
                application.state.metrics_store = RedisMetricsStore()
                application.state.redis_status = "not_verified"
            except ValueError:
                application.state.redis_status = "not_configured"
        try:
            yield
        finally:
            for resource in (application.state.publisher, application.state.metrics_store):
                close = getattr(resource, "close", None)
                if close is not None:
                    try:
                        result = close()
                        if inspect.isawaitable(result):
                            await result
                    except Exception:
                        # Shutdown remains clean; adapters expose no secret-bearing details.
                        pass

    application = FastAPI(title="StreamPulse API", version="0.4.0", lifespan=lifespan)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5500", "http://localhost:5500"],
        allow_methods=["GET", "POST", "PUT"],
        allow_headers=["Content-Type"],
    )
    application.state.publisher = publisher
    application.state.metrics_store = metrics_store
    application.state.rabbitmq_status = "not_verified" if publisher is not None else "not_configured"
    application.state.redis_status = "not_verified" if metrics_store is not None else "not_configured"
    application.state.recent_events = deque(maxlen=recent_event_limit)
    batch_publish_lock = asyncio.Lock()
    batch_next_publish_at = 0.0

    @application.get("/api/health")
    async def health() -> Dict[str, Any]:
        """Report API availability separately from checked external services."""
        return {
            "status": "available",
            "services": {
                "rabbitmq": application.state.rabbitmq_status,
                "redis": application.state.redis_status,
            },
        }

    def configuration_response(active: WindowConfiguration, pending: Optional[WindowConfiguration]) -> Dict[str, Any]:
        return {
            "active": asdict(active),
            "pending": asdict(pending) if pending is not None else None,
            "status": "pending" if pending is not None else "active",
        }

    async def active_configuration_or_default(store: Any) -> WindowConfiguration:
        active = await store.read_active_configuration()
        if active is None:
            active = await store.initialize_active_configuration()
        return active

    def raise_configuration_storage_error(error: Exception) -> None:
        if isinstance(error, RedisConfigurationDataError):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Stored window configuration is invalid",
            ) from None
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Configuration storage is unavailable",
        ) from None

    @application.get("/api/config")
    async def get_configuration() -> Dict[str, Any]:
        """Return persisted active and pending window configurations."""
        store = application.state.metrics_store
        if store is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Configuration storage is not configured",
            )
        try:
            active = await active_configuration_or_default(store)
            pending = await store.read_pending_configuration()
        except (RedisConfigurationDataError, RedisConfigurationError) as exc:
            raise_configuration_storage_error(exc)
        except Exception:
            raise_configuration_storage_error(RuntimeError())
        return configuration_response(active, pending)

    @application.put("/api/config")
    async def put_configuration(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a candidate and persist it as pending for boundary activation."""
        try:
            configuration = WindowConfiguration(**payload)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

        store = application.state.metrics_store
        if store is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Configuration storage is not configured",
            )
        try:
            active = await active_configuration_or_default(store)
            await store.save_pending_configuration(configuration)
            pending = await store.read_pending_configuration()
        except (RedisConfigurationDataError, RedisConfigurationError) as exc:
            raise_configuration_storage_error(exc)
        except Exception:
            raise_configuration_storage_error(RuntimeError())

        response = configuration_response(active, pending)
        response["message"] = "Configuration will activate after the current tumbling window completes."
        return response

    @application.get("/api/monitoring")
    async def monitoring() -> Dict[str, Any]:
        """Return best-effort component state without failing the whole response."""
        api_redis_status = application.state.redis_status
        store = application.state.metrics_store
        snapshot = None
        consumer_status = "unavailable"
        consumer_reason = "monitoring_not_configured" if store is None else "not_initialized"

        if store is not None:
            try:
                snapshot = await store.read_monitoring_state()
                api_redis_status = "available"
            except RedisMonitoringDataError:
                # Redis returned data successfully; the stored monitoring payload is invalid.
                api_redis_status = "available"
                consumer_reason = "invalid_snapshot"
            except RedisMonitoringError:
                api_redis_status = "unavailable"
                consumer_reason = "redis_unavailable"
            except Exception:
                api_redis_status = "unavailable"
                consumer_reason = "redis_unavailable"
            else:
                if snapshot is not None:
                    try:
                        heartbeat_fresh = await store.is_monitoring_heartbeat_fresh()
                        api_redis_status = "available"
                    except RedisMonitoringError:
                        heartbeat_fresh = None
                        api_redis_status = "unavailable"
                        consumer_reason = "redis_unavailable"
                    except Exception:
                        heartbeat_fresh = None
                        api_redis_status = "unavailable"
                        consumer_reason = "redis_unavailable"

                    if api_redis_status != "unavailable":
                        if snapshot.consumer_state == "stopped":
                            consumer_status = "stopped"
                            consumer_reason = None
                        elif snapshot.consumer_state == "running" and heartbeat_fresh is True:
                            consumer_status = "running"
                            consumer_reason = None
                        elif snapshot.consumer_state == "running" and heartbeat_fresh is False:
                            consumer_status = "stale"
                            consumer_reason = "heartbeat_expired"
                        elif heartbeat_fresh is None:
                            consumer_reason = "not_initialized"
                        else:
                            consumer_reason = "invalid_snapshot"

        publisher_adapter = application.state.publisher
        publisher_settings = getattr(publisher_adapter, "settings", None)
        queue_name = getattr(publisher_settings, "queue", None)
        rabbitmq_status = application.state.rabbitmq_status
        messages_ready = None
        consumer_count = None
        counts_observed_at = None
        inspect_queue = getattr(publisher_adapter, "inspect_queue", None)
        if callable(inspect_queue):
            try:
                queue_data = await inspect_queue()
                if queue_data is not None:
                    rabbitmq_status = "available"
                    messages_ready = queue_data.get("messages_ready")
                    consumer_count = queue_data.get("consumer_count")
                    counts_observed_at = datetime.now(timezone.utc).isoformat()
            except Exception:
                # Do not expose transport exception details or mutate existing health state.
                rabbitmq_status = "unavailable"

        return {
            "api": {
                "status": "available",
                "redis_status": api_redis_status,
            },
            "rabbitmq": {
                "status": rabbitmq_status,
                "queue_name": queue_name,
                "messages_ready": messages_ready,
                "unacknowledged_messages": None,
                "unacknowledged_status": "unavailable",
                "consumer_count": consumer_count,
                "counts_observed_at": counts_observed_at,
            },
            "consumer": {
                "status": consumer_status,
                "reason": consumer_reason,
                "last_heartbeat": snapshot.last_heartbeat_at if snapshot is not None else None,
                "processed_since_start": snapshot.processed_events_since_start if snapshot is not None else None,
                "processing_failures_since_start": snapshot.processing_failures_since_start if snapshot is not None else None,
                "redis_status": snapshot.consumer_redis_status if snapshot is not None else None,
                "last_successful_aggregation_write": snapshot.last_successful_aggregation_write_at if snapshot is not None else None,
                "windows": {
                    "tumbling": {
                        "size": snapshot.tumbling_window_size if snapshot is not None else None,
                        "progress": snapshot.tumbling_window_progress if snapshot is not None else None,
                        "last_completed_at": snapshot.last_completed_tumbling_aggregation_at if snapshot is not None else None,
                    },
                    "sliding": {
                        "size": snapshot.sliding_window_size if snapshot is not None else None,
                        "progress": snapshot.sliding_window_progress if snapshot is not None else None,
                        "interval": snapshot.sliding_interval if snapshot is not None else None,
                    },
                },
            },
        }

    @application.post("/api/events", status_code=status.HTTP_202_ACCEPTED)
    async def create_library_event(submission: EventSubmission) -> Dict[str, Any]:
        """Validate and publish an action; acknowledge success only after broker publish."""
        if submission.value is not None and not math.isfinite(submission.value):
            raise HTTPException(status_code=422, detail="value must be finite")
        timestamp = datetime.now(timezone.utc)
        try:
            event = LibraryEvent.from_mapping({
                "event_id": str(uuid4()),
                "event_type": submission.event_type,
                "student_id": submission.student_id,
                "book_id": submission.book_id,
                "timestamp": timestamp,
                "value": None,
            })
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

        publisher_adapter = application.state.publisher
        if publisher_adapter is None:
            raise HTTPException(status_code=503, detail="RabbitMQ publisher is not configured")
        try:
            await publisher_adapter.publish(event)
        except Exception:
            application.state.rabbitmq_status = "unavailable"
            raise HTTPException(status_code=503, detail="Could not publish event") from None

        application.state.rabbitmq_status = "available"
        recent_events: Deque[Dict[str, Any]] = application.state.recent_events
        event_data = _event_data(event)
        recent_events.appendleft(event_data)
        return {"status": "published", "event": event_data}

    @application.post("/api/events/batch")
    async def create_sample_event_batch(submission: SampleBatchSubmission) -> Dict[str, Any]:
        """Publish a finite batch through the existing publisher at no more than 10/s."""
        nonlocal batch_next_publish_at
        if submission.batch_size not in (5, 8, 10):
            raise HTTPException(status_code=422, detail="batch_size must be 5, 8, or 10")

        publisher_adapter = application.state.publisher
        published = 0
        failures = []
        recent_events: Deque[Dict[str, Any]] = application.state.recent_events

        async with batch_publish_lock:
            for event_number in range(1, submission.batch_size + 1):
                while True:
                    delay = batch_next_publish_at - _monotonic_time()
                    if delay <= 0:
                        break
                    await _wait_for_batch_interval(delay)
                batch_next_publish_at = _monotonic_time() + BATCH_EVENT_INTERVAL_SECONDS
                event = _random_library_event()

                if publisher_adapter is None:
                    failures.append({
                        "event_number": event_number,
                        "detail": "RabbitMQ publisher is not configured",
                    })
                    continue
                try:
                    await publisher_adapter.publish(event)
                except Exception:
                    application.state.rabbitmq_status = "unavailable"
                    failures.append({
                        "event_number": event_number,
                        "detail": "Could not publish event",
                    })
                    continue

                application.state.rabbitmq_status = "available"
                recent_events.appendleft(_event_data(event))
                published += 1

        failed = len(failures)
        result_status = "completed" if failed == 0 else "failed" if published == 0 else "partial"
        return {
            "status": result_status,
            "requested": submission.batch_size,
            "published": published,
            "failed": failed,
            "failures": failures,
        }

    @application.get("/api/events")
    async def recent_events() -> Dict[str, Any]:
        """Return this API process's newest successfully published events."""
        events = list(application.state.recent_events)
        return {"source": "in_memory", "count": len(events), "events": events}

    @application.get("/api/metrics")
    async def current_metrics() -> Dict[str, Any]:
        """Read latest summaries from Redis; missing values remain null."""
        store = application.state.metrics_store
        if store is None:
            raise HTTPException(status_code=503, detail="Metrics storage is not configured")
        try:
            tumbling = await store.get_metrics("tumbling")
            sliding = await store.get_metrics("sliding")
        except RedisStorageError:
            application.state.redis_status = "unavailable"
            raise HTTPException(status_code=503, detail="Metrics storage is unavailable") from None
        except Exception:
            application.state.redis_status = "unavailable"
            raise HTTPException(status_code=503, detail="Metrics storage is unavailable") from None

        application.state.redis_status = "available"
        return {
            "tumbling": _metrics_data(tumbling),
            "sliding": _metrics_data(sliding),
        }

    return application


app = create_app()

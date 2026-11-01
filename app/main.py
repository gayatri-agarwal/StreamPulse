"""FastAPI application for publishing library events and reading metrics."""

from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import inspect
import math
from typing import Any, Deque, Dict, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict

from app.messaging.rabbitmq import RabbitMQError, RabbitMQPublisher
from app.schemas import LibraryEvent
from app.storage.redis_metrics import RedisMetricsStore, RedisStorageError


class EventSubmission(BaseModel):
    """Event input; numeric latency is assigned by the consumer."""

    model_config = ConfigDict(extra="forbid", strict=True)

    event_type: str
    student_id: str
    book_id: str
    value: Optional[float] = None  # Accepted for older clients; never used as the metric value.



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
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )
    application.state.publisher = publisher
    application.state.metrics_store = metrics_store
    application.state.rabbitmq_status = "not_verified" if publisher is not None else "not_configured"
    application.state.redis_status = "not_verified" if metrics_store is not None else "not_configured"
    application.state.recent_events = deque(maxlen=recent_event_limit)

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

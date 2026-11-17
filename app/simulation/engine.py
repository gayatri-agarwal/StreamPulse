"""Single-session asynchronous event generators for the dashboard simulator."""

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import logging
import traceback
from typing import Any, Callable, Deque, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.experiments.runner import EventPublishResult
from app.processing.window_config import WindowConfiguration
from app.schemas import EventType, LibraryEvent


logger = logging.getLogger(__name__)


class SimulationAlreadyRunning(RuntimeError):
    """A generator is already active in this API process."""


class SimulationStartupError(RuntimeError):
    """A startup failure already recorded in the server log."""


class SimulationStart(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    flow_mode: str
    interval_ms: int = Field(default=500, ge=10, le=60_000)
    burst_size: int = Field(default=10, ge=1, le=1_000)
    burst_interval_ms: int = Field(default=2_000, ge=10, le=60_000)
    lateness_ms: int = Field(default=5_000, ge=1, le=86_400_000)
    out_of_order_every: int = Field(default=3, ge=2, le=100)

    @model_validator(mode="after")
    def validate_mode(self):
        if self.flow_mode not in {"steady", "bursty", "late_arriving"}:
            raise ValueError("flow_mode must be steady, bursty, or late_arriving")
        return self


class SimulationEngine:
    """Manage one generator and keep each session's accounting isolated."""

    def __init__(self, metrics_store: Any, publish: Callable[[LibraryEvent], Any],
                 queue_inspector: Optional[Callable[[], Any]] = None,
                 monitoring_reader: Optional[Callable[[], Any]] = None) -> None:
        self._metrics_store = metrics_store
        self._publish = publish
        self._queue_inspector = queue_inspector
        self._monitoring_reader = monitoring_reader
        self._lock = asyncio.Lock()
        self._session: Optional[Dict[str, Any]] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._last_event_time: Optional[datetime] = None

    async def start(self, config: SimulationStart) -> Dict[str, Any]:
        async with self._lock:
            if self._task is not None and not self._task.done():
                raise SimulationAlreadyRunning("A simulation is already active")
            operation = "preparing_simulation_startup"
            try:
                current = datetime.now(timezone.utc).isoformat()
                simulation_id = str(uuid4())
                operation = "reading_active_window_configuration"
                configuration = await self._metrics_store.read_active_configuration()
                if configuration is None:
                    operation = "initializing_missing_active_configuration"
                    configuration = await self._metrics_store.initialize_active_configuration(
                        WindowConfiguration()
                    )
                operation = "saving_simulation_specific_window_configuration"
                await self._metrics_store.save_simulation_configuration(simulation_id, configuration)
                operation = "creating_simulation_session"
                self._stop = asyncio.Event()
                self._last_event_time = None
                self._session = {
                    "simulation_id": simulation_id,
                    "flow_mode": config.flow_mode,
                    "state": "running",
                    "started_at": current,
                    "stopped_at": None,
                    "last_updated_at": current,
                    "configuration": config.model_dump(),
                    "window_configuration": configuration.__dict__,
                    "generated_count": 0,
                    "publish_confirmed_count": 0,
                    "publish_failed_count": 0,
                    "recent_events": deque(maxlen=100),
                    "error": None,
                }
                self._task = asyncio.create_task(self._run(config), name=f"streampulse-simulation-{simulation_id}")
                return self._public_session()
            except Exception as exc:
                frames = "".join(
                    f'  File "{frame.filename}", line {frame.lineno}, in {frame.name}\n'
                    for frame in traceback.extract_tb(exc.__traceback__)
                )
                logger.error(
                    "simulation_startup_failed operation=%s exception_type=%s\n%s",
                    operation, type(exc).__name__, frames,
                )
                raise SimulationStartupError from None

    async def stop(self) -> Dict[str, Any]:
        async with self._lock:
            if self._task is None or self._task.done() or self._session is None:
                return self._public_session()
            self._session["state"] = "stopping"
            self._touch()
            self._stop.set()
            return self._public_session()

    async def shutdown(self) -> None:
        task = self._task
        if task is None or task.done():
            return
        self._stop.set()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=10)
        except asyncio.TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def status(self) -> Dict[str, Any]:
        session = self._public_session()
        if session is None:
            return {"simulation": None, "queue": None, "consumer": None,
                    "measurements": None, "aggregations": None}
        simulation_id = session["simulation_id"]
        queue = None
        if self._queue_inspector is not None:
            try:
                queue = await self._queue_inspector()
            except Exception:
                queue = None
        consumer = None
        if self._monitoring_reader is not None:
            try:
                monitoring = await self._monitoring_reader()
                snapshot = monitoring[0] if isinstance(monitoring, tuple) else monitoring
                if snapshot is not None:
                    consumer = {
                        "status": snapshot.consumer_state,
                        "processed_since_start": snapshot.processed_events_since_start,
                        "processing_failures_since_start": snapshot.processing_failures_since_start,
                        "last_heartbeat": snapshot.last_heartbeat_at,
                    }
            except Exception:
                consumer = None
        measurements = None
        try:
            summary = await self._metrics_store.get_measurement_summary(simulation_id=simulation_id)
            if summary.measurements_present or not session["publish_confirmed_count"]:
                measurements = {
                    "processed": summary.consumer_unique_successes,
                    "processing_attempts": summary.consumer_attempts,
                    "processing_failures": summary.consumer_failures,
                    "throughput_events_per_second": summary.throughput_events_per_second,
                    "average_latency_ms": summary.average_latency_ms,
                    "p95_latency_ms": summary.p95_latency_ms,
                }
        except Exception:
            pass
        aggregations = {}
        try:
            for kind in ("tumbling", "sliding"):
                metrics = await self._metrics_store.get_simulation_metrics(simulation_id, kind)
                aggregations[kind] = None if metrics is None else {
                    "count": metrics.count, "sum": metrics.total, "average": metrics.average,
                    "minimum": metrics.minimum, "maximum": metrics.maximum,
                }
        except Exception:
            aggregations = None
        return {"simulation": session, "queue": queue, "consumer": consumer,
                "measurements": measurements, "aggregations": aggregations}

    async def activity(self) -> Dict[str, Any]:
        session = self._public_session()
        events = session["recent_events"] if session else []
        processed = {}
        if session is not None:
            try:
                processed = {item["event_id"]: item for item in
                             await self._metrics_store.get_simulation_activity(session["simulation_id"])}
            except Exception:
                pass
        for event in events:
            if event["event_id"] in processed:
                event.update(processed[event["event_id"]])
        return {"simulation_id": session["simulation_id"] if session else None,
                "events": events, "count": len(events)}

    async def _run(self, config: SimulationStart) -> None:
        count = 0
        try:
            while not self._stop.is_set():
                burst = config.burst_size if config.flow_mode == "bursty" else 1
                for _ in range(burst):
                    if self._stop.is_set():
                        break
                    count += 1
                    event = self._make_event(config, count)
                    record = {
                        "event_id": event.event_id,
                        "event_type": event.event_type.value,
                        "timestamp": event.timestamp.isoformat(),
                        "event_time": event.timestamp.isoformat(),
                        "published_at": None,
                        "status": "generated",
                    }
                    self._session["generated_count"] += 1
                    self._session["recent_events"].appendleft(record)
                    self._touch()
                    published_at = datetime.now(timezone.utc)
                    event = LibraryEvent(
                        event_id=event.event_id, event_type=event.event_type,
                        student_id=event.student_id, book_id=event.book_id,
                        timestamp=event.timestamp, simulation_id=event.simulation_id,
                        published_at=published_at,
                    )
                    record["published_at"] = published_at.isoformat()
                    record["status"] = "publishing"
                    try:
                        result = await self._publish(event)
                        succeeded = isinstance(result, EventPublishResult) and result.disposition == "confirmed"
                    except asyncio.CancelledError:
                        self._session["publish_failed_count"] += 1
                        record["status"] = "unknown"
                        self._touch()
                        raise
                    except Exception:
                        succeeded = False
                    if succeeded:
                        self._session["publish_confirmed_count"] += 1
                        record["status"] = "published"
                    else:
                        self._session["publish_failed_count"] += 1
                        record["status"] = "failed"
                    self._touch()
                    if self._stop.is_set():
                        break
                    if config.flow_mode != "bursty" and await self._wait(config.interval_ms):
                        break
                if self._stop.is_set():
                    break
                if config.flow_mode == "bursty" and await self._wait(config.burst_interval_ms):
                    break
        except asyncio.CancelledError:
            if self._session is not None:
                self._session["state"] = "failed"
                self._session["error"] = "Generator cancelled during application shutdown"
                self._finish()
            raise
        except Exception as exc:
            self._session["state"] = "failed"
            self._session["error"] = type(exc).__name__
            self._finish()
            return
        self._session["state"] = "stopped"
        self._finish()

    async def _wait(self, milliseconds: int) -> bool:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=milliseconds / 1000)
            return True
        except asyncio.TimeoutError:
            return False

    def _make_event(self, config: SimulationStart, count: int) -> LibraryEvent:
        now = datetime.now(timezone.utc)
        lateness = 0
        if config.flow_mode == "late_arriving":
            lateness = config.lateness_ms
            if count % config.out_of_order_every == 0:
                lateness *= 2
        event_time = now - timedelta(milliseconds=lateness)
        if (config.flow_mode == "late_arriving" and count % config.out_of_order_every == 0
                and self._last_event_time is not None):
            event_time = min(event_time, self._last_event_time - timedelta(milliseconds=1))
        self._last_event_time = event_time
        return LibraryEvent(
            event_id=str(uuid4()), event_type=tuple(EventType)[(count - 1) % len(EventType)],
            student_id=f"STU-{uuid4().hex[:12].upper()}",
            book_id=f"BK-{uuid4().hex[:12].upper()}",
            timestamp=event_time,
            simulation_id=self._session["simulation_id"],
        )

    def _touch(self) -> None:
        self._session["last_updated_at"] = datetime.now(timezone.utc).isoformat()

    def _finish(self) -> None:
        self._session["stopped_at"] = datetime.now(timezone.utc).isoformat()
        self._touch()

    def _public_session(self) -> Optional[Dict[str, Any]]:
        if self._session is None:
            return None
        return {**{k: v for k, v in self._session.items() if k != "recent_events"},
                "recent_events": list(self._session["recent_events"])}


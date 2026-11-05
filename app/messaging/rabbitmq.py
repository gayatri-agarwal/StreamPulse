"""RabbitMQ publisher and asynchronous consumer for library events."""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional
from uuid import uuid4

import aio_pika

from app.processing.event_processor import LibraryEventProcessor
from app.schemas import LibraryEvent
from app.storage.redis_metrics import (
    ConsumerMonitoringSnapshot,
)


logger = logging.getLogger(__name__)
CONSUMER_HEARTBEAT_INTERVAL_SECONDS = 10
MONITORING_STORAGE_OPERATION_TIMEOUT_SECONDS = 2


class RabbitMQError(RuntimeError):
    """A connection, publishing, or consumer setup error without secret details."""


@dataclass(frozen=True)
class RabbitMQSettings:
    url: str
    queue: str = "streampulse.events"

    @classmethod
    def from_env(cls) -> "RabbitMQSettings":
        url = os.getenv("RABBITMQ_URL", "").strip()
        if not url:
            raise ValueError("RABBITMQ_URL must be configured")
        queue = os.getenv("RABBITMQ_QUEUE", "streampulse.events").strip()
        if not queue:
            raise ValueError("RABBITMQ_QUEUE must not be empty")
        return cls(url=url, queue=queue)


def _event_json(event: LibraryEvent) -> bytes:
    """Serialize the validated event into stable JSON without Python-specific types."""
    return json.dumps({
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "student_id": event.student_id,
        "book_id": event.book_id,
        "timestamp": event.timestamp.isoformat(),
        "value": event.value,
    }, separators=(",", ":"), allow_nan=False).encode("utf-8")


class RabbitMQPublisher:
    """Publishes persistent event messages to the configured durable queue."""

    def __init__(self, settings: Optional[RabbitMQSettings] = None, *, connect: Optional[Callable[..., Awaitable[Any]]] = None) -> None:
        self.settings = settings or RabbitMQSettings.from_env()
        self._connect = connect or aio_pika.connect_robust
        self._connection: Any = None
        self._channel: Any = None
        self._exchange: Any = None
        self._queue: Any = None

    async def inspect_queue(self) -> Optional[dict]:
        """Refresh queue-declare counts on the existing publisher channel, if established."""
        if self._queue is None or self._channel is None:
            return None
        try:
            declaration = await self._queue.declare()
        except Exception:
            raise RabbitMQError("Could not inspect RabbitMQ queue") from None
        return {
            "messages_ready": getattr(declaration, "message_count", None),
            "consumer_count": getattr(declaration, "consumer_count", None),
        }

    async def publish(self, event: LibraryEvent) -> None:
        if not isinstance(event, LibraryEvent):
            raise TypeError("event must be a validated LibraryEvent")
        try:
            if self._exchange is None:
                self._connection = await self._connect(self.settings.url)
                self._channel = await self._connection.channel()
                self._queue = await self._channel.declare_queue(
                    self.settings.queue,
                    durable=True,
                    arguments={"x-single-active-consumer": True},
                )
                self._exchange = self._channel.default_exchange
            message = aio_pika.Message(
                body=_event_json(event),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await self._exchange.publish(message, routing_key=self.settings.queue)
        except Exception:
            connection, self._connection = self._connection, None
            self._channel = None
            self._exchange = None
            self._queue = None
            if connection is not None:
                try:
                    await connection.close()
                except Exception:
                    pass
            raise RabbitMQError("Could not publish library event to RabbitMQ") from None

    async def close(self) -> None:
        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception:
                raise RabbitMQError("Could not close RabbitMQ publisher connection") from None
            finally:
                self._connection = self._channel = self._exchange = self._queue = None


class RabbitMQConsumer:
    """Consumes validated events and delegates all window logic to the processor."""

    def __init__(self, processor: LibraryEventProcessor, settings: Optional[RabbitMQSettings] = None,
                 *, connect: Optional[Callable[..., Awaitable[Any]]] = None) -> None:
        self.processor = processor
        self.settings = settings or RabbitMQSettings.from_env()
        self._connect = connect or aio_pika.connect_robust
        self._connection: Any = None
        self._channel: Any = None
        self._consumer_tag: Any = None
        self.processed_since_start = 0
        self.processing_failures_since_start = 0
        self.monitoring_failures_since_start = 0
        self._consumer_instance_id = str(uuid4())
        self._last_completed_tumbling_at: Optional[str] = None
        self._monitoring_store = getattr(processor, "metrics_store", None)
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_refresh_requested = asyncio.Event()
        self._monitoring_started = False

    def _make_monitoring_snapshot(self, consumer_state: str) -> ConsumerMonitoringSnapshot:
        processor = self.processor
        tumbling = getattr(processor, "tumbling", None)
        sliding = getattr(processor, "sliding", None)
        store = self._monitoring_store
        return ConsumerMonitoringSnapshot(
            consumer_state=consumer_state,
            consumer_instance_id=self._consumer_instance_id,
            last_heartbeat_at=datetime.now(timezone.utc).isoformat(),
            processed_events_since_start=self.processed_since_start,
            processing_failures_since_start=self.processing_failures_since_start,
            consumer_redis_status=getattr(store, "aggregation_write_status", None),
            last_successful_aggregation_write_at=getattr(
                store, "last_successful_aggregation_write_at", None
            ),
            tumbling_window_size=getattr(tumbling, "size", None),
            tumbling_window_progress=getattr(tumbling, "pending_count", None),
            sliding_window_size=getattr(sliding, "size", None),
            sliding_window_progress=getattr(sliding, "event_count", None),
            sliding_interval=(
                "per_event" if getattr(sliding, "interval", None) == 1
                else getattr(sliding, "interval", None)
            ),
            last_completed_tumbling_aggregation_at=self._last_completed_tumbling_at,
        )

    def _start_heartbeat(self) -> None:
        """Start at most one monitor loop after queue consumption is established."""
        if not callable(getattr(self._monitoring_store, "write_monitoring_state", None)) or not callable(
            getattr(self._monitoring_store, "record_consumer_shutdown", None)
        ):
            return
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        self._monitoring_started = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            started_at = loop.time()
            await self._write_monitoring_snapshot()
            elapsed = loop.time() - started_at
            try:
                await asyncio.wait_for(
                    self._heartbeat_refresh_requested.wait(),
                    timeout=max(0, CONSUMER_HEARTBEAT_INTERVAL_SECONDS - elapsed),
                )
            except asyncio.TimeoutError:
                pass
            finally:
                self._heartbeat_refresh_requested.clear()

    async def _write_monitoring_snapshot(self) -> None:
        try:
            await asyncio.wait_for(
                self._monitoring_store.write_monitoring_state(self._make_monitoring_snapshot("running")),
                timeout=MONITORING_STORAGE_OPERATION_TIMEOUT_SECONDS,
            )
        except Exception:
            self.monitoring_failures_since_start += 1
            logger.warning("Consumer monitoring snapshot write failed; event processing continues.")

    async def _stop_heartbeat_and_record_shutdown(self) -> None:
        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if not self._monitoring_started:
            return
        self._monitoring_started = False
        try:
            await asyncio.wait_for(
                self._monitoring_store.record_consumer_shutdown(
                    self._make_monitoring_snapshot("stopped")
                ),
                timeout=MONITORING_STORAGE_OPERATION_TIMEOUT_SECONDS,
            )
        except Exception:
            self.monitoring_failures_since_start += 1
            logger.warning("Could not record consumer shutdown state; continuing cleanup.")

    async def handle_message(self, message: Any) -> None:
        """Measure consumer-side decode latency, then ack after processing and storage."""
        started_at = time.perf_counter()
        try:
            payload = json.loads(message.body.decode("utf-8"))
            event = LibraryEvent.from_mapping(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            await message.reject(requeue=False)
            return

        # The latency sample must exist before the window manager can aggregate it.
        # Queue wait is excluded because this timer starts only in the delivery callback.
        latency_ms = (time.perf_counter() - started_at) * 1000.0
        event = replace(event, value=latency_ms)
        try:
            processing_result = await self.processor.process(event)
        except Exception:
            self.processing_failures_since_start += 1
            if getattr(self._monitoring_store, "aggregation_write_status", None) == "unavailable":
                self._heartbeat_refresh_requested.set()
            # A dead-letter exchange can retain failures; otherwise RabbitMQ discards them.
            await message.reject(requeue=False)
            return
        self.processed_since_start += 1
        if (
            isinstance(processing_result, dict)
            and processing_result
            and getattr(self._monitoring_store, "aggregation_write_status", None) in {"available", "unavailable"}
        ):
            self._heartbeat_refresh_requested.set()
        if isinstance(processing_result, dict) and "tumbling" in processing_result:
            self._last_completed_tumbling_at = datetime.now(timezone.utc).isoformat()
        await message.ack()

    async def start(self) -> None:
        """Connect and begin consuming; RabbitMQ permits only one active consumer per queue."""
        try:
            self._connection = await self._connect(self.settings.url)
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=1)
            queue = await self._channel.declare_queue(
                self.settings.queue,
                durable=True,
                arguments={"x-single-active-consumer": True},
            )
            self._consumer_tag = await queue.consume(self.handle_message)
            self._start_heartbeat()
        except Exception:
            await self.close()
            raise RabbitMQError("Could not start RabbitMQ consumer") from None

    async def run_forever(self) -> None:
        """Start consumption and keep the task alive until cancelled."""
        await self.start()
        try:
            await asyncio.Future()
        finally:
            await self.close()

    async def close(self) -> None:
        await self._stop_heartbeat_and_record_shutdown()
        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception:
                raise RabbitMQError("Could not close RabbitMQ consumer connection") from None
            finally:
                self._connection = self._channel = None
                self._consumer_tag = None

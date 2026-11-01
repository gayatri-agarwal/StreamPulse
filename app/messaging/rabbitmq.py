"""RabbitMQ publisher and asynchronous consumer for library events."""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from dataclasses import replace
from typing import Any, Awaitable, Callable, Optional

import aio_pika

from app.processing.event_processor import LibraryEventProcessor
from app.schemas import LibraryEvent


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

    async def publish(self, event: LibraryEvent) -> None:
        if not isinstance(event, LibraryEvent):
            raise TypeError("event must be a validated LibraryEvent")
        try:
            if self._exchange is None:
                self._connection = await self._connect(self.settings.url)
                self._channel = await self._connection.channel()
                await self._channel.declare_queue(
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
                self._connection = self._channel = self._exchange = None


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
            await self.processor.process(event)
        except Exception:
            # A dead-letter exchange can retain failures; otherwise RabbitMQ discards them.
            await message.reject(requeue=False)
            return
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
        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception:
                raise RabbitMQError("Could not close RabbitMQ consumer connection") from None
            finally:
                self._connection = self._channel = None
                self._consumer_tag = None

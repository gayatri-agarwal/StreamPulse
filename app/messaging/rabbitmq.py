"""RabbitMQ publisher and asynchronous consumer for library events."""

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import aio_pika
from aiormq import spec as aiormq_spec
from aiormq.exceptions import DeliveryError, PublishError

from app.processing.event_processor import LibraryEventProcessor
from app.schemas import LibraryEvent
from app.experiments.store import (
    ExperimentRepository, InvalidRunConfiguration, RunNotFound,
)
from app.processing.run_processor_manager import RunProcessorManager
from app.storage.redis_metrics import (
    ConsumerMonitoringSnapshot,
    MEASUREMENT_STORAGE_OPERATION_TIMEOUT_SECONDS,
)


logger = logging.getLogger(__name__)
CONSUMER_HEARTBEAT_INTERVAL_SECONDS = 10
MONITORING_STORAGE_OPERATION_TIMEOUT_SECONDS = 2
RUN_PROCESSOR_CLEANUP_INTERVAL_SECONDS = 30
DEFAULT_CONSUMER_DRAIN_TIMEOUT_SECONDS = 30.0
CONSUMER_DRAIN_TIMEOUT_ENV = "RABBITMQ_CONSUMER_DRAIN_TIMEOUT_SECONDS"


class RabbitMQError(RuntimeError):
    """A connection, publishing, or consumer setup error without secret details."""


class RabbitMQPublishError(RabbitMQError):
    """Publish failure with a safe classification for run event accounting."""

    def __init__(self, message: str, *, ambiguous: bool) -> None:
        super().__init__(message)
        self.ambiguous = ambiguous


@dataclass(frozen=True)
class ConsumerDrainResult:
    """Outcome of stopping delivery intake and settling active handlers."""

    completed: bool
    timed_out: bool
    active_handlers_remaining: int
    subscription_cancelled: bool = True
    handlers_drained: bool = True


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
        **({"run_id": event.run_id} if event.run_id is not None else {}),
    }, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _redacted_error_detail(error: Exception, connection_url: str) -> str:
    """Keep useful transport diagnostics while removing URL credentials."""
    detail = str(error)
    parsed_url = urlsplit(connection_url)
    for secret in (parsed_url.username, parsed_url.password):
        if secret:
            detail = detail.replace(secret, "[redacted]")
            decoded_secret = unquote(secret)
            if decoded_secret != secret:
                detail = detail.replace(decoded_secret, "[redacted]")
    if connection_url:
        detail = detail.replace(connection_url, "[redacted RabbitMQ URL]")
    return detail or "No additional details"


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
        """Inspect the configured queue on a short-lived, isolated connection."""
        connection = None
        channel = None
        try:
            connection = await self._connect(self.settings.url)
            channel = await connection.channel()
            queue = await channel.declare_queue(
                self.settings.queue,
                durable=True,
                arguments={"x-single-active-consumer": True},
            )
            # aio-pika stores the broker's DeclareOk response here. Reuse it
            # instead of issuing a second declaration on the queue object.
            declaration = getattr(queue, "declaration_result", None)
            if declaration is None:
                declaration = await queue.declare()
        except Exception as exc:
            logger.warning(
                "RabbitMQ queue inspection failed (%s): %s",
                type(exc).__name__,
                _redacted_error_detail(exc, self.settings.url),
            )
            raise RabbitMQError("Could not inspect RabbitMQ queue") from None
        finally:
            if channel is not None:
                try:
                    await channel.close()
                except Exception as exc:
                    logger.warning("Could not close RabbitMQ monitoring channel (%s).", type(exc).__name__)
            if connection is not None:
                try:
                    await connection.close()
                except Exception as exc:
                    logger.warning("Could not close RabbitMQ monitoring connection (%s).", type(exc).__name__)
        return {
            "messages_ready": getattr(declaration, "message_count", None),
            "consumer_count": getattr(declaration, "consumer_count", None),
        }

    async def publish(self, event: LibraryEvent) -> None:
        if not isinstance(event, LibraryEvent):
            raise RabbitMQPublishError(
                "event must be a validated LibraryEvent", ambiguous=False
            )
        publish_started = False
        try:
            if self._exchange is None:
                self._connection = await self._connect(self.settings.url)
                self._channel = await self._connection.channel(
                    publisher_confirms=True, on_return_raises=True
                )
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
            publish_started = True
            confirmation = await self._exchange.publish(
                message, routing_key=self.settings.queue, mandatory=True
            )
            if not isinstance(confirmation, aiormq_spec.Basic.Ack):
                raise RabbitMQPublishError(
                    "RabbitMQ did not return a positive publish confirmation",
                    ambiguous=True,
                )
        except (Exception, asyncio.CancelledError) as exc:
            # Nack and mandatory returns conclusively reject delivery. Setup
            # errors happen before publish invocation; transport loss after it
            # begins leaves acceptance unknown.
            ambiguous = publish_started and not isinstance(exc, (DeliveryError, PublishError))
            connection, self._connection = self._connection, None
            self._channel = None
            self._exchange = None
            self._queue = None
            if connection is not None:
                try:
                    await connection.close()
                except Exception:
                    pass
            if isinstance(exc, RabbitMQPublishError):
                raise
            classification = "unknown" if ambiguous else "failed"
            detail = _redacted_error_detail(exc, self.settings.url)
            raise RabbitMQPublishError(
                f"RabbitMQ publish {classification} ({type(exc).__name__}): {detail}",
                ambiguous=ambiguous,
            ) from exc

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
                 *, connect: Optional[Callable[..., Awaitable[Any]]] = None,
                 experiment_repository: Optional[ExperimentRepository] = None,
                 drain_timeout_seconds: Optional[float] = None) -> None:
        self.processor = processor
        self.settings = settings or RabbitMQSettings.from_env()
        self._connect = connect or aio_pika.connect_robust
        self._connection: Any = None
        self._channel: Any = None
        self._queue: Any = None
        self._consumer_tag: Any = None
        timeout_value = drain_timeout_seconds
        if timeout_value is None:
            raw_timeout = os.getenv(
                CONSUMER_DRAIN_TIMEOUT_ENV,
                str(DEFAULT_CONSUMER_DRAIN_TIMEOUT_SECONDS),
            )
            try:
                timeout_value = float(raw_timeout)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{CONSUMER_DRAIN_TIMEOUT_ENV} must be a positive number") from exc
        if (isinstance(timeout_value, bool) or not isinstance(timeout_value, (int, float))
                or not math.isfinite(timeout_value) or timeout_value <= 0):
            raise ValueError(f"{CONSUMER_DRAIN_TIMEOUT_ENV} must be a positive finite number")
        self.drain_timeout_seconds = float(timeout_value)
        self._draining = False
        self._active_handler_count = 0
        self._active_handler_tasks: set[asyncio.Task] = set()
        self._handlers_drained = asyncio.Event()
        self._handlers_drained.set()
        self._drain_task: Optional[asyncio.Task] = None
        self._last_drain_result: Optional[ConsumerDrainResult] = None
        self.processed_since_start = 0
        self.processing_failures_since_start = 0
        self.monitoring_failures_since_start = 0
        self._consumer_instance_id = str(uuid4())
        self._last_completed_tumbling_at: Optional[str] = None
        self._monitoring_store = getattr(processor, "metrics_store", None)
        self._experiment_repository = experiment_repository
        self._run_processor_manager = (
            RunProcessorManager(experiment_repository, self._monitoring_store)
            if experiment_repository is not None else None
        )
        self._run_processor_cleanup_task: Optional[asyncio.Task] = None
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

    async def _record_measurements(self, *, run_id: Optional[str], counters: dict,
                                   samples: dict, success_timestamp: Optional[float] = None) -> None:
        recorder = getattr(self._monitoring_store, "record_measurements", None)
        if not callable(recorder):
            return
        try:
            await asyncio.wait_for(
                recorder(run_id=run_id, counters=counters, samples=samples,
                         success_timestamp=success_timestamp),
                timeout=MEASUREMENT_STORAGE_OPERATION_TIMEOUT_SECONDS,
            )
        except Exception:
            # Measurement storage is best effort and must not alter event ack semantics.
            logger.warning("Could not persist consumer measurements; event processing continues.")

    async def _record_consumer_delivery(self, *, event: LibraryEvent, succeeded: bool,
                                        samples: dict, success_timestamp: Optional[float] = None) -> None:
        recorder = getattr(self._monitoring_store, "record_consumer_delivery", None)
        if callable(recorder):
            try:
                await asyncio.wait_for(
                    recorder(
                        run_id=event.run_id,
                        event_id=event.event_id,
                        succeeded=succeeded,
                        samples=samples,
                        success_timestamp=success_timestamp,
                    ),
                    timeout=MEASUREMENT_STORAGE_OPERATION_TIMEOUT_SECONDS,
                )
            except Exception:
                logger.warning("Could not persist consumer measurements; event processing continues.")
            return
        # Compatibility for injected stores predating the idempotent interface.
        counters = {"consumer_attempts": 1}
        counters["consumer_successes" if succeeded else "consumer_failures"] = 1
        await self._record_measurements(
            run_id=event.run_id,
            counters=counters,
            samples=samples,
            success_timestamp=success_timestamp,
        )

    async def _claim_run_event(self, event: LibraryEvent, message: Any) -> bool:
        if event.run_id is None:
            return True
        if self._experiment_repository is None:
            logger.warning("Run-scoped event accounting is not configured; message will be retried.")
            await message.reject(requeue=True)
            return False
        try:
            claimed = await asyncio.to_thread(
                self._experiment_repository.claim_run_event,
                event.run_id,
                event.event_id,
            )
        except Exception:
            logger.warning("Could not verify run event reservation; message will be retried.")
            await message.reject(requeue=True)
            return False
        if not claimed:
            # The event was rejected at publish time or is not part of an accepted run.
            await message.reject(requeue=False)
            return False
        return True

    async def _settle_run_event(self, event: LibraryEvent, succeeded: bool) -> bool:
        if event.run_id is None or self._experiment_repository is None:
            return True
        try:
            return await asyncio.to_thread(
                self._experiment_repository.settle_run_event,
                event.run_id,
                event.event_id,
                succeeded,
            )
        except Exception:
            logger.warning("Could not settle run event accounting; message will be retried.")
            return False

    @staticmethod
    async def _release_processor_lease(manager: RunProcessorManager, run_id: str) -> None:
        """Finish lease release despite repeated cancellation of the handler."""
        release_task = asyncio.create_task(manager.release(run_id))
        cancelled = False
        while not release_task.done():
            try:
                await asyncio.shield(release_task)
            except asyncio.CancelledError:
                cancelled = True
                continue
        try:
            release_task.result()
        except BaseException:
            if not cancelled:
                raise
            logger.exception("Could not release run processor lease during cancellation")
        if cancelled:
            raise asyncio.CancelledError

    async def _run_processor_cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(RUN_PROCESSOR_CLEANUP_INTERVAL_SECONDS)
            if self._run_processor_manager is not None:
                try:
                    await self._run_processor_manager.cleanup_terminal()
                except Exception:
                    logger.warning("Run processor cleanup failed; retaining cached contexts.")

    async def _stop_run_processor_cleanup(self) -> None:
        task = self._run_processor_cleanup_task
        self._run_processor_cleanup_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @property
    def active_handler_count(self) -> int:
        return self._active_handler_count

    @property
    def is_draining(self) -> bool:
        return self._draining

    async def handle_message(self, message: Any) -> None:
        """Track a delivery from callback entry through its final settlement."""
        task = asyncio.current_task()
        self._active_handler_count += 1
        if task is not None:
            self._active_handler_tasks.add(task)
        self._handlers_drained.clear()
        try:
            if self._draining:
                # A delivery already dispatched during Basic.Cancel is returned
                # to RabbitMQ without touching processor or receipt state.
                await message.reject(requeue=True)
                return
            await self._handle_message(message)
        finally:
            if task is not None:
                self._active_handler_tasks.discard(task)
            self._active_handler_count -= 1
            if self._active_handler_count == 0:
                self._handlers_drained.set()

    async def _handle_message(self, message: Any) -> None:
        """Measure delivery-to-processing boundaries; ack only after required writes."""
        started_at_ns = time.perf_counter_ns()
        decode_started_at = time.perf_counter()
        delivered_at = datetime.now(timezone.utc)
        try:
            payload = json.loads(message.body.decode("utf-8"))
            event = LibraryEvent.from_mapping(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            await self._record_measurements(
                run_id=None,
                counters={"consumer_attempts": 1, "consumer_failures": 1},
                samples={},
            )
            await message.reject(requeue=False)
            return
        run_experiment_id = None
        run_configuration = None
        run_configuration_error = None
        configuration_lookup_ms = 0.0
        if event.run_id is not None:
            if self._run_processor_manager is None:
                logger.warning("Run-scoped event processing is not configured; message will be retried.")
                await message.reject(requeue=True)
                return
            lookup_started = time.perf_counter()
            try:
                run_experiment_id, run_configuration = (
                    await self._run_processor_manager.configuration_for(event.run_id)
                )
            except InvalidRunConfiguration as exc:
                # It is safe to settle this permanently invalid event only after
                # confirming its durable reservation below.
                run_configuration_error = exc
            except RunNotFound:
                await message.reject(requeue=False)
                return
            except Exception:
                logger.warning("Could not load run configuration; message will be retried.")
                await message.reject(requeue=True)
                return
            configuration_lookup_ms = (time.perf_counter() - lookup_started) * 1000.0
        if not await self._claim_run_event(event, message):
            return
        publish_to_delivery_ms = None
        try:
            event_timestamp = event.timestamp
            if event_timestamp.tzinfo is not None:
                elapsed = (delivered_at - event_timestamp).total_seconds() * 1000.0
                publish_to_delivery_ms = elapsed if elapsed >= 0 else None
        except (TypeError, ValueError, OverflowError):
            pass
        # Exclude the new snapshot lookup so this stage retains its former
        # decode/validation plus reservation-claim measurement boundary.
        decode_latency_ms = max(
            0.0,
            (time.perf_counter() - decode_started_at) * 1000.0 - configuration_lookup_ms,
        )
        event = replace(event, value=decode_latency_ms)
        processor = self.processor if event.run_id is None else None
        leased_run_processor = False
        try:
            try:
                if run_configuration_error is not None:
                    raise run_configuration_error
                if event.run_id is not None:
                    processor = await self._run_processor_manager.acquire(
                        event.run_id, run_experiment_id, run_configuration
                    )
                    leased_run_processor = True
                processing_result = await processor.process(event)
            except Exception:
                self.processing_failures_since_start += 1
                if getattr(self._monitoring_store, "aggregation_write_status", None) == "unavailable":
                    self._heartbeat_refresh_requested.set()
                processing_ms = (time.perf_counter_ns() - started_at_ns) / 1_000_000.0
                persistence_ms = getattr(processor, "last_redis_persistence_latency_ms", None)
                await self._record_consumer_delivery(
                    event=event,
                    succeeded=False,
                    samples={
                        "publish_to_delivery_latency_ms": publish_to_delivery_ms,
                        "decode_validation_latency_ms": decode_latency_ms,
                        "consumer_processing_latency_ms": processing_ms,
                        "redis_persistence_latency_ms": persistence_ms,
                    },
                )
                if not await self._settle_run_event(event, succeeded=False):
                    await message.reject(requeue=True)
                    return
                await message.reject(requeue=False)
                return

            processing_ms = (time.perf_counter_ns() - started_at_ns) / 1_000_000.0
            persistence_ms = getattr(processor, "last_redis_persistence_latency_ms", None)
            end_to_end_ms = None
            if event.timestamp.tzinfo is not None:
                elapsed = (datetime.now(timezone.utc) - event.timestamp).total_seconds() * 1000.0
                end_to_end_ms = elapsed if elapsed >= 0 else None
            await self._record_consumer_delivery(
                event=event,
                succeeded=True,
                samples={
                    "publish_to_delivery_latency_ms": publish_to_delivery_ms,
                    "decode_validation_latency_ms": decode_latency_ms,
                    "consumer_processing_latency_ms": processing_ms,
                    "redis_persistence_latency_ms": persistence_ms,
                    "end_to_end_latency_ms": end_to_end_ms,
                },
                success_timestamp=datetime.now(timezone.utc).timestamp(),
            )
            if not await self._settle_run_event(event, succeeded=True):
                await message.reject(requeue=True)
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
        finally:
            if leased_run_processor:
                await self._release_processor_lease(
                    self._run_processor_manager, event.run_id,
                )

    async def start(self) -> None:
        """Connect and begin consuming; RabbitMQ permits only one active consumer per queue."""
        if self._draining:
            raise RabbitMQError("A draining RabbitMQ consumer cannot be restarted")
        try:
            if self._experiment_repository is not None:
                await asyncio.to_thread(self._experiment_repository.initialize)
            self._connection = await self._connect(self.settings.url)
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=1)
            queue = await self._channel.declare_queue(
                self.settings.queue,
                durable=True,
                arguments={"x-single-active-consumer": True},
            )
            self._consumer_tag = await queue.consume(self.handle_message)
            self._queue = queue
            self._start_heartbeat()
            if self._run_processor_manager is not None:
                self._run_processor_cleanup_task = asyncio.create_task(
                    self._run_processor_cleanup_loop()
                )
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

    async def _close_transport(self, *, force: bool = False) -> None:
        channel, self._channel = self._channel, None
        connection, self._connection = self._connection, None
        self._queue = None
        self._consumer_tag = None
        close_error = None
        if force and connection is not None:
            # Closing the aio-pika connection tears down its underlying
            # aiormq transport and all channels. It does not wait for
            # Basic.Channel.CloseOk / Basic.Connection.CloseOk, unlike a
            # graceful channel-close RPC.
            try:
                await connection.close()
            except Exception as exc:
                close_error = exc
        elif force and channel is not None:
            # Without the owning connection there is no supported transport
            # shutdown path to guarantee unacked deliveries are released.
            close_error = RuntimeError("RabbitMQ connection is unavailable for forced shutdown")
        elif channel is not None:
            try:
                await channel.close()
            except Exception as exc:
                close_error = exc
        if connection is not None and not force:
            try:
                await connection.close()
            except Exception as exc:
                if close_error is None:
                    close_error = exc
        if close_error is not None:
            raise RabbitMQError("Could not close RabbitMQ consumer connection") from None

    async def _drain_and_close(self) -> ConsumerDrainResult:
        self._draining = True
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.drain_timeout_seconds
        await self._stop_run_processor_cleanup()

        cancel_error = None
        subscription_cancelled = self._queue is None or self._consumer_tag is None
        handlers_drained = False
        timed_out = False
        if self._queue is not None and self._consumer_tag is not None:
            try:
                async with asyncio.timeout_at(deadline):
                    remaining = max(0.0, deadline - loop.time())
                    await self._queue.cancel(self._consumer_tag, timeout=remaining)
                    subscription_cancelled = True
                    # aio-pika schedules callbacks for deliveries received
                    # before Basic.CancelOk. Let them enter tracking first.
                    await asyncio.sleep(0)
            except Exception as exc:
                cancel_error = exc
                if isinstance(exc, TimeoutError):
                    timed_out = True
            finally:
                self._consumer_tag = None

        if not timed_out:
            remaining = deadline - loop.time()
            if remaining <= 0:
                timed_out = True
            else:
                try:
                    await asyncio.wait_for(
                        self._handlers_drained.wait(), timeout=remaining,
                    )
                    handlers_drained = True
                except asyncio.TimeoutError:
                    timed_out = True

        transport_closed = True
        if timed_out:
            # Stop callback tasks before closing the channel. Unacknowledged
            # deliveries are then requeued by RabbitMQ when the channel closes.
            current = asyncio.current_task()
            active_tasks = [
                task for task in self._active_handler_tasks
                if task is not current and not task.done()
            ]
            for task in active_tasks:
                task.cancel()
            try:
                await self._close_transport(force=True)
            except RabbitMQError:
                transport_closed = False
                logger.error("Could not fully close RabbitMQ transport after drain timeout")
            # Give cancellation callbacks one event-loop turn to release their
            # leases. Do not start a second timeout after the overall deadline.
            await asyncio.sleep(0)
            remaining = self._active_handler_count
            result = ConsumerDrainResult(
                False, True, remaining,
                subscription_cancelled=subscription_cancelled,
                handlers_drained=False,
            )
        else:
            try:
                await self._close_transport()
            except RabbitMQError:
                transport_closed = False
                logger.error("Could not fully close RabbitMQ transport after consumer drain")
            remaining = self._active_handler_count
            result = ConsumerDrainResult(
                cancel_error is None and subscription_cancelled
                and handlers_drained and remaining == 0 and transport_closed,
                False,
                remaining,
                subscription_cancelled=subscription_cancelled,
                handlers_drained=handlers_drained,
            )

        await self._stop_heartbeat_and_record_shutdown()
        if cancel_error is not None:
            logger.warning(
                "Could not cancel RabbitMQ consumer subscription (%s); channel was closed",
                type(cancel_error).__name__,
            )
        self._last_drain_result = result
        if not result.completed:
            logger.warning(
                "Consumer drain incomplete (timed_out=%s active_handlers=%d)",
                result.timed_out, result.active_handlers_remaining,
            )
        return result

    async def close(self) -> ConsumerDrainResult:
        """Drain deliveries and close transport; repeated calls share one outcome."""
        self._draining = True
        task = self._drain_task
        if task is None:
            task = asyncio.create_task(self._drain_and_close(), name="rabbitmq-consumer-drain")
            self._drain_task = task
        return await self._await_drain_task(task)

    @staticmethod
    async def _await_drain_task(task: asyncio.Task) -> ConsumerDrainResult:
        """Wait for cleanup despite repeated cancellation, then re-propagate it."""
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
                continue
        result = task.result()
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def drain(self) -> ConsumerDrainResult:
        """Explicitly drain and close the consumer using the same idempotent path."""
        return await self.close()

    @property
    def last_drain_result(self) -> Optional[ConsumerDrainResult]:
        return self._last_drain_result

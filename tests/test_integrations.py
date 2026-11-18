import asyncio
import fnmatch
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from aiormq import spec as aiormq_spec
from aiormq.exceptions import DeliveryError, PublishError
from fastapi.testclient import TestClient

import run_consumer
from app.messaging.rabbitmq import RabbitMQConsumer, RabbitMQError, RabbitMQPublisher, RabbitMQSettings
from app.main import create_app
from app.experiments.runner import EventPublishResult
from app.processing.event_processor import LibraryEventProcessor
from app.processing.measurements import MeasurementSummary
from app.processing.metrics import Metrics
from app.processing.window_manager import SlidingWindow, TumblingWindow
from app.schemas import LibraryEvent
from app.experiments.store import ExperimentRepository
from app.experiments.schemas import RunStatus
from app.storage.redis_metrics import (
    CONSUMER_AGGREGATION_WRITE_EVIDENCE_TTL_SECONDS,
    ConsumerMonitoringSnapshot,
    MONITORING_HEARTBEAT_TTL_SECONDS,
    RedisConfigurationDataError,
    RedisConfigurationError,
    RedisMetricsStore,
    RedisMonitoringError,
    RedisStorageError,
)
from app.processing.window_config import WindowConfiguration
from app.messaging.rabbitmq import _event_json
from app.simulation.engine import SimulationEngine, SimulationStart


def make_event(value=1, event_id="event-1", run_id=None):
    return LibraryEvent(
        event_id=event_id,
        event_type="BOOK_ISSUED",
        student_id="student-1",
        book_id="book-1",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        value=value,
        run_id=run_id,
    )


_DEFAULT_CONFIRMATION = object()


class FakeExchange:
    def __init__(self, fail=False, result=_DEFAULT_CONFIRMATION):
        self.calls = []
        self.fail = fail
        self.result = (
            aiormq_spec.Basic.Ack(delivery_tag=1, multiple=False)
            if result is _DEFAULT_CONFIRMATION else result
        )

    async def publish(self, message, routing_key, **kwargs):
        if self.fail:
            raise self.fail if isinstance(self.fail, BaseException) else RuntimeError("fake transport failure")
        self.calls.append((message, routing_key))
        return self.result


class FakeChannel:
    def __init__(self, exchange=None, queue=None):
        self.default_exchange = exchange or FakeExchange()
        self.declared = []
        self.qos = []
        self.queue = queue or FakeQueue()
        self.closed = False
        self.order = None

    async def declare_queue(self, name, **kwargs):
        self.declared.append((name, kwargs))
        return self.queue

    async def set_qos(self, **kwargs):
        self.qos.append(kwargs)

    async def close(self):
        if self.order is not None:
            self.order.append("channel_close")
        self.closed = True


class FakeQueue:
    def __init__(self, *, message_count=4, consumer_count=1, declare_fail=False):
        self.message_count = message_count
        self.consumer_count = consumer_count
        self.declare_fail = declare_fail
        self.declare_calls = 0
        self.cancel_calls = []
        self.cancel_timeouts = []
        self.cancelled = None

    async def declare(self):
        self.declare_calls += 1
        if self.declare_fail:
            raise RuntimeError("fake queue inspection failure")
        return self

    async def consume(self, callback):
        self.callback = callback
        return "fake-tag"

    async def cancel(self, consumer_tag, timeout=None):
        self.cancel_calls.append(consumer_tag)
        self.cancel_timeouts.append(timeout)
        if self.cancelled is not None:
            self.cancelled.set()


class FakeConnection:
    def __init__(self, channel):
        self._channel = channel
        self.closed = False
        self.transport_closed = False
        self.close_ack_received = False
        self.order = None

    async def channel(self, **kwargs):
        return self._channel

    async def close(self):
        if self.order is not None:
            self.order.append("connection_close")
        # Model aio-pika's connection close: tear down its transport without
        # waiting for a broker Connection.CloseOk response.
        self.transport_closed = True
        self._channel.closed = True
        self.closed = True


@pytest.mark.asyncio
async def test_publisher_sends_valid_event_to_configured_queue():
    channel = FakeChannel()
    connection = FakeConnection(channel)

    async def connect(url):
        assert url == "amqps://fake-host"
        return connection

    publisher = RabbitMQPublisher(RabbitMQSettings("amqps://fake-host", "events"), connect=connect)
    await publisher.publish(make_event(3))

    message, routing_key = channel.default_exchange.calls[0]
    event = json.loads(message.body)
    assert routing_key == "events"
    assert event["event_id"] == "event-1"
    assert event["event_type"] == "BOOK_ISSUED"
    assert event["value"] == 3.0
    assert channel.declared == [(
        "events", {"durable": True, "arguments": {"x-single-active-consumer": True}}
    )]


@pytest.mark.asyncio
async def test_publisher_explicitly_enables_confirms_and_mandatory_returns():
    channel = FakeChannel()

    class ConfirmingConnection(FakeConnection):
        async def channel(self, **kwargs):
            assert kwargs == {"publisher_confirms": True, "on_return_raises": True}
            return self._channel

    publisher = RabbitMQPublisher(
        RabbitMQSettings("amqps://fake-host", "events"),
        connect=lambda url: asyncio.sleep(0, result=ConfirmingConnection(channel)),
    )
    await publisher.publish(make_event())


@pytest.mark.asyncio
async def test_publisher_classifies_nack_and_unroutable_as_definitive():
    from app.messaging.rabbitmq import RabbitMQPublishError

    nack = DeliveryError(None, aiormq_spec.Basic.Nack(delivery_tag=1, multiple=False, requeue=False))
    returned = aiormq_spec.Basic.Return(
        reply_code=312, reply_text="NO_ROUTE", exchange="", routing_key="events"
    )
    for error in (nack, PublishError(type("Returned", (), {"delivery": returned})(), returned)):
        channel = FakeChannel(FakeExchange(fail=error))
        publisher = RabbitMQPublisher(
            RabbitMQSettings("amqps://fake-host", "events"),
            connect=lambda url: asyncio.sleep(0, result=FakeConnection(channel)),
        )
        with pytest.raises(RabbitMQPublishError) as caught:
            await publisher.publish(make_event())
        assert caught.value.ambiguous is False


@pytest.mark.asyncio
async def test_publisher_classifies_post_invocation_timeout_and_unexpected_result_as_ambiguous():
    from app.messaging.rabbitmq import RabbitMQPublishError

    for exchange in (
        FakeExchange(fail=TimeoutError()),
        FakeExchange(fail=ConnectionError("connection lost after send")),
        FakeExchange(result=None),
    ):
        publisher = RabbitMQPublisher(
            RabbitMQSettings("amqps://fake-host", "events"),
            connect=lambda url: asyncio.sleep(0, result=FakeConnection(FakeChannel(exchange))),
        )
        with pytest.raises(RabbitMQPublishError) as caught:
            await publisher.publish(make_event())
        assert caught.value.ambiguous is True


@pytest.mark.asyncio
async def test_publisher_classifies_connection_setup_failure_as_definitive():
    from app.messaging.rabbitmq import RabbitMQPublishError

    async def connect(url):
        raise ConnectionError("could not connect")

    publisher = RabbitMQPublisher(RabbitMQSettings("amqps://fake-host"), connect=connect)
    with pytest.raises(RabbitMQPublishError) as caught:
        await publisher.publish(make_event())
    assert caught.value.ambiguous is False


@pytest.mark.asyncio
async def test_publisher_inspects_queue_counts_using_existing_connection():
    queue = FakeQueue(message_count=7, consumer_count=2)
    channel = FakeChannel(queue=queue)
    publisher_connection = FakeConnection(FakeChannel())
    monitoring_connection = FakeConnection(channel)
    connect_calls = []

    async def connect(url):
        connect_calls.append(url)
        return publisher_connection if len(connect_calls) == 1 else monitoring_connection

    publisher = RabbitMQPublisher(RabbitMQSettings("amqps://fake-host", "events"), connect=connect)
    await publisher.publish(make_event())
    counts = await publisher.inspect_queue()

    assert counts == {"messages_ready": 7, "consumer_count": 2}
    assert queue.declare_calls == 1
    assert len(connect_calls) == 2
    assert publisher_connection.closed is False
    assert monitoring_connection.closed is True
    assert channel.closed is True


@pytest.mark.asyncio
async def test_publisher_inspection_connects_and_verifies_queue_when_not_yet_publishing():
    queue = FakeQueue(message_count=2, consumer_count=1)
    channel = FakeChannel(queue=queue)
    connection = FakeConnection(channel)
    connect_calls = []

    async def connect(url):
        connect_calls.append(url)
        return connection

    publisher = RabbitMQPublisher(RabbitMQSettings("amqps://fake-host", "events"), connect=connect)
    counts = await publisher.inspect_queue()

    assert counts == {"messages_ready": 2, "consumer_count": 1}
    assert connect_calls == ["amqps://fake-host"]
    assert channel.declared == [(
        "events", {"durable": True, "arguments": {"x-single-active-consumer": True}}
    )]
    assert queue.declare_calls == 1
    assert connection.closed is True
    assert channel.closed is True


@pytest.mark.asyncio
async def test_publisher_queue_inspection_failure_logs_reason_without_credentials(caplog):
    queue = FakeQueue(declare_fail=True)
    channel = FakeChannel(queue=queue)

    async def connect(url):
        return FakeConnection(channel)

    publisher = RabbitMQPublisher(RabbitMQSettings("amqps://user:secret@host", "events"), connect=connect)
    await publisher.publish(make_event())

    with pytest.raises(RabbitMQError) as error:
        await publisher.inspect_queue()
    assert str(error.value) == "Could not inspect RabbitMQ queue"
    assert "secret" not in str(error.value)
    assert "fake queue inspection failure" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_publisher_wraps_transport_error_without_exposing_details():
    channel = FakeChannel(FakeExchange(fail=True))

    async def connect(url):
        return FakeConnection(channel)

    publisher = RabbitMQPublisher(RabbitMQSettings("amqps://user:secret@host", "events"), connect=connect)
    with pytest.raises(RabbitMQError) as error:
        await publisher.publish(make_event())
    assert "secret" not in str(error.value)
    assert "publish unknown" in str(error.value)


class FakeMessage:
    def __init__(self, body):
        self.body = body
        self.acks = 0
        self.rejects = []

    async def ack(self):
        self.acks += 1

    async def reject(self, requeue):
        self.rejects.append(requeue)


class FakeProcessor:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail

    async def process(self, event):
        if self.fail:
            raise RuntimeError("storage failed")
        self.events.append(event)


@pytest.mark.asyncio
async def test_consumer_validates_and_processes_before_acknowledging():
    processor = FakeProcessor()
    consumer = RabbitMQConsumer(processor, RabbitMQSettings("amqp://fake", "events"))
    message = FakeMessage(json.dumps({
        "event_id": "e1", "event_type": "BOOK_RETURNED", "student_id": "s1",
        "book_id": "b1", "timestamp": "2026-01-01T00:00:00Z", "value": 7.5,
    }).encode())

    await consumer.handle_message(message)

    assert message.acks == 1
    assert message.rejects == []
    assert processor.events[0].event_type.value == "BOOK_RETURNED"
    assert processor.events[0].value is not None
    assert processor.events[0].value != 7.5
    assert consumer.processed_since_start == 1
    assert consumer.processing_failures_since_start == 0


@pytest.mark.asyncio
async def test_consumer_replaces_legacy_value_with_mocked_latency_in_milliseconds(monkeypatch):
    import app.messaging.rabbitmq as rabbitmq_module

    readings = iter([12.0, 12.025])
    monkeypatch.setattr(rabbitmq_module.time, "perf_counter", lambda: next(readings))
    processor = FakeProcessor()
    consumer = RabbitMQConsumer(processor, RabbitMQSettings("amqp://fake", "events"))
    message = FakeMessage(json.dumps({
        "event_id": "e1", "event_type": "BOOK_ISSUED", "student_id": "s1",
        "book_id": "b1", "timestamp": "2026-01-01T00:00:00Z", "value": 999,
    }).encode())

    await consumer.handle_message(message)

    assert processor.events[0].value == pytest.approx(25.0)
    assert message.acks == 1
    assert message.rejects == []


@pytest.mark.asyncio
async def test_measured_latency_flows_into_tumbling_and_sliding_metrics(monkeypatch):
    import app.messaging.rabbitmq as rabbitmq_module

    readings = iter([0.0, 0.01, 1.0, 1.02])
    monkeypatch.setattr(rabbitmq_module.time, "perf_counter", lambda: next(readings))
    store = RecordingStore()
    processor = LibraryEventProcessor(TumblingWindow(2), SlidingWindow(2), store)
    consumer = RabbitMQConsumer(processor, RabbitMQSettings("amqp://fake", "events"))
    for event_id in ("e1", "e2"):
        message = FakeMessage(json.dumps({
            "event_id": event_id, "event_type": "BOOK_ISSUED", "student_id": "s1",
            "book_id": "b1", "timestamp": "2026-01-01T00:00:00Z",
        }).encode())
        await consumer.handle_message(message)
        assert message.acks == 1

    sliding = [metrics for name, metrics in store.saved if name == "sliding"][-1]
    tumbling = [metrics for name, metrics in store.saved if name == "tumbling"][-1]
    for metrics in (sliding, tumbling):
        assert metrics.count == 2
        assert metrics.total == pytest.approx(30.0)
        assert metrics.average == pytest.approx(15.0)
        assert metrics.minimum == pytest.approx(10.0)
        assert metrics.maximum == pytest.approx(20.0)


@pytest.mark.asyncio
async def test_consumer_starts_with_single_active_consumer_and_prefetch_one(tmp_path, caplog):
    caplog.set_level("INFO", logger="app.messaging.rabbitmq")
    channel = FakeChannel()

    async def connect(url):
        return FakeConnection(channel)

    repository = ExperimentRepository(tmp_path / "consumer-start.sqlite3")
    consumer = RabbitMQConsumer(
        FakeProcessor(), RabbitMQSettings("amqp://fake", "events"), connect=connect,
        experiment_repository=repository,
    )
    await consumer.start()
    assert channel.qos == [{"prefetch_count": 1}]
    assert channel.declared == [(
        "events", {"durable": True, "arguments": {"x-single-active-consumer": True}}
    )]
    assert f"component=consumer database_path={repository.database_path.resolve()}" in caplog.text
    await consumer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "reason_code"),
    [(b"not json", "invalid_json"), (b"{}", "invalid_event_schema"), (b"\xff", "invalid_json")],
)
async def test_consumer_rejects_malformed_messages_without_requeue(body, reason_code, caplog):
    processor = FakeProcessor()
    consumer = RabbitMQConsumer(processor, RabbitMQSettings("amqp://fake", "events"))
    message = FakeMessage(body)
    await consumer.handle_message(message)
    assert message.acks == 0
    assert message.rejects == [False]
    assert processor.events == []
    assert f"reason_code={reason_code}" in caplog.text
    assert "queue=events" in caplog.text
    assert "requeue=false" in caplog.text


@pytest.mark.asyncio
async def test_malformed_event_schema_logs_only_bounded_identifiers_not_payload(tmp_path, caplog):
    run_id = str(uuid4())
    body = json.dumps({
        "event_id": "event-safe-1", "run_id": run_id,
        "credential": "DO_NOT_LOG_THIS_VALUE",
    }).encode()
    consumer = RabbitMQConsumer(FakeProcessor(), RabbitMQSettings("amqp://fake", "events"))
    message = FakeMessage(body)

    await consumer.handle_message(message)

    assert message.rejects == [False]
    assert f"reason_code=invalid_event_schema run_id={run_id} event_id=event-safe-1" in caplog.text
    assert "DO_NOT_LOG_THIS_VALUE" not in caplog.text
    assert body.decode() not in caplog.text


def scoped_event_message(run_id, event_id="event-1"):
    return FakeMessage(json.dumps({
        "event_id": event_id, "event_type": "BOOK_ISSUED", "student_id": "s1",
        "book_id": "b1", "timestamp": "2026-01-01T00:00:00Z", "run_id": run_id,
    }).encode())


@pytest.mark.asyncio
async def test_missing_run_processor_logs_and_requeues_without_ack(caplog):
    run_id = str(uuid4())
    consumer = RabbitMQConsumer(FakeProcessor(), RabbitMQSettings("amqp://fake", "events"))
    message = scoped_event_message(run_id)

    await consumer.handle_message(message)

    assert message.acks == 0 and message.rejects == [True]
    assert f"reason_code=run_processor_unavailable run_id={run_id} event_id=event-1" in caplog.text
    assert "requeue=true" in caplog.text


@pytest.mark.asyncio
async def test_run_not_found_logs_and_rejects_without_requeue(tmp_path, caplog):
    run_id = str(uuid4())
    repository = ExperimentRepository(tmp_path / "missing-consumer-run.sqlite3")
    repository.initialize()
    consumer = RabbitMQConsumer(
        FakeProcessor(), RabbitMQSettings("amqp://fake", "events"),
        experiment_repository=repository,
    )
    message = scoped_event_message(run_id)

    await consumer.handle_message(message)

    assert message.acks == 0 and message.rejects == [False]
    assert f"reason_code=run_not_found run_id={run_id} event_id=event-1" in caplog.text
    assert "requeue=false" in caplog.text


@pytest.mark.asyncio
async def test_split_api_consumer_repositories_leave_run_reservation_pending(tmp_path, caplog):
    api_repository = ExperimentRepository(tmp_path / "api.sqlite3")
    consumer_repository = ExperimentRepository(tmp_path / "consumer.sqlite3")
    api_repository.initialize()
    consumer_repository.initialize()
    experiment = api_repository.create_experiment({
        "name": "Split repository", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 5, "sliding_size": 5, "sliding_interval": 1},
    })
    run = api_repository.create_run(experiment["experiment_id"], workload_configuration={"target_event_count": 1})
    api_repository.transition_run(experiment["experiment_id"], run["run_id"], RunStatus.RUNNING)
    event_id = "split-db-event"
    api_repository.reserve_run_event(run["run_id"], event_id)
    api_repository.mark_run_event_published(run["run_id"], event_id)

    processor = FakeProcessor()
    consumer = RabbitMQConsumer(
        processor, RabbitMQSettings("amqp://fake", "events"),
        experiment_repository=consumer_repository,
    )
    message = scoped_event_message(run["run_id"], event_id)
    await consumer.handle_message(message)

    inventory = api_repository.get_run_pending_event_inventory(
        experiment["experiment_id"], run["run_id"],
    )
    assert message.acks == 0 and message.rejects == [False]
    assert processor.events == []
    assert inventory["summary"]["pending_count"] == 1
    assert "reason_code=run_not_found" in caplog.text


def test_connection_identity_diagnostics_exclude_credentials():
    rabbit = RabbitMQSettings("amqps://user:secret@broker.example:5671/team-vhost")
    redis = RedisMetricsStore("rediss://user:secret@cache.example:6380/3")

    assert rabbit.connection_identity == "broker.example:5671/team-vhost"
    assert redis.connection_identity == "cache.example:6380/db3"
    assert "secret" not in rabbit.connection_identity
    assert "secret" not in redis.connection_identity


@pytest.mark.asyncio
async def test_configuration_lookup_failure_logs_exception_type_and_requeues(caplog):
    class FailingConfigurationRepository:
        def get_run_with_configuration_snapshot(self, run_id):
            raise RuntimeError("do not log connection credentials")

    run_id = str(uuid4())
    consumer = RabbitMQConsumer(
        FakeProcessor(), RabbitMQSettings("amqp://fake", "events"),
        experiment_repository=FailingConfigurationRepository(),
    )
    message = scoped_event_message(run_id)

    await consumer.handle_message(message)

    assert message.acks == 0 and message.rejects == [True]
    assert "reason_code=configuration_lookup_failed" in caplog.text
    assert f"run_id={run_id} event_id=event-1" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "do not log connection credentials" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("claim_behavior", "reason_code", "requeue"),
    [("raise", "reservation_claim_failed", True), ("false", "reservation_claim_returned_false", False)],
)
async def test_reservation_claim_diagnostics_preserve_disposition(
    claim_behavior, reason_code, requeue, caplog,
):
    class ClaimRepository:
        def get_run_with_configuration_snapshot(self, run_id):
            return {
                "experiment_id": str(uuid4()),
                "configuration_snapshot": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
            }

        def claim_run_event(self, run_id, event_id):
            if claim_behavior == "raise":
                raise RuntimeError("claim error")
            return False

    run_id = str(uuid4())
    consumer = RabbitMQConsumer(
        FakeProcessor(), RabbitMQSettings("amqp://fake", "events"),
        experiment_repository=ClaimRepository(),
    )
    message = scoped_event_message(run_id)

    await consumer.handle_message(message)

    assert message.acks == 0 and message.rejects == [requeue]
    assert f"reason_code={reason_code} run_id={run_id} event_id=event-1" in caplog.text
    assert f"requeue={str(requeue).lower()}" in caplog.text


@pytest.mark.asyncio
async def test_reservation_repository_unavailable_diagnostic_preserves_requeue(caplog):
    run_id = str(uuid4())
    consumer = RabbitMQConsumer(
        FakeProcessor(), RabbitMQSettings("amqp://fake", "events"),
        experiment_repository=ExperimentRepository(":memory:"),
    )
    consumer._experiment_repository = None
    event = LibraryEvent.from_mapping({
        "event_id": "event-1", "event_type": "BOOK_ISSUED", "student_id": "s1",
        "book_id": "b1", "timestamp": "2026-01-01T00:00:00Z", "run_id": run_id,
    })
    message = FakeMessage(b"unused")

    claimed = await consumer._claim_run_event(event, message)

    assert claimed is False and message.rejects == [True]
    assert f"reason_code=reservation_repository_unavailable run_id={run_id} event_id=event-1" in caplog.text
    assert "requeue=true" in caplog.text


@pytest.mark.asyncio
async def test_draining_rejection_logs_only_identifiers_and_requeues(caplog):
    consumer = RabbitMQConsumer(FakeProcessor(), RabbitMQSettings("amqp://fake", "events"))
    consumer._draining = True
    run_id = str(uuid4())
    body = json.dumps({
        "run_id": run_id, "event_id": "draining-event", "private_value": "DO_NOT_LOG",
    }).encode()
    message = FakeMessage(body)

    await consumer.handle_message(message)

    assert message.acks == 0 and message.rejects == [True]
    assert f"reason_code=consumer_draining run_id={run_id} event_id=draining-event" in caplog.text
    assert "queue=events" in caplog.text and "requeue=true" in caplog.text
    assert "DO_NOT_LOG" not in caplog.text
    assert body.decode() not in caplog.text


@pytest.mark.asyncio
async def test_consumer_rejects_processing_failure_without_ack_or_requeue():
    consumer = RabbitMQConsumer(FakeProcessor(fail=True), RabbitMQSettings("amqp://fake", "events"))
    message = FakeMessage(json.dumps({
        "event_id": "e1", "event_type": "BOOK_ISSUED", "student_id": "s1",
        "book_id": "b1", "timestamp": "2026-01-01T00:00:00Z", "value": 1,
    }).encode())
    await consumer.handle_message(message)
    assert message.acks == 0
    assert message.rejects == [False]
    assert consumer.processed_since_start == 0
    assert consumer.processing_failures_since_start == 1


class FakeRedis:
    def __init__(self, fail=False):
        self.data = {}
        self.expirations = {}
        self.fail = fail
        self.pipeline_batches = []
        self.before_pipeline_execute = None

    async def set(self, key, value, ex=None, nx=False):
        if self.fail:
            raise RuntimeError("redis transport error")
        if nx and key in self.data:
            return None
        self.data[key] = value
        if ex is not None:
            self.expirations[key] = ex
        else:
            self.expirations.pop(key, None)
        return True

    async def get(self, key):
        if self.fail:
            raise RuntimeError("redis transport error")
        return self.data.get(key)

    async def exists(self, key):
        if self.fail:
            raise RuntimeError("redis transport error")
        return int(key in self.data)

    async def delete(self, key):
        if self.fail:
            raise RuntimeError("redis transport error")
        existed = key in self.data
        self.data.pop(key, None)
        self.expirations.pop(key, None)
        return int(existed)

    async def eval(self, script, number_of_keys, *arguments):
        key, instance_id, *extra = arguments
        if "EXPIRE" in script:
            if self.data.get(key) == instance_id:
                self.expirations[key] = int(extra[0])
                return 1
            return 0
        if "DEL" in script:
            if self.data.get(key) == instance_id:
                await self.delete(key)
                return 1
            return 0
        raise AssertionError("Unexpected Lua script")

    async def scan_iter(self, *, match):
        for key in list(self.data):
            if fnmatch.fnmatch(key, match):
                yield key

    def pipeline(self, transaction=True):
        return FakeRedisPipeline(self, transaction)

    def expire_now(self, key):
        self.data.pop(key, None)
        self.expirations.pop(key, None)

    async def aclose(self):
        pass


class ConsumerRedisAdapter:
    """Consumer-facing subset that shares Redis keys but keeps instance identity local."""

    def __init__(self, store):
        self.store = store
        self.consumer_instance_id = str(uuid4())

    @property
    def aggregation_write_status(self):
        return self.store.aggregation_write_status

    @property
    def last_successful_aggregation_write_at(self):
        return self.store.last_successful_aggregation_write_at

    async def record_active_consumer_delivery(self, instance_id):
        await self.store.record_active_consumer_delivery(instance_id)

    async def write_monitoring_state(self, snapshot):
        await self.store.write_monitoring_state(snapshot)

    async def record_consumer_shutdown(self, snapshot):
        await self.store.record_consumer_shutdown(snapshot)

    async def save_metrics(self, window_type, metrics):
        self.store.consumer_instance_id = self.consumer_instance_id
        await self.store.save_metrics(window_type, metrics)

    async def save_run_metrics(self, run_id, window_type, metrics):
        self.store.consumer_instance_id = self.consumer_instance_id
        await self.store.save_run_metrics(run_id, window_type, metrics)


class FakeRedisPipeline:
    def __init__(self, client, transaction):
        self.client = client
        self.transaction = transaction
        self.commands = []
        self.watched_keys = []
        self.watched_values = {}
        self.in_multi = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def watch(self, *keys):
        if self.client.fail:
            raise RuntimeError("redis transport error")
        self.watched_keys.extend(keys)
        self.watched_values.update({key: self.client.data.get(key) for key in keys})

    async def get(self, key):
        return await self.client.get(key)

    async def unwatch(self):
        self.watched_keys = []

    def multi(self):
        self.in_multi = True

    def set(self, key, value, ex=None):
        self.commands.append(("set", key, value, ex))
        return self

    def delete(self, key):
        self.commands.append(("delete", key))
        return self

    async def execute(self):
        if self.client.fail:
            raise RuntimeError("redis transport error")
        assert self.transaction is True
        if self.client.before_pipeline_execute is not None:
            callback = self.client.before_pipeline_execute
            self.client.before_pipeline_execute = None
            callback()
        if any(self.client.data.get(key) != self.watched_values[key] for key in self.watched_keys):
            from redis.exceptions import WatchError
            raise WatchError()
        batch = list(self.commands)
        self.client.pipeline_batches.append(batch)
        for command in batch:
            if command[0] == "set":
                _, key, value, expiry = command
                self.client.data[key] = value
                if expiry is not None:
                    self.client.expirations[key] = expiry
                else:
                    self.client.expirations.pop(key, None)
            else:
                _, key = command
                self.client.data.pop(key, None)
                self.client.expirations.pop(key, None)


def make_monitoring_snapshot(**overrides):
    values = {
        "consumer_state": "running",
        "consumer_instance_id": str(uuid4()),
        "last_heartbeat_at": "2026-11-04T12:00:00+00:00",
        "processed_events_since_start": 12,
        "processing_failures_since_start": 1,
        "consumer_redis_status": "available",
        "last_successful_aggregation_write_at": "2026-11-04T11:59:50+00:00",
        "tumbling_window_size": 10,
        "tumbling_window_progress": 2,
        "sliding_window_size": 5,
        "sliding_window_progress": 5,
        "sliding_interval": "per_event",
        "last_completed_tumbling_aggregation_at": None,
    }
    values.update(overrides)
    return ConsumerMonitoringSnapshot(**values)


@pytest.mark.asyncio
async def test_redis_metrics_store_round_trip_and_keys():
    store = RedisMetricsStore(client=FakeRedis())
    metrics = Metrics(2, 5.0, 2.5, 2.0, 3.0)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.save_metrics("sliding", metrics)
    assert await store.get_metrics("sliding") == metrics
    assert await store.get_metrics("tumbling") is None
    assert store.key_for("tumbling") == "streampulse:metrics:tumbling"
    assert store.key_for("sliding") == "streampulse:metrics:sliding"


@pytest.mark.asyncio
async def test_aggregation_write_evidence_is_atomic_expiring_and_readable():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.save_metrics("tumbling", Metrics(2, 5.0, 2.5, 2.0, 3.0))

    evidence_key = store.CONSUMER_AGGREGATION_WRITE_EVIDENCE_KEY
    batch = client.pipeline_batches[-1]
    assert [command[1] for command in batch] == [store.key_for("tumbling"), evidence_key]
    assert client.expirations[evidence_key] > 0
    evidence = await store.read_consumer_aggregation_write_evidence()
    assert evidence["consumer_instance_id"] == store.consumer_instance_id
    assert evidence["written_at"]


@pytest.mark.asyncio
async def test_failed_aggregation_write_does_not_create_success_evidence():
    client = FakeRedis(fail=True)
    store = RedisMetricsStore(client=client)

    with pytest.raises(RedisStorageError):
        await store.save_metrics("tumbling", Metrics(2, 5.0, 2.5, 2.0, 3.0))

    assert store.CONSUMER_AGGREGATION_WRITE_EVIDENCE_KEY not in client.data


@pytest.mark.asyncio
async def test_aggregation_transaction_cannot_commit_after_active_owner_changes():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    next_owner = str(uuid4())
    client.before_pipeline_execute = lambda: client.data.__setitem__(
        store.ACTIVE_CONSUMER_KEY, next_owner,
    )

    with pytest.raises(RedisStorageError):
        await store.save_metrics("sliding", Metrics(1, 999.0, 999.0, 999.0, 999.0))

    assert store.key_for("sliding") not in client.data
    assert store.CONSUMER_AGGREGATION_WRITE_EVIDENCE_KEY not in client.data


@pytest.mark.asyncio
async def test_standby_heartbeat_cannot_replace_delivery_owner_or_aggregation_evidence(tmp_path):
    shared_redis = FakeRedis()
    api_store = RedisMetricsStore(client=shared_redis)
    active_store = ConsumerRedisAdapter(api_store)
    standby_store = ConsumerRedisAdapter(api_store)
    active = RabbitMQConsumer(
        LibraryEventProcessor(TumblingWindow(5), SlidingWindow(5), active_store),
        RabbitMQSettings("amqp://fake", "events"),
    )
    standby = RabbitMQConsumer(
        LibraryEventProcessor(TumblingWindow(5), SlidingWindow(5), standby_store),
        RabbitMQSettings("amqp://fake", "events"),
    )

    class PassivePublisher:
        settings = RabbitMQSettings("amqp://fake", "events")

        async def inspect_queue(self):
            return {"messages_ready": 0, "consumer_count": 2}

    async def process_event(consumer, event_id):
        body = json.dumps({
            "event_id": event_id,
            "event_type": "BOOK_ISSUED",
            "student_id": "student-test",
            "book_id": "book-test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }).encode()
        message = FakeMessage(body)
        await consumer.handle_message(message)
        assert message.acks == 1 and message.rejects == []

    await process_event(active, "active-event-1")
    await active._write_monitoring_snapshot()
    active_evidence = await api_store.read_consumer_aggregation_write_evidence()
    assert active.processed_since_start == 1
    assert active_evidence["consumer_instance_id"] == active._consumer_instance_id

    # A standby heartbeat is visible per-instance but cannot claim the active lease.
    await standby._write_monitoring_snapshot()
    assert await api_store.read_consumer_aggregation_write_evidence() == active_evidence

    with TestClient(create_app(
        publisher=PassivePublisher(), metrics_store=api_store,
        experiment_db_path=tmp_path / "monitoring-owner.sqlite3",
    )) as client:
        response = client.get("/api/monitoring").json()
        assert response["consumer"]["active_instance_id"] == active._consumer_instance_id
        assert response["consumer"]["processed_since_start"] == 1
        assert response["consumer"]["redis_status"] == "available"
        instance_states = {
            instance["consumer_instance_id"]: instance
            for instance in response["consumer"]["instances"]
        }
        assert instance_states[active._consumer_instance_id]["active"] is True
        assert instance_states[active._consumer_instance_id]["heartbeat_fresh"] is True
        assert instance_states[standby._consumer_instance_id]["active"] is False
        assert instance_states[standby._consumer_instance_id]["heartbeat_fresh"] is True

        await active_store.record_consumer_shutdown(active._make_monitoring_snapshot("stopped"))
        await standby._write_monitoring_snapshot()
        stopped = client.get("/api/monitoring").json()["consumer"]
        assert stopped["status"] == "not_verified"
        assert stopped["reason"] == "no_delivery_evidence"
        assert stopped["active_instance_id"] is None
        assert stopped["redis_status"] == "not_verified"
        assert stopped["last_successful_aggregation_write"] is None

        # The backup's first real delivery after failover establishes new ownership.
        await process_event(standby, "standby-event-after-promotion")
        await standby._write_monitoring_snapshot()
        promoted = client.get("/api/monitoring").json()["consumer"]
        assert promoted["active_instance_id"] == standby._consumer_instance_id
        assert promoted["processed_since_start"] == 1
        assert promoted["redis_status"] == "available"
        evidence = await api_store.read_consumer_aggregation_write_evidence()
        assert evidence["consumer_instance_id"] == standby._consumer_instance_id
        backup_summary = await api_store.get_metrics("sliding")
        with pytest.raises(RedisStorageError):
            await active_store.save_metrics(
                "sliding", Metrics(1, 999.0, 999.0, 999.0, 999.0),
            )
        assert await api_store.read_consumer_aggregation_write_evidence() == evidence
        assert await api_store.get_metrics("sliding") == backup_summary


@pytest.mark.asyncio
async def test_expired_active_consumer_snapshot_is_stale_and_aggregation_unverified(tmp_path):
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.write_monitoring_state(make_monitoring_snapshot(
        consumer_instance_id=store.consumer_instance_id,
    ))
    client.expire_now(store.instance_heartbeat_key(store.consumer_instance_id))

    class PassivePublisher:
        settings = RabbitMQSettings("amqp://fake", "events")

        async def inspect_queue(self):
            return {"messages_ready": 0, "consumer_count": 2}

    with TestClient(create_app(
        publisher=PassivePublisher(), metrics_store=store,
        experiment_db_path=tmp_path / "expired-monitor.sqlite3",
    )) as api:
        consumer = api.get("/api/monitoring").json()["consumer"]

    assert consumer["status"] == "stale"
    assert consumer["reason"] == "heartbeat_expired"
    assert consumer["active_instance_id"] is None
    assert consumer["redis_status"] == "not_verified"
    assert consumer["last_successful_aggregation_write"] is None


def test_api_and_consumer_share_the_configured_experiment_database_path(tmp_path, monkeypatch):
    database_path = tmp_path / "same-experiment-db.sqlite3"
    monkeypatch.setenv("STREAMPULSE_EXPERIMENT_DB_PATH", str(database_path))
    api_repository = create_app().state.experiment_store
    consumer_repository = ExperimentRepository()

    assert api_repository.database_path.resolve() == consumer_repository.database_path.resolve()


@pytest.mark.asyncio
async def test_run_summary_keys_are_isolated_from_global_and_other_runs():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    run_a, run_b = str(uuid4()), str(uuid4())
    global_metrics = Metrics(1, 2.0, 2.0, 2.0, 2.0)
    metrics_a = Metrics(2, 7.0, 3.5, 3.0, 4.0)
    metrics_b = Metrics(3, 18.0, 6.0, 5.0, 7.0)

    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.save_metrics("sliding", global_metrics)
    await store.save_run_metrics(run_a, "sliding", metrics_a)
    await store.save_run_metrics(run_b, "sliding", metrics_b)

    assert store.key_for("sliding") == "streampulse:metrics:sliding"
    assert store.run_key_for("sliding", run_a) == (
        f"streampulse:experiments:runs:{run_a}:metrics:sliding"
    )
    assert await store.get_metrics("sliding") == global_metrics
    assert await store.get_run_metrics(run_a, "sliding") == metrics_a
    assert await store.get_run_metrics(run_b, "sliding") == metrics_b
    assert await store.get_run_metrics(run_a, "tumbling") is None
    assert client.expirations == {
        store.CONSUMER_AGGREGATION_WRITE_EVIDENCE_KEY: CONSUMER_AGGREGATION_WRITE_EVIDENCE_TTL_SECONDS,
        store.ACTIVE_CONSUMER_KEY: MONITORING_HEARTBEAT_TTL_SECONDS,
    }
    with pytest.raises(ValueError, match="run_id must be a valid UUID"):
        store.run_key_for("sliding", "not-a-uuid")
    with pytest.raises(ValueError, match="run_id must be a valid UUID"):
        await store.get_run_metrics("not-a-uuid", "sliding")


@pytest.mark.asyncio
async def test_active_configuration_initializes_with_defaults_only_when_missing():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)

    initialized = await store.initialize_active_configuration()
    assert initialized == WindowConfiguration()
    assert await store.read_active_configuration() == WindowConfiguration()
    assert client.expirations == {}


@pytest.mark.asyncio
async def test_active_configuration_initialization_does_not_overwrite_existing_value():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    existing = WindowConfiguration(tumbling_size=8, sliding_size=10, sliding_interval=3)
    client.data[store.ACTIVE_CONFIGURATION_KEY] = store._serialize_configuration(existing)

    assert await store.initialize_active_configuration() == existing
    assert await store.read_active_configuration() == existing


@pytest.mark.asyncio
async def test_pending_configuration_round_trip_and_keys_are_separate():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    configuration = WindowConfiguration(tumbling_size=5, sliding_size=8, sliding_interval=7)

    await store.save_pending_configuration(configuration)

    assert await store.read_active_configuration() is None
    assert await store.read_pending_configuration() == configuration
    assert set(client.data) == {"streampulse:config:pending"}
    assert client.expirations == {}


@pytest.mark.asyncio
async def test_pending_configuration_promotion_is_atomic_and_preserves_active_key():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    active = WindowConfiguration()
    pending = WindowConfiguration(tumbling_size=8, sliding_size=10, sliding_interval=2)
    await store.initialize_active_configuration(active)
    await store.save_pending_configuration(pending)

    promoted = await store.promote_pending_configuration()

    assert promoted == pending
    assert await store.read_active_configuration() == pending
    assert await store.read_pending_configuration() is None
    promotion_batch = client.pipeline_batches[-1]
    assert [command[0] for command in promotion_batch] == ["set", "delete"]
    assert promotion_batch[0][1] == store.ACTIVE_CONFIGURATION_KEY
    assert promotion_batch[1][1] == store.PENDING_CONFIGURATION_KEY


@pytest.mark.asyncio
async def test_promoting_missing_pending_configuration_is_safe():
    store = RedisMetricsStore(client=FakeRedis())
    assert await store.promote_pending_configuration() is None


@pytest.mark.asyncio
async def test_clear_pending_configuration_reports_removed_state():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    await store.save_pending_configuration(WindowConfiguration())

    assert await store.clear_pending_configuration() is True
    assert await store.clear_pending_configuration() is False
    assert await store.read_pending_configuration() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [
    "not-json",
    "[]",
    '{"tumbling_size":10,"sliding_size":5}',
    '{"tumbling_size":7,"sliding_size":5,"sliding_interval":1}',
    '{"tumbling_size":true,"sliding_size":5,"sliding_interval":1}',
])
async def test_invalid_stored_window_configuration_has_typed_data_error(payload):
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    client.data[store.ACTIVE_CONFIGURATION_KEY] = payload

    with pytest.raises(RedisConfigurationDataError, match="Stored window configuration is invalid"):
        await store.read_active_configuration()


@pytest.mark.asyncio
async def test_invalid_pending_configuration_is_not_promoted_over_active_value():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    active = WindowConfiguration()
    await store.initialize_active_configuration(active)
    client.data[store.PENDING_CONFIGURATION_KEY] = '{"tumbling_size":7}'

    with pytest.raises(RedisConfigurationDataError):
        await store.promote_pending_configuration()

    assert await store.read_active_configuration() == active
    assert store.PENDING_CONFIGURATION_KEY in client.data


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read", "save", "initialize", "promote", "clear"])
async def test_window_configuration_redis_failures_are_sanitized(operation):
    store = RedisMetricsStore(client=FakeRedis(fail=True))
    with pytest.raises(RedisConfigurationError) as error:
        if operation == "read":
            await store.read_active_configuration()
        elif operation == "save":
            await store.save_pending_configuration(WindowConfiguration())
        elif operation == "initialize":
            await store.initialize_active_configuration()
        elif operation == "promote":
            await store.promote_pending_configuration()
        else:
            await store.clear_pending_configuration()
    assert "redis transport error" not in str(error.value)


@pytest.mark.asyncio
async def test_monitoring_snapshot_write_and_read_round_trip():
    store = RedisMetricsStore(client=FakeRedis())
    snapshot = make_monitoring_snapshot(consumer_instance_id=store.consumer_instance_id)

    await store.record_active_consumer_delivery(snapshot.consumer_instance_id)
    await store.write_monitoring_state(snapshot)

    assert await store.read_monitoring_state() == snapshot


@pytest.mark.asyncio
async def test_monitoring_write_atomically_updates_snapshot_and_ttl_heartbeat():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)

    await store.write_monitoring_state(
        make_monitoring_snapshot(consumer_instance_id=store.consumer_instance_id)
    )

    assert len(client.pipeline_batches) == 1
    batch = client.pipeline_batches[0]
    assert [command[0] for command in batch] == ["set", "set"]
    assert batch[0][1] == store.instance_state_key(store.consumer_instance_id)
    assert batch[1][1] == store.instance_heartbeat_key(store.consumer_instance_id)
    assert batch[1][3] == MONITORING_HEARTBEAT_TTL_SECONDS == 30
    assert client.expirations[store.instance_heartbeat_key(store.consumer_instance_id)] == 30
    assert client.expirations[store.instance_state_key(store.consumer_instance_id)] == 30


@pytest.mark.asyncio
async def test_monitoring_heartbeat_can_be_refreshed_with_30_second_ttl():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    snapshot = make_monitoring_snapshot(consumer_instance_id=store.consumer_instance_id)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.write_monitoring_state(snapshot)

    refreshed = await store.refresh_monitoring_heartbeat("2026-11-04T12:00:10Z")

    assert refreshed is True
    heartbeat_key = store.instance_heartbeat_key(store.consumer_instance_id)
    assert client.data[heartbeat_key] == "2026-11-04T12:00:10Z"
    assert client.expirations[heartbeat_key] == 30
    assert (await store.read_monitoring_state()).last_heartbeat_at == "2026-11-04T12:00:10Z"


@pytest.mark.asyncio
async def test_monitoring_heartbeat_refresh_does_not_create_missing_state():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)

    refreshed = await store.refresh_monitoring_heartbeat("2026-11-04T12:00:10Z")

    assert refreshed is False
    assert client.data == {}


@pytest.mark.asyncio
async def test_missing_monitoring_state_is_not_treated_as_stale_consumer():
    store = RedisMetricsStore(client=FakeRedis())

    assert await store.read_monitoring_state() is None
    assert await store.is_monitoring_heartbeat_fresh() is None


@pytest.mark.asyncio
async def test_expired_monitoring_heartbeat_with_snapshot_is_stale():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    snapshot = make_monitoring_snapshot(consumer_instance_id=store.consumer_instance_id)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.write_monitoring_state(snapshot)
    client.expire_now(store.instance_heartbeat_key(store.consumer_instance_id))

    assert await store.read_monitoring_state() is None
    assert await store.is_monitoring_heartbeat_fresh() is False


@pytest.mark.asyncio
async def test_monitoring_write_failure_is_distinct_and_does_not_leak_transport_details():
    client = FakeRedis(fail=True)
    store = RedisMetricsStore(client=client)

    with pytest.raises(RedisMonitoringError) as error:
        await store.write_monitoring_state(make_monitoring_snapshot())

    assert isinstance(error.value, RedisStorageError)
    assert "redis transport error" not in str(error.value)
    assert client.data == {}


@pytest.mark.asyncio
async def test_graceful_shutdown_persists_stopped_state_and_removes_heartbeat_atomically():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.write_monitoring_state(
        make_monitoring_snapshot(consumer_instance_id=store.consumer_instance_id)
    )
    stopped = make_monitoring_snapshot(
        consumer_state="stopped", consumer_instance_id=store.consumer_instance_id,
    )

    await store.record_consumer_shutdown(stopped)

    assert await store.read_monitoring_state() is None
    assert await store.is_monitoring_heartbeat_fresh() is None
    assert [command[0] for command in client.pipeline_batches[-1]] == ["set", "delete"]
    assert (await store.read_consumer_instances())[0]["consumer_state"] == "stopped"


@pytest.mark.asyncio
async def test_monitoring_keys_do_not_change_aggregation_summary_keys_or_values():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    tumbling = Metrics(10, 18.0, 1.8, 1.0, 3.0)
    sliding = Metrics(5, 9.0, 1.8, 1.0, 3.0)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.save_metrics("tumbling", tumbling)
    await store.save_metrics("sliding", sliding)

    await store.write_monitoring_state(
        make_monitoring_snapshot(consumer_instance_id=store.consumer_instance_id)
    )

    assert await store.get_metrics("tumbling") == tumbling
    assert await store.get_metrics("sliding") == sliding
    assert set(client.data) == {
        "streampulse:metrics:tumbling",
        "streampulse:metrics:sliding",
        "streampulse:monitoring:consumer:aggregation-write-evidence",
        store.ACTIVE_CONSUMER_KEY,
        store.instance_state_key(store.consumer_instance_id),
        store.instance_heartbeat_key(store.consumer_instance_id),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["save", "get"])
async def test_redis_errors_are_wrapped_safely(operation):
    store = RedisMetricsStore(client=FakeRedis(fail=True))
    with pytest.raises(RedisStorageError) as error:
        if operation == "save":
            await store.save_metrics("sliding", Metrics(1, 1.0, 1.0, 1.0, 1.0))
        else:
            await store.get_metrics("sliding")
    assert "redis transport error" not in str(error.value)


class RecordingStore:
    def __init__(self, *, fail_aggregation=False, fail_monitoring=False, fail_shutdown=False):
        self.saved = []
        self.aggregation_write_status = None
        self.last_successful_aggregation_write_at = None
        self.fail_aggregation = fail_aggregation
        self.fail_monitoring = fail_monitoring
        self.fail_shutdown = fail_shutdown
        self.monitoring_snapshots = []
        self.shutdown_snapshots = []
        self.monitoring_failures = 0
        self.shutdown_failures = 0

    async def save_metrics(self, window_type, metrics):
        if self.fail_aggregation:
            self.aggregation_write_status = "unavailable"
            raise RedisStorageError("Could not save metrics to Redis")
        self.saved.append((window_type, metrics))
        self.aggregation_write_status = "available"
        self.last_successful_aggregation_write_at = datetime.now(timezone.utc).isoformat()

    async def write_monitoring_state(self, snapshot):
        if self.fail_monitoring:
            self.monitoring_failures += 1
            raise RedisMonitoringError("Could not write consumer monitoring state")
        self.monitoring_snapshots.append(snapshot)

    async def record_consumer_shutdown(self, snapshot):
        if self.fail_shutdown:
            self.shutdown_failures += 1
            raise RedisMonitoringError("Could not record consumer shutdown state")
        self.shutdown_snapshots.append(snapshot)


def make_consumer_event_body(event_id):
    return json.dumps({
        "event_id": event_id,
        "event_type": "BOOK_ISSUED",
        "student_id": "student-1",
        "book_id": "book-1",
        "timestamp": "2026-01-01T00:00:00Z",
    }).encode()


def make_monitoring_consumer(store, *, tumbling_size=10, sliding_size=5, queue=None):
    processor = LibraryEventProcessor(
        TumblingWindow(tumbling_size),
        SlidingWindow(sliding_size),
        store,
    )
    channel = FakeChannel()
    if queue is not None:
        channel.declare_queue = queue

    async def connect(url):
        return FakeConnection(channel)

    consumer = RabbitMQConsumer(
        processor,
        RabbitMQSettings("amqp://fake", "events"),
        connect=connect,
    )
    return consumer, channel


@pytest.mark.asyncio
async def test_consumer_heartbeat_starts_only_after_successful_queue_consumption():
    store = RecordingStore()
    consumer, _ = make_monitoring_consumer(store)
    await consumer.start()

    assert consumer._heartbeat_task is not None
    await consumer.close()

    class FailingQueue:
        async def consume(self, callback):
            raise RuntimeError("broker consume setup failed")

    store = RecordingStore()
    async def declare_failing_queue(name, **kwargs):
        return FailingQueue()
    failed_consumer, _ = make_monitoring_consumer(store, queue=declare_failing_queue)
    with pytest.raises(RabbitMQError):
        await failed_consumer.start()
    assert failed_consumer._heartbeat_task is None
    assert store.monitoring_snapshots == []


@pytest.mark.asyncio
async def test_consumer_heartbeat_refreshes_at_configured_interval(monkeypatch):
    import app.messaging.rabbitmq as rabbitmq_module
    monkeypatch.setattr(rabbitmq_module, "CONSUMER_HEARTBEAT_INTERVAL_SECONDS", 0.02)
    store = RecordingStore()
    consumer, _ = make_monitoring_consumer(store)
    await consumer.start()
    deadline = asyncio.get_running_loop().time() + 0.5
    while len(store.monitoring_snapshots) < 3 and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    count_before_close = len(store.monitoring_snapshots)

    await consumer.close()
    await asyncio.sleep(0.03)

    assert count_before_close >= 3
    assert len(store.monitoring_snapshots) == count_before_close
    assert all(snapshot.consumer_state == "running" for snapshot in store.monitoring_snapshots)
    assert all(snapshot.last_heartbeat_at is not None for snapshot in store.monitoring_snapshots)


@pytest.mark.asyncio
async def test_consumer_starts_only_one_heartbeat_task(monkeypatch):
    import app.messaging.rabbitmq as rabbitmq_module
    monkeypatch.setattr(rabbitmq_module, "CONSUMER_HEARTBEAT_INTERVAL_SECONDS", 0.02)
    store = RecordingStore()
    consumer, _ = make_monitoring_consumer(store)
    await consumer.start()
    original_task = consumer._heartbeat_task
    consumer._start_heartbeat()
    await asyncio.sleep(0.005)

    assert consumer._heartbeat_task is original_task
    assert len(store.monitoring_snapshots) == 1
    await consumer.close()


@pytest.mark.asyncio
async def test_consumer_monitoring_snapshot_captures_progress_and_completed_tumbling_time():
    store = RecordingStore()
    consumer, _ = make_monitoring_consumer(store, tumbling_size=2, sliding_size=3)

    first = FakeMessage(make_consumer_event_body("e1"))
    second = FakeMessage(make_consumer_event_body("e2"))
    await consumer.handle_message(first)
    snapshot = consumer._make_monitoring_snapshot("running")
    assert snapshot.tumbling_window_size == 2
    assert snapshot.tumbling_window_progress == 1
    assert snapshot.sliding_window_size == 3
    assert snapshot.sliding_window_progress == 1
    assert snapshot.sliding_interval == "per_event"
    assert snapshot.last_completed_tumbling_aggregation_at is None

    await consumer.handle_message(second)
    completed = consumer._make_monitoring_snapshot("running")
    assert consumer.processed_since_start == 2
    assert completed.tumbling_window_progress == 0
    assert completed.sliding_window_progress == 2
    assert datetime.fromisoformat(completed.last_completed_tumbling_aggregation_at).utcoffset() == timezone.utc.utcoffset(None)


@pytest.mark.asyncio
async def test_aggregation_write_failure_is_counted_as_processing_failure():
    store = RecordingStore(fail_aggregation=True)
    consumer, _ = make_monitoring_consumer(store)
    message = FakeMessage(make_consumer_event_body("e1"))

    await consumer.handle_message(message)

    assert message.acks == 0
    assert message.rejects == [False]
    assert consumer.processed_since_start == 0
    assert consumer.processing_failures_since_start == 1
    assert store.aggregation_write_status == "unavailable"


@pytest.mark.asyncio
async def test_aggregation_write_status_refreshes_running_snapshot_and_recovers():
    store = RecordingStore(fail_aggregation=True)
    consumer, _ = make_monitoring_consumer(store, sliding_size=2)
    await consumer.start()
    await asyncio.sleep(0)

    failed_message = FakeMessage(make_consumer_event_body("redis-write-failed"))
    await consumer.handle_message(failed_message)
    assert failed_message.rejects == [False]
    for _ in range(10):
        if store.monitoring_snapshots and store.monitoring_snapshots[-1].consumer_redis_status == "unavailable":
            break
        await asyncio.sleep(0)
    assert store.monitoring_snapshots[-1].consumer_redis_status == "unavailable"

    store.fail_aggregation = False
    recovered_message = FakeMessage(make_consumer_event_body("redis-write-recovered"))
    await consumer.handle_message(recovered_message)
    assert recovered_message.acks == 1
    for _ in range(10):
        if store.monitoring_snapshots[-1].consumer_redis_status == "available":
            break
        await asyncio.sleep(0)
    assert store.monitoring_snapshots[-1].consumer_redis_status == "available"
    assert store.monitoring_snapshots[-1].last_successful_aggregation_write_at is not None
    await consumer.close()


@pytest.mark.asyncio
async def test_monitoring_redis_failure_does_not_interrupt_processing_or_change_ack_behavior(monkeypatch):
    import app.messaging.rabbitmq as rabbitmq_module
    monkeypatch.setattr(rabbitmq_module, "CONSUMER_HEARTBEAT_INTERVAL_SECONDS", 60)
    store = RecordingStore(fail_monitoring=True)
    consumer, _ = make_monitoring_consumer(store)
    await consumer.start()
    await asyncio.sleep(0)
    message = FakeMessage(make_consumer_event_body("e1"))

    await consumer.handle_message(message)
    await consumer.close()

    assert message.acks == 1
    assert message.rejects == []
    assert consumer.processed_since_start == 1
    assert consumer.processing_failures_since_start == 0
    assert consumer.monitoring_failures_since_start >= 1
    assert len(store.saved) == 1


@pytest.mark.asyncio
async def test_graceful_consumer_shutdown_records_stopped_state(monkeypatch):
    import app.messaging.rabbitmq as rabbitmq_module
    monkeypatch.setattr(rabbitmq_module, "CONSUMER_HEARTBEAT_INTERVAL_SECONDS", 60)
    store = RecordingStore()
    consumer, _ = make_monitoring_consumer(store)
    await consumer.start()
    await asyncio.sleep(0)
    connection = consumer._connection

    await consumer.close()

    assert consumer._heartbeat_task is None
    assert len(store.shutdown_snapshots) == 1
    assert store.shutdown_snapshots[0].consumer_state == "stopped"
    assert connection.closed is True


@pytest.mark.asyncio
async def test_shutdown_monitoring_failure_does_not_prevent_resource_cleanup(monkeypatch):
    import app.messaging.rabbitmq as rabbitmq_module
    monkeypatch.setattr(rabbitmq_module, "CONSUMER_HEARTBEAT_INTERVAL_SECONDS", 60)
    store = RecordingStore(fail_shutdown=True)
    consumer, _ = make_monitoring_consumer(store)
    await consumer.start()
    await asyncio.sleep(0)
    connection = consumer._connection

    await consumer.close()

    assert store.shutdown_failures == 1
    assert consumer.monitoring_failures_since_start >= 1
    assert connection.closed is True


@pytest.mark.asyncio
async def test_consumer_handler_count_releases_on_success_failure_and_cancellation():
    started = asyncio.Event()
    release = asyncio.Event()

    class BlockingProcessor:
        async def process(self, event):
            started.set()
            await release.wait()
            return {}

    consumer = RabbitMQConsumer(
        BlockingProcessor(), RabbitMQSettings("amqp://fake", "events"),
    )
    message = FakeMessage(json.dumps({
        "event_id": "tracked", "event_type": "BOOK_ISSUED", "student_id": "s",
        "book_id": "b", "timestamp": "2026-01-01T00:00:00Z",
    }).encode())
    task = asyncio.create_task(consumer.handle_message(message))
    await started.wait()
    assert consumer.active_handler_count == 1
    release.set()
    await task
    assert consumer.active_handler_count == 0
    assert message.acks == 1

    failed_consumer = RabbitMQConsumer(
        FakeProcessor(fail=True), RabbitMQSettings("amqp://fake", "events"),
    )
    failed_message = FakeMessage(message.body)
    await failed_consumer.handle_message(failed_message)
    assert failed_consumer.active_handler_count == 0
    assert failed_message.rejects == [False]

    started = asyncio.Event()
    release = asyncio.Event()
    cancelled_consumer = RabbitMQConsumer(
        BlockingProcessor(), RabbitMQSettings("amqp://fake", "events"),
    )
    cancelled_message = FakeMessage(message.body)
    cancelled_task = asyncio.create_task(cancelled_consumer.handle_message(cancelled_message))
    await started.wait()
    assert cancelled_consumer.active_handler_count == 1
    cancelled_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_task
    assert cancelled_consumer.active_handler_count == 0
    assert cancelled_message.acks == 0


@pytest.mark.asyncio
async def test_consumer_drain_cancels_subscription_then_waits_for_ack(tmp_path):
    import json

    from app.messaging.rabbitmq import ConsumerDrainResult

    order = []
    started = asyncio.Event()
    release = asyncio.Event()

    class BlockingProcessor:
        async def process(self, event):
            order.append("process")
            started.set()
            await release.wait()
            return {}

    queue = FakeQueue()
    queue.cancelled = asyncio.Event()
    queue.order = order
    original_cancel = queue.cancel

    async def cancel(tag, timeout=None):
        order.append("cancel_subscription")
        await original_cancel(tag, timeout=timeout)

    queue.cancel = cancel
    channel = FakeChannel(queue=queue)
    channel.order = order
    connection = FakeConnection(channel)
    connection.order = order
    consumer = RabbitMQConsumer(
        BlockingProcessor(), RabbitMQSettings("amqp://fake", "events"),
        connect=lambda _url: asyncio.sleep(0, result=connection),
        drain_timeout_seconds=1,
    )
    await consumer.start()

    class OrderedMessage(FakeMessage):
        async def ack(self):
            order.append("ack")
            await super().ack()

    message = OrderedMessage(json.dumps({
        "event_id": "drain", "event_type": "BOOK_RETURNED", "student_id": "s",
        "book_id": "b", "timestamp": "2026-01-01T00:00:00Z",
    }).encode())
    handler = asyncio.create_task(queue.callback(message))
    await started.wait()
    drain_task = asyncio.create_task(consumer.drain())
    await queue.cancelled.wait()
    assert channel.closed is False
    assert consumer.active_handler_count == 1
    assert order[:2] == ["process", "cancel_subscription"]

    release.set()
    await handler
    result = await drain_task

    assert result == ConsumerDrainResult(True, False, 0)
    assert order.index("ack") < order.index("channel_close") < order.index("connection_close")
    assert queue.cancel_calls == ["fake-tag"]
    assert message.acks == 1
    assert consumer.active_handler_count == 0
    assert await consumer.close() == result


@pytest.mark.asyncio
async def test_consumer_drain_timeout_closes_channel_without_ack_and_is_idempotent():
    import json

    started = asyncio.Event()
    never_release = asyncio.Event()

    class BlockingProcessor:
        async def process(self, event):
            started.set()
            await never_release.wait()

    queue = FakeQueue()
    channel = FakeChannel(queue=queue)
    connection = FakeConnection(channel)
    consumer = RabbitMQConsumer(
        BlockingProcessor(), RabbitMQSettings("amqp://fake", "events"),
        connect=lambda _url: asyncio.sleep(0, result=connection),
        drain_timeout_seconds=0.01,
    )
    await consumer.start()
    message = FakeMessage(json.dumps({
        "event_id": "timeout", "event_type": "BOOK_RESERVED", "student_id": "s",
        "book_id": "b", "timestamp": "2026-01-01T00:00:00Z",
    }).encode())
    handler = asyncio.create_task(queue.callback(message))
    await started.wait()

    result = await consumer.close()
    with pytest.raises(asyncio.CancelledError):
        await handler

    assert result.completed is False
    assert result.timed_out is True
    assert result.subscription_cancelled is True
    assert result.handlers_drained is False
    assert result.active_handlers_remaining == 0
    assert channel.closed and connection.closed
    assert message.acks == 0
    assert message.rejects == []
    assert queue.cancel_calls == ["fake-tag"]
    assert await consumer.drain() == result


@pytest.mark.asyncio
async def test_subscription_cancel_timeout_is_bounded_and_closes_transport():
    import json

    cancel_started = asyncio.Event()
    never_cancel_complete = asyncio.Event()
    handler_started = asyncio.Event()
    never_finish_handler = asyncio.Event()

    class BlockingProcessor:
        async def process(self, event):
            handler_started.set()
            await never_finish_handler.wait()

    queue = FakeQueue()

    async def cancel_without_cancel_ok(tag, timeout=None):
        queue.cancel_calls.append(tag)
        queue.cancel_timeouts.append(timeout)
        cancel_started.set()
        await never_cancel_complete.wait()

    queue.cancel = cancel_without_cancel_ok
    channel = FakeChannel(queue=queue)
    channel_close_started = asyncio.Event()

    async def channel_close_without_close_ok():
        channel_close_started.set()
        await asyncio.Future()

    channel.close = channel_close_without_close_ok
    connection = FakeConnection(channel)
    consumer = RabbitMQConsumer(
        BlockingProcessor(), RabbitMQSettings("amqp://fake", "events"),
        connect=lambda _url: asyncio.sleep(0, result=connection),
        drain_timeout_seconds=0.05,
    )
    await consumer.start()
    message = FakeMessage(json.dumps({
        "event_id": "cancel-timeout", "event_type": "BOOK_RESERVED", "student_id": "s",
        "book_id": "b", "timestamp": "2026-01-01T00:00:00Z",
    }).encode())
    handler = asyncio.create_task(queue.callback(message))
    await handler_started.wait()

    async with asyncio.timeout(1):
        result = await consumer.close()
    with pytest.raises(asyncio.CancelledError):
        await handler

    assert cancel_started.is_set()
    assert 0 < queue.cancel_timeouts[0] <= consumer.drain_timeout_seconds
    assert result.completed is False
    assert result.timed_out is True
    assert result.subscription_cancelled is False
    assert result.handlers_drained is False
    assert result.active_handlers_remaining == 0
    assert not channel_close_started.is_set()
    assert connection.closed and connection.transport_closed
    assert connection.close_ack_received is False
    assert message.acks == 0
    assert message.rejects == []


@pytest.mark.asyncio
async def test_cancel_subscription_uses_shared_deadline_for_handler_drain():
    import json

    loop = asyncio.get_running_loop()
    cancel_started = asyncio.Event()
    allow_cancel_ok = asyncio.Event()
    handler_started = asyncio.Event()
    handler_release = asyncio.Event()

    class BlockingProcessor:
        async def process(self, event):
            handler_started.set()
            await handler_release.wait()
            return {}

    queue = FakeQueue()

    async def delayed_cancel_ok(tag, timeout=None):
        queue.cancel_calls.append(tag)
        queue.cancel_timeouts.append(timeout)
        cancel_started.set()
        await allow_cancel_ok.wait()

    queue.cancel = delayed_cancel_ok
    channel = FakeChannel(queue=queue)
    connection = FakeConnection(channel)
    consumer = RabbitMQConsumer(
        BlockingProcessor(), RabbitMQSettings("amqp://fake", "events"),
        connect=lambda _url: asyncio.sleep(0, result=connection),
        drain_timeout_seconds=0.5,
    )
    await consumer.start()
    message = FakeMessage(json.dumps({
        "event_id": "shared-deadline", "event_type": "BOOK_ISSUED", "student_id": "s",
        "book_id": "b", "timestamp": "2026-01-01T00:00:00Z",
    }).encode())
    handler = asyncio.create_task(queue.callback(message))
    await handler_started.wait()

    drain_task = asyncio.create_task(consumer.drain())
    await cancel_started.wait()
    loop.call_later(0.05, allow_cancel_ok.set)
    loop.call_later(0.2, handler_release.set)
    result = await drain_task
    await handler

    assert result.completed is True
    assert result.timed_out is False
    assert result.subscription_cancelled is True
    assert result.handlers_drained is True
    assert queue.cancel_timeouts[0] <= consumer.drain_timeout_seconds
    assert message.acks == 1
    assert channel.closed and connection.closed


@pytest.mark.asyncio
async def test_delivery_dispatched_during_subscription_cancel_is_requeued_without_processing():
    import json

    processor = FakeProcessor()
    queue = FakeQueue()
    late_message = FakeMessage(json.dumps({
        "event_id": "late", "event_type": "BOOK_ISSUED", "student_id": "s",
        "book_id": "b", "timestamp": "2026-01-01T00:00:00Z",
    }).encode())
    channel = FakeChannel(queue=queue)
    connection = FakeConnection(channel)
    consumer = RabbitMQConsumer(
        processor, RabbitMQSettings("amqp://fake", "events"),
        connect=lambda _url: asyncio.sleep(0, result=connection),
    )
    await consumer.start()
    original_cancel = queue.cancel

    async def cancel_with_inflight_delivery(tag, timeout=None):
        await original_cancel(tag, timeout=timeout)
        queue.late_task = asyncio.create_task(queue.callback(late_message))

    queue.cancel = cancel_with_inflight_delivery
    result = await consumer.drain()
    await queue.late_task

    assert result.completed is True
    assert processor.events == []
    assert late_message.acks == 0
    assert late_message.rejects == [True]
    assert consumer.active_handler_count == 0


@pytest.mark.asyncio
async def test_processor_preserves_tumbling_and_sliding_semantics():
    store = RecordingStore()
    processor = LibraryEventProcessor(TumblingWindow(2), SlidingWindow(2), store)
    first = await processor.process(make_event(2, "e1"))
    assert set(first) == {"sliding"}
    assert [name for name, _ in store.saved] == ["sliding"]

    second = await processor.process(make_event(4, "e2"))
    assert set(second) == {"tumbling", "sliding"}
    assert [name for name, _ in store.saved] == ["sliding", "tumbling", "sliding"]
    assert second["tumbling"] == Metrics(2, 6.0, 3.0, 2.0, 4.0)
    assert second["sliding"] == Metrics(2, 6.0, 3.0, 2.0, 4.0)


@pytest.mark.asyncio
async def test_run_processor_uses_immutable_snapshot_and_ignores_global_pending_configuration():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    dashboard_active = WindowConfiguration(5, 5, 1)
    dashboard_pending = WindowConfiguration(5, 5, 2)
    snapshot = WindowConfiguration(8, 10, 2)
    run_id = str(uuid4())
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.initialize_active_configuration(dashboard_active)
    await store.save_pending_configuration(dashboard_pending)
    processor = LibraryEventProcessor(
        TumblingWindow(snapshot.tumbling_size),
        SlidingWindow(snapshot.sliding_size, snapshot.sliding_interval),
        store,
        snapshot,
        run_id=run_id,
    )

    for value in range(1, 9):
        await processor.process(make_event(value, f"run-{value}", run_id=run_id))

    assert processor.active_configuration == snapshot
    assert processor.tumbling.size == 8 and processor.tumbling.pending_count == 0
    assert processor.sliding.size == 10 and processor.sliding.interval == 2
    assert await store.read_active_configuration() == dashboard_active
    assert await store.read_pending_configuration() == dashboard_pending
    assert await store.get_run_metrics(run_id, "tumbling") == Metrics(8, 36.0, 4.5, 1.0, 8.0)
    assert await store.get_run_metrics(run_id, "sliding") == Metrics(8, 36.0, 4.5, 1.0, 8.0)
    assert await store.get_metrics("tumbling") is None
    assert await store.get_metrics("sliding") is None


@pytest.mark.asyncio
async def test_simulation_processor_windows_and_summaries_are_isolated_from_other_scopes():
    class SimulationStore(RedisMetricsStore):
        async def record_consumer_delivery(self, **_kwargs):
            return True

        async def record_simulation_activity(self, *_args):
            return None

    store = SimulationStore(client=FakeRedis())
    configuration = WindowConfiguration(5, 5, 1)
    simulation_one, simulation_two = str(uuid4()), str(uuid4())
    await store.save_simulation_configuration(simulation_one, configuration)
    await store.save_simulation_configuration(simulation_two, configuration)
    consumer = RabbitMQConsumer(
        LibraryEventProcessor(TumblingWindow(10), SlidingWindow(5), store),
        RabbitMQSettings("amqp://fake", "events"),
    )

    async def deliver(simulation_id, event_id):
        timestamp = datetime.now(timezone.utc).isoformat()
        message = FakeMessage(json.dumps({
            "event_id": event_id, "event_type": "BOOK_ISSUED", "student_id": "s",
            "book_id": "b", "timestamp": timestamp, "simulation_id": simulation_id,
            "published_at": timestamp,
        }).encode())
        await consumer.handle_message(message)
        assert message.acks == 1
        assert message.rejects == []

    for index in range(5):
        await deliver(simulation_one, f"one-{index}")
    for index in range(2):
        await deliver(simulation_two, f"two-{index}")

    assert (await store.get_simulation_metrics(simulation_one, "tumbling")).count == 5
    assert (await store.get_simulation_metrics(simulation_one, "sliding")).count == 5
    assert await store.get_simulation_metrics(simulation_two, "tumbling") is None
    assert (await store.get_simulation_metrics(simulation_two, "sliding")).count == 2
    assert await store.get_metrics("tumbling") is None
    assert await store.get_metrics("sliding") is None


@pytest.mark.asyncio
async def test_simulation_engine_consumer_and_status_share_simulation_scoped_summaries():
    class SimulationTestStore(RedisMetricsStore):
        def __init__(self, client):
            super().__init__(client=client)
            self.delivery_counts = {}
            self.activities = {}
            self.fail_simulation_writes = False

        async def save_metrics_batch(self, summaries, *, run_id=None, simulation_id=None):
            if simulation_id is not None and self.fail_simulation_writes:
                raise RedisStorageError("simulated summary write failure")
            await super().save_metrics_batch(
                summaries, run_id=run_id, simulation_id=simulation_id,
            )

        async def record_consumer_delivery(self, *, simulation_id=None, succeeded, **_kwargs):
            if simulation_id is not None:
                counts = self.delivery_counts.setdefault(simulation_id, {"attempts": 0, "successes": 0})
                counts["attempts"] += 1
                counts["successes"] += int(succeeded)
            return bool(succeeded)

        async def get_measurement_summary(self, run_id=None, simulation_id=None):
            counts = self.delivery_counts.get(simulation_id, {})
            attempts = counts.get("attempts", 0)
            successes = counts.get("successes", 0)
            return MeasurementSummary(
                publish_attempts=0, publish_successes=0, publish_failures=0,
                publish_failure_rate=None, consumer_attempts=attempts,
                consumer_successes=successes, consumer_failures=attempts - successes,
                consumer_failure_rate=(attempts - successes) / attempts if attempts else None,
                throughput_events_per_second=None, sample_count={},
                average_latency_ms={"consumer_processing_latency_ms": 2.0},
                p95_latency_ms={},
                consumer_unique_successes=successes, measurements_present=bool(attempts),
                minimum_latency_ms={"consumer_processing_latency_ms": 1.0},
                maximum_latency_ms={"consumer_processing_latency_ms": 3.0},
            )

        async def record_simulation_activity(
            self, simulation_id, event_id, event_time, published_at, processed_at,
            status=None, processing_error=None,
        ):
            item = {
                "event_id": event_id, "event_time": event_time,
                "published_at": published_at, "processed_at": processed_at,
                "status": status or ("processed" if processed_at else "processing_failed"),
            }
            if processing_error is not None:
                item["processing_error"] = processing_error
            self.activities.setdefault(simulation_id, []).append(item)

        async def get_simulation_activity(self, simulation_id):
            return list(self.activities.get(simulation_id, []))

    store = SimulationTestStore(FakeRedis())
    configuration = WindowConfiguration(5, 5, 1)
    await store.initialize_active_configuration(configuration)
    consumer = RabbitMQConsumer(
        LibraryEventProcessor(TumblingWindow(10), SlidingWindow(5), store),
        RabbitMQSettings("amqp://fake", "events"),
    )

    async def publish(event):
        assert event.simulation_id is not None
        message = FakeMessage(_event_json(event))
        await consumer.handle_message(message)
        assert (message.acks == 1 and message.rejects == []) or message.rejects == [False]
        return EventPublishResult("confirmed")

    engine = SimulationEngine(store, publish)
    empty = await engine.status()
    assert empty == {
        "simulation": None, "queue": None, "consumer": None,
        "measurements": None, "aggregations": None,
    }

    await engine.start(SimulationStart(
        flow_mode="bursty", burst_size=5, burst_interval_ms=60_000,
    ))
    for _ in range(500):
        if engine._session["publish_confirmed_count"] >= 5:
            break
        await asyncio.sleep(0.002)
    assert engine._session["publish_confirmed_count"] == 5
    simulation_id = engine._session["simulation_id"]
    await engine.stop()
    await engine._task

    status = await engine.status()
    assert status["simulation"]["simulation_id"] == simulation_id
    assert status["measurements"]["processed"] == 5
    assert status["measurements"]["processing_failures"] == 0
    assert status["measurements"]["average_latency_ms"]["consumer_processing_latency_ms"] == 2.0
    assert status["measurements"]["minimum_latency_ms"]["consumer_processing_latency_ms"] == 1.0
    assert status["measurements"]["maximum_latency_ms"]["consumer_processing_latency_ms"] == 3.0
    assert status["aggregations"]["tumbling"]["count"] == 5
    assert status["aggregations"]["sliding"]["count"] == 5
    assert status["aggregations"]["tumbling"]["sum"] > 0
    assert status["aggregations"]["sliding"]["average"] > 0
    assert store.simulation_key_for("tumbling", simulation_id) in store._client.data
    assert store.simulation_key_for("sliding", simulation_id) in store._client.data
    activity = await engine.activity()
    assert activity["count"] == 5
    assert all(event.get("processed_at") for event in activity["events"])

    store.fail_simulation_writes = True
    await engine.start(SimulationStart(
        flow_mode="bursty", burst_size=1, burst_interval_ms=60_000,
    ))
    for _ in range(500):
        if engine._session["publish_confirmed_count"] >= 1:
            break
        await asyncio.sleep(0.002)
    assert engine._session["publish_confirmed_count"] == 1
    failed_simulation_id = engine._session["simulation_id"]
    await engine.stop()
    await engine._task

    failed_status = await engine.status()
    assert failed_status["measurements"]["processed"] == 0
    assert failed_status["measurements"]["processing_failures"] == 1
    assert failed_status["aggregations"]["tumbling"] is None
    assert failed_status["aggregations"]["sliding"] is None
    failed_activity = await engine.activity()
    failed_event = failed_activity["events"][0]
    assert failed_event["status"] == "processing_failed"
    assert failed_event["processed_at"] is None
    assert failed_event["processing_error"] == "RedisStorageError"


@pytest.mark.asyncio
async def test_distinct_run_processors_write_only_to_their_own_summary_namespaces():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    processors = {}
    for run_id in (str(uuid4()), str(uuid4())):
        configuration = WindowConfiguration(5, 5, 1)
        processors[run_id] = LibraryEventProcessor(
            TumblingWindow(configuration.tumbling_size),
            SlidingWindow(configuration.sliding_size, configuration.sliding_interval),
            store,
            configuration,
            run_id=run_id,
        )

    run_a, run_b = tuple(processors)
    for value in range(1, 6):
        await processors[run_a].process(make_event(value, f"a-{value}", run_id=run_a))
        await processors[run_b].process(make_event(value + 10, f"b-{value}", run_id=run_b))

    assert await store.get_run_metrics(run_a, "tumbling") == Metrics(5, 15.0, 3.0, 1.0, 5.0)
    assert await store.get_run_metrics(run_b, "tumbling") == Metrics(5, 65.0, 13.0, 11.0, 15.0)
    assert await store.get_run_metrics(run_a, "sliding") != await store.get_run_metrics(run_b, "sliding")
    assert await store.get_metrics("tumbling") is None
    assert await store.get_metrics("sliding") is None


@pytest.mark.asyncio
async def test_backup_restores_durable_simulation_windows_and_deduplicates_redelivery():
    client = FakeRedis()
    primary_store = RedisMetricsStore(client=client)
    backup_store = RedisMetricsStore(client=client)
    simulation_id = str(uuid4())
    configuration = WindowConfiguration(5, 5, 1)
    await primary_store.save_simulation_configuration(simulation_id, configuration)
    await primary_store.record_active_consumer_delivery(primary_store.consumer_instance_id)
    primary = LibraryEventProcessor(
        TumblingWindow(5), SlidingWindow(5), primary_store, configuration,
        simulation_id=simulation_id,
    )

    def event(index):
        return LibraryEvent(
            event_id=f"failover-{index}", event_type="BOOK_ISSUED",
            student_id="student", book_id="book",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc), value=float(index),
            simulation_id=simulation_id,
        )

    await primary.process(event(1))
    await primary.process(event(2))
    await backup_store.record_active_consumer_delivery(backup_store.consumer_instance_id)
    backup = LibraryEventProcessor(
        TumblingWindow(5), SlidingWindow(5), backup_store, configuration,
        simulation_id=simulation_id,
    )

    await backup.process(event(3))
    duplicate_result = await backup.process(event(3))
    await backup.process(event(4))
    final = await backup.process(event(5))

    assert duplicate_result == {}
    assert final["tumbling"] == Metrics(5, 15.0, 3.0, 1.0, 5.0)
    assert final["sliding"] == Metrics(5, 15.0, 3.0, 1.0, 5.0)
    assert await backup_store.get_simulation_metrics(simulation_id, "tumbling") == final["tumbling"]
    checkpoint = await backup_store.load_processing_checkpoint(simulation_id=simulation_id)
    assert checkpoint["version"] == 5
    assert checkpoint["tumbling"] == []
    assert len(checkpoint["sliding"]["events"]) == 5


@pytest.mark.asyncio
async def test_checkpoint_write_failure_keeps_window_state_uncommitted_and_recoverable():
    client = FakeRedis()
    store = RedisMetricsStore(client=client)
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    processor = LibraryEventProcessor(TumblingWindow(2), SlidingWindow(2), store)
    client.before_pipeline_execute = lambda: (_ for _ in ()).throw(RuntimeError("offline write"))

    with pytest.raises(RedisStorageError):
        await processor.process(make_event(1, "checkpoint-failure"))
    assert processor.tumbling.pending_count == 0
    assert processor.sliding.event_count == 0

    await processor.process(make_event(1, "checkpoint-failure"))
    assert processor.tumbling.pending_count == 1
    assert processor.sliding.event_count == 1
    checkpoint = await store.load_processing_checkpoint()
    assert checkpoint["version"] == 1


def test_run_processor_validates_uuid_configuration_and_window_match():
    store = RecordingStore()
    configuration = WindowConfiguration(5, 5, 1)
    with pytest.raises(ValueError, match="valid UUID"):
        LibraryEventProcessor(
            TumblingWindow(5), SlidingWindow(5), store, configuration, run_id="invalid"
        )
    with pytest.raises(ValueError, match="require an immutable"):
        LibraryEventProcessor(
            TumblingWindow(5), SlidingWindow(5), store, run_id=str(uuid4())
        )
    with pytest.raises(ValueError, match="must match its configuration"):
        LibraryEventProcessor(
            TumblingWindow(8), SlidingWindow(5), store, configuration, run_id=str(uuid4())
        )


@pytest.mark.asyncio
async def test_ten_and_five_event_windows_aggregate_actual_values_and_retain_tumbling_summary():
    store = RedisMetricsStore(client=FakeRedis())
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    processor = LibraryEventProcessor(TumblingWindow(10), SlidingWindow(5), store)

    values = [2, 4, 6, 8, 10, 12]
    for index, value in enumerate(values, start=1):
        await processor.process(make_event(value, f"e{index}"))
        sliding = await store.get_metrics("sliding")
        assert sliding is not None
        if index < 5:
            assert sliding == Metrics(index, float(sum(values[:index])), sum(values[:index]) / index,
                                      float(values[0]), float(value))
        elif index == 5:
            assert sliding == Metrics(5, 30.0, 6.0, 2.0, 10.0)
        else:
            assert sliding == Metrics(5, 40.0, 8.0, 4.0, 12.0)

    assert await store.get_metrics("tumbling") is None
    assert await store.get_metrics("sliding") == Metrics(5, 40.0, 8.0, 4.0, 12.0)

    for index, value in enumerate([14, 16, 18, 20], start=7):
        await processor.process(make_event(value, f"e{index}"))
        tumbling = await store.get_metrics("tumbling")
        if index < 10:
            assert tumbling is None
        else:
            assert tumbling == Metrics(10, 110.0, 11.0, 2.0, 20.0)
    completed = Metrics(10, 110.0, 11.0, 2.0, 20.0)

    await processor.process(make_event(22, "e11"))
    assert await store.get_metrics("tumbling") == completed


async def create_configured_processor(*, store=None, active=None, pending=None):
    store = store or RedisMetricsStore(client=FakeRedis())
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    active = active or WindowConfiguration(5, 5, 1)
    processor = LibraryEventProcessor(
        TumblingWindow(active.tumbling_size),
        SlidingWindow(active.sliding_size, active.sliding_interval),
        store,
        active,
    )
    await store.initialize_active_configuration(active)
    if pending is not None:
        await store.save_pending_configuration(pending)
    return processor, store


@pytest.mark.asyncio
async def test_pending_configuration_waits_for_old_tumbling_boundary_and_activates_after_persistence():
    old = WindowConfiguration(5, 5, 1)
    pending = WindowConfiguration(8, 10, 2)
    processor, store = await create_configured_processor(active=old, pending=pending)
    old_tumbling = processor.tumbling
    old_sliding = processor.sliding

    for index in range(1, 5):
        result = await processor.process(make_event(index, f"old-{index}"))
        assert "tumbling" not in result
        assert processor.tumbling is old_tumbling
        assert processor.sliding is old_sliding
        assert await store.read_active_configuration() == old
        assert await store.read_pending_configuration() == pending

    completed = await processor.process(make_event(5, "old-5"))

    assert completed["tumbling"].count == 5
    assert await store.get_metrics("tumbling") == completed["tumbling"]
    assert await store.read_active_configuration() == pending
    assert await store.read_pending_configuration() is None
    assert processor.active_configuration == pending
    assert processor.tumbling.size == 8
    assert processor.tumbling.pending_count == 0
    assert processor.sliding.size == 10
    assert processor.sliding.interval == 2
    assert processor.sliding.event_count == 0
    consumer = RabbitMQConsumer(processor, RabbitMQSettings("amqp://fake", "events"))
    snapshot = consumer._make_monitoring_snapshot("running")
    assert snapshot.tumbling_window_size == 8
    assert snapshot.sliding_window_size == 10
    assert snapshot.sliding_interval == 2


@pytest.mark.asyncio
async def test_pending_configuration_activates_at_simulation_tumbling_boundary_after_23_events():
    store = RedisMetricsStore(client=FakeRedis())
    old = WindowConfiguration(5, 5, 1)
    pending = WindowConfiguration(8, 5, 1)
    simulation_id = str(uuid4())
    await store.record_active_consumer_delivery(store.consumer_instance_id)
    await store.initialize_active_configuration(old)
    await store.save_pending_configuration(pending)
    processor = LibraryEventProcessor(
        TumblingWindow(old.tumbling_size),
        SlidingWindow(old.sliding_size, old.sliding_interval),
        store,
        old,
        simulation_id=simulation_id,
    )

    results = [
        await processor.process(make_event(index, f"simulation-{index}"))
        for index in range(1, 24)
    ]

    assert results[4]["tumbling"].count == 5
    assert results[12]["tumbling"].count == 8
    assert results[20]["tumbling"].count == 8
    assert await store.read_active_configuration() == pending
    assert await store.read_pending_configuration() is None
    assert processor.active_configuration == pending
    assert processor.tumbling.size == 8
    assert processor.tumbling.pending_count == 2
    assert (await store.get_simulation_metrics(simulation_id, "tumbling")).count == 8
    assert (await store.get_simulation_metrics(simulation_id, "sliding")).count == 5


@pytest.mark.asyncio
async def test_first_events_after_activation_use_new_sizes_and_sliding_interval():
    old = WindowConfiguration(5, 5, 1)
    pending = WindowConfiguration(8, 10, 2)
    processor, store = await create_configured_processor(active=old, pending=pending)

    for index in range(5):
        await processor.process(make_event(index, f"old-{index}"))

    first = await processor.process(make_event(10, "new-1"))
    assert first == {}
    assert processor.tumbling.pending_count == 1
    assert processor.sliding.event_count == 1
    assert processor.sliding.interval == 2
    assert (await store.get_metrics("sliding")).count == 5

    second = await processor.process(make_event(11, "new-2"))
    assert second["sliding"].count == 2
    assert processor.tumbling.pending_count == 2
    assert processor.sliding.event_count == 2


@pytest.mark.asyncio
async def test_no_pending_configuration_preserves_existing_window_instances_and_results():
    processor, _ = await create_configured_processor(active=WindowConfiguration(5, 5, 1))
    tumbling = processor.tumbling
    sliding = processor.sliding

    for index in range(5):
        await processor.process(make_event(index, f"event-{index}"))

    assert processor.tumbling is tumbling
    assert processor.sliding is sliding
    assert processor.tumbling.pending_count == 0
    assert processor.sliding.event_count == 5


@pytest.mark.asyncio
async def test_promotion_failure_keeps_windows_usable_and_message_is_acknowledged():
    class PromotionFailureStore(RedisMetricsStore):
        async def promote_pending_configuration(self):
            raise RedisConfigurationError("configuration store unavailable")

    old = WindowConfiguration(5, 5, 1)
    pending = WindowConfiguration(8, 10, 2)
    processor, store = await create_configured_processor(
        store=PromotionFailureStore(client=FakeRedis()), active=old, pending=pending
    )
    consumer = RabbitMQConsumer(processor, RabbitMQSettings("amqp://fake", "events"))
    tumbling = processor.tumbling
    sliding = processor.sliding

    for index in range(4):
        await consumer.handle_message(FakeMessage(make_consumer_event_body(f"event-{index}")))
    boundary_message = FakeMessage(make_consumer_event_body("event-boundary"))
    await consumer.handle_message(boundary_message)

    assert boundary_message.acks == 1
    assert boundary_message.rejects == []
    assert processor.tumbling is tumbling
    assert processor.sliding is sliding
    assert processor.active_configuration == old
    assert await store.read_active_configuration() == old
    assert await store.read_pending_configuration() == pending


@pytest.mark.asyncio
async def test_aggregation_persistence_failure_prevents_configuration_activation():
    class TumblingWriteFailureStore(RedisMetricsStore):
        async def save_metrics(self, window_type, metrics):
            if window_type == "tumbling":
                raise RedisStorageError("Could not save metrics to Redis")
            await super().save_metrics(window_type, metrics)

        async def promote_pending_configuration(self):
            raise AssertionError("pending config must not be promoted after storage failure")

    old = WindowConfiguration(5, 5, 1)
    pending = WindowConfiguration(8, 10, 2)
    processor, store = await create_configured_processor(
        store=TumblingWriteFailureStore(client=FakeRedis()), active=old, pending=pending
    )
    consumer = RabbitMQConsumer(processor, RabbitMQSettings("amqp://fake", "events"))

    for index in range(4):
        await consumer.handle_message(FakeMessage(make_consumer_event_body(f"event-{index}")))
    boundary_message = FakeMessage(make_consumer_event_body("event-boundary"))
    await consumer.handle_message(boundary_message)

    assert boundary_message.acks == 0
    assert boundary_message.rejects == [False]
    assert processor.active_configuration == old
    assert processor.tumbling.size == 5
    assert processor.sliding.size == 5
    assert await store.read_active_configuration() == old
    assert await store.read_pending_configuration() == pending


@pytest.mark.asyncio
async def test_api_redis_consumer_boundary_and_monitoring_lifecycle_end_to_end(monkeypatch):
    redis_client = FakeRedis()
    store = RedisMetricsStore(client=redis_client)
    channel = FakeChannel()

    async def connect(url):
        return FakeConnection(channel)

    import app.messaging.rabbitmq as rabbitmq_module
    monkeypatch.setattr(rabbitmq_module.aio_pika, "connect_robust", connect)
    monkeypatch.setattr(run_consumer, "RedisMetricsStore", lambda: store)
    monkeypatch.setenv("RABBITMQ_URL", "amqp://integration-test")
    monkeypatch.setenv("RABBITMQ_QUEUE", "integration-events")

    consumer, built_store = await run_consumer.build_consumer()
    assert built_store is store
    assert consumer.processor.active_configuration == WindowConfiguration()
    assert await store.read_active_configuration() == WindowConfiguration()
    await consumer.start()
    await consumer._write_monitoring_snapshot()

    class PassivePublisher:
        settings = RabbitMQSettings("amqp://fake", "integration-events")

        async def inspect_queue(self):
            return None

    first = {"tumbling_size": 5, "sliding_size": 8, "sliding_interval": 2}
    latest = {"tumbling_size": 8, "sliding_size": 10, "sliding_interval": 3}
    try:
        with TestClient(create_app(publisher=PassivePublisher(), metrics_store=store)) as client:
            first_put = client.put("/api/config", json=first)
            latest_put = client.put("/api/config", json=latest)
            before_boundary = client.get("/api/config")
            assert first_put.status_code == latest_put.status_code == 200
            assert before_boundary.json() == {
                "active": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
                "pending": latest,
                "status": "pending",
            }

            restarted, _ = await run_consumer.build_consumer()
            assert restarted.processor.active_configuration == WindowConfiguration()
            assert await store.read_pending_configuration() == WindowConfiguration(**latest)

            for index in range(9):
                message = FakeMessage(make_consumer_event_body(f"integration-{index}"))
                await channel.queue.callback(message)
                assert message.acks == 1
                assert message.rejects == []
            assert consumer.processor.tumbling.size == 10
            assert consumer.processor.tumbling.pending_count == 9
            assert await store.read_active_configuration() == WindowConfiguration()

            old_boundary_message = FakeMessage(make_consumer_event_body("integration-boundary"))
            await channel.queue.callback(old_boundary_message)
            assert old_boundary_message.acks == 1
            assert old_boundary_message.rejects == []
            assert (await store.get_metrics("tumbling")).count == 10
            assert await store.read_active_configuration() == WindowConfiguration(**latest)
            assert await store.read_pending_configuration() is None
            assert consumer.processor.tumbling.size == 8
            assert consumer.processor.sliding.size == 10
            assert consumer.processor.sliding.interval == 3
            assert consumer.processor.tumbling.pending_count == 0
            assert consumer.processor.sliding.event_count == 0

            for index in range(1, 3):
                next_message = FakeMessage(make_consumer_event_body(f"integration-next-{index}"))
                await channel.queue.callback(next_message)
                assert next_message.acks == 1
                assert consumer.processor.tumbling.pending_count == index
                assert consumer.processor.sliding.event_count == index
                assert (await store.get_metrics("sliding")).count == 5

            interval_message = FakeMessage(make_consumer_event_body("integration-next-3"))
            await channel.queue.callback(interval_message)
            assert interval_message.acks == 1
            assert consumer.processor.tumbling.pending_count == 3
            assert consumer.processor.sliding.event_count == 3
            assert (await store.get_metrics("sliding")).count == 3
            last_completed_at = consumer._last_completed_tumbling_at
            assert last_completed_at is not None

            await consumer._write_monitoring_snapshot()
            monitoring = client.get("/api/monitoring").json()
            assert monitoring["consumer"]["status"] == "running"
            assert monitoring["consumer"]["windows"] == {
                "tumbling": {
                    "size": 8, "progress": 3, "last_completed_at": last_completed_at,
                },
                "sliding": {"size": 10, "progress": 3, "interval": 3},
            }
            assert client.get("/api/config").json() == {
                "active": latest,
                "pending": None,
                "status": "active",
            }

            await consumer.close()
            stopped_monitoring = client.get("/api/monitoring").json()
            assert stopped_monitoring["consumer"]["status"] == "stopped"
            assert stopped_monitoring["consumer"]["active_instance_id"] is None
            assert stopped_monitoring["consumer"]["windows"]["tumbling"]["size"] is None
            assert stopped_monitoring["consumer"]["windows"]["sliding"]["size"] is None
    finally:
        if consumer._connection is not None:
            await consumer.close()


@pytest.mark.asyncio
async def test_consumer_startup_recovers_custom_active_config_without_applying_pending(monkeypatch):
    store = RedisMetricsStore(client=FakeRedis())
    active = WindowConfiguration(8, 10, 3)
    pending = WindowConfiguration(5, 8, 2)
    await store.initialize_active_configuration(active)
    await store.save_pending_configuration(pending)
    monkeypatch.setattr(run_consumer, "RedisMetricsStore", lambda: store)
    monkeypatch.setenv("RABBITMQ_URL", "amqp://integration-test")

    consumer, _ = await run_consumer.build_consumer()

    assert consumer.processor.active_configuration == active
    assert consumer.processor.tumbling.size == 8
    assert consumer.processor.sliding.size == 10
    assert consumer.processor.sliding.interval == 3
    assert await store.read_active_configuration() == active
    assert await store.read_pending_configuration() == pending

import json
from datetime import datetime, timezone

import pytest

from app.messaging.rabbitmq import RabbitMQConsumer, RabbitMQError, RabbitMQPublisher, RabbitMQSettings
from app.processing.event_processor import LibraryEventProcessor
from app.processing.metrics import Metrics
from app.processing.window_manager import SlidingWindow, TumblingWindow
from app.schemas import LibraryEvent
from app.storage.redis_metrics import RedisMetricsStore, RedisStorageError


def make_event(value=1, event_id="event-1"):
    return LibraryEvent(
        event_id=event_id,
        event_type="BOOK_ISSUED",
        student_id="student-1",
        book_id="book-1",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        value=value,
    )


class FakeExchange:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    async def publish(self, message, routing_key):
        if self.fail:
            raise RuntimeError("fake transport failure")
        self.calls.append((message, routing_key))


class FakeChannel:
    def __init__(self, exchange=None):
        self.default_exchange = exchange or FakeExchange()
        self.declared = []
        self.qos = []

    async def declare_queue(self, name, **kwargs):
        self.declared.append((name, kwargs))
        return FakeQueue()

    async def set_qos(self, **kwargs):
        self.qos.append(kwargs)


class FakeQueue:
    async def consume(self, callback):
        self.callback = callback
        return "fake-tag"


class FakeConnection:
    def __init__(self, channel):
        self._channel = channel
        self.closed = False

    async def channel(self):
        return self._channel

    async def close(self):
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
async def test_publisher_wraps_transport_error_without_exposing_details():
    channel = FakeChannel(FakeExchange(fail=True))

    async def connect(url):
        return FakeConnection(channel)

    publisher = RabbitMQPublisher(RabbitMQSettings("amqps://user:secret@host", "events"), connect=connect)
    with pytest.raises(RabbitMQError) as error:
        await publisher.publish(make_event())
    assert "secret" not in str(error.value)
    assert "Could not publish" in str(error.value)


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
async def test_consumer_starts_with_single_active_consumer_and_prefetch_one():
    channel = FakeChannel()

    async def connect(url):
        return FakeConnection(channel)

    consumer = RabbitMQConsumer(
        FakeProcessor(), RabbitMQSettings("amqp://fake", "events"), connect=connect
    )
    await consumer.start()
    assert channel.qos == [{"prefetch_count": 1}]
    assert channel.declared == [(
        "events", {"durable": True, "arguments": {"x-single-active-consumer": True}}
    )]
    await consumer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"not json", b"{}", b"\xff"])
async def test_consumer_rejects_malformed_messages_without_requeue(body):
    processor = FakeProcessor()
    consumer = RabbitMQConsumer(processor, RabbitMQSettings("amqp://fake", "events"))
    message = FakeMessage(body)
    await consumer.handle_message(message)
    assert message.acks == 0
    assert message.rejects == [False]
    assert processor.events == []


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


class FakeRedis:
    def __init__(self, fail=False):
        self.data = {}
        self.fail = fail

    async def set(self, key, value):
        if self.fail:
            raise RuntimeError("redis transport error")
        self.data[key] = value

    async def get(self, key):
        if self.fail:
            raise RuntimeError("redis transport error")
        return self.data.get(key)

    async def aclose(self):
        pass


@pytest.mark.asyncio
async def test_redis_metrics_store_round_trip_and_keys():
    store = RedisMetricsStore(client=FakeRedis())
    metrics = Metrics(2, 5.0, 2.5, 2.0, 3.0)
    await store.save_metrics("sliding", metrics)
    assert await store.get_metrics("sliding") == metrics
    assert await store.get_metrics("tumbling") is None
    assert store.key_for("tumbling") == "streampulse:metrics:tumbling"


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
    def __init__(self):
        self.saved = []

    async def save_metrics(self, window_type, metrics):
        self.saved.append((window_type, metrics))


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
async def test_ten_and_five_event_windows_aggregate_actual_values_and_retain_tumbling_summary():
    store = RedisMetricsStore(client=FakeRedis())
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

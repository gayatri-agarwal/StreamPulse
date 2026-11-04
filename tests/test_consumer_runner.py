import asyncio

import pytest

import run_consumer
from app.messaging.rabbitmq import RabbitMQSettings
from app.processing.event_processor import LibraryEventProcessor
from app.processing.window_config import WindowConfiguration
from app.processing.window_manager import SlidingWindow, TumblingWindow


@pytest.mark.asyncio
async def test_build_consumer_initializes_default_configuration_when_missing(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqps://runner-test")
    monkeypatch.setenv("RABBITMQ_QUEUE", "runner-events")
    monkeypatch.setenv("REDIS_URL", "redis://runner-test")
    created = {}

    class FakeMetricsStore:
        def __init__(self):
            created["store"] = self
            self.active = None
            self.pending = None

        async def read_active_configuration(self):
            return self.active

        async def initialize_active_configuration(self, configuration):
            if self.active is None:
                self.active = configuration
            return self.active

        async def read_pending_configuration(self):
            return self.pending

    class FakeConsumer:
        def __init__(self, processor, settings):
            created["processor"] = processor
            created["settings"] = settings

    monkeypatch.setattr(run_consumer, "RedisMetricsStore", FakeMetricsStore)
    monkeypatch.setattr(run_consumer, "RabbitMQConsumer", FakeConsumer)

    consumer, store = await run_consumer.build_consumer()

    assert isinstance(consumer, FakeConsumer)
    assert store is created["store"]
    assert isinstance(created["processor"], LibraryEventProcessor)
    assert created["processor"].metrics_store is store
    assert isinstance(created["processor"].tumbling, TumblingWindow)
    assert created["processor"].tumbling.size == run_consumer.TUMBLING_WINDOW_SIZE == 10
    assert isinstance(created["processor"].sliding, SlidingWindow)
    assert created["processor"].sliding.size == run_consumer.SLIDING_WINDOW_SIZE == 5
    assert created["processor"].sliding.interval == 1
    assert created["processor"].active_configuration == WindowConfiguration()
    assert created["settings"] == RabbitMQSettings("amqps://runner-test", "runner-events")


@pytest.mark.asyncio
async def test_build_consumer_recovers_custom_active_configuration_and_retains_pending(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqps://runner-test")
    monkeypatch.setenv("REDIS_URL", "redis://runner-test")
    created = {}
    custom = WindowConfiguration(tumbling_size=8, sliding_size=10, sliding_interval=3)
    pending = WindowConfiguration(tumbling_size=5, sliding_size=8, sliding_interval=2)
    persisted = {"active": custom, "pending": pending, "initializations": 0}

    class FakeMetricsStore:
        def __init__(self):
            created["store"] = self

        async def read_active_configuration(self):
            return persisted["active"]

        async def initialize_active_configuration(self, configuration):
            persisted["initializations"] += 1
            if persisted["active"] is None:
                persisted["active"] = configuration
            return persisted["active"]

        async def read_pending_configuration(self):
            return persisted["pending"]

    class FakeConsumer:
        def __init__(self, processor, settings):
            created["processor"] = processor

    monkeypatch.setattr(run_consumer, "RedisMetricsStore", FakeMetricsStore)
    monkeypatch.setattr(run_consumer, "RabbitMQConsumer", FakeConsumer)

    _, store = await run_consumer.build_consumer()
    processor = created["processor"]

    assert processor.active_configuration == custom
    assert processor.tumbling.size == 8
    assert processor.sliding.size == 10
    assert processor.sliding.interval == 3
    assert persisted["active"] == custom
    assert persisted["pending"] == pending
    assert persisted["initializations"] == 0

    # A second construction models a process restart against the same Redis state.
    _, restarted_store = await run_consumer.build_consumer()
    assert restarted_store is not store
    assert created["processor"].active_configuration == custom
    assert created["processor"].tumbling.size == 8
    assert created["processor"].sliding.size == 10
    assert persisted["pending"] == pending
    assert persisted["initializations"] == 0


@pytest.mark.asyncio
async def test_run_consumer_closes_metrics_store_after_consumer_stops(monkeypatch):
    class FakeMetricsStore:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    class FakeConsumer:
        async def run_forever(self):
            raise asyncio.CancelledError

    consumer = FakeConsumer()
    store = FakeMetricsStore()

    async def build_consumer():
        return consumer, store

    monkeypatch.setattr(run_consumer, "build_consumer", build_consumer)

    with pytest.raises(asyncio.CancelledError):
        await run_consumer.run_consumer()

    assert store.closed

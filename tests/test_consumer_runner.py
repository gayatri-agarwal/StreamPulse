import asyncio

import pytest

import run_consumer
from app.messaging.rabbitmq import RabbitMQSettings
from app.processing.event_processor import LibraryEventProcessor
from app.processing.window_manager import SlidingWindow, TumblingWindow


def test_build_consumer_uses_existing_configuration_and_window_sizes(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqps://runner-test")
    monkeypatch.setenv("RABBITMQ_QUEUE", "runner-events")
    monkeypatch.setenv("REDIS_URL", "redis://runner-test")
    created = {}

    class FakeMetricsStore:
        def __init__(self):
            created["store"] = self

    class FakeConsumer:
        def __init__(self, processor, settings):
            created["processor"] = processor
            created["settings"] = settings

    monkeypatch.setattr(run_consumer, "RedisMetricsStore", FakeMetricsStore)
    monkeypatch.setattr(run_consumer, "RabbitMQConsumer", FakeConsumer)

    consumer, store = run_consumer.build_consumer()

    assert isinstance(consumer, FakeConsumer)
    assert store is created["store"]
    assert isinstance(created["processor"], LibraryEventProcessor)
    assert created["processor"].metrics_store is store
    assert isinstance(created["processor"].tumbling, TumblingWindow)
    assert created["processor"].tumbling.size == run_consumer.TUMBLING_WINDOW_SIZE == 10
    assert isinstance(created["processor"].sliding, SlidingWindow)
    assert created["processor"].sliding.size == run_consumer.SLIDING_WINDOW_SIZE == 5
    assert created["settings"] == RabbitMQSettings("amqps://runner-test", "runner-events")


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
    monkeypatch.setattr(run_consumer, "build_consumer", lambda: (consumer, store))

    with pytest.raises(asyncio.CancelledError):
        await run_consumer.run_consumer()

    assert store.closed

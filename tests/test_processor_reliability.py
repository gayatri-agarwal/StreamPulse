import asyncio
import json
import logging
from datetime import datetime, timezone

import pytest
from redis.exceptions import WatchError

from app.messaging.rabbitmq import RabbitMQConsumer, RabbitMQSettings
from app.processing.event_processor import LibraryEventProcessor
from app.processing.metrics import Metrics
from app.processing.window_manager import SlidingWindow, TumblingWindow
from app.safe_diagnostics import safe_exception_details
from app.schemas import EventType, LibraryEvent
from app.storage.redis_metrics import RedisMetricsStore, RedisStorageError


def make_event(value, event_id, *, timestamp=None):
    return LibraryEvent(
        event_id=event_id,
        event_type=EventType.BOOK_ISSUED,
        student_id="student",
        book_id="book",
        timestamp=timestamp or datetime(2026, 1, 1, tzinfo=timezone.utc),
        value=float(value),
    )


class BatchStore:
    def __init__(self):
        self.fail = False
        self.batches = []

    async def save_metrics_batch(self, summaries, *, run_id=None, simulation_id=None):
        if self.fail:
            raise RedisStorageError("storage unavailable")
        self.batches.append((dict(summaries), run_id, simulation_id))

    async def read_pending_configuration(self):
        return None


@pytest.mark.asyncio
async def test_successful_processor_keeps_count_based_window_summaries():
    store = BatchStore()
    processor = LibraryEventProcessor(TumblingWindow(3), SlidingWindow(3), store)

    assert await processor.process(make_event(1, "e1")) == {"sliding": Metrics(1, 1, 1, 1, 1)}
    assert await processor.process(make_event(2, "e2")) == {"sliding": Metrics(2, 3, 1.5, 1, 2)}
    result = await processor.process(make_event(3, "e3"))

    assert result == {
        "tumbling": Metrics(3, 6, 2, 1, 3),
        "sliding": Metrics(3, 6, 2, 1, 3),
    }
    assert processor.tumbling.pending_count == 0
    assert processor.sliding.event_count == 3
    assert set(store.batches[-1][0]) == {"tumbling", "sliding"}


@pytest.mark.asyncio
async def test_failed_persistence_does_not_commit_windows_and_recovery_retries_once():
    store = BatchStore()
    processor = LibraryEventProcessor(TumblingWindow(2), SlidingWindow(2), store)
    await processor.process(make_event(1, "e1"))
    store.fail = True

    with pytest.raises(RedisStorageError):
        await processor.process(make_event(2, "e2"))

    assert processor.tumbling.pending_count == 1
    assert processor.sliding.event_count == 1
    assert len(store.batches) == 1

    store.fail = False
    result = await processor.process(make_event(2, "e2"))
    assert result["tumbling"] == result["sliding"] == Metrics(2, 3, 1.5, 1, 2)
    assert processor.tumbling.pending_count == 0
    assert processor.sliding.event_count == 2
    assert len(store.batches) == 2


@pytest.mark.asyncio
async def test_processor_calculation_failure_leaves_both_windows_unchanged(monkeypatch):
    store = BatchStore()
    processor = LibraryEventProcessor(TumblingWindow(3), SlidingWindow(3), store)
    await processor.process(make_event(1, "e1"))
    original_add = SlidingWindow.add

    def fail_once(window, event):
        if event.event_id == "e2":
            raise ArithmeticError("staged calculation failed")
        return original_add(window, event)

    monkeypatch.setattr(SlidingWindow, "add", fail_once)
    with pytest.raises(ArithmeticError):
        await processor.process(make_event(2, "e2"))
    assert processor.tumbling.pending_count == 1
    assert processor.sliding.event_count == 1

    monkeypatch.setattr(SlidingWindow, "add", original_add)
    await processor.process(make_event(2, "e2"))
    assert processor.tumbling.pending_count == 2
    assert processor.sliding.event_count == 2


@pytest.mark.asyncio
async def test_steady_and_bursty_events_keep_arrival_order_and_late_event_policy():
    store = BatchStore()
    processor = LibraryEventProcessor(TumblingWindow(3), SlidingWindow(3), store)
    events = [
        make_event(1, "on-time-1"),
        make_event(2, "on-time-2"),
        # Timestamps are intentionally out of order. Existing policy is processing
        # arrival order; event timestamps do not reorder window membership.
        make_event(3, "late", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc)),
    ]
    for event in events[:2]:
        await processor.process(event)
    # A burst follows the same sequential processing path as steady arrivals.
    result = await asyncio.gather(*(processor.process(event) for event in events[2:]))
    assert result[0]["tumbling"] == Metrics(3, 6, 2, 1, 3)
    assert result[0]["sliding"] == Metrics(3, 6, 2, 1, 3)


class FakeMessage:
    def __init__(self, body):
        self.body = body
        self.acks = 0
        self.rejects = []

    async def ack(self):
        self.acks += 1

    async def reject(self, requeue=False):
        self.rejects.append(requeue)


class FailingProcessor:
    metrics_store = None

    async def process(self, event):
        raise RuntimeError(
            'redis://safe-user:do-not-log@host:6379 password=hunter2 '
            'details={"token": "also-secret"}'
        )


@pytest.mark.asyncio
async def test_consumer_logs_traceback_with_redacted_exception_details(caplog):
    consumer = RabbitMQConsumer(FailingProcessor(), RabbitMQSettings("amqp://fake", "events"))
    message = FakeMessage(json.dumps({
        "event_id": "diagnostic-event",
        "event_type": "BOOK_ISSUED",
        "student_id": "s1",
        "book_id": "b1",
        "timestamp": "2026-01-01T00:00:00Z",
    }).encode())

    with caplog.at_level(logging.ERROR):
        await consumer.handle_message(message)

    assert "event_id=diagnostic-event" in caplog.text
    assert "stage=processor.process" in caplog.text
    assert "exception_class=RuntimeError" in caplog.text
    assert "Traceback (most recent call last)" in caplog.text
    assert all(secret not in caplog.text for secret in (
        "do-not-log", "hunter2", "also-secret",
    ))
    assert message.acks == 0 and message.rejects == [False]


class FailingPipelineClient:
    def pipeline(self, transaction=True):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def watch(self, _key):
        raise RuntimeError("redis://user:private@redis-host password=private")

    async def get(self, _key):
        return None


@pytest.mark.asyncio
async def test_redis_persistence_logs_original_exception_safely(caplog):
    store = RedisMetricsStore(client=FailingPipelineClient())
    store.consumer_instance_id = "00000000-0000-0000-0000-000000000001"

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RedisStorageError):
            await store.save_metrics("sliding", Metrics(1, 1, 1, 1, 1))

    assert "exception_class=RuntimeError" in caplog.text
    assert "Traceback (most recent call last)" in caplog.text
    assert "private" not in caplog.text


def test_exception_details_redact_urls_and_common_secret_assignments():
    error = RuntimeError('url=amqps://user:pass@host/v secret="value"')
    details, formatted_traceback = safe_exception_details(error)
    assert "amqps://" not in details and "user:pass" not in formatted_traceback
    assert "value" not in details and "secret" in details


class RetryWatchPipeline:
    def __init__(self, client):
        self.client = client
        self.commands = []
        self.owner = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def watch(self, _key):
        self.owner = self.client.owner

    async def get(self, _key):
        return self.client.owner

    def multi(self):
        pass

    def set(self, key, value, ex=None):
        self.commands.append((key, value, ex))

    async def execute(self):
        self.client.attempts += 1
        if self.client.attempts == 1:
            self.client.owner = self.client.owner  # Simulate WATCH invalidation below.
            raise WatchError()
        if self.owner != self.client.owner:
            raise WatchError()
        self.client.writes.extend(self.commands)


class RetryWatchClient:
    def __init__(self, owner):
        self.owner = owner
        self.attempts = 0
        self.writes = []

    def pipeline(self, transaction=True):
        assert transaction is True
        return RetryWatchPipeline(self)


@pytest.mark.asyncio
async def test_redis_owner_watch_race_retries_without_duplicate_commits():
    owner = "00000000-0000-0000-0000-000000000002"
    client = RetryWatchClient(owner)
    store = RedisMetricsStore(client=client)
    store.consumer_instance_id = owner

    await store.save_metrics_batch({
        "tumbling": Metrics(2, 3, 1.5, 1, 2),
        "sliding": Metrics(2, 3, 1.5, 1, 2),
    })

    assert client.attempts == 2
    assert len(client.writes) == 3  # two summaries plus one evidence record


class ActivityPipeline:
    def __init__(self, client):
        self.client = client
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def lpush(self, key, value):
        self.commands.append(("lpush", key, value))

    def ltrim(self, key, start, stop):
        self.commands.append(("ltrim", key, start, stop))

    def expire(self, key, ttl):
        self.commands.append(("expire", key, ttl))

    async def execute(self):
        for command in self.commands:
            if command[0] == "lpush":
                self.client.values.setdefault(command[1], []).insert(0, command[2])
            elif command[0] == "ltrim":
                _, key, start, stop = command
                self.client.values[key] = self.client.values.get(key, [])[start:stop + 1]


class ActivityRedisClient:
    def __init__(self):
        self.values = {}

    def pipeline(self, transaction=True):
        assert transaction is True
        return ActivityPipeline(self)

    async def lrange(self, key, start, stop):
        return self.values.get(key, [])[start:stop + 1]


@pytest.mark.asyncio
async def test_simulation_activity_records_processing_failure_without_processed_time():
    store = RedisMetricsStore(client=ActivityRedisClient())
    simulation_id = "00000000-0000-0000-0000-000000000003"
    now = datetime.now(timezone.utc).isoformat()

    await store.record_simulation_activity(
        simulation_id, "event-1", now, now, None,
        "processing_failed", "RedisStorageError",
    )
    records = await store.get_simulation_activity(simulation_id)

    assert records == [{
        "event_id": "event-1", "event_time": now, "published_at": now,
        "processed_at": None, "status": "processing_failed",
        "processing_error": "RedisStorageError",
    }]

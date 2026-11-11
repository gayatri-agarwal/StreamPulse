import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import WatchError

from app.main import create_app
from app.experiments.schemas import RunStatus
from app.experiments.store import ExperimentRepository
from app.messaging.rabbitmq import RabbitMQConsumer, RabbitMQSettings, _event_json
from app.processing.measurements import average_latency, failure_rate, percentile_95
from app.schemas import LibraryEvent
from app.storage.redis_metrics import (
    MEASUREMENT_SAMPLE_LIMIT,
    MeasurementSummary, RedisMetricsStore, RedisStorageError,
)


def test_latency_statistics_use_actual_samples_and_nearest_rank_p95():
    assert average_latency([]) is None
    assert average_latency([2, 4, 6]) == 4
    assert percentile_95([]) is None
    assert percentile_95([9]) == 9
    assert percentile_95(list(range(1, 21))) == 19
    assert failure_rate(0, 0) is None
    assert failure_rate(2, 8) == 0.25


@pytest.mark.asyncio
async def test_duplicate_deliveries_count_attempts_but_unique_success_and_samples_once():
    client = _AtomicRedis()
    store = RedisMetricsStore(client=client)
    run_id = str(uuid4())

    async def deliver():
        return await store.record_consumer_delivery(
            run_id=run_id,
            event_id="same-event",
            succeeded=True,
            samples={"consumer_processing_latency_ms": 4.0},
            success_timestamp=20.0,
        )

    results = await asyncio.gather(deliver(), deliver())
    assert sum(results) == 1
    summary = await store.get_measurement_summary(run_id)
    assert summary.consumer_attempts == 2
    assert summary.consumer_successes == 2
    assert summary.consumer_unique_successes == 1
    assert summary.sample_count["consumer_processing_latency_ms"] == 1
    assert summary.inter_arrival_rate_events_per_second is None
    assert summary.throughput_events_per_second == summary.inter_arrival_rate_events_per_second
    assert len(client.lists[f"{store.measurement_prefix(run_id)}:success_timestamps"]) == 1


@pytest.mark.asyncio
async def test_consumer_redelivery_updates_unique_metrics_once(tmp_path):
    store = RedisMetricsStore(client=_AtomicRedis())
    run_id = str(uuid4())
    repository = ExperimentRepository(tmp_path / "redelivery.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Redelivery", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
    })
    repository.create_run(experiment["experiment_id"], run_id=run_id)
    repository.transition_run(experiment["experiment_id"], run_id, RunStatus.RUNNING)
    repository.reserve_run_event(run_id, "redelivered-event")
    event = {
        "event_id": "redelivered-event", "event_type": "BOOK_ISSUED", "student_id": "s",
        "book_id": "b", "timestamp": datetime.now(timezone.utc).isoformat(), "run_id": run_id,
    }

    class Processor:
        metrics_store = store
        last_redis_persistence_latency_ms = None

        async def process(self, received):
            return {}

    consumer = RabbitMQConsumer(
        Processor(), RabbitMQSettings("amqp://unused"), experiment_repository=repository
    )
    messages = [_Message(json.dumps(event).encode()), _Message(json.dumps(event).encode())]
    await asyncio.gather(*(consumer.handle_message(message) for message in messages))
    summary = await store.get_measurement_summary(run_id)
    assert all(message.acked and not message.rejected for message in messages)
    assert summary.consumer_attempts == 2
    assert summary.consumer_successes == 2
    assert summary.consumer_unique_successes == 1
    assert summary.sample_count["consumer_processing_latency_ms"] == 1


@pytest.mark.asyncio
async def test_distinct_event_ids_and_run_scopes_remain_isolated():
    client = _AtomicRedis()
    store = RedisMetricsStore(client=client)
    run_a, run_b = str(uuid4()), str(uuid4())
    for event_id in ("event-a", "event-b"):
        await store.record_consumer_delivery(
            run_id=run_a, event_id=event_id, succeeded=True,
            samples={"consumer_processing_latency_ms": 1.0}, success_timestamp=10.0,
        )
    await store.record_consumer_delivery(
        run_id=run_b, event_id="event-a", succeeded=True,
        samples={"consumer_processing_latency_ms": 2.0}, success_timestamp=12.0,
    )
    a, b, overall = await asyncio.gather(
        store.get_measurement_summary(run_a), store.get_measurement_summary(run_b),
        store.get_measurement_summary(),
    )
    assert a.consumer_unique_successes == 2
    assert b.consumer_unique_successes == 1
    assert a.sample_count["consumer_processing_latency_ms"] == 2
    assert b.sample_count["consumer_processing_latency_ms"] == 1
    assert overall.consumer_unique_successes == 2


@pytest.mark.asyncio
async def test_failed_atomic_record_does_not_commit_success_measurements():
    client = _AtomicRedis(fail_exec=True)
    store = RedisMetricsStore(client=client)
    with pytest.raises(RedisStorageError):
        await store.record_consumer_delivery(
            run_id=None, event_id="event-fail", succeeded=True,
            samples={"consumer_processing_latency_ms": 3.0}, success_timestamp=30.0,
        )
    summary = await store.get_measurement_summary()
    assert summary.consumer_attempts == 0
    assert summary.consumer_successes == 0
    assert summary.consumer_unique_successes == 0
    assert summary.sample_count["consumer_processing_latency_ms"] == 0


def test_event_run_id_is_optional_validated_and_serialized_only_when_present():
    identifier = str(uuid4())
    data = {"event_id": "e-1", "event_type": "BOOK_ISSUED", "student_id": "s",
            "book_id": "b", "timestamp": datetime.now(timezone.utc).isoformat()}
    ordinary = LibraryEvent.from_mapping(data)
    assert ordinary.run_id is None
    assert "run_id" not in json.loads(_event_json(ordinary))
    scoped = LibraryEvent.from_mapping({**data, "run_id": identifier})
    assert scoped.run_id == identifier
    assert json.loads(_event_json(scoped))["run_id"] == identifier
    with pytest.raises(ValueError, match="UUID"):
        LibraryEvent.from_mapping({**data, "run_id": "not-a-uuid"})


class _Pipeline:
    def __init__(self, client):
        self.client = client
        self.operations = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def hincrby(self, key, field, amount):
        self.operations.append(("hincrby", key, field, amount))
        return self

    def rpush(self, key, value):
        self.operations.append(("rpush", key, value))
        return self

    def ltrim(self, key, start, end):
        self.operations.append(("ltrim", key, start, end))
        return self

    def expire(self, key, seconds):
        self.operations.append(("expire", key, seconds))
        return self

    async def execute(self):
        for operation in self.operations:
            kind, key, *args = operation
            if kind == "hincrby":
                field, amount = args
                self.client.hashes.setdefault(key, {})[field] = int(self.client.hashes.setdefault(key, {}).get(field, 0)) + amount
            elif kind == "rpush":
                self.client.lists.setdefault(key, []).append(args[0])
            elif kind == "ltrim":
                start, end = args
                values = self.client.lists.get(key, [])
                stop = None if end == -1 else end + 1
                self.client.lists[key] = values[start:stop]
            elif kind == "expire":
                self.client.expiries[key] = args[0]


class _Redis:
    def __init__(self):
        self.hashes, self.lists, self.expiries = {}, {}, {}

    def pipeline(self, transaction=True):
        return _Pipeline(self)

    async def hgetall(self, key):
        return self.hashes.get(key, {})

    async def lrange(self, key, start, end):
        return self.lists.get(key, [])[start:]


class _AtomicPipeline:
    """Model WATCH/MULTI conflicts and atomic EXEC commits for idempotency tests."""

    def __init__(self, client):
        self.client = client
        self.watched = {}
        self.operations = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def watch(self, *keys):
        self.watched = {key: self.client.versions.get(key, 0) for key in keys}

    async def mget(self, *keys):
        await asyncio.sleep(0)
        return [self.client.strings.get(key) for key in keys]

    def multi(self):
        return None

    def hincrby(self, key, field, amount):
        self.operations.append(("hincrby", key, field, amount))
        return self

    def set(self, key, value, ex=None, nx=False):
        self.operations.append(("set", key, value, ex, nx))
        return self

    def rpush(self, key, value):
        self.operations.append(("rpush", key, value))
        return self

    def ltrim(self, key, start, end):
        self.operations.append(("ltrim", key, start, end))
        return self

    def expire(self, key, seconds):
        self.operations.append(("expire", key, seconds))
        return self

    async def execute(self):
        async with self.client.lock:
            if any(self.client.versions.get(key, 0) != version for key, version in self.watched.items()):
                raise WatchError()
            if self.client.fail_exec:
                raise RuntimeError("simulated Redis transaction failure")
            for operation in self.operations:
                kind, key, *args = operation
                if kind == "hincrby":
                    field, amount = args
                    values = self.client.hashes.setdefault(key, {})
                    values[field] = int(values.get(field, 0)) + amount
                elif kind == "set":
                    value, ttl, nx = args
                    if nx and key in self.client.strings:
                        continue
                    self.client.strings[key] = value
                    if ttl is not None:
                        self.client.expiries[key] = ttl
                elif kind == "rpush":
                    self.client.lists.setdefault(key, []).append(args[0])
                elif kind == "ltrim":
                    start, end = args
                    values = self.client.lists.get(key, [])
                    stop = None if end == -1 else end + 1
                    self.client.lists[key] = values[start:stop]
                elif kind == "expire":
                    self.client.expiries[key] = args[0]
                self.client.versions[key] = self.client.versions.get(key, 0) + 1


class _AtomicRedis(_Redis):
    def __init__(self, fail_exec=False):
        super().__init__()
        self.strings = {}
        self.versions = {}
        self.lock = asyncio.Lock()
        self.fail_exec = fail_exec

    async def set(self, key, value, ex=None):
        async with self.lock:
            self.strings[key] = value
            if ex is not None:
                self.expiries[key] = ex
            else:
                self.expiries.pop(key, None)
            self.versions[key] = self.versions.get(key, 0) + 1
        return True

    def pipeline(self, transaction=True):
        return _AtomicPipeline(self)

    async def hgetall(self, key):
        return self.hashes.get(key, {})

    async def lrange(self, key, start, end):
        values = self.lists.get(key, [])
        stop = None if end == -1 else end + 1
        return values[start:stop]


@pytest.mark.asyncio
async def test_measurements_are_isolated_bounded_and_expire():
    client = _Redis()
    store = RedisMetricsStore(client=client)
    run_a, run_b = str(uuid4()), str(uuid4())
    await store.record_measurements(
        run_id=run_a,
        counters={"publish_attempts": 1, "publish_successes": 1, "consumer_attempts": 1,
                  "consumer_successes": 1},
        samples={"publish_latency_ms": 3, "consumer_processing_latency_ms": 7},
        success_timestamp=100.0,
    )
    await store.record_measurements(
        run_id=run_a, counters={"consumer_attempts": 1, "consumer_successes": 1},
        samples={"consumer_processing_latency_ms": 9}, success_timestamp=102.0,
    )
    await store.record_measurements(
        run_id=run_b, counters={"publish_attempts": 1, "publish_failures": 1},
        samples={"publish_latency_ms": 11},
    )
    await store.record_measurements(
        run_id=None, counters={"consumer_attempts": 1, "consumer_successes": 1},
        samples={"consumer_processing_latency_ms": 2}, success_timestamp=101.0,
    )
    a, b, overall = await asyncio.gather(
        store.get_measurement_summary(run_a), store.get_measurement_summary(run_b),
        store.get_measurement_summary(),
    )
    assert a.publish_successes == 1 and a.consumer_successes == 2
    assert a.average_latency_ms["consumer_processing_latency_ms"] == 8
    assert a.throughput_events_per_second == 0.5
    assert b.publish_failure_rate == 1
    assert b.consumer_attempts == 0
    assert overall.consumer_successes == 3
    assert overall.publish_attempts == 2
    assert all(seconds == 7 * 24 * 60 * 60 for seconds in client.expiries.values())

    prefix = store.measurement_prefix(run_a)
    client.lists[f"{prefix}:samples:publish_latency_ms"] = ["1"] * (MEASUREMENT_SAMPLE_LIMIT + 1)
    await store.record_measurements(run_id=run_a, counters={}, samples={"publish_latency_ms": 2})
    assert client.lists[f"{prefix}:samples:publish_latency_ms"] == ["1"] * (MEASUREMENT_SAMPLE_LIMIT - 1) + ["2.0"]


class _Message:
    def __init__(self, body):
        self.body, self.acked, self.rejected = body, False, False

    async def ack(self):
        self.acked = True

    async def reject(self, requeue):
        self.rejected = True


@pytest.mark.asyncio
async def test_consumer_preserves_run_id_and_reports_stage_samples(tmp_path):
    run_id = str(uuid4())
    repository = ExperimentRepository(tmp_path / "consumer-run.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Consumer run", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
    })
    repository.create_run(experiment["experiment_id"], run_id=run_id)
    repository.transition_run(experiment["experiment_id"], run_id, RunStatus.RUNNING)
    repository.reserve_run_event(run_id, "event-1")
    event = {"event_id": "event-1", "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b",
             "timestamp": datetime.now(timezone.utc).isoformat(), "run_id": run_id}
    captured = {}

    class Processor:
        metrics_store = None
        last_redis_persistence_latency_ms = 1.25
        calls = 0

        async def process(self, received):
            self.calls += 1
            captured["event"] = received
            return {"sliding": object()}

    class Recorder:
        aggregation_write_status = None

        def __init__(self):
            self.records = []
            self.run_summaries = []

        async def record_measurements(self, **kwargs):
            self.records.append(kwargs)

        async def save_run_metrics(self, run_id, window_type, metrics):
            self.run_summaries.append((run_id, window_type, metrics))

    processor = Processor()
    recorder = Recorder()
    processor.metrics_store = recorder
    consumer = RabbitMQConsumer(
        processor, RabbitMQSettings("amqp://unused"), experiment_repository=repository
    )
    message = _Message(json.dumps(event).encode())
    await consumer.handle_message(message)
    assert message.acked and not message.rejected
    assert processor.calls == 0
    run_processor = consumer._run_processor_manager._contexts[run_id].processor
    assert run_processor.sliding._events[-1].run_id == run_id
    measurement = recorder.records[-1]
    assert measurement["run_id"] == run_id
    assert measurement["counters"]["consumer_successes"] == 1
    assert measurement["samples"]["consumer_processing_latency_ms"] >= 0
    assert measurement["samples"]["redis_persistence_latency_ms"] >= 0
    assert measurement["samples"]["publish_to_delivery_latency_ms"] is not None
    assert measurement["samples"]["end_to_end_latency_ms"] is not None
    assert all(item[0] == run_id for item in recorder.run_summaries)


@pytest.mark.asyncio
async def test_consumer_rejects_unreserved_run_scoped_message(tmp_path):
    repository = ExperimentRepository(tmp_path / "unreserved.sqlite3")
    repository.initialize()
    processed = []

    class Processor:
        metrics_store = None

        async def process(self, event):
            processed.append(event)

    event = {
        "event_id": str(uuid4()), "event_type": "BOOK_ISSUED", "student_id": "s",
        "book_id": "b", "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": str(uuid4()),
    }
    message = _Message(json.dumps(event).encode())
    consumer = RabbitMQConsumer(
        Processor(), RabbitMQSettings("amqp://unused"), experiment_repository=repository
    )
    await consumer.handle_message(message)
    assert message.rejected is True
    assert not message.acked
    assert processed == []


def test_api_accepts_optional_run_id_without_changing_ordinary_event_shape():
    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, event):
            self.events.append(event)

    class Store:
        async def get_measurement_summary(self, run_id=None):
            raise AssertionError("not used")

    publisher = Publisher()
    with TestClient(create_app(publisher=publisher, metrics_store=Store())) as client:
        normal = client.post("/api/events", json={"event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b"})
        experiment = client.post("/api/experiments", json={"name": "Scoped"}).json()
        run = client.post(f"/api/experiments/{experiment['experiment_id']}/runs", json={}).json()
        client.post(f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}/start")
        run_id = run["run_id"]
        scoped = client.post("/api/events", json={"event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": run_id})
        invalid = client.post("/api/events", json={"event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": "bad"})
    assert normal.status_code == 202 and "run_id" not in normal.json()["event"]
    assert scoped.status_code == 202 and scoped.json()["event"]["run_id"] == run_id
    assert publisher.events[0].run_id is None and publisher.events[1].run_id == run_id
    assert invalid.status_code == 422


def test_api_records_monotonic_publish_latency_and_stage_failure_counts():
    class Publisher:
        async def publish(self, event):
            raise RuntimeError("broker unavailable")

    class Store:
        def __init__(self):
            self.records = []

        async def record_measurements(self, **record):
            self.records.append(record)

    store = Store()
    with TestClient(create_app(publisher=Publisher(), metrics_store=store)) as client:
        experiment = client.post("/api/experiments", json={"name": "Publish failure"}).json()
        run = client.post(f"/api/experiments/{experiment['experiment_id']}/runs", json={}).json()
        client.post(f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}/start")
        run_id = run["run_id"]
        response = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": run_id,
        })
    assert response.status_code == 503
    assert store.records[0]["run_id"] == run_id
    assert store.records[0]["counters"] == {
        "publish_attempts": 1, "publish_successes": 0, "publish_failures": 1,
    }
    assert store.records[0]["samples"]["publish_latency_ms"] >= 0


def test_measurements_endpoint_exposes_overall_and_run_scoped_summaries():
    class Store:
        async def get_measurement_summary(self, run_id=None):
            return MeasurementSummary(1, 1, 0, 0.0, 1, 1, 0, 0.0, None,
                                     {"publish_latency_ms": 1},
                                     {"publish_latency_ms": 2.5},
                                     {"publish_latency_ms": 2.5})

    run_id = str(uuid4())
    with TestClient(create_app(metrics_store=Store())) as client:
        overall = client.get("/api/measurements")
        scoped = client.get("/api/measurements", params={"run_id": run_id})
        invalid = client.get("/api/measurements", params={"run_id": "invalid"})
    assert overall.status_code == 200 and overall.json()["scope"] == "overall"
    assert scoped.status_code == 200 and scoped.json()["run_id"] == run_id
    assert scoped.json()["p95_latency_ms"]["publish_latency_ms"] == 2.5
    assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_measurement_redis_failure_does_not_change_ack_behavior():
    class Processor:
        last_redis_persistence_latency_ms = None

        class Store:
            aggregation_write_status = None

            async def record_measurements(self, **kwargs):
                raise RuntimeError("redis unavailable")

        metrics_store = Store()

        async def process(self, event):
            return {}

    event = {"event_id": "event-1", "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b",
             "timestamp": datetime.now(timezone.utc).isoformat()}
    consumer = RabbitMQConsumer(Processor(), RabbitMQSettings("amqp://unused"))
    message = _Message(json.dumps(event).encode())
    await consumer.handle_message(message)
    assert message.acked and not message.rejected

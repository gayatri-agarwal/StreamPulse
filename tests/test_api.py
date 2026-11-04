import json
from datetime import datetime, timezone

from fastapi.testclient import TestClient
import pytest

import app.main as main_module
from app.main import create_app
from app.messaging.rabbitmq import RabbitMQError, RabbitMQSettings
from app.processing.metrics import Metrics
from app.processing.window_config import WindowConfiguration
from app.schemas import EventType, LibraryEvent
from app.storage.redis_metrics import (
    ConsumerMonitoringSnapshot,
    RedisConfigurationDataError,
    RedisConfigurationError,
    RedisMonitoringDataError,
    RedisMonitoringError,
    RedisStorageError,
)


class FakePublisher:
    def __init__(self, fail=False, *, queue_info=None, inspect_fail=False, queue="streampulse.events"):
        self.events = []
        self.fail = fail
        self.queue_info = queue_info
        self.inspect_fail = inspect_fail
        self.settings = RabbitMQSettings("amqps://user:secret@rabbit-host", queue)

    async def publish(self, event):
        if self.fail:
            raise RabbitMQError("Could not publish library event to RabbitMQ")
        self.events.append(event)

    async def inspect_queue(self):
        if self.inspect_fail:
            raise RabbitMQError("Could not inspect RabbitMQ queue")
        return self.queue_info


class FakeMetricsStore:
    def __init__(self, fail=False, *, monitoring_snapshot=None, heartbeat_fresh=None,
                 monitoring_error=None, heartbeat_error=None, configuration_error=None,
                 active_configuration=None, pending_configuration=None):
        self.fail = fail
        self.metrics = {
            "tumbling": Metrics(2, 3.0, 1.5, 1.0, 2.0),
            "sliding": None,
        }
        self.requested = []
        self.monitoring_snapshot = monitoring_snapshot
        self.heartbeat_fresh = heartbeat_fresh
        self.monitoring_error = monitoring_error
        self.heartbeat_error = heartbeat_error
        self.configuration_error = configuration_error
        self.active_configuration = active_configuration
        self.pending_configuration = pending_configuration
        self.pending_writes = []

    async def get_metrics(self, window_type):
        self.requested.append(window_type)
        if self.fail:
            raise RedisStorageError("Could not retrieve metrics to Redis")
        return self.metrics[window_type]

    async def read_monitoring_state(self):
        if self.monitoring_error is not None:
            raise self.monitoring_error
        return self.monitoring_snapshot

    async def is_monitoring_heartbeat_fresh(self):
        if self.heartbeat_error is not None:
            raise self.heartbeat_error
        return self.heartbeat_fresh

    async def read_active_configuration(self):
        self._raise_configuration_error()
        return self.active_configuration

    async def initialize_active_configuration(self, configuration=None):
        self._raise_configuration_error()
        if self.active_configuration is None:
            self.active_configuration = configuration or WindowConfiguration()
        return self.active_configuration

    async def read_pending_configuration(self):
        self._raise_configuration_error()
        return self.pending_configuration

    async def save_pending_configuration(self, configuration):
        self._raise_configuration_error()
        self.pending_configuration = configuration
        self.pending_writes.append(configuration)

    def _raise_configuration_error(self):
        if self.configuration_error is not None:
            raise self.configuration_error
        if self.fail:
            raise RedisConfigurationError("Could not read window configuration")


def make_consumer_snapshot(consumer_state="running"):
    return ConsumerMonitoringSnapshot(
        consumer_state=consumer_state,
        consumer_instance_id="consumer-test-1",
        last_heartbeat_at="2026-11-04T12:00:00+00:00",
        processed_events_since_start=9,
        processing_failures_since_start=2,
        consumer_redis_status="available",
        last_successful_aggregation_write_at="2026-11-04T11:59:50+00:00",
        tumbling_window_size=10,
        tumbling_window_progress=3,
        sliding_window_size=5,
        sliding_window_progress=5,
        sliding_interval="per_event",
        last_completed_tumbling_aggregation_at="2026-11-04T11:58:00+00:00",
    )


def install_virtual_batch_clock(monkeypatch):
    now = [0.0]
    delays = []

    def monotonic_time():
        return now[0]

    async def wait_for_interval(delay):
        delays.append(delay)
        now[0] += delay

    monkeypatch.setattr(main_module, "_monotonic_time", monotonic_time)
    monkeypatch.setattr(main_module, "_wait_for_batch_interval", wait_for_interval)
    return now, delays


def test_health_reports_api_availability_without_claiming_unverified_services(monkeypatch):
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    with TestClient(create_app()) as client:
        response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {
        "status": "available",
        "services": {"rabbitmq": "not_configured", "redis": "not_configured"},
    }


def test_get_config_initializes_persisted_defaults_when_active_is_missing():
    store = FakeMetricsStore()
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/config")

    assert response.status_code == 200
    assert response.json() == {
        "active": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
        "pending": None,
        "status": "active",
    }
    assert store.active_configuration == WindowConfiguration()


def test_get_config_returns_existing_custom_active_configuration():
    active = WindowConfiguration(8, 10, 3)
    store = FakeMetricsStore(active_configuration=active)
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/config")

    assert response.status_code == 200
    assert response.json()["active"] == {
        "tumbling_size": 8, "sliding_size": 10, "sliding_interval": 3,
    }
    assert response.json()["pending"] is None
    assert response.json()["status"] == "active"


def test_get_config_returns_pending_configuration_with_pending_status():
    active = WindowConfiguration()
    pending = WindowConfiguration(5, 8, 2)
    store = FakeMetricsStore(active_configuration=active, pending_configuration=pending)
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/config")

    assert response.status_code == 200
    assert response.json() == {
        "active": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
        "pending": {"tumbling_size": 5, "sliding_size": 8, "sliding_interval": 2},
        "status": "pending",
    }


def test_put_config_saves_validated_pending_without_changing_active():
    active = WindowConfiguration()
    candidate = {"tumbling_size": 8, "sliding_size": 10, "sliding_interval": 3}
    store = FakeMetricsStore(active_configuration=active)
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.put("/api/config", json=candidate)

    assert response.status_code == 200
    assert response.json() == {
        "active": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
        "pending": candidate,
        "status": "pending",
        "message": "Configuration will activate after the current tumbling window completes.",
    }
    assert store.active_configuration == active
    assert store.pending_configuration == WindowConfiguration(**candidate)


@pytest.mark.parametrize(
    "payload",
    [
        {"tumbling_size": 7, "sliding_size": 5, "sliding_interval": 1},
        {"tumbling_size": 10, "sliding_size": 7, "sliding_interval": 1},
    ],
)
def test_put_config_rejects_unsupported_window_sizes(payload):
    store = FakeMetricsStore(active_configuration=WindowConfiguration())
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.put("/api/config", json=payload)

    assert response.status_code == 422
    assert "window sizes must be one of 5, 8, or 10" in response.json()["detail"]
    assert store.pending_configuration is None


@pytest.mark.parametrize("interval", [0, -1, 5, True, 1.5, "2"])
def test_put_config_rejects_invalid_sliding_intervals(interval):
    store = FakeMetricsStore(active_configuration=WindowConfiguration())
    payload = {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": interval}
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.put(
            "/api/config",
            content=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )

    assert response.status_code == 422
    assert store.pending_configuration is None


def test_repeated_put_config_replaces_pending_using_storage_semantics():
    store = FakeMetricsStore(active_configuration=WindowConfiguration())
    first = {"tumbling_size": 8, "sliding_size": 10, "sliding_interval": 2}
    second = {"tumbling_size": 5, "sliding_size": 8, "sliding_interval": 3}
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        first_response = client.put("/api/config", json=first)
        second_response = client.put("/api/config", json=second)

    assert first_response.status_code == second_response.status_code == 200
    assert second_response.json()["active"] == {
        "tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1,
    }
    assert second_response.json()["pending"] == second
    assert store.pending_writes == [WindowConfiguration(**first), WindowConfiguration(**second)]


def test_config_endpoints_return_503_when_redis_storage_is_unavailable_without_leaking_details():
    store = FakeMetricsStore(fail=True)
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        get_response = client.get("/api/config")
        put_response = client.put("/api/config", json={
            "tumbling_size": 8, "sliding_size": 10, "sliding_interval": 2,
        })

    assert get_response.status_code == put_response.status_code == 503
    assert "redis://" not in get_response.text + put_response.text
    assert "Could not read window configuration" not in get_response.text + put_response.text


def test_config_endpoint_returns_service_error_for_malformed_persisted_configuration():
    store = FakeMetricsStore(
        configuration_error=RedisConfigurationDataError("Stored window configuration is invalid")
    )
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/config")

    assert response.status_code == 500
    assert response.json() == {"detail": "Stored window configuration is invalid"}


def test_local_frontend_origin_is_allowed_by_cors():
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=FakeMetricsStore())) as client:
        response = client.get("/api/health", headers={"Origin": "http://127.0.0.1:5500"})
    assert response.headers["access-control-allow-origin"] == "http://127.0.0.1:5500"


def test_event_endpoint_publishes_schema_valid_event_and_tracks_recent_history():
    publisher = FakePublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s-1", "book_id": "b-1",
        })
        recent = client.get("/api/events")

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "published"
    assert body["event"]["event_id"]
    assert body["event"]["event_type"] == "BOOK_ISSUED"
    assert "value" not in body["event"]
    assert datetime.fromisoformat(body["event"]["timestamp"]).tzinfo == timezone.utc
    assert len(publisher.events) == 1
    assert recent.json()["source"] == "in_memory"
    assert recent.json()["count"] == 1


@pytest.mark.parametrize("batch_size", [5, 8, 10])
def test_sample_batch_publishes_requested_finite_number_of_valid_library_events(batch_size, monkeypatch):
    install_virtual_batch_clock(monkeypatch)
    publisher = FakePublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.post("/api/events/batch", json={"batch_size": batch_size})
        recent = client.get("/api/events")

    assert response.status_code == 200
    assert response.json() == {
        "status": "completed",
        "requested": batch_size,
        "published": batch_size,
        "failed": 0,
        "failures": [],
    }
    assert len(publisher.events) == batch_size
    assert recent.json()["count"] == batch_size
    assert all(isinstance(event, LibraryEvent) for event in publisher.events)
    assert all(event.event_type in tuple(EventType) for event in publisher.events)


def test_sample_batch_defaults_to_eight_events():
    publisher = FakePublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.post("/api/events/batch", json={})

    assert response.status_code == 200
    assert response.json()["requested"] == 8
    assert response.json()["published"] == 8
    assert response.json()["failed"] == 0
    assert len(publisher.events) == 8


def test_sample_batch_enforces_ten_per_second_across_consecutive_batches(monkeypatch):
    _, delays = install_virtual_batch_clock(monkeypatch)

    class TimedPublisher(FakePublisher):
        def __init__(self):
            super().__init__()
            self.publish_times = []

        async def publish(self, event):
            self.publish_times.append(main_module._monotonic_time())
            await super().publish(event)

    publisher = TimedPublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        first = client.post("/api/events/batch", json={"batch_size": 5})
        second = client.post("/api/events/batch", json={"batch_size": 5})

    assert first.status_code == second.status_code == 200
    assert len(publisher.publish_times) == 10
    assert all(
        later - earlier >= main_module.BATCH_EVENT_INTERVAL_SECONDS - 1e-9
        for earlier, later in zip(publisher.publish_times, publisher.publish_times[1:])
    )
    assert len(delays) == 9


def test_sample_batch_reports_actual_successes_and_failures():
    class IntermittentPublisher(FakePublisher):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        async def publish(self, event):
            self.attempts += 1
            if self.attempts in (2, 4):
                raise RabbitMQError("transport details should not be returned")
            await super().publish(event)

    publisher = IntermittentPublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.post("/api/events/batch", json={"batch_size": 5})
        recent = client.get("/api/events")

    assert response.status_code == 200
    assert response.json() == {
        "status": "partial",
        "requested": 5,
        "published": 3,
        "failed": 2,
        "failures": [
            {"event_number": 2, "detail": "Could not publish event"},
            {"event_number": 4, "detail": "Could not publish event"},
        ],
    }
    assert recent.json()["count"] == 3
    assert "transport details" not in response.text


def test_sample_batch_reports_zero_published_when_publisher_fails():
    publisher = FakePublisher(fail=True)
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.post("/api/events/batch", json={"batch_size": 5})

    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert response.json()["requested"] == 5
    assert response.json()["published"] == 0
    assert response.json()["failed"] == 5
    assert [failure["event_number"] for failure in response.json()["failures"]] == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("batch_size", [0, 4, 11, True, 5.0, "8"])
def test_sample_batch_rejects_unsupported_or_non_integer_sizes(batch_size):
    publisher = FakePublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.post(
            "/api/events/batch",
            content=json.dumps({"batch_size": batch_size}),
            headers={"Content-Type": "application/json"},
        )

    assert response.status_code == 422
    assert publisher.events == []


def test_event_endpoint_ignores_legacy_numeric_value():
    publisher = FakePublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s-1", "book_id": "b-1", "value": 2.75,
        })

    assert response.status_code == 202
    assert "value" not in response.json()["event"]
    assert publisher.events[0].value is None


@pytest.mark.parametrize("payload", [
    {"event_type": "BOOK_LOST", "student_id": "s-1", "book_id": "b-1"},
    {"event_type": "BOOK_ISSUED", "student_id": "s-1"},
    {"event_type": "BOOK_ISSUED", "student_id": " ", "book_id": "b-1"},
    {"event_type": "BOOK_RETURNED", "student_id": "s-1", "book_id": "b-1", "extra": True},
])
def test_event_endpoint_rejects_invalid_input(payload):
    publisher = FakePublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.post("/api/events", json=payload)
    assert response.status_code == 422
    assert publisher.events == []


@pytest.mark.parametrize("value", [True, "2", float("nan"), float("inf")])
def test_event_endpoint_rejects_non_numeric_or_non_finite_values(value):
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=FakeMetricsStore())) as client:
        payload = {
            "event_type": "BOOK_ISSUED", "student_id": "s-1", "book_id": "b-1", "value": value,
        }
        response = client.post(
            "/api/events",
            content=json.dumps(payload, allow_nan=True),
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 422


def test_event_endpoint_does_not_claim_success_if_publishing_fails():
    with TestClient(create_app(publisher=FakePublisher(fail=True), metrics_store=FakeMetricsStore())) as client:
        response = client.post("/api/events", json={
            "event_type": "BOOK_RESERVED", "student_id": "s-1", "book_id": "b-1",
        })
        recent = client.get("/api/events")
    assert response.status_code == 503
    assert response.json()["detail"] == "Could not publish event"
    assert recent.json()["count"] == 0


def test_metrics_endpoint_returns_current_values_and_null_for_missing_windows():
    store = FakeMetricsStore()
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/metrics")
    assert response.status_code == 200
    assert response.json() == {
        "tumbling": {"count": 2, "sum": 3.0, "average": 1.5, "minimum": 1.0, "maximum": 2.0},
        "sliding": None,
    }
    assert store.requested == ["tumbling", "sliding"]


def test_metrics_endpoint_reports_unavailable_redis_without_fabricating_metrics():
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=FakeMetricsStore(fail=True))) as client:
        response = client.get("/api/metrics")
    assert response.status_code == 503
    assert response.json() == {"detail": "Metrics storage is unavailable"}


def test_recent_events_are_bounded_to_configured_limit():
    publisher = FakePublisher()
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore(), recent_event_limit=2)) as client:
        for index in range(3):
            response = client.post("/api/events", json={
                "event_type": "BOOK_ISSUED", "student_id": f"s-{index}", "book_id": "b-1",
            })
            assert response.status_code == 202
        events = client.get("/api/events").json()["events"]
    assert len(events) == 2
    assert [event["student_id"] for event in events] == ["s-2", "s-1"]


def test_monitoring_endpoint_reports_running_consumer_and_supported_queue_information():
    publisher = FakePublisher(queue_info={"messages_ready": 6, "consumer_count": 1})
    store = FakeMetricsStore(monitoring_snapshot=make_consumer_snapshot(), heartbeat_fresh=True)

    with TestClient(create_app(publisher=publisher, metrics_store=store)) as client:
        response = client.get("/api/monitoring")

    assert response.status_code == 200
    data = response.json()
    assert data["api"] == {"status": "available", "redis_status": "available"}
    assert data["rabbitmq"]["status"] == "available"
    assert data["rabbitmq"]["queue_name"] == "streampulse.events"
    assert data["rabbitmq"]["messages_ready"] == 6
    assert data["rabbitmq"]["consumer_count"] == 1
    assert data["rabbitmq"]["unacknowledged_messages"] is None
    assert data["rabbitmq"]["unacknowledged_status"] == "unavailable"
    assert data["consumer"] == {
        "status": "running",
        "reason": None,
        "last_heartbeat": "2026-11-04T12:00:00+00:00",
        "processed_since_start": 9,
        "processing_failures_since_start": 2,
        "redis_status": "available",
        "last_successful_aggregation_write": "2026-11-04T11:59:50+00:00",
        "windows": {
            "tumbling": {
                "size": 10,
                "progress": 3,
                "last_completed_at": "2026-11-04T11:58:00+00:00",
            },
            "sliding": {"size": 5, "progress": 5, "interval": "per_event"},
        },
    }


@pytest.mark.parametrize(
    ("snapshot_state", "heartbeat_fresh", "expected"),
    [
        ("running", False, "stale"),
        ("stopped", False, "stopped"),
    ],
)
def test_monitoring_endpoint_distinguishes_stale_and_gracefully_stopped_consumer(
    snapshot_state, heartbeat_fresh, expected
):
    store = FakeMetricsStore(
        monitoring_snapshot=make_consumer_snapshot(snapshot_state),
        heartbeat_fresh=heartbeat_fresh,
    )
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/monitoring")

    assert response.status_code == 200
    assert response.json()["consumer"]["status"] == expected


def test_monitoring_endpoint_does_not_infer_crash_when_monitoring_never_initialized():
    store = FakeMetricsStore(monitoring_snapshot=None, heartbeat_fresh=None)
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/monitoring")

    assert response.status_code == 200
    assert response.json()["consumer"]["status"] == "unavailable"
    assert response.json()["consumer"]["reason"] == "not_initialized"
    assert response.json()["consumer"]["last_heartbeat"] is None


def test_monitoring_endpoint_reports_redis_unavailable_without_returning_503():
    store = FakeMetricsStore(monitoring_error=RedisMonitoringError("Could not read consumer monitoring state"))
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/monitoring")

    assert response.status_code == 200
    assert response.json()["api"]["redis_status"] == "unavailable"
    assert response.json()["consumer"]["status"] == "unavailable"
    assert response.json()["consumer"]["reason"] == "redis_unavailable"


def test_monitoring_endpoint_reports_invalid_snapshot_as_monitoring_unavailable():
    store = FakeMetricsStore(monitoring_error=RedisMonitoringDataError("Stored consumer monitoring state is invalid"))
    with TestClient(create_app(publisher=FakePublisher(), metrics_store=store)) as client:
        response = client.get("/api/monitoring")

    assert response.status_code == 200
    assert response.json()["api"]["redis_status"] == "available"
    assert response.json()["consumer"]["status"] == "unavailable"
    assert response.json()["consumer"]["reason"] == "invalid_snapshot"


def test_monitoring_endpoint_reports_queue_count_failure_without_exposing_exception():
    publisher = FakePublisher(inspect_fail=True, queue="events")
    with TestClient(create_app(publisher=publisher, metrics_store=FakeMetricsStore())) as client:
        response = client.get("/api/monitoring")

    assert response.status_code == 200
    data = response.json()
    assert data["rabbitmq"]["status"] == "unavailable"
    assert data["rabbitmq"]["queue_name"] == "events"
    assert data["rabbitmq"]["messages_ready"] is None
    assert data["rabbitmq"]["consumer_count"] is None
    assert "fake" not in response.text.lower()


def test_monitoring_response_does_not_expose_urls_or_credentials_and_health_contract_is_unchanged():
    publisher = FakePublisher(queue_info={"messages_ready": 0, "consumer_count": 0})
    store = FakeMetricsStore(monitoring_snapshot=make_consumer_snapshot(), heartbeat_fresh=True)
    with TestClient(create_app(publisher=publisher, metrics_store=store)) as client:
        monitoring_response = client.get("/api/monitoring")
        health_response = client.get("/api/health")

    assert monitoring_response.status_code == 200
    assert "amqps://" not in monitoring_response.text
    assert "rabbit-host" not in monitoring_response.text
    assert "secret" not in monitoring_response.text
    assert health_response.json() == {
        "status": "available",
        "services": {"rabbitmq": "not_verified", "redis": "not_verified"},
    }


def test_monitoring_endpoint_does_not_change_metrics_or_events_endpoint_contracts():
    publisher = FakePublisher(queue_info={"messages_ready": 0, "consumer_count": 1})
    store = FakeMetricsStore(monitoring_snapshot=make_consumer_snapshot(), heartbeat_fresh=True)
    with TestClient(create_app(publisher=publisher, metrics_store=store)) as client:
        monitoring = client.get("/api/monitoring")
        metrics = client.get("/api/metrics")
        events = client.get("/api/events")

    assert monitoring.status_code == 200
    assert metrics.json() == {
        "tumbling": {"count": 2, "sum": 3.0, "average": 1.5, "minimum": 1.0, "maximum": 2.0},
        "sliding": None,
    }
    assert events.json() == {"source": "in_memory", "count": 0, "events": []}

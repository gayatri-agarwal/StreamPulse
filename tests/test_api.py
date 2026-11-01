import json
from datetime import datetime, timezone

from fastapi.testclient import TestClient
import pytest

from app.main import create_app
from app.messaging.rabbitmq import RabbitMQError
from app.processing.metrics import Metrics
from app.storage.redis_metrics import RedisStorageError


class FakePublisher:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail

    async def publish(self, event):
        if self.fail:
            raise RabbitMQError("Could not publish library event to RabbitMQ")
        self.events.append(event)


class FakeMetricsStore:
    def __init__(self, fail=False):
        self.fail = fail
        self.metrics = {
            "tumbling": Metrics(2, 3.0, 1.5, 1.0, 2.0),
            "sliding": None,
        }
        self.requested = []

    async def get_metrics(self, window_type):
        self.requested.append(window_type)
        if self.fail:
            raise RedisStorageError("Could not retrieve metrics to Redis")
        return self.metrics[window_type]


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

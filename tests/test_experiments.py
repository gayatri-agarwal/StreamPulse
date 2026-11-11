import asyncio
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from app.main import create_app
from app.processing.measurements import MeasurementSummary
from app.experiments.schemas import RunStatus
from app.experiments.store import (
    ExecutionClaimConflict, ExecutionProgressConflict, ExperimentRepository,
    ExperimentStorageError, InvalidRunConfiguration, InvalidRunWorkloadConfiguration,
    InvalidRunTransition, RunEventNotFound, RunNotFound, utc_now,
)
from app.experiments.runner import EventPublishResult, ExperimentWorkloadRunner
from app.messaging.rabbitmq import RabbitMQConsumer, RabbitMQSettings, RabbitMQPublishError
from app.processing.event_processor import LibraryEventProcessor
from app.processing.window_config import WindowConfiguration
from app.processing.window_manager import SlidingWindow, TumblingWindow


def measurement_summary(*, present=True, unique_successes=3, attempts=4, published=4):
    return MeasurementSummary(
        publish_attempts=published,
        publish_successes=published,
        publish_failures=0,
        publish_failure_rate=0.0 if published else None,
        consumer_attempts=attempts,
        consumer_successes=attempts,
        consumer_failures=0,
        consumer_failure_rate=0.0 if attempts else None,
        throughput_events_per_second=None,
        sample_count={"consumer_processing_latency_ms": int(present)},
        average_latency_ms={"consumer_processing_latency_ms": 2.5 if present else None},
        p95_latency_ms={"consumer_processing_latency_ms": 3.5 if present else None},
        consumer_unique_successes=unique_successes,
        inter_arrival_rate_events_per_second=None,
        measurements_present=present,
    )


class MeasurementStore:
    def __init__(self, summary=None, fail=False):
        self.summary = summary or measurement_summary(present=False, unique_successes=0, attempts=0, published=0)
        self.fail = fail
        self.requested_runs = []

    async def get_measurement_summary(self, run_id=None):
        self.requested_runs.append(run_id)
        if self.fail:
            raise RuntimeError("measurement store unavailable")
        return self.summary


def create_experiment(client, *, name="Window run", configuration=None):
    body = {"name": name, "experiment_type": "window_performance"}
    if configuration is not None:
        body["default_configuration"] = configuration
    response = client.post("/api/experiments", json=body)
    assert response.status_code == 201, response.text
    return response.json()


def create_run(client, experiment_id, **body):
    response = client.post(f"/api/experiments/{experiment_id}/runs", json=body)
    assert response.status_code == 201, response.text
    return response.json()


def record_run_event_outcomes(client, run_id, *, successes, failures=0):
    repository = client.app.state.experiment_store
    for index in range(successes + failures):
        event_id = str(uuid4())
        repository.reserve_run_event(run_id, event_id)
        repository.mark_run_event_published(run_id, event_id)
        repository.settle_run_event(run_id, event_id, succeeded=index < successes)


def record_generated_progress(client, run_id, generated, *, publish_confirmed=0):
    repository = client.app.state.experiment_store
    claim = repository.claim_run_execution(run_id)
    repository.update_run_execution_progress(
        run_id, claim["owner_token"], operation_id=str(uuid4()), generated=generated,
        publish_confirmed=publish_confirmed,
    )
    repository.release_run_execution_claim(run_id, claim["owner_token"])


def finalize_requested_run(client, experiment_id, run_id):
    import asyncio

    run_url = f"/api/experiments/{experiment_id}/runs/{run_id}"
    client.post(f"{run_url}/complete")
    run = client.app.state.experiment_store.get_run(experiment_id, run_id)
    if run["result_available"]:
        return run
    return asyncio.run(client.app.state.finalize_experiment_run(experiment_id, run_id))


class RunSummaryStore:
    def __init__(self):
        self.global_summaries = {}
        self.run_summaries = {}
        self.deliveries = []
        self.aggregation_write_status = "available"
        self.write_started = None
        self.write_release = None
        self.order = None
        self.active_configuration = None
        self.pending_configuration = None

    async def save_metrics(self, window_type, metrics):
        self.global_summaries[window_type] = metrics

    async def save_run_metrics(self, run_id, window_type, metrics):
        if self.order is not None:
            self.order.append("process")
        if self.write_started is not None:
            self.write_started.set()
            await self.write_release.wait()
        self.run_summaries[(run_id, window_type)] = metrics

    async def record_consumer_delivery(self, **delivery):
        self.deliveries.append(delivery)

    async def read_pending_configuration(self):
        return self.pending_configuration

    async def promote_pending_configuration(self):
        if self.pending_configuration is None:
            return None
        self.active_configuration = self.pending_configuration
        self.pending_configuration = None
        return self.active_configuration


class RunDelivery:
    def __init__(self, run_id=None, event_id="delivery"):
        self.body = json.dumps({
            "event_id": event_id,
            "event_type": "BOOK_ISSUED",
            "student_id": "student-1",
            "book_id": "book-1",
            "timestamp": "2026-01-01T00:00:00+00:00",
            **({"run_id": run_id} if run_id is not None else {}),
        }).encode()
        self.acks = 0
        self.rejects = []
        self.ack_started = None
        self.ack_release = None
        self.order = None

    async def ack(self):
        if self.order is not None:
            self.order.append("ack")
        if self.ack_started is not None:
            self.ack_started.set()
            await self.ack_release.wait()
        self.acks += 1

    async def reject(self, requeue):
        self.rejects.append(requeue)


def start_test_run(repository, configuration=None, workload_configuration=None):
    experiment = repository.create_experiment({
        "name": "Processor routing", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 5, "sliding_size": 5, "sliding_interval": 1},
    })
    run = repository.create_run(
        experiment["experiment_id"], configuration=configuration,
        workload_configuration=workload_configuration,
    )
    repository.transition_run(
        experiment["experiment_id"], run["run_id"], RunStatus.RUNNING
    )
    return experiment, run


def record_successful_workload(client, experiment_id, run_id, *, keep_claim=False):
    repository = client.app.state.experiment_store
    run = repository.get_run(experiment_id, run_id)
    planned = run["progress"]["planned"]
    claim = repository.claim_run_execution(run_id)
    for _ in range(planned):
        event_id = str(uuid4())
        repository.update_run_execution_progress(
            run_id, claim["owner_token"], operation_id=str(uuid4()), generated=1,
        )
        repository.reserve_run_event(run_id, event_id)
        repository.mark_run_event_published(run_id, event_id)
        repository.update_run_execution_progress(
            run_id, claim["owner_token"], operation_id=str(uuid4()), publish_confirmed=1,
        )
        repository.claim_run_event(run_id, event_id)
        repository.settle_run_event(run_id, event_id, succeeded=True)
    if not keep_claim:
        repository.release_run_execution_claim(run_id, claim["owner_token"])
    return claim


async def deliver_scoped_event(consumer, repository, run_id, event_id):
    repository.reserve_run_event(run_id, event_id)
    repository.mark_run_event_published(run_id, event_id)
    message = RunDelivery(run_id, event_id)
    await consumer.handle_message(message)
    return message


@pytest.mark.asyncio
async def test_consumer_routes_dashboard_and_interleaved_runs_to_reused_isolated_processors(tmp_path):
    repository = ExperimentRepository(tmp_path / "processor-routing.sqlite3")
    repository.initialize()
    config_a = {"tumbling_size": 5, "sliding_size": 5, "sliding_interval": 1}
    config_b = {"tumbling_size": 8, "sliding_size": 10, "sliding_interval": 2}
    _, run_a = start_test_run(repository, config_a)
    _, run_b = start_test_run(repository, config_b)
    store = RunSummaryStore()
    dashboard_configuration = WindowConfiguration(5, 5, 1)
    dashboard_pending = WindowConfiguration(8, 10, 2)
    store.active_configuration = dashboard_configuration
    store.pending_configuration = dashboard_pending
    dashboard_processor = LibraryEventProcessor(
        TumblingWindow(5), SlidingWindow(5), store, dashboard_configuration
    )
    consumer = RabbitMQConsumer(
        dashboard_processor, RabbitMQSettings("amqp://unused"),
        experiment_repository=repository,
    )

    dashboard_delivery = RunDelivery(event_id="dashboard-event")
    await consumer.handle_message(dashboard_delivery)
    assert dashboard_delivery.acks == 1
    assert dashboard_processor.tumbling.pending_count == 1

    for index in range(1, 6):
        delivery_a = await deliver_scoped_event(
            consumer, repository, run_a["run_id"], f"a-{index}"
        )
        delivery_b = await deliver_scoped_event(
            consumer, repository, run_b["run_id"], f"b-{index}"
        )
        assert delivery_a.acks == delivery_b.acks == 1

    manager = consumer._run_processor_manager
    assert await manager.context_count() == 2
    context_a = manager._contexts[run_a["run_id"]]
    context_b = manager._contexts[run_b["run_id"]]
    assert context_a.processor is not context_b.processor
    assert context_a.configuration == WindowConfiguration(5, 5, 1)
    assert context_b.configuration == WindowConfiguration(8, 10, 2)
    assert context_a.processor.tumbling.pending_count == 0
    assert context_b.processor.tumbling.pending_count == 5
    assert context_a.processor.sliding.event_count == 5
    assert context_b.processor.sliding.event_count == 5
    assert dashboard_processor.tumbling.pending_count == 1
    assert dashboard_processor.sliding.event_count == 1
    assert store.active_configuration == dashboard_configuration
    assert store.pending_configuration == dashboard_pending
    assert store.run_summaries[(run_a["run_id"], "tumbling")].count == 5
    assert (run_b["run_id"], "tumbling") not in store.run_summaries
    assert store.global_summaries["sliding"].count == 1
    assert all(key[0] in {run_a["run_id"], run_b["run_id"]} for key in store.run_summaries)


@pytest.mark.asyncio
async def test_run_consumer_preserves_claim_process_receipt_ack_order(tmp_path):
    order = []

    class OrderedRepository(ExperimentRepository):
        def claim_run_event(self, run_id, event_id):
            order.append("claim")
            return super().claim_run_event(run_id, event_id)

        def settle_run_event(self, run_id, event_id, succeeded):
            order.append("receipt")
            return super().settle_run_event(run_id, event_id, succeeded)

    class OrderedStore(RunSummaryStore):
        async def save_run_metrics(self, run_id, window_type, metrics):
            order.append("process")
            await super().save_run_metrics(run_id, window_type, metrics)

    class OrderedDelivery(RunDelivery):
        async def ack(self):
            order.append("ack")
            await super().ack()

    repository = OrderedRepository(tmp_path / "run-order.sqlite3")
    repository.initialize()
    _, run = start_test_run(repository)
    repository.reserve_run_event(run["run_id"], "ordered-event")
    repository.mark_run_event_published(run["run_id"], "ordered-event")
    store = OrderedStore()
    dashboard = LibraryEventProcessor(TumblingWindow(5), SlidingWindow(5), store)
    consumer = RabbitMQConsumer(
        dashboard, RabbitMQSettings("amqp://unused"), experiment_repository=repository
    )

    message = OrderedDelivery(run["run_id"], "ordered-event")
    await consumer.handle_message(message)

    assert order == ["claim", "process", "receipt", "ack"]
    assert message.acks == 1 and message.rejects == []


@pytest.mark.asyncio
async def test_invalid_and_missing_run_configuration_never_uses_dashboard_processor(tmp_path):
    repository = ExperimentRepository(tmp_path / "invalid-run-routing.sqlite3")
    repository.initialize()
    _, run = start_test_run(repository)
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET configuration_snapshot = ? WHERE run_id = ?",
            ('{"tumbling_size": 7,"sliding_size": 5,"sliding_interval": 1}', run["run_id"]),
        )
    repository.reserve_run_event(run["run_id"], "invalid-config-event")
    repository.mark_run_event_published(run["run_id"], "invalid-config-event")
    store = RunSummaryStore()
    dashboard = LibraryEventProcessor(TumblingWindow(5), SlidingWindow(5), store)
    consumer = RabbitMQConsumer(
        dashboard, RabbitMQSettings("amqp://unused"), experiment_repository=repository
    )

    invalid_delivery = RunDelivery(run["run_id"], "invalid-config-event")
    await consumer.handle_message(invalid_delivery)
    missing_delivery = RunDelivery(str(uuid4()), "missing-run-event")
    await consumer.handle_message(missing_delivery)

    assert invalid_delivery.acks == 0 and invalid_delivery.rejects == [False]
    assert missing_delivery.acks == 0 and missing_delivery.rejects == [False]
    assert dashboard.tumbling.pending_count == 0
    assert dashboard.sliding.event_count == 0
    assert await consumer._run_processor_manager.context_count() == 0
    accounting = repository.run_event_accounting(run["run_id"])
    assert accounting["consumer_processed_failures"] == 1


@pytest.mark.asyncio
async def test_transient_snapshot_lookup_failure_requeues_before_reservation_claim():
    class TransientRepository:
        def __init__(self):
            self.claim_calls = 0

        def get_run_with_configuration_snapshot(self, run_id):
            raise ExperimentStorageError("temporary sqlite read failure")

        def claim_run_event(self, run_id, event_id):
            self.claim_calls += 1
            return True

    repository = TransientRepository()
    store = RunSummaryStore()
    dashboard = LibraryEventProcessor(TumblingWindow(5), SlidingWindow(5), store)
    consumer = RabbitMQConsumer(
        dashboard, RabbitMQSettings("amqp://unused"), experiment_repository=repository
    )
    message = RunDelivery(str(uuid4()), "transient-lookup")

    await consumer.handle_message(message)

    assert message.acks == 0 and message.rejects == [True]
    assert repository.claim_calls == 0
    assert dashboard.tumbling.pending_count == 0
    assert await consumer._run_processor_manager.context_count() == 0


@pytest.mark.asyncio
async def test_terminal_cleanup_waits_for_processing_lease_and_receipt_drain(tmp_path):
    repository = ExperimentRepository(tmp_path / "processor-cleanup.sqlite3")
    repository.initialize()
    experiment, run = start_test_run(
        repository, workload_configuration={"target_event_count": 1, "publish_interval_ms": 10},
    )
    store = RunSummaryStore()
    store.write_started = asyncio.Event()
    store.write_release = asyncio.Event()
    dashboard = LibraryEventProcessor(TumblingWindow(5), SlidingWindow(5), store)
    consumer = RabbitMQConsumer(
        dashboard, RabbitMQSettings("amqp://unused"), experiment_repository=repository
    )
    repository.reserve_run_event(run["run_id"], "cleanup-event")
    repository.mark_run_event_published(run["run_id"], "cleanup-event")
    execution_claim = repository.claim_run_execution(run["run_id"])
    repository.update_run_execution_progress(
        run["run_id"], execution_claim["owner_token"],
        operation_id=str(uuid4()), generated=1,
    )
    repository.update_run_execution_progress(
        run["run_id"], execution_claim["owner_token"],
        operation_id=str(uuid4()), publish_confirmed=1,
    )
    repository.release_run_execution_claim(run["run_id"], execution_claim["owner_token"])
    message = RunDelivery(run["run_id"], "cleanup-event")
    task = asyncio.create_task(consumer.handle_message(message))
    await store.write_started.wait()

    assert repository.close_run_event_ingestion(
        experiment["experiment_id"], run["run_id"]
    ) == 1
    assert await consumer._run_processor_manager.cleanup_terminal() == 0
    assert await consumer._run_processor_manager.context_count() == 1

    store.write_release.set()
    await task
    assert message.acks == 1
    assert repository.close_run_event_ingestion(
        experiment["experiment_id"], run["run_id"]
    ) == 0
    repository.request_run_stop(
        experiment["experiment_id"], run["run_id"], RunStatus.COMPLETED,
    )
    repository.finalize_run_after_drain(
        experiment["experiment_id"], run["run_id"], ended_at=utc_now(),
        results={"event_accounting": repository.run_event_accounting(run["run_id"])},
    )
    assert await consumer._run_processor_manager.cleanup_terminal() == 1
    assert await consumer._run_processor_manager.context_count() == 0


@pytest.mark.asyncio
async def test_cancelled_handler_releases_run_processor_lease_in_finally(tmp_path):
    repository = ExperimentRepository(tmp_path / "processor-lease-cancel.sqlite3")
    repository.initialize()
    _, run = start_test_run(repository)
    store = RunSummaryStore()
    store.write_started = asyncio.Event()
    store.write_release = asyncio.Event()
    dashboard = LibraryEventProcessor(TumblingWindow(5), SlidingWindow(5), store)
    consumer = RabbitMQConsumer(
        dashboard, RabbitMQSettings("amqp://unused"), experiment_repository=repository,
    )
    repository.reserve_run_event(run["run_id"], "cancelled-lease-event")
    repository.mark_run_event_published(run["run_id"], "cancelled-lease-event")
    message = RunDelivery(run["run_id"], "cancelled-lease-event")
    task = asyncio.create_task(consumer.handle_message(message))
    await store.write_started.wait()

    context = consumer._run_processor_manager._contexts[run["run_id"]]
    assert context.users == 1
    assert consumer.active_handler_count == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert context.users == 0
    assert consumer.active_handler_count == 0
    assert message.acks == 0
    assert await consumer._run_processor_manager.context_count() == 1


@pytest.mark.asyncio
async def test_subscription_cancel_timeout_keeps_run_processor_context_cached(tmp_path):
    repository = ExperimentRepository(tmp_path / "processor-drain-timeout.sqlite3")
    repository.initialize()
    _, run = start_test_run(repository)
    store = RunSummaryStore()
    store.write_started = asyncio.Event()
    store.write_release = asyncio.Event()
    dashboard = LibraryEventProcessor(TumblingWindow(5), SlidingWindow(5), store)
    consumer = RabbitMQConsumer(
        dashboard, RabbitMQSettings("amqp://unused"),
        experiment_repository=repository, drain_timeout_seconds=0.02,
    )
    repository.reserve_run_event(run["run_id"], "timeout-context-event")
    repository.mark_run_event_published(run["run_id"], "timeout-context-event")
    message = RunDelivery(run["run_id"], "timeout-context-event")
    handler = asyncio.create_task(consumer.handle_message(message))
    await store.write_started.wait()
    context = consumer._run_processor_manager._contexts[run["run_id"]]
    assert context.users == 1

    class StalledQueue:
        async def cancel(self, tag, timeout=None):
            await asyncio.Future()

    class Transport:
        def __init__(self):
            self.closed = False
            self.close_calls = 0

        async def close(self):
            self.close_calls += 1
            self.closed = True

    consumer._queue = StalledQueue()
    consumer._consumer_tag = "stalled-tag"
    consumer._channel = Transport()
    consumer._connection = Transport()
    channel = consumer._channel
    connection = consumer._connection

    result = await consumer.close()
    with pytest.raises(asyncio.CancelledError):
        await handler

    assert result.timed_out is True
    assert result.subscription_cancelled is False
    assert result.handlers_drained is False
    assert connection.closed
    assert channel.close_calls == 0
    assert context.users == 0
    assert await consumer._run_processor_manager.cleanup_terminal() == 0
    assert await consumer._run_processor_manager.context_count() == 1
    assert message.acks == 0


def test_experiment_crud_listing_and_configuration_snapshot_isolation(tmp_path):
    database = tmp_path / "experiments.sqlite3"
    with TestClient(create_app(experiment_db_path=database)) as client:
        created = create_experiment(
            client,
            configuration={"tumbling_size": 8, "sliding_size": 5, "sliding_interval": 2},
        )
        listed = client.get("/api/experiments")
        detail = client.get(f"/api/experiments/{created['experiment_id']}")
        run = create_run(client, created["experiment_id"])
        updated = client.patch(f"/api/experiments/{created['experiment_id']}", json={
            "name": "Updated window run",
            "default_configuration": {"tumbling_size": 10, "sliding_size": 8, "sliding_interval": 3},
        })
        fetched_run = client.get(
            f"/api/experiments/{created['experiment_id']}/runs/{run['run_id']}"
        )

    assert listed.json()["count"] == 1
    assert detail.json()["default_configuration"] == {
        "tumbling_size": 8, "sliding_size": 5, "sliding_interval": 2,
    }
    assert updated.json()["name"] == "Updated window run"
    assert fetched_run.json()["configuration_snapshot"] == {
        "tumbling_size": 8, "sliding_size": 5, "sliding_interval": 2,
    }


def test_experiment_and_run_records_survive_application_reinitialization(tmp_path):
    database = tmp_path / "persistent.sqlite3"
    first_app = create_app(experiment_db_path=database)
    with TestClient(first_app) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"], workload_configuration={"target_event_count": 1})
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        assert client.post(f"{run_url}/start").status_code == 200
        record_successful_workload(client, experiment["experiment_id"], run["run_id"])
        stop_request = client.post(f"{run_url}/complete")
        assert stop_request.status_code == 202

    second_app = create_app(experiment_db_path=database)
    with TestClient(second_app) as client:
        loaded_experiment = client.get(f"/api/experiments/{experiment['experiment_id']}")
        loaded_run = client.get(
            f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        )
        for _ in range(100):
            if loaded_run.json()["result_available"]:
                break
            time.sleep(0.01)
            loaded_run = client.get(
                f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
            )
    assert loaded_experiment.status_code == 200
    assert loaded_run.status_code == 200
    assert loaded_run.json()["status"] == "COMPLETED"
    assert loaded_run.json()["stop_intent"] == stop_request.json()["stop_intent"]
    assert loaded_run.json()["event_ingestion_closed"] is True
    assert loaded_run.json()["result_available"] is True


def test_run_snapshot_lookup_by_global_run_id_validates_configuration(tmp_path):
    repository = ExperimentRepository(tmp_path / "snapshot-lookup.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Snapshot lookup", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 8, "sliding_size": 5, "sliding_interval": 2},
    })
    run = repository.create_run(
        experiment["experiment_id"],
    )

    loaded = repository.get_run_with_configuration_snapshot(run["run_id"])
    assert loaded["run_id"] == run["run_id"]
    assert loaded["experiment_id"] == experiment["experiment_id"]
    assert loaded["configuration_snapshot"] == {
        "tumbling_size": 8, "sliding_size": 5, "sliding_interval": 2,
    }
    with pytest.raises(RunNotFound):
        repository.get_run_with_configuration_snapshot(str(uuid4()))

    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET configuration_snapshot = ? WHERE run_id = ?",
            ('{"tumbling_size": 7,"sliding_size": 5,"sliding_interval": 1}', run["run_id"]),
        )
    with pytest.raises(InvalidRunConfiguration):
        repository.get_run_with_configuration_snapshot(run["run_id"])


def test_run_snapshot_lookup_reports_sqlite_failure_separately(tmp_path):
    repository = ExperimentRepository(tmp_path / "missing-parent" / "experiments.sqlite3")
    with pytest.raises(ExperimentStorageError):
        repository.get_run_with_configuration_snapshot(str(uuid4()))


def test_run_lifecycle_valid_transitions_and_terminal_guards(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "lifecycle.sqlite3")) as client:
        experiment = create_experiment(client)
        experiment_id = experiment["experiment_id"]
        created = create_run(client, experiment_id, workload_configuration={"target_event_count": 1})
        run_url = f"/api/experiments/{experiment_id}/runs/{created['run_id']}"
        assert client.post(f"{run_url}/complete").status_code == 409
        started = client.post(f"{run_url}/start")
        record_successful_workload(client, experiment_id, created["run_id"])
        completed = client.post(f"{run_url}/complete")
        assert started.json()["status"] == "RUNNING"
        assert completed.status_code == 202
        assert completed.json()["status"] == "RUNNING"
        assert completed.json()["drain_state"] == "REQUESTED"
        assert completed.json()["stop_intent"]["target"] == "COMPLETED"
        assert completed.json()["event_ingestion_closed"] is True
        assert client.post(f"{run_url}/complete").json()["already_requested"] is True
        assert client.post(f"{run_url}/cancel").status_code == 409
        assert client.post(f"{run_url}/start").status_code == 409

        failed_run = create_run(client, experiment_id)
        failed_url = f"/api/experiments/{experiment_id}/runs/{failed_run['run_id']}"
        assert client.post(f"{failed_url}/fail", json={"reason": "consumer stopped"}).status_code == 409
        client.post(f"{failed_url}/start")
        failed = client.post(f"{failed_url}/fail", json={"reason": "consumer stopped"})
        assert failed.status_code == 202
        assert failed.json()["status"] == "RUNNING"
        assert failed.json()["stop_intent"]["target"] == "FAILED"
        assert failed.json()["stop_intent"]["reason"] == "consumer stopped"
        assert client.post(f"{failed_url}/complete").status_code == 409

        cancelled_created = create_run(client, experiment_id)
        cancelled_url = f"/api/experiments/{experiment_id}/runs/{cancelled_created['run_id']}"
        cancelled_request = client.post(f"{cancelled_url}/cancel")
        assert cancelled_request.status_code == 202
        assert cancelled_request.json()["status"] == "CREATED"
        assert cancelled_request.json()["stop_intent"]["target"] == "CANCELLED"
        assert client.post(f"{cancelled_url}/start").status_code == 409
        cancelled_running = create_run(client, experiment_id)
        running_url = f"/api/experiments/{experiment_id}/runs/{cancelled_running['run_id']}"
        client.post(f"{running_url}/start")
        cancelled_running_request = client.post(
            f"{running_url}/cancel", json={"reason": "user stopped"},
        )
        assert cancelled_running_request.status_code == 202
        assert cancelled_running_request.json()["status"] == "RUNNING"
        assert cancelled_running_request.json()["stop_intent"]["reason"] == "user stopped"


def test_stop_intent_is_durable_idempotent_and_conflict_checked(tmp_path):
    repository = ExperimentRepository(tmp_path / "stop-intent.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Stop intent", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
    })
    created = repository.create_run(experiment["experiment_id"])

    with pytest.raises(InvalidRunTransition):
        repository.request_run_stop(
            experiment["experiment_id"], created["run_id"], "COMPLETED",
        )
    with pytest.raises(InvalidRunTransition):
        repository.request_run_stop(
            experiment["experiment_id"], created["run_id"], "RUNNING",
        )

    cancelled = repository.request_run_stop(
        experiment["experiment_id"], created["run_id"], RunStatus.CANCELLED,
        reason="requested by operator",
    )
    repeated = repository.request_run_stop(
        experiment["experiment_id"], created["run_id"], RunStatus.CANCELLED,
        reason="different retry text is ignored",
    )
    assert cancelled["run"]["status"] == RunStatus.CREATED.value
    assert cancelled["run"]["event_ingestion_closed"] is True
    assert cancelled["run"]["stop_intent"]["target"] == RunStatus.CANCELLED.value
    assert cancelled["run"]["stop_intent"]["reason"] == "requested by operator"
    assert cancelled["already_requested"] is False
    assert repeated["already_requested"] is True
    assert repeated["run"]["stop_intent"] == cancelled["run"]["stop_intent"]
    assert repository.get_run_stop_intent(created["run_id"]) == cancelled["run"]["stop_intent"]
    with pytest.raises(InvalidRunTransition, match="already has a CANCELLED stop intent"):
        repository.request_run_stop(
            experiment["experiment_id"], created["run_id"], RunStatus.FAILED,
        )

    running = repository.create_run(experiment["experiment_id"])
    repository.transition_run(experiment["experiment_id"], running["run_id"], RunStatus.RUNNING)
    failed = repository.request_run_stop(
        experiment["experiment_id"], running["run_id"], RunStatus.FAILED,
        reason="failure requested",
    )
    assert failed["run"]["status"] == RunStatus.RUNNING.value
    assert failed["run"]["failure_reason"] is None
    assert failed["run"]["stop_intent"]["target"] == RunStatus.FAILED.value
    assert failed["run"]["stop_intent"]["requested_at"]


def test_stop_intent_and_event_reservation_have_serialized_admission(tmp_path):
    repository = ExperimentRepository(tmp_path / "stop-reservation-race.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Stop reservation race", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
    })

    reservation_wins = repository.create_run(experiment["experiment_id"])
    repository.transition_run(
        experiment["experiment_id"], reservation_wins["run_id"], RunStatus.RUNNING,
    )
    accepted_event = str(uuid4())
    repository.reserve_run_event(reservation_wins["run_id"], accepted_event)
    stop_after_reservation = repository.request_run_stop(
        experiment["experiment_id"], reservation_wins["run_id"], RunStatus.CANCELLED,
    )
    assert stop_after_reservation["pending_events"] == 1
    assert repository.claim_run_event(reservation_wins["run_id"], accepted_event) is True
    assert repository.settle_run_event(
        reservation_wins["run_id"], accepted_event, succeeded=True,
    ) is True

    stop_wins = repository.create_run(experiment["experiment_id"])
    repository.transition_run(experiment["experiment_id"], stop_wins["run_id"], RunStatus.RUNNING)
    stop_before_reservation = repository.request_run_stop(
        experiment["experiment_id"], stop_wins["run_id"], RunStatus.CANCELLED,
    )
    assert stop_before_reservation["pending_events"] == 0
    with pytest.raises(InvalidRunTransition):
        repository.reserve_run_event(stop_wins["run_id"], str(uuid4()))


def test_terminal_routes_accept_stop_intent_without_claiming_run_stopped(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "stop-api.sqlite3")) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"], workload_configuration={"target_event_count": 1})
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")
        claim = record_successful_workload(
            client, experiment["experiment_id"], run["run_id"], keep_claim=True,
        )

        first = client.post(f"{run_url}/complete")
        repeated = client.post(f"{run_url}/complete")
        conflict = client.post(f"{run_url}/cancel")
        detail = client.get(run_url)
        progress = client.get(f"{run_url}/execution")

        rejected_event = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b",
            "run_id": run["run_id"],
        })

    assert first.status_code == 202
    assert first.json()["status"] == "RUNNING"
    assert first.json()["drain_state"] == "REQUESTED"
    assert first.json()["execution_state"] == "CLAIMED"
    assert first.json()["stop_intent"]["target"] == "COMPLETED"
    assert "owner_token" not in first.text and claim["owner_token"] not in first.text
    assert repeated.status_code == 202
    assert repeated.json()["already_requested"] is True
    assert conflict.status_code == 409
    assert detail.json()["status"] == "RUNNING"
    assert detail.json()["event_ingestion_closed"] is True
    assert progress.json()["stop_intent"]["target"] == "COMPLETED"
    assert rejected_event.status_code == 409


def test_stop_intent_migration_preserves_existing_run(tmp_path):
    path = tmp_path / "legacy-stop-intent.sqlite3"
    experiment_id = str(uuid4())
    run_id = str(uuid4())
    connection = sqlite3.connect(path)
    connection.executescript("""
        CREATE TABLE experiments (
            experiment_id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT,
            experiment_type TEXT NOT NULL, created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL, default_configuration TEXT NOT NULL
        );
        CREATE TABLE experiment_runs (
            run_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
            status TEXT NOT NULL, configuration_snapshot TEXT NOT NULL, created_at TEXT NOT NULL,
            started_at TEXT, ended_at TEXT, failure_reason TEXT, results_json TEXT,
            event_ingestion_closed INTEGER NOT NULL DEFAULT 0
        );
    """)
    connection.execute(
        "INSERT INTO experiments VALUES (?, ?, ?, ?, ?, ?, ?)",
        (experiment_id, "legacy", None, "window_performance", utc_now(), utc_now(),
         '{"tumbling_size":10,"sliding_size":5,"sliding_interval":1}'),
    )
    connection.execute(
        "INSERT INTO experiment_runs (run_id, experiment_id, status, configuration_snapshot, created_at) "
        "VALUES (?, ?, 'RUNNING', ?, ?)",
        (run_id, experiment_id, '{"tumbling_size":10,"sliding_size":5,"sliding_interval":1}', utc_now()),
    )
    connection.commit()
    connection.close()

    repository = ExperimentRepository(path)
    repository.initialize()
    loaded = repository.get_run(experiment_id, run_id)
    assert loaded["status"] == RunStatus.RUNNING.value
    assert loaded["stop_intent"] is None
    requested = repository.request_run_stop(experiment_id, run_id, RunStatus.CANCELLED)
    assert requested["run"]["status"] == RunStatus.RUNNING.value
    assert requested["run"]["stop_intent"]["target"] == RunStatus.CANCELLED.value


def test_run_ids_are_unique_and_unknown_ids_return_not_found(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "ids.sqlite3")) as client:
        experiment = create_experiment(client)
        experiment_id = experiment["experiment_id"]
        supplied_id = str(uuid4())
        run = create_run(client, experiment_id, run_id=supplied_id)
        duplicate = client.post(f"/api/experiments/{experiment_id}/runs", json={"run_id": supplied_id})
        unknown_experiment = client.get(f"/api/experiments/{uuid4()}")
        unknown_run = client.get(f"/api/experiments/{experiment_id}/runs/{uuid4()}")
        malformed_run = client.post(f"/api/experiments/{experiment_id}/runs", json={"run_id": "bad"})
        other_experiment = create_experiment(client, name="Other")
        wrong_parent = client.get(
            f"/api/experiments/{other_experiment['experiment_id']}/runs/{run['run_id']}"
        )
    assert duplicate.status_code == 409
    assert unknown_experiment.status_code == 404
    assert unknown_run.status_code == 404
    assert malformed_run.status_code == 422
    assert wrong_parent.status_code == 404


def test_run_lists_and_unstarted_cancellation_are_supported(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "lists.sqlite3")) as client:
        experiment = create_experiment(client)
        experiment_id = experiment["experiment_id"]
        first = create_run(client, experiment_id)
        second = create_run(client, experiment_id)
        runs = client.get(f"/api/experiments/{experiment_id}/runs")
        result_response = client.get(
            f"/api/experiments/{experiment_id}/runs/{first['run_id']}/results"
        )
        cancelled = client.post(
            f"/api/experiments/{experiment_id}/runs/{second['run_id']}/cancel"
        )
    assert runs.json()["count"] == 2
    assert result_response.status_code == 409
    assert cancelled.status_code == 202
    assert cancelled.json()["status"] == "CREATED"
    assert cancelled.json()["stop_intent"]["target"] == "CANCELLED"
    assert cancelled.json()["event_ingestion_closed"] is True


def test_completed_run_result_uses_unique_successes_and_run_duration(tmp_path, monkeypatch):
    import app.main as main_module

    timestamps = iter(["2026-11-10T10:00:00+00:00", "2026-11-10T10:00:10+00:00"])
    monkeypatch.setattr(main_module, "utc_now", lambda: next(timestamps, "2026-11-10T10:00:10+00:00"))
    measurement_store = MeasurementStore(measurement_summary(unique_successes=30, attempts=31, published=30))
    with TestClient(create_app(metrics_store=measurement_store, experiment_db_path=tmp_path / "results.sqlite3")) as client:
        experiment = create_experiment(client)
        experiment_id = experiment["experiment_id"]
        run = create_run(client, experiment_id, workload_configuration={"target_event_count": 30})
        run_url = f"/api/experiments/{experiment_id}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")
        record_generated_progress(client, run["run_id"], 30, publish_confirmed=30)
        record_run_event_outcomes(client, run["run_id"], successes=30)
        completed = finalize_requested_run(client, experiment_id, run["run_id"])
        result = client.get(f"{run_url}/results")

    assert measurement_store.requested_runs and all(item == run["run_id"] for item in measurement_store.requested_runs)
    assert completed["results"]["unique_successful_events"] == 30
    assert completed["results"]["duration_seconds"] == 10
    assert completed["results"]["benchmark_throughput_events_per_second"] == 3
    assert completed["results"]["result_complete"] is True
    assert result.json()["results"] == completed["results"]
    assert completed["results"]["latency_complete"] is True


def test_zero_duration_and_missing_measurements_are_incomplete(tmp_path, monkeypatch):
    import app.main as main_module

    same_time = "2026-11-10T10:00:00+00:00"
    monkeypatch.setattr(main_module, "utc_now", lambda: same_time)
    measurements = MeasurementStore(measurement_summary(present=False, unique_successes=0, attempts=0, published=0))
    with TestClient(create_app(metrics_store=measurements, experiment_db_path=tmp_path / "missing.sqlite3")) as client:
        experiment = create_experiment(client)
        experiment_id = experiment["experiment_id"]
        run = create_run(client, experiment_id, workload_configuration={"target_event_count": 1})
        url = f"/api/experiments/{experiment_id}/runs/{run['run_id']}"
        client.post(f"{url}/start")
        record_successful_workload(client, experiment_id, run["run_id"])
        completed = finalize_requested_run(client, experiment_id, run["run_id"])
    result = completed["results"]
    assert result["duration_seconds"] is None
    assert result["measurements_available"] is False
    assert result["unique_successful_events"] is None
    assert result["benchmark_throughput_events_per_second"] is None
    assert result["result_complete"] is False
    assert len(result["incomplete_reasons"]) >= 2


def test_unavailable_measurements_produce_persisted_incomplete_result(tmp_path, monkeypatch):
    import app.main as main_module

    timestamps = iter(["2026-11-10T10:00:00+00:00", "2026-11-10T10:00:05+00:00"])
    monkeypatch.setattr(main_module, "utc_now", lambda: next(timestamps, "2026-11-10T10:00:05+00:00"))
    with TestClient(create_app(metrics_store=MeasurementStore(fail=True), experiment_db_path=tmp_path / "unavailable.sqlite3")) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"], workload_configuration={"target_event_count": 1})
        url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{url}/start")
        record_successful_workload(client, experiment["experiment_id"], run["run_id"])
        client.post(f"{url}/complete")
        result = client.get(f"{url}/results")
        if result.status_code == 409:
            import asyncio
            asyncio.run(client.app.state.finalize_experiment_run(experiment["experiment_id"], run["run_id"]))
            result = client.get(f"{url}/results")
    assert result.status_code == 200
    assert result.json()["results"]["result_complete"] is False
    assert result.json()["results"]["measurements"] is None
    assert result.json()["results"]["latency_complete"] is False
    assert result.status_code == 200
    assert any(
        "unavailable" in reason.lower()
        for reason in result.json()["results"]["incomplete_reasons"]
    )


def test_run_scoped_events_require_running_run_and_terminal_runs_reject_events(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, event):
            self.events.append(event)

    publisher = Publisher()
    with TestClient(create_app(publisher=publisher, experiment_db_path=tmp_path / "ingestion.sqlite3")) as client:
        experiment = create_experiment(client)
        experiment_id = experiment["experiment_id"]
        event_url = "/api/events"
        unknown = client.post(event_url, json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": str(uuid4()),
        })
        created = create_run(client, experiment_id)
        created_event = client.post(event_url, json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": created["run_id"],
        })
        client.post(f"/api/experiments/{experiment_id}/runs/{created['run_id']}/start")
        accepted = client.post(event_url, json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": created["run_id"],
        })

        completed = create_run(client, experiment_id, workload_configuration={"target_event_count": 1})
        completed_url = f"/api/experiments/{experiment_id}/runs/{completed['run_id']}"
        client.post(f"{completed_url}/start")
        record_successful_workload(client, experiment_id, completed["run_id"])
        assert client.post(f"{completed_url}/complete").status_code == 202
        asyncio.run(client.app.state.finalize_experiment_run(experiment_id, completed["run_id"]))

        failed = create_run(client, experiment_id)
        failed_url = f"/api/experiments/{experiment_id}/runs/{failed['run_id']}"
        client.post(f"{failed_url}/start")
        assert client.post(f"{failed_url}/fail", json={"reason": "controlled"}).status_code == 202
        asyncio.run(client.app.state.finalize_experiment_run(experiment_id, failed["run_id"]))

        cancelled = create_run(client, experiment_id)
        cancelled_url = f"/api/experiments/{experiment_id}/runs/{cancelled['run_id']}"
        assert client.post(f"{cancelled_url}/cancel").status_code == 202
        asyncio.run(client.app.state.finalize_experiment_run(experiment_id, cancelled["run_id"]))

        terminal_responses = [
            client.post(event_url, json={
                "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": run_id,
            })
            for run_id in (completed["run_id"], failed["run_id"], cancelled["run_id"])
        ]
    assert unknown.status_code == 404
    assert created_event.status_code == 409
    assert accepted.status_code == 202
    assert [response.status_code for response in terminal_responses] == [409, 409, 409]
    assert len(publisher.events) == 1


def test_batch_run_id_validation_and_unscoped_batch_compatibility(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, event):
            self.events.append(event)

    publisher = Publisher()
    with TestClient(create_app(publisher=publisher, experiment_db_path=tmp_path / "batch-ingestion.sqlite3")) as client:
        unscoped = client.post("/api/events/batch", json={"batch_size": 5})
        experiment = create_experiment(client)
        unknown = client.post("/api/events/batch", json={
            "batch_size": 5, "run_id": str(uuid4()),
        })
        created = create_run(client, experiment["experiment_id"])
        rejected = client.post("/api/events/batch", json={
            "batch_size": 5, "run_id": created["run_id"],
        })
        client.post(f"/api/experiments/{experiment['experiment_id']}/runs/{created['run_id']}/start")
        scoped = client.post("/api/events/batch", json={
            "batch_size": 5, "run_id": created["run_id"],
        })
        pending = client.app.state.experiment_store.close_run_event_ingestion(
            experiment["experiment_id"], created["run_id"]
        )
    assert unscoped.status_code == 200 and unscoped.json()["published"] == 5
    assert unknown.status_code == 404
    assert rejected.status_code == 409
    assert scoped.status_code == 200 and scoped.json()["published"] == 5
    assert pending == 5
    assert all(event.run_id == created["run_id"] for event in publisher.events[5:])


def test_completion_closes_ingestion_and_waits_for_accepted_event_processing(tmp_path):
    class Publisher:
        def __init__(self):
            self.entered = Event()
            self.release = Event()
            self.events = []

        async def publish(self, event):
            self.entered.set()
            await asyncio.to_thread(self.release.wait, 5)
            self.events.append(event)

    class MetricsStore:
        def __init__(self):
            self.publish_successes = 0
            self.consumer_attempts = 0
            self.consumer_successes = 0
            self.unique_successes = 0

        async def record_measurements(self, *, counters, **kwargs):
            self.publish_successes += counters.get("publish_successes", 0)

        async def save_run_metrics(self, run_id, window_type, metrics):
            pass

        async def record_consumer_delivery(self, *, succeeded, **kwargs):
            self.consumer_attempts += 1
            self.consumer_successes += int(succeeded)
            self.unique_successes += int(succeeded)

        async def get_measurement_summary(self, run_id=None):
            return measurement_summary(
                unique_successes=self.unique_successes,
                attempts=self.consumer_attempts,
                published=self.publish_successes,
            )

    class Processor:
        def __init__(self, store):
            self.metrics_store = store

        async def process(self, event):
            return {}

    class Message:
        def __init__(self, body):
            self.body = body
            self.acked = False
            self.rejected = None

        async def ack(self):
            self.acked = True

        async def reject(self, requeue):
            self.rejected = requeue

    publisher = Publisher()
    metrics = MetricsStore()
    app = create_app(publisher=publisher, metrics_store=metrics, experiment_db_path=tmp_path / "race.sqlite3")
    with TestClient(app) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"], workload_configuration={"target_event_count": 1})
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")
        request_body = {
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": run["run_id"],
        }
        with ThreadPoolExecutor(max_workers=1) as executor:
            submit_future = executor.submit(client.post, "/api/events", json=request_body)
            assert publisher.entered.wait(5)
            first_completion = client.post(f"{run_url}/cancel")
            denied_after_close = client.post("/api/events", json=request_body)
            publisher.release.set()
            accepted = submit_future.result(timeout=5)

        assert first_completion.status_code == 202
        assert first_completion.json()["status"] == "RUNNING"
        assert first_completion.json()["drain_state"] == "REQUESTED"
        assert first_completion.json()["pending_events"] == 1
        assert denied_after_close.status_code == 409
        assert accepted.status_code == 202

        consumer = RabbitMQConsumer(
            Processor(metrics),
            RabbitMQSettings("amqp://unused"),
            experiment_repository=app.state.experiment_store,
        )
        message = Message(json.dumps({
            "event_id": publisher.events[0].event_id,
            "event_type": publisher.events[0].event_type.value,
            "student_id": "s", "book_id": "b",
            "timestamp": publisher.events[0].timestamp.isoformat(),
            "run_id": run["run_id"],
        }).encode())
        asyncio.run(consumer.handle_message(message))
        completed = client.post(f"{run_url}/cancel")
        assert message.acked is True
        assert completed.status_code == 202
        assert completed.json()["status"] == "RUNNING"
        assert completed.json()["pending_events"] == 0
        finalized = app.state.experiment_store.get_run(experiment["experiment_id"], run["run_id"])
        for _ in range(100):
            if finalized["status"] == RunStatus.CANCELLED.value:
                break
            time.sleep(0.01)
            finalized = app.state.experiment_store.get_run(
                experiment["experiment_id"], run["run_id"],
            )
    assert finalized["status"] == RunStatus.CANCELLED.value
    assert finalized["results"]["event_accounting"]["accepted_events"] == 1


def test_terminal_request_releases_reservation_after_publish_failure(tmp_path):
    class Publisher:
        async def publish(self, event):
            raise RabbitMQPublishError("broker rejected", ambiguous=False)

    with TestClient(create_app(publisher=Publisher(), experiment_db_path=tmp_path / "release.sqlite3")) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"])
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")
        failed_publish = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": run["run_id"],
        })
        accounting = client.get(run_url).json()["event_accounting"]
        completed = client.post(f"{run_url}/complete")
    assert failed_publish.status_code == 503
    assert accounting["accepted_events"] == 0
    assert completed.status_code == 409


def test_ambiguous_publish_is_retained_and_matching_delivery_resolves_it(tmp_path):
    class Publisher:
        event = None

        async def publish(self, event):
            self.event = event
            raise RabbitMQPublishError("confirm lost", ambiguous=True)

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher,
        metrics_store=MeasurementStore(),
        experiment_db_path=tmp_path / "publish-unknown.sqlite3",
    )) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"], workload_configuration={"target_event_count": 1})
        url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{url}/start")
        response = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b",
            "run_id": run["run_id"],
        })
        assert response.status_code == 503
        repository = client.app.state.experiment_store
        assert repository.release_unpublished_run_event(
            run["run_id"], publisher.event.event_id
        ) is False
        assert repository.unresolved_publish_count(run["run_id"]) == 1
        run_state = client.get(url).json()
        assert run_state["event_accounting"]["unresolved_publish_count"] == 1
        blocked_completion = client.post(f"{url}/complete")
        assert blocked_completion.status_code == 409

        class Message:
            body = json.dumps({
                "event_id": publisher.event.event_id,
                "event_type": publisher.event.event_type.value,
                "student_id": publisher.event.student_id,
                "book_id": publisher.event.book_id,
                "timestamp": publisher.event.timestamp.isoformat(),
                "run_id": publisher.event.run_id,
            }).encode()
            acked = False
            rejects = []

            async def ack(self):
                self.acked = True

            async def reject(self, requeue):
                self.rejects.append(requeue)

        class Processor:
            def __init__(self):
                self.metrics_store = RunSummaryStore()

            async def process(self, event):
                return None

        message = Message()
        consumer = RabbitMQConsumer(
            Processor(), RabbitMQSettings("amqps://fake-host"),
            experiment_repository=client.app.state.experiment_store,
        )
        asyncio.run(consumer.handle_message(message))
        resolved = client.get(url).json()
        assert message.acked is True
        assert resolved["event_accounting"]["unresolved_publish_count"] == 0
        assert resolved["event_accounting"]["consumer_processed_successes"] == 1


def test_unknown_reservation_is_pending_until_receipt_and_migration_preserves_it(tmp_path):
    database = tmp_path / "legacy-reservation.sqlite3"
    repository = ExperimentRepository(database)
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Migration", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
    })
    run = repository.create_run(experiment["experiment_id"])
    repository.transition_run(experiment["experiment_id"], run["run_id"], RunStatus.RUNNING)
    event_id = str(uuid4())
    repository.reserve_run_event(run["run_id"], event_id)
    repository.mark_run_event_publish_unknown(run["run_id"], event_id)
    assert repository.unresolved_publish_count(run["run_id"]) == 1
    assert repository.close_run_event_ingestion(experiment["experiment_id"], run["run_id"]) == 1

    # Simulate the prior schema (same row, without the additive uncertainty column).
    with repository._connect() as connection:
        connection.execute("ALTER TABLE experiment_run_events DROP COLUMN publish_unknown")
    repository.initialize()
    assert repository.unresolved_publish_count(run["run_id"]) == 0
    repository.mark_run_event_publish_unknown(run["run_id"], event_id)
    assert repository.claim_run_event(run["run_id"], event_id) is True
    assert repository.unresolved_publish_count(run["run_id"]) == 1
    assert repository.settle_run_event(run["run_id"], event_id, succeeded=True) is True
    assert repository.unresolved_publish_count(run["run_id"]) == 0


def test_pending_inventory_covers_unsettled_states_and_blocks_completion(tmp_path):
    repository = ExperimentRepository(tmp_path / "pending-inventory.sqlite3")
    repository.initialize()
    experiment, run = start_test_run(repository)
    ids = {name: str(uuid4()) for name in (
        "reserved", "unknown", "published", "processing", "processing_unknown", "settled_inflight",
    )}
    for event_id in ids.values():
        repository.reserve_run_event(run["run_id"], event_id)
    repository.mark_run_event_publish_unknown(run["run_id"], ids["unknown"])
    repository.mark_run_event_published(run["run_id"], ids["published"])
    repository.claim_run_event(run["run_id"], ids["processing"])
    repository.mark_run_event_publish_unknown(run["run_id"], ids["processing_unknown"])
    repository.claim_run_event(run["run_id"], ids["processing_unknown"])
    repository.mark_run_event_published(run["run_id"], ids["settled_inflight"])
    repository.claim_run_event(run["run_id"], ids["settled_inflight"])
    repository.settle_run_event(run["run_id"], ids["settled_inflight"], succeeded=True)
    repository.claim_run_event(run["run_id"], ids["settled_inflight"])

    inventory = repository.get_run_pending_event_inventory(
        experiment["experiment_id"], run["run_id"],
    )
    assert {event["event_id"] for event in inventory["events"]} == set(ids.values())
    assert inventory["summary"] == {
        "pending_count": 6,
        "unresolved_publish_count": 2,
        "published_flag_unsettled_count": 2,
        "publish_outcome_not_established_count": 2,
        "consumer_claim_activity_count": 3,
        "consumer_receipt_persisted_count": 1,
    }
    assert inventory["events"]

    repository.close_run_event_ingestion(experiment["experiment_id"], run["run_id"])
    with pytest.raises(InvalidRunTransition):
        repository.transition_run(
            experiment["experiment_id"], run["run_id"], RunStatus.COMPLETED,
            ended_at=utc_now(), results={},
        )


def test_pending_inventory_api_is_run_scoped_and_empty_inventory_is_explicit(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "pending-api.sqlite3")) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"])
        url = (
            f"/api/experiments/{experiment['experiment_id']}/runs/"
            f"{run['run_id']}/pending-events"
        )
        response = client.get(url)
        mismatch = client.get(
            f"/api/experiments/{uuid4()}/runs/{run['run_id']}/pending-events"
        )
    assert response.status_code == 200
    assert response.json()["events"] == []
    assert response.json()["summary"]["pending_count"] == 0
    assert mismatch.status_code == 404


def test_reconciliation_evidence_is_durable_and_never_releases_reservation(tmp_path):
    database = tmp_path / "reconciliation-audit.sqlite3"
    repository = ExperimentRepository(database)
    repository.initialize()
    experiment, run = start_test_run(repository)
    event_id = str(uuid4())
    repository.reserve_run_event(run["run_id"], event_id)
    before = repository.get_run_pending_event_inventory(experiment["experiment_id"], run["run_id"])

    with pytest.raises(ValueError):
        repository.record_run_event_reconciliation_evidence(
            experiment["experiment_id"], run["run_id"], event_id,
            evidence_source=" ", evidence_reference="ref", evidence_details="details",
        )
    with pytest.raises(RunEventNotFound):
        repository.record_run_event_reconciliation_evidence(
            experiment["experiment_id"], run["run_id"], str(uuid4()),
            evidence_source="operator", evidence_reference="case-0",
            evidence_details="reservation ID does not match",
        )
    assessment = repository.record_run_event_reconciliation_evidence(
        experiment["experiment_id"], run["run_id"], event_id,
        evidence_source="operator_queue_snapshot", evidence_reference="snapshot-17",
        evidence_details="ready=0; captured during inspection",
    )
    after = repository.get_run_pending_event_inventory(experiment["experiment_id"], run["run_id"])
    assert assessment["disposition"] == "NOT_RECONCILED"
    assert assessment["reservation_changed"] is False
    assert "not independently verifiable" in assessment["decision_reason"]
    assert after["events"] == before["events"]
    with repository._reader() as connection:
        rows = connection.execute(
            "SELECT disposition, evidence_reference FROM experiment_run_event_reconciliation_audit "
            "WHERE run_id = ? AND event_id = ?", (run["run_id"], event_id),
        ).fetchall()
    assert [(row["disposition"], row["evidence_reference"]) for row in rows] == [
        ("NOT_RECONCILED", "snapshot-17"),
    ]

    repository.initialize()
    with repository._reader() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM experiment_run_event_reconciliation_audit WHERE run_id = ?",
            (run["run_id"],),
        ).fetchone()[0] == 1

    processing_id = str(uuid4())
    repository.reserve_run_event(run["run_id"], processing_id)
    repository.claim_run_event(run["run_id"], processing_id)
    processing_assessment = repository.record_run_event_reconciliation_evidence(
        experiment["experiment_id"], run["run_id"], processing_id,
        evidence_source="operator", evidence_reference="case-processing",
        evidence_details="delivery may still be active",
    )
    assert processing_assessment["disposition"] == "NOT_RECONCILED"
    assert "in-flight" in processing_assessment["decision_reason"]

    settled_id = str(uuid4())
    repository.reserve_run_event(run["run_id"], settled_id)
    repository.claim_run_event(run["run_id"], settled_id)
    repository.settle_run_event(run["run_id"], settled_id, succeeded=True)
    settled_assessment = repository.record_run_event_reconciliation_evidence(
        experiment["experiment_id"], run["run_id"], settled_id,
        evidence_source="operator", evidence_reference="case-settled",
        evidence_details="receipt already exists",
    )
    assert settled_assessment["disposition"] == "NOT_RECONCILED"
    assert "receipt is already persisted" in settled_assessment["decision_reason"]
    with repository._reader() as connection:
        settled = connection.execute(
            "SELECT settled, outcome FROM experiment_run_events WHERE run_id = ? AND event_id = ?",
            (run["run_id"], settled_id),
        ).fetchone()
    assert tuple(settled) == (1, "SUCCESS")


def test_reconciliation_assessment_serializes_with_consumer_claim(tmp_path):
    repository = ExperimentRepository(tmp_path / "reconciliation-claim-race.sqlite3")
    repository.initialize()
    experiment, run = start_test_run(repository)
    event_id = str(uuid4())
    repository.reserve_run_event(run["run_id"], event_id)
    from threading import Barrier
    barrier = Barrier(2)

    def assess():
        barrier.wait()
        return repository.record_run_event_reconciliation_evidence(
            experiment["experiment_id"], run["run_id"], event_id,
            evidence_source="operator", evidence_reference="case-1",
            evidence_details="message absent from one queue snapshot",
        )

    def claim():
        barrier.wait()
        return repository.claim_run_event(run["run_id"], event_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assessment_future = pool.submit(assess)
        claim_future = pool.submit(claim)
        assessment = assessment_future.result()
        assert claim_future.result() is True

    inventory = repository.get_run_pending_event_inventory(experiment["experiment_id"], run["run_id"])
    event = inventory["events"][0]
    assert assessment["disposition"] == "NOT_RECONCILED"
    assert event["state"] == "PROCESSING"
    assert event["in_flight_deliveries"] == 1
    assert event["consumer_claim_activity_recorded"] is True


def test_accepted_events_remain_visible_when_redis_measurements_are_unavailable(tmp_path):
    class Publisher:
        async def publish(self, event):
            self.event = event

    metrics = MeasurementStore(fail=True)
    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher,
        metrics_store=metrics,
        experiment_db_path=tmp_path / "missing-run-measurements.sqlite3",
    )) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"], workload_configuration={"target_event_count": 1})
        url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{url}/start")
        accepted = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b", "run_id": run["run_id"],
        })
        repository = client.app.state.experiment_store
        repository.claim_run_event(run["run_id"], publisher.event.event_id)
        repository.settle_run_event(run["run_id"], publisher.event.event_id, succeeded=True)
        record_generated_progress(client, run["run_id"], 1, publish_confirmed=1)
        completed = finalize_requested_run(client, experiment["experiment_id"], run["run_id"])
    assert accepted.status_code == 202
    assert completed["results"]["event_accounting"]["accepted_events"] == 1
    assert completed["results"]["unique_successful_events"] is None
    assert completed["results"]["measurements"] is None
    assert completed["results"]["result_complete"] is False
    assert completed["results"]["latency_complete"] is False


def test_direct_terminal_transitions_cannot_bypass_guarded_finalization(tmp_path):
    repository = ExperimentRepository(tmp_path / "concurrent.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Concurrent", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
    })
    run = repository.create_run(
        experiment["experiment_id"], workload_configuration={"target_event_count": 1},
    )
    repository.transition_run(experiment["experiment_id"], run["run_id"], RunStatus.RUNNING)
    claim = repository.claim_run_execution(run["run_id"])
    event_id = str(uuid4())
    repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=str(uuid4()), generated=1,
    )
    repository.reserve_run_event(run["run_id"], event_id)
    repository.mark_run_event_published(run["run_id"], event_id)
    repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=str(uuid4()), publish_confirmed=1,
    )
    repository.claim_run_event(run["run_id"], event_id)
    repository.settle_run_event(run["run_id"], event_id, succeeded=True)
    repository.release_run_execution_claim(run["run_id"], claim["owner_token"])
    repository.request_run_stop(
        experiment["experiment_id"], run["run_id"], RunStatus.COMPLETED,
    )

    def transition(target):
        try:
            return repository.transition_run(
                experiment["experiment_id"], run["run_id"], target,
                ended_at="2026-11-10T10:00:01+00:00", results={"result_complete": False},
            )["status"]
        except InvalidRunTransition:
            return "CONFLICT"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(transition, (RunStatus.COMPLETED, RunStatus.FAILED)))
    assert outcomes == ["CONFLICT", "CONFLICT"]
    assert repository.get_run(experiment["experiment_id"], run["run_id"])["status"] == "RUNNING"
    assert repository.finalize_run_after_drain(
        experiment["experiment_id"], run["run_id"], ended_at=utc_now(),
        results={"event_accounting": repository.run_event_accounting(run["run_id"])},
    )["status"] == RunStatus.COMPLETED.value


def test_concurrent_database_initialization_is_idempotent(tmp_path):
    database = tmp_path / "concurrent-initialize.sqlite3"
    repositories = [ExperimentRepository(database), ExperimentRepository(database)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(lambda repository: repository.initialize(), repositories))
    assert repositories[0].list_experiments() == []


def test_redelivered_processing_attempt_is_included_in_drain_count(tmp_path):
    repository = ExperimentRepository(tmp_path / "redelivery-drain.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Redelivery drain", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
    })
    run = repository.create_run(experiment["experiment_id"])
    repository.transition_run(experiment["experiment_id"], run["run_id"], RunStatus.RUNNING)
    event_id = str(uuid4())
    repository.reserve_run_event(run["run_id"], event_id)
    repository.mark_run_event_published(run["run_id"], event_id)
    repository.claim_run_event(run["run_id"], event_id)
    repository.settle_run_event(run["run_id"], event_id, succeeded=True)
    repository.claim_run_event(run["run_id"], event_id)

    pending = repository.close_run_event_ingestion(experiment["experiment_id"], run["run_id"])
    assert pending == 1
    repository.settle_run_event(run["run_id"], event_id, succeeded=True)
    assert repository.close_run_event_ingestion(experiment["experiment_id"], run["run_id"]) == 0


def test_non_completed_runs_never_report_benchmark_throughput(tmp_path):
    measurements = MeasurementStore(measurement_summary())
    with TestClient(create_app(metrics_store=measurements, experiment_db_path=tmp_path / "terminal.sqlite3")) as client:
        experiment = create_experiment(client)
        experiment_id = experiment["experiment_id"]
        run = create_run(client, experiment_id)
        url = f"/api/experiments/{experiment_id}/runs/{run['run_id']}"
        client.post(f"{url}/start")
        client.post(f"{url}/fail", json={"reason": "controlled stop"})
        import asyncio
        failed = asyncio.run(client.app.state.finalize_experiment_run(experiment_id, run["run_id"]))
    assert failed["results"]["benchmark_throughput_events_per_second"] is None
    assert failed["results"]["result_complete"] is False


def test_throughput_completeness_is_separate_from_latency_completeness(tmp_path, monkeypatch):
    import app.main as main_module

    timestamps = iter(["2026-11-10T10:00:00+00:00", "2026-11-10T10:00:10+00:00"])
    monkeypatch.setattr(main_module, "utc_now", lambda: next(timestamps, "2026-11-10T10:00:10+00:00"))
    summary = replace(
        measurement_summary(unique_successes=3, attempts=4, published=3),
        sample_count={"consumer_processing_latency_ms": 0},
        average_latency_ms={"consumer_processing_latency_ms": None},
        p95_latency_ms={"consumer_processing_latency_ms": None},
    )
    with TestClient(create_app(
        metrics_store=MeasurementStore(summary),
        experiment_db_path=tmp_path / "latency-incomplete.sqlite3",
    )) as client:
        experiment = create_experiment(client)
        run = create_run(
            client, experiment["experiment_id"],
            workload_configuration={"target_event_count": 3},
        )
        url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{url}/start")
        record_generated_progress(client, run["run_id"], 3, publish_confirmed=3)
        record_run_event_outcomes(client, run["run_id"], successes=3)
        completed = finalize_requested_run(client, experiment["experiment_id"], run["run_id"])
    assert completed["results"]["benchmark_throughput_events_per_second"] == 0.3
    assert completed["results"]["result_complete"] is True
    assert completed["results"]["latency_complete"] is False
    assert "No consumer processing latency samples" in completed["results"]["latency_incomplete_reasons"][0]


def test_manual_dashboard_events_remain_unscoped_when_runs_exist(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, event):
            self.events.append(event)

    publisher = Publisher()
    with TestClient(create_app(publisher=publisher, experiment_db_path=tmp_path / "unscoped.sqlite3")) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"])
        response = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b",
        })
    assert response.status_code == 202
    assert run["run_id"] != response.json()["event"].get("run_id")
    assert publisher.events[0].run_id is None


def test_workload_configuration_is_validated_and_snapshotted_immutably(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "workload-snapshot.sqlite3")) as client:
        created = client.post("/api/experiments", json={
            "name": "bounded",
            "default_workload_configuration": {
                "target_event_count": 12,
                "publish_interval_ms": 25,
                "event_type_distribution": {"BOOK_ISSUED": 3, "BOOK_RETURNED": 1},
            },
        })
        assert created.status_code == 201, created.text
        experiment = created.json()
        run = create_run(client, experiment["experiment_id"])
        changed = client.patch(f"/api/experiments/{experiment['experiment_id']}", json={
            "default_workload_configuration": {
                "target_event_count": 3,
                "publish_interval_ms": 10,
                "event_type_distribution": {"BOOK_RESERVED": 1},
            },
        })
        assert changed.status_code == 200
        assert run["workload_snapshot"]["target_event_count"] == 12
        assert run["progress"]["planned"] == 12
        assert create_run(client, experiment["experiment_id"])["workload_snapshot"]["target_event_count"] == 3
        invalid_count = client.post(
            f"/api/experiments/{experiment['experiment_id']}/runs",
            json={"workload_configuration": {"target_event_count": 10001}},
        )
        invalid_type = client.post("/api/experiments", json={
            "name": "invalid distribution",
            "default_workload_configuration": {"event_type_distribution": {"INVALID": 1}},
        })
        invalid_interval = client.post("/api/experiments", json={
            "name": "unpaced",
            "default_workload_configuration": {"publish_interval_ms": 0},
        })
        boundary_interval = client.post("/api/experiments", json={
            "name": "minimum paced",
            "default_workload_configuration": {
                "target_event_count": 10000, "publish_interval_ms": 10,
            },
        })
    assert invalid_count.status_code == 422
    assert invalid_type.status_code == 422
    assert invalid_interval.status_code == 422
    assert boundary_interval.status_code == 201


def test_execution_claim_is_atomic_and_requires_running_run(tmp_path):
    repository = ExperimentRepository(tmp_path / "execution-claim.sqlite3")
    repository.initialize()
    experiment, run = start_test_run(repository)
    token = repository.claim_run_execution(run["run_id"])
    assert token["owner_token"]
    with pytest.raises(ExecutionClaimConflict):
        repository.claim_run_execution(run["run_id"])

    def attempt_claim():
        try:
            repository.claim_run_execution(run["run_id"])
            return True
        except ExecutionClaimConflict:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: attempt_claim(), range(2)))
    assert results == [False, False]
    unstarted = repository.create_run(experiment["experiment_id"])
    with pytest.raises(InvalidRunTransition):
        repository.claim_run_execution(unstarted["run_id"])
    with pytest.raises(RunNotFound):
        repository.claim_run_execution(str(uuid4()))

    concurrent = repository.create_run(experiment["experiment_id"])
    repository.transition_run(experiment["experiment_id"], concurrent["run_id"], RunStatus.RUNNING)
    from threading import Barrier
    barrier = Barrier(2)

    def concurrent_claim():
        barrier.wait()
        return attempt_claim_for(repository, concurrent["run_id"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        concurrent_results = list(pool.map(lambda _: concurrent_claim(), range(2)))
    assert sum(result is not None for result in concurrent_results) == 1


def test_expired_execution_claim_is_persisted_and_never_reacquired(tmp_path):
    repository = ExperimentRepository(tmp_path / "execution-expiry.sqlite3")
    repository.initialize()
    _, run = start_test_run(repository)
    claim = repository.claim_run_execution(run["run_id"])
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
            ("2000-01-01T00:00:00+00:00", run["run_id"]),
        )
    with pytest.raises(ExecutionClaimConflict, match="expired"):
        repository.claim_run_execution(run["run_id"])
    assert repository.get_run_execution_progress(
        run["experiment_id"], run["run_id"]
    )["execution"]["state"] == "EXPIRED"
    with pytest.raises(ExecutionClaimConflict, match="reacquired"):
        repository.claim_run_execution(run["run_id"])
    with pytest.raises(ExecutionClaimConflict):
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"], operation_id=str(uuid4()), generated=1,
        )


def attempt_claim_for(repository, run_id):
    try:
        return repository.claim_run_execution(run_id)
    except ExecutionClaimConflict:
        return None


def test_execution_claim_renewal_and_release_are_owner_checked_and_one_time(tmp_path):
    repository = ExperimentRepository(tmp_path / "execution-release.sqlite3")
    repository.initialize()
    experiment, run = start_test_run(repository)
    claim = repository.claim_run_execution(run["run_id"], lease_seconds=5)
    renewed = repository.renew_run_execution_claim(
        run["run_id"], claim["owner_token"], lease_seconds=10,
    )
    assert renewed > claim["lease_expires_at"]
    with pytest.raises(ExecutionClaimConflict):
        repository.release_run_execution_claim(run["run_id"], "wrong-owner")
    assert repository.release_run_execution_claim(run["run_id"], claim["owner_token"])
    assert repository.get_run_execution_progress(
        experiment["experiment_id"], run["run_id"]
    )["execution"]["state"] == "RELEASED"
    with pytest.raises(ExecutionClaimConflict):
        repository.claim_run_execution(run["run_id"])


def test_execution_progress_updates_are_atomic_validated_and_persistent(tmp_path):
    path = tmp_path / "execution-progress.sqlite3"
    repository = ExperimentRepository(path)
    repository.initialize()
    experiment, run = start_test_run(repository)
    claim = repository.claim_run_execution(run["run_id"])
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"], operation_id=str(uuid4()),
            generated=1, publish_confirmed=1,
        ), range(4)))
    retry_operation = str(uuid4())
    first = repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=retry_operation,
        generated=1, publish_confirmed=1,
    )
    retried = repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=retry_operation,
        generated=1, publish_confirmed=1,
    )
    assert retried == first
    with pytest.raises(ExecutionProgressConflict):
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"], operation_id=retry_operation,
            generated=1, publish_failed=1,
        )
    with pytest.raises(ValueError):
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"], operation_id=str(uuid4()), generated=-1,
        )
    with pytest.raises(ValueError, match="outcomes"):
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"], operation_id=str(uuid4()), publish_unknown=1,
        )
    with pytest.raises(ValueError, match="planned"):
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"], operation_id=str(uuid4()), generated=101,
        )
    repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=str(uuid4()),
        generated=1, publish_failed=1,
    )
    repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=str(uuid4()),
        generated=1, publish_unknown=1,
    )
    repository = ExperimentRepository(path)
    repository.initialize()
    retried_after_reinit = repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=retry_operation,
        generated=1, publish_confirmed=1,
    )
    progress = repository.get_run_execution_progress(experiment["experiment_id"], run["run_id"])
    assert retried_after_reinit == progress["progress"]
    assert progress["progress"] == {
        "planned": 100, "generated": 7, "publish_confirmed": 5,
        "publish_failed": 1, "publish_unknown": 1,
    }
    assert progress["execution"]["state"] == "CLAIMED"


def test_execution_progress_api_is_scoped_and_never_exposes_owner_token(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "execution-api.sqlite3")) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"])
        client.post(f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}/start")
        claim = record_successful_workload(
            client, experiment["experiment_id"], run["run_id"], keep_claim=True,
        )
        response = client.get(
            f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}/execution"
        )
        missing = client.get(
            f"/api/experiments/{experiment['experiment_id']}/runs/{uuid4()}/execution"
        )
    assert response.status_code == 200
    assert response.json()["execution"]["state"] == "CLAIMED"
    assert response.json()["progress"]["planned"] == 100
    assert claim["owner_token"] not in response.text
    assert missing.status_code == 404


def test_active_execution_claim_accepts_stop_intent_before_terminal_finalization(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "active-terminal.sqlite3")) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"], workload_configuration={"target_event_count": 1})
        url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        assert client.post(f"{url}/start").status_code == 200
        claim = record_successful_workload(
            client, experiment["experiment_id"], run["run_id"], keep_claim=True,
        )
        accepted = client.post(f"{url}/complete")
        still_open = client.app.state.experiment_store.get_run(
            experiment["experiment_id"], run["run_id"]
        )
        client.app.state.experiment_store.release_run_execution_claim(
            run["run_id"], claim["owner_token"],
        )
        repeated = client.post(f"{url}/complete")
    assert accepted.status_code == 202
    assert still_open["status"] == RunStatus.RUNNING.value
    assert still_open["event_ingestion_closed"] is True
    assert still_open["stop_intent"]["target"] == RunStatus.COMPLETED.value
    assert repeated.status_code == 202
    assert repeated.json()["already_requested"] is True


def test_execution_claim_race_with_terminal_gate_has_one_winner(tmp_path):
    repository = ExperimentRepository(tmp_path / "claim-terminal-race.sqlite3")
    repository.initialize()
    experiment, run = start_test_run(repository)
    from threading import Barrier
    barrier = Barrier(2)

    def try_claim():
        barrier.wait()
        try:
            return repository.claim_run_execution(run["run_id"])
        except InvalidRunTransition:
            return None

    def try_terminal():
        barrier.wait()
        try:
            repository.close_run_event_ingestion(experiment["experiment_id"], run["run_id"])
            return True
        except ExecutionClaimConflict:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        claim_future = pool.submit(try_claim)
        terminal_future = pool.submit(try_terminal)
        claim = claim_future.result()
        terminal_won = terminal_future.result()
    assert (claim is not None) != terminal_won
    state = repository.get_run(experiment["experiment_id"], run["run_id"])
    if claim is not None:
        assert state["status"] == RunStatus.RUNNING.value
        assert state["event_ingestion_closed"] is False
    else:
        assert state["status"] == RunStatus.RUNNING.value
        assert state["event_ingestion_closed"] is True


def test_expired_claim_status_is_consistent_across_run_reads(tmp_path):
    with TestClient(create_app(experiment_db_path=tmp_path / "expired-read.sqlite3")) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"])
        client.post(f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}/start")
        client.app.state.experiment_store.claim_run_execution(run["run_id"])
        with client.app.state.experiment_store._transaction() as connection:
            connection.execute(
                "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
                ("2000-01-01T00:00:00+00:00", run["run_id"]),
            )
        single = client.get(
            f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        )
        listed = client.get(f"/api/experiments/{experiment['experiment_id']}/runs")
        progress = client.get(
            f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}/execution"
        )
    assert single.json()["execution"]["state"] == "EXPIRED"
    assert listed.json()["runs"][0]["execution"]["state"] == "EXPIRED"
    assert progress.json()["execution"]["state"] == "EXPIRED"


@pytest.mark.parametrize(
    "detector",
    ["claim", "renew", "release", "progress", "reserve", "close", "transition", "stop"],
)
def test_expiry_detection_persists_interruption_and_closes_admission(tmp_path, detector):
    repository = ExperimentRepository(tmp_path / f"interruption-{detector}.sqlite3")
    repository.initialize()
    experiment, run = start_test_run(repository)
    event_id = str(uuid4())
    repository.reserve_run_event(run["run_id"], event_id)
    claim = repository.claim_run_execution(run["run_id"])
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
            ("2000-01-01T00:00:00+00:00", run["run_id"]),
        )

    if detector == "claim":
        with pytest.raises(ExecutionClaimConflict):
            repository.claim_run_execution(run["run_id"])
    elif detector == "renew":
        with pytest.raises(ExecutionClaimConflict):
            repository.renew_run_execution_claim(run["run_id"], claim["owner_token"])
    elif detector == "release":
        with pytest.raises(ExecutionClaimConflict):
            repository.release_run_execution_claim(run["run_id"], claim["owner_token"])
    elif detector == "progress":
        with pytest.raises(ExecutionClaimConflict):
            repository.update_run_execution_progress(
                run["run_id"], claim["owner_token"], operation_id=str(uuid4()), generated=1,
            )
    elif detector == "reserve":
        with pytest.raises(InvalidRunTransition):
            repository.reserve_run_event(run["run_id"], str(uuid4()))
    elif detector == "close":
        assert repository.close_run_event_ingestion(
            experiment["experiment_id"], run["run_id"],
        ) == 1
    elif detector == "transition":
        with pytest.raises(InvalidRunTransition, match="finalize_run_after_drain"):
            repository.transition_run(
                experiment["experiment_id"], run["run_id"], RunStatus.COMPLETED,
                ended_at=utc_now(), results={},
            )
        with pytest.raises((InvalidRunTransition, ExecutionClaimConflict)):
            repository.request_run_stop(
                experiment["experiment_id"], run["run_id"], RunStatus.COMPLETED,
            )
    else:
        with pytest.raises((InvalidRunTransition, ExecutionClaimConflict)):
            repository.request_run_stop(
                experiment["experiment_id"], run["run_id"], RunStatus.COMPLETED,
            )

    persisted = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert persisted["status"] == RunStatus.RUNNING.value
    assert persisted["event_ingestion_closed"] is True
    assert persisted["execution"]["state"] == "EXPIRED"
    assert persisted["execution"]["interruption_recorded"] is True
    assert persisted["execution"]["interrupted_at"]
    assert len(persisted["execution"]["interruption_reason"]) <= 200
    assert persisted["stop_intent"] is None
    with pytest.raises(ExecutionClaimConflict):
        repository.claim_run_execution(run["run_id"])
    with pytest.raises(ExecutionClaimConflict):
        repository.renew_run_execution_claim(run["run_id"], claim["owner_token"])
    with pytest.raises(ExecutionClaimConflict):
        repository.release_run_execution_claim(run["run_id"], claim["owner_token"])
    with pytest.raises(ExecutionClaimConflict):
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"], operation_id=str(uuid4()), generated=1,
        )
    inventory = repository.get_run_pending_event_inventory(experiment["experiment_id"], run["run_id"])
    assert [item["event_id"] for item in inventory["events"]] == [event_id]
    with pytest.raises(InvalidRunTransition):
        repository.reserve_run_event(run["run_id"], str(uuid4()))


def test_expiry_reservation_race_is_serialized_and_repeated_detection_is_idempotent(tmp_path):
    from threading import Barrier

    repository = ExperimentRepository(tmp_path / "interruption-race.sqlite3")
    repository.initialize()
    experiment, run = start_test_run(repository)
    claim = repository.claim_run_execution(run["run_id"])
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
            ("2000-01-01T00:00:00+00:00", run["run_id"]),
        )
    barrier = Barrier(2)

    def detect_expiry():
        barrier.wait()
        with pytest.raises(ExecutionClaimConflict):
            repository.claim_run_execution(run["run_id"])
        return True

    def attempt_reservation():
        barrier.wait()
        try:
            repository.reserve_run_event(run["run_id"], "race-event")
            return True
        except InvalidRunTransition:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        detect = pool.submit(detect_expiry)
        reserve = pool.submit(attempt_reservation)
        assert detect.result() is True
        assert reserve.result() is False

    first = repository.get_run(experiment["experiment_id"], run["run_id"])
    interrupted_at = first["execution"]["interrupted_at"]
    assert interrupted_at
    with pytest.raises(ExecutionClaimConflict):
        repository.release_run_execution_claim(run["run_id"], claim["owner_token"])
    with pytest.raises(ExecutionClaimConflict):
        repository.claim_run_execution(run["run_id"])
    second = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert second["execution"]["interrupted_at"] == interrupted_at
    assert second["event_ingestion_closed"] is True
    with repository._reader() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM experiment_run_events WHERE run_id = ?",
            (run["run_id"],),
        ).fetchone()[0] == 0


def test_expired_interruption_preserves_progress_retry_and_migrates_durably(tmp_path):
    path = tmp_path / "interruption-migration.sqlite3"
    repository = ExperimentRepository(path)
    repository.initialize()
    experiment, run = start_test_run(repository)
    claim = repository.claim_run_execution(run["run_id"])
    operation_id = str(uuid4())
    first = repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=operation_id, generated=1,
    )
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
            ("2000-01-01T00:00:00+00:00", run["run_id"]),
        )
    retried = repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=operation_id, generated=1,
    )
    assert retried == first
    before = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert before["execution"]["interruption_recorded"] is True
    assert before["event_ingestion_closed"] is True
    assert before["progress"]["generated"] == 1
    assert repository.get_run_pending_event_inventory(
        experiment["experiment_id"], run["run_id"],
    )["events"] == []

    with repository._connect() as connection:
        connection.execute("ALTER TABLE experiment_runs DROP COLUMN execution_interrupted_at")
        connection.execute("ALTER TABLE experiment_runs DROP COLUMN execution_interruption_reason")
        connection.execute(
            "UPDATE experiment_runs SET execution_state = 'EXPIRED', event_ingestion_closed = 0, "
            "execution_owner_token = 'legacy-stale-token' "
            "WHERE run_id = ?", (run["run_id"],),
        )
    repository.initialize()
    after = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert after["execution"]["interruption_recorded"] is True
    assert after["execution"]["interrupted_at"]
    assert after["event_ingestion_closed"] is True
    assert after["progress"] == before["progress"]
    with repository._reader() as connection:
        assert connection.execute(
            "SELECT execution_owner_token FROM experiment_runs WHERE run_id = ?",
            (run["run_id"],),
        ).fetchone()[0] is None


def test_interruption_rejects_manual_run_event_submission_without_dropping_reservations(tmp_path):
    class Publisher:
        def __init__(self):
            self.calls = []

        async def publish(self, event):
            self.calls.append(event)

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "interrupted-manual-event.sqlite3",
    )) as client:
        experiment = create_experiment(client)
        run = create_run(client, experiment["experiment_id"])
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        assert client.post(f"{run_url}/start").status_code == 200
        repository = client.app.state.experiment_store
        existing_event_id = str(uuid4())
        repository.reserve_run_event(run["run_id"], existing_event_id)
        claim = repository.claim_run_execution(run["run_id"])
        with repository._transaction() as connection:
            connection.execute(
                "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
                ("2000-01-01T00:00:00+00:00", run["run_id"]),
            )
        response = client.post("/api/events", json={
            "event_type": "BOOK_ISSUED", "student_id": "s", "book_id": "b",
            "run_id": run["run_id"],
        })
        inventory = client.get(f"{run_url}/pending-events").json()
        progress = client.get(f"{run_url}/execution").json()
    assert response.status_code == 409
    assert publisher.calls == []
    assert [row["event_id"] for row in inventory["events"]] == [existing_event_id]
    assert progress["execution"]["interruption_recorded"] is True
    assert claim["owner_token"] not in response.text


@pytest.mark.asyncio
async def test_already_invoked_runner_publish_remains_unknown_after_interruption(tmp_path):
    repository = ExperimentRepository(tmp_path / "interruption-inflight-publish.sqlite3")
    repository.initialize()
    _, run = start_test_run(repository)
    claim = repository.claim_run_execution(run["run_id"])
    invocation_started = asyncio.Event()
    allow_outcome_settlement = asyncio.Event()
    invocations = []

    async def publish(event, lease_guard):
        assert lease_guard()
        repository.reserve_run_event(run["run_id"], event.event_id)
        invocations.append(event.event_id)
        invocation_started.set()
        await allow_outcome_settlement.wait()
        repository.mark_run_event_publish_unknown(run["run_id"], event.event_id)
        return EventPublishResult("unknown", "publisher outcome unavailable")

    runner = ExperimentWorkloadRunner(
        repository, publish, lease_seconds=60, renewal_interval_seconds=3600,
    )
    task = asyncio.create_task(runner.execute(run, claim))
    await invocation_started.wait()
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
            ("2000-01-01T00:00:00+00:00", run["run_id"]),
        )
    with pytest.raises(ExecutionClaimConflict):
        repository.renew_run_execution_claim(run["run_id"], claim["owner_token"])
    allow_outcome_settlement.set()
    await task

    inventory = repository.get_run_pending_event_inventory(run["experiment_id"], run["run_id"])
    assert len(invocations) == 1
    assert len(inventory["events"]) == 1
    assert inventory["events"][0]["publish_unknown"] == 1
    assert inventory["events"][0]["settled"] == 0
    assert inventory["summary"]["pending_count"] == 1
    assert inventory["summary"]["unresolved_publish_count"] == 1
    assert repository.get_run(run["experiment_id"], run["run_id"])["progress"]["generated"] == 1
    with pytest.raises(ExecutionClaimConflict):
        repository.claim_run_execution(run["run_id"])


def test_execution_schema_migration_preserves_existing_experiment_and_run(tmp_path):
    path = tmp_path / "legacy-experiments.sqlite3"
    experiment_id = str(uuid4())
    run_id = str(uuid4())
    connection = sqlite3.connect(path)
    connection.executescript("""
        CREATE TABLE experiments (
            experiment_id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT,
            experiment_type TEXT NOT NULL, created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL, default_configuration TEXT NOT NULL
        );
        CREATE TABLE experiment_runs (
            run_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
            status TEXT NOT NULL, configuration_snapshot TEXT NOT NULL, created_at TEXT NOT NULL,
            started_at TEXT, ended_at TEXT, failure_reason TEXT, results_json TEXT,
            event_ingestion_closed INTEGER NOT NULL DEFAULT 0
        );
    """)
    connection.execute(
        "INSERT INTO experiments VALUES (?, ?, ?, ?, ?, ?, ?)",
        (experiment_id, "legacy", None, "window_performance",
         "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00",
         '{"tumbling_size":10,"sliding_size":5,"sliding_interval":1}'),
    )
    connection.execute(
        "INSERT INTO experiment_runs "
        "(run_id, experiment_id, status, configuration_snapshot, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (run_id, experiment_id, "CREATED",
         '{"tumbling_size":7,"sliding_size":5,"sliding_interval":1}',
         "2026-01-01T00:00:00+00:00"),
    )
    connection.commit()
    connection.close()

    repository = ExperimentRepository(path)
    repository.initialize()
    run = repository.get_run(experiment_id, run_id)
    assert run["configuration_snapshot"]["tumbling_size"] == 7
    assert run["workload_snapshot"]["target_event_count"] == 100
    assert run["execution"]["state"] == "NOT_STARTED"
    assert run["progress"]["planned"] == 100


def test_runner_facing_lookup_accepts_a_valid_persisted_workload_snapshot(tmp_path):
    repository = ExperimentRepository(tmp_path / "valid-workload-read.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "valid snapshot", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
        "default_workload_configuration": {
            "target_event_count": 8, "publish_interval_ms": 10,
            "event_type_distribution": {"BOOK_ISSUED": 2},
        },
    })
    run = repository.create_run(experiment["experiment_id"])
    loaded = repository.get_run_with_workload_configuration_snapshot(run["run_id"])
    assert loaded["workload_snapshot"] == run["workload_snapshot"]
    assert loaded["progress"]["planned"] == 8


def test_invalid_legacy_workload_snapshot_fails_closed_without_mutation(tmp_path):
    repository = ExperimentRepository(tmp_path / "invalid-workload-read.sqlite3")
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "legacy invalid snapshot", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1},
        "default_workload_configuration": {
            "target_event_count": 7, "publish_interval_ms": 100,
            "event_type_distribution": {"BOOK_RETURNED": 1},
        },
    })
    run = repository.create_run(experiment["experiment_id"])
    repository.transition_run(experiment["experiment_id"], run["run_id"], RunStatus.RUNNING)
    original_progress = run["progress"].copy()
    legacy_snapshot = {
        **run["workload_snapshot"],
        "publish_interval_ms": 0,
    }
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET workload_snapshot = ? WHERE run_id = ?",
            (json.dumps(legacy_snapshot), run["run_id"]),
        )

    with pytest.raises(InvalidRunWorkloadConfiguration, match="workload snapshot is invalid"):
        repository.get_run_with_workload_configuration_snapshot(run["run_id"])
    with pytest.raises(InvalidRunWorkloadConfiguration, match="workload snapshot is invalid"):
        repository.claim_run_execution(run["run_id"])

    persisted = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert persisted["workload_snapshot"] == legacy_snapshot
    assert persisted["progress"] == original_progress
    assert persisted["execution"]["state"] == "NOT_STARTED"

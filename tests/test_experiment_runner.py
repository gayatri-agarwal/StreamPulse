import asyncio
import json
import logging
from threading import Event as ThreadEvent
import time
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from app.experiments.runner import EventPublishResult, ExperimentWorkloadRunner
from app.experiments.schemas import RunStatus
from app.experiments.store import (
    ExecutionClaimConflict,
    ExperimentRepository,
    InvalidRunWorkloadConfiguration,
    utc_now,
)
from app.main import create_app
from app.messaging.rabbitmq import RabbitMQPublishError
from app.schemas import EventType


DEFAULT_WINDOW = {"tumbling_size": 10, "sliding_size": 5, "sliding_interval": 1}


def start_run(repository, *, planned=3, interval_ms=10, distribution=None):
    experiment = repository.create_experiment({
        "name": "runner test",
        "experiment_type": "window_performance",
        "default_configuration": DEFAULT_WINDOW,
        "default_workload_configuration": {
            "target_event_count": planned,
            "publish_interval_ms": interval_ms,
            "event_type_distribution": distribution or {"BOOK_ISSUED": 1},
        },
    })
    run = repository.create_run(experiment["experiment_id"])
    repository.transition_run(experiment["experiment_id"], run["run_id"], RunStatus.RUNNING)
    return experiment, repository.get_run_with_workload_configuration_snapshot(run["run_id"])


def wait_for_runner_state(client, experiment_id, run_id, expected="RELEASED", timeout=5):
    url = f"/api/experiments/{experiment_id}/runs/{run_id}/execution"
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        response = client.get(url)
        assert response.status_code == 200, response.text
        latest = response.json()
        if latest["execution"]["state"] == expected:
            return latest
        time.sleep(0.01)
    raise AssertionError(f"runner did not reach {expected}: {latest}")


def test_runner_executes_exact_bounded_count_through_run_reservations(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []
            self.starts = []

        async def publish(self, event):
            self.events.append(event)
            self.starts.append(time.monotonic())

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "runner-success.sqlite3",
    )) as client:
        created = client.post("/api/experiments", json={
            "name": "runner", "default_workload_configuration": {
                "target_event_count": 4, "publish_interval_ms": 10,
                "event_type_distribution": {"BOOK_RESERVED": 1},
            },
        })
        experiment = created.json()
        run = client.post(f"/api/experiments/{experiment['experiment_id']}/runs", json={}).json()
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        assert client.post(f"{run_url}/start").status_code == 200
        accepted = client.post(f"{run_url}/execute")
        progress = wait_for_runner_state(client, experiment["experiment_id"], run["run_id"])
        run_after = client.app.state.experiment_store.get_run(
            experiment["experiment_id"], run["run_id"],
        )
        accounting = client.app.state.experiment_store.run_event_accounting(run["run_id"])

    assert accepted.status_code == 202
    assert "owner_token" not in accepted.text
    assert len(publisher.events) == 4
    assert len({event.event_id for event in publisher.events}) == 4
    assert all(event.run_id == run["run_id"] for event in publisher.events)
    assert all(event.event_type == EventType.BOOK_RESERVED for event in publisher.events)
    assert all(event.student_id and event.book_id for event in publisher.events)
    assert all(
        later - earlier >= 0.009
        for earlier, later in zip(publisher.starts, publisher.starts[1:])
    )
    assert progress["progress"] == {
        "planned": 4, "generated": 4, "publish_confirmed": 4,
        "publish_failed": 0, "publish_unknown": 0,
    }
    assert accounting["accepted_events"] == 4
    assert run_after["status"] == RunStatus.RUNNING.value
    assert run_after["event_ingestion_closed"] is True
    assert run_after["stop_intent"]["target"] == RunStatus.COMPLETED.value


def test_completed_runner_is_finalized_after_later_stop_intent(tmp_path):
    class Publisher:
        async def publish(self, event):
            pass

    with TestClient(create_app(
        publisher=Publisher(), experiment_db_path=tmp_path / "finalize-after-runner.sqlite3",
    )) as client:
        experiment = client.post("/api/experiments", json={"name": "finalize runner"}).json()
        run = client.post(
            f"/api/experiments/{experiment['experiment_id']}/runs",
            json={"workload_configuration": {"target_event_count": 1, "publish_interval_ms": 10}},
        ).json()
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")
        assert client.post(f"{run_url}/execute").status_code == 202
        deadline = time.monotonic() + 5
        while str(run["run_id"]) in client.app.state.experiment_runner_tasks:
            assert time.monotonic() < deadline
            time.sleep(0.005)
        before = client.app.state.experiment_store.get_run(
            experiment["experiment_id"], run["run_id"],
        )
        assert before["execution"]["state"] == "RELEASED"
        assert before["status"] == RunStatus.RUNNING.value
        assert before["stop_intent"]["target"] == RunStatus.COMPLETED.value
        repository = client.app.state.experiment_store
        event_id = repository.get_run_pending_event_inventory(
            experiment["experiment_id"], run["run_id"],
        )["events"][0]["event_id"]
        repository.claim_run_event(run["run_id"], event_id)
        repository.settle_run_event(run["run_id"], event_id, succeeded=True)

        deadline = time.monotonic() + 5
        while True:
            after = client.app.state.experiment_store.get_run(
                experiment["experiment_id"], run["run_id"],
            )
            if after["status"] == RunStatus.COMPLETED.value:
                break
            assert time.monotonic() < deadline
            time.sleep(0.005)

    assert after["results"]["planned_event_count"] == 1
    assert after["results"]["generated_event_count"] == 1


def test_two_run_workloads_keep_event_ids_and_progress_scoped(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, event):
            self.events.append(event)

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "runner-isolation.sqlite3",
    )) as client:
        experiment = client.post("/api/experiments", json={
            "name": "two runs", "default_workload_configuration": {
                "target_event_count": 2, "publish_interval_ms": 10,
                "event_type_distribution": {"BOOK_RETURNED": 1},
            },
        }).json()
        runs = [
            client.post(f"/api/experiments/{experiment['experiment_id']}/runs", json={}).json()
            for _ in range(2)
        ]
        urls = [
            f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
            for run in runs
        ]
        for url in urls:
            assert client.post(f"{url}/start").status_code == 200
        accepted = [client.post(f"{url}/execute") for url in urls]
        progress = [
            wait_for_runner_state(client, experiment["experiment_id"], run["run_id"])
            for run in runs
        ]
        accounting = [
            client.app.state.experiment_store.run_event_accounting(run["run_id"])
            for run in runs
        ]
    assert all(response.status_code == 202 for response in accepted)
    assert len(publisher.events) == 4
    assert len({event.event_id for event in publisher.events}) == 4
    assert {event.run_id for event in publisher.events} == {run["run_id"] for run in runs}
    assert all(item["progress"]["generated"] == 2 for item in progress)
    assert all(item["accepted_events"] == 2 for item in accounting)


def test_weighted_selector_uses_configured_weights(monkeypatch):
    captured = {}

    def choose(population, *, weights, k):
        captured["population"] = population
        captured["weights"] = weights
        captured["k"] = k
        return [population[0]]

    monkeypatch.setattr("app.experiments.runner.random.choices", choose)
    selected = ExperimentWorkloadRunner.choose_event_type({
        "BOOK_ISSUED": 4, "BOOK_RETURNED": 2,
    })
    assert selected is EventType.BOOK_ISSUED
    assert captured == {
        "population": [EventType.BOOK_ISSUED, EventType.BOOK_RETURNED],
        "weights": [4, 2], "k": 1,
    }


def test_ambiguous_and_definite_publish_outcomes_are_separate_and_not_retried(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []
            self.outcomes = [
                "confirmed",
                RabbitMQPublishError("nack", ambiguous=False),
                RabbitMQPublishError("timeout", ambiguous=True),
            ]

        async def publish(self, event):
            self.events.append(event)
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "runner-outcomes.sqlite3",
    )) as client:
        experiment = client.post("/api/experiments", json={
            "name": "outcomes", "default_workload_configuration": {
                "target_event_count": 3, "publish_interval_ms": 10,
                "event_type_distribution": {"BOOK_ISSUED": 1},
            },
        }).json()
        run = client.post(f"/api/experiments/{experiment['experiment_id']}/runs", json={}).json()
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")
        assert client.post(f"{run_url}/execute").status_code == 202
        progress = wait_for_runner_state(client, experiment["experiment_id"], run["run_id"])
        accounting = client.app.state.experiment_store.run_event_accounting(run["run_id"])

    assert len(publisher.events) == 3
    assert progress["progress"] == {
        "planned": 3, "generated": 3, "publish_confirmed": 1,
        "publish_failed": 1, "publish_unknown": 1,
    }
    assert accounting["unresolved_publish_count"] == 1


def test_invalid_snapshot_and_duplicate_execution_are_rejected(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, event):
            self.events.append(event)
            await asyncio.sleep(0.1)

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "runner-guard.sqlite3",
    )) as client:
        experiment = client.post("/api/experiments", json={
            "name": "runner guard", "default_workload_configuration": {
                "target_event_count": 5, "publish_interval_ms": 10,
                "event_type_distribution": {"BOOK_ISSUED": 1},
            },
        }).json()
        run = client.post(f"/api/experiments/{experiment['experiment_id']}/runs", json={}).json()
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")
        with client.app.state.experiment_store._transaction() as connection:
            connection.execute(
                "UPDATE experiment_runs SET workload_snapshot = ? WHERE run_id = ?",
                (json.dumps({**run["workload_snapshot"], "publish_interval_ms": 0}), run["run_id"]),
            )
        invalid = client.post(f"{run_url}/execute")

        valid = client.post("/api/experiments", json={
            "name": "valid guard", "default_workload_configuration": {
                "target_event_count": 5, "publish_interval_ms": 10,
                "event_type_distribution": {"BOOK_ISSUED": 1},
            },
        }).json()
        valid_run = client.post(f"/api/experiments/{valid['experiment_id']}/runs", json={}).json()
        valid_url = f"/api/experiments/{valid['experiment_id']}/runs/{valid_run['run_id']}"
        client.post(f"{valid_url}/start")
        first = client.post(f"{valid_url}/execute")
        duplicate = client.post(f"{valid_url}/execute")
        wait_for_runner_state(client, valid["experiment_id"], valid_run["run_id"])
        invalid_state = client.app.state.experiment_store.get_run(
            experiment["experiment_id"], run["run_id"],
        )

    assert invalid.status_code == 409
    assert invalid_state["execution"]["state"] == "NOT_STARTED"
    assert first.status_code == 202
    assert duplicate.status_code == 409
    assert len(publisher.events) == 5


@pytest.mark.asyncio
async def test_lease_renewal_failure_stops_new_publishes_after_inflight_event(tmp_path):
    repository = ExperimentRepository(tmp_path / "runner-lease-loss.sqlite3")
    repository.initialize()
    _, run = start_run(repository, planned=6)
    claim = repository.claim_run_execution(run["run_id"], lease_seconds=2)
    original_renew = repository.renew_run_execution_claim
    publish_started = asyncio.Event()

    def fail_renewal(*args, **kwargs):
        if publish_started.is_set():
            raise ExecutionClaimConflict("injected lease renewal failure")
        return original_renew(*args, **kwargs)

    repository.renew_run_execution_claim = fail_renewal
    published = []

    async def publish(event, _lease_guard):
        published.append(event)
        publish_started.set()
        await asyncio.sleep(0.06)
        return EventPublishResult("confirmed")

    runner = ExperimentWorkloadRunner(
        repository, publish, lease_seconds=2, renewal_interval_seconds=0.01,
    )
    report = await runner.execute(run, claim)
    repository.renew_run_execution_claim = original_renew
    progress = repository.get_run(run["experiment_id"], run["run_id"])["progress"]

    assert report["stopped_for_lease_loss"] is True
    assert report["claim_released"] is False
    assert len(published) == 1
    assert progress["generated"] == 1
    assert progress["publish_confirmed"] == 1


@pytest.mark.asyncio
async def test_lease_renews_during_publish_and_releases_after_final_progress(tmp_path):
    repository = ExperimentRepository(tmp_path / "runner-lease-renew.sqlite3")
    repository.initialize()
    experiment, run = start_run(repository, planned=1)
    claim = repository.claim_run_execution(run["run_id"], lease_seconds=2)
    renew = repository.renew_run_execution_claim
    renew_count = 0
    release = repository.release_run_execution_claim
    release_observations = []

    def count_renewals(*args, **kwargs):
        nonlocal renew_count
        renew_count += 1
        return renew(*args, **kwargs)

    def assert_progress_then_release(run_id, owner_token):
        state = repository.get_run(experiment["experiment_id"], run_id)
        release_observations.append(state["progress"].copy())
        assert state["progress"]["generated"] == 1
        assert state["progress"]["publish_confirmed"] == 1
        return release(run_id, owner_token)

    repository.renew_run_execution_claim = count_renewals
    repository.release_run_execution_claim = assert_progress_then_release

    async def publish(event, _lease_guard):
        repository.reserve_run_event(run["run_id"], event.event_id)
        repository.mark_run_event_published(run["run_id"], event.event_id)
        await asyncio.sleep(0.05)
        return EventPublishResult("confirmed")

    runner = ExperimentWorkloadRunner(
        repository, publish, lease_seconds=2, renewal_interval_seconds=0.005,
    )
    report = await runner.execute(run, claim)
    repository.renew_run_execution_claim = renew
    repository.release_run_execution_claim = release

    assert renew_count >= 1
    assert report["claim_released"] is True
    assert release_observations == [{
        "planned": 1, "generated": 1, "publish_confirmed": 1,
        "publish_failed": 0, "publish_unknown": 0,
    }]


@pytest.mark.asyncio
async def test_progress_retry_reuses_idempotency_key_and_storage_failure_stops_safely(tmp_path):
    repository = ExperimentRepository(tmp_path / "runner-progress-retry.sqlite3")
    repository.initialize()
    _, run = start_run(repository, planned=2)
    claim = repository.claim_run_execution(run["run_id"])
    original_update = repository.update_run_execution_progress
    calls = []
    uncertain_once = set()

    def commit_then_raise(run_id, owner_token, **kwargs):
        calls.append(kwargs["operation_id"])
        result = original_update(run_id, owner_token, **kwargs)
        if kwargs["operation_id"] not in uncertain_once:
            uncertain_once.add(kwargs["operation_id"])
            raise OSError("simulated lost response after commit")
        return result

    repository.update_run_execution_progress = commit_then_raise
    published = []

    async def publish(event, _lease_guard):
        published.append(event)
        return EventPublishResult("confirmed")

    runner = ExperimentWorkloadRunner(repository, publish, renewal_interval_seconds=5)
    await runner.execute(run, claim)
    repository.update_run_execution_progress = original_update
    progress = repository.get_run(run["experiment_id"], run["run_id"])["progress"]
    assert len(published) == 2
    assert progress["generated"] == 2
    assert progress["publish_confirmed"] == 2
    assert len(calls) == 8
    assert len(set(calls[0:2])) == 1

    _, failed_run = start_run(repository, planned=1)
    failed_claim = repository.claim_run_execution(failed_run["run_id"])
    repository.update_run_execution_progress = lambda *args, **kwargs: (_ for _ in ()).throw(
        OSError("progress store unavailable")
    )
    no_publish = []

    async def should_not_publish(event, _lease_guard):
        no_publish.append(event)
        return EventPublishResult("confirmed")

    failed_runner = ExperimentWorkloadRunner(
        repository, should_not_publish, progress_retry_attempts=2,
        renewal_interval_seconds=5,
    )
    report = await failed_runner.execute(failed_run, failed_claim)
    repository.update_run_execution_progress = original_update
    failed_state = repository.get_run(failed_run["experiment_id"], failed_run["run_id"])
    assert not no_publish
    assert report["claim_released"] is False
    assert failed_state["execution"]["state"] == "CLAIMED"
    assert failed_state["progress"]["generated"] == 0


def test_lease_loss_during_reservation_prevents_broker_invocation(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, event):
            self.events.append(event)

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "runner-reservation-lease.sqlite3",
    )) as client:
        experiment = client.post("/api/experiments", json={
            "name": "lease during reservation", "default_workload_configuration": {
                "target_event_count": 2, "publish_interval_ms": 10,
                "event_type_distribution": {"BOOK_ISSUED": 1},
            },
        }).json()
        run = client.post(f"/api/experiments/{experiment['experiment_id']}/runs", json={}).json()
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")
        repository = client.app.state.experiment_store
        original_reserve = repository.reserve_run_event
        started = ThreadEvent()
        reservation_finished = ThreadEvent()
        allow_reservation = ThreadEvent()
        lease_failure_observed = ThreadEvent()

        def block_reservation(run_id, event_id):
            started.set()
            assert allow_reservation.wait(timeout=3)
            try:
                return original_reserve(run_id, event_id)
            finally:
                reservation_finished.set()

        repository.reserve_run_event = block_reservation
        runner = client.app.state.experiment_runner
        runner.renewal_interval_seconds = 0.01

        async def fail_renewal(_run_id, _owner_token, lost):
            assert await asyncio.to_thread(started.wait, 3)
            lost.set()
            lease_failure_observed.set()

        runner._renew_lease = fail_renewal
        accepted = client.post(f"{run_url}/execute")
        assert accepted.status_code == 202
        assert started.wait(timeout=3)
        assert lease_failure_observed.wait(timeout=3)
        assert not reservation_finished.is_set()
        allow_reservation.set()
        deadline = time.monotonic() + 3
        while str(run["run_id"]) in client.app.state.experiment_runner_tasks:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        repository.reserve_run_event = original_reserve
        state = repository.get_run(experiment["experiment_id"], run["run_id"])
        progress = state["progress"]
        with repository._reader() as connection:
            reservations = connection.execute(
                "SELECT COUNT(*) FROM experiment_run_events WHERE run_id = ?",
                (run["run_id"],),
            ).fetchone()[0]

    assert publisher.events == []
    assert reservations == 0
    assert progress["generated"] == 1
    assert progress["publish_failed"] == 1
    assert state["execution"]["state"] == "CLAIMED"


def test_cancellation_after_broker_invocation_marks_unknown_and_propagates(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []
            self.started = None

        async def publish(self, event):
            self.events.append(event)
            self.started.set()
            await asyncio.Future()

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "cancel-inflight.sqlite3",
    )) as client:
        repository = client.app.state.experiment_store
        experiment, run = start_run(repository, planned=1)
        marked_unknown = []
        original_mark = repository.mark_run_event_publish_unknown

        def record_unknown(run_id, event_id):
            marked_unknown.append((run_id, event_id))
            return original_mark(run_id, event_id)

        repository.mark_run_event_publish_unknown = record_unknown
        event = ExperimentWorkloadRunner.build_event(run["run_id"], EventType.BOOK_ISSUED)

        async def cancel_after_publish_starts():
            publisher.started = asyncio.Event()
            task = asyncio.create_task(
                client.app.state.experiment_runner.publish_event(event, lambda: True)
            )
            await publisher.started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_after_publish_starts())
        repository.mark_run_event_publish_unknown = original_mark
        with repository._reader() as connection:
            reservation = connection.execute(
                "SELECT publish_unknown, published FROM experiment_run_events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()

    assert len(publisher.events) == 1
    assert marked_unknown == [(run["run_id"], event.event_id)]
    assert dict(reservation) == {"publish_unknown": 1, "published": 0}
    assert repository.unresolved_publish_count(run["run_id"]) == 1


def test_second_cancellation_waits_for_unknown_settlement_and_propagates(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []
            self.started = None

        async def publish(self, event):
            self.events.append(event)
            self.started.set()
            await asyncio.Future()

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "cancel-twice-settlement.sqlite3",
    )) as client:
        repository = client.app.state.experiment_store
        experiment, run = start_run(repository, planned=1)
        original_mark = repository.mark_run_event_publish_unknown
        settlement_started = ThreadEvent()
        allow_settlement = ThreadEvent()

        def block_settlement(run_id, event_id):
            settlement_started.set()
            assert allow_settlement.wait(timeout=5)
            return original_mark(run_id, event_id)

        repository.mark_run_event_publish_unknown = block_settlement
        event = ExperimentWorkloadRunner.build_event(run["run_id"], EventType.BOOK_ISSUED)

        async def cancel_twice_during_settlement():
            publisher.started = asyncio.Event()
            task = asyncio.create_task(
                client.app.state.experiment_runner.publish_event(event, lambda: True)
            )
            await publisher.started.wait()
            task.cancel()
            assert await asyncio.to_thread(settlement_started.wait, 3)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            allow_settlement.set()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_twice_during_settlement())
        repository.mark_run_event_publish_unknown = original_mark
        completion = client.post(
            f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}/complete"
        )
        with repository._reader() as connection:
            row = connection.execute(
                "SELECT publish_unknown, published, settled FROM experiment_run_events "
                "WHERE event_id = ?", (event.event_id,),
            ).fetchone()

    assert len(publisher.events) == 1
    assert dict(row) == {"publish_unknown": 1, "published": 0, "settled": 0}
    assert repository.unresolved_publish_count(run["run_id"]) == 1
    assert completion.status_code == 409


def test_settlement_failure_during_repeated_cancellation_stays_pending(tmp_path, caplog):
    class Publisher:
        def __init__(self):
            self.events = []
            self.started = None

        async def publish(self, event):
            self.events.append(event)
            self.started.set()
            await asyncio.Future()

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "cancel-twice-failed.sqlite3",
    )) as client:
        repository = client.app.state.experiment_store
        experiment, run = start_run(repository, planned=1)
        original_mark = repository.mark_run_event_publish_unknown
        settlement_started = ThreadEvent()
        allow_settlement = ThreadEvent()

        def fail_settlement(*_args):
            settlement_started.set()
            assert allow_settlement.wait(timeout=5)
            raise OSError("injected settlement failure")

        repository.mark_run_event_publish_unknown = fail_settlement
        event = ExperimentWorkloadRunner.build_event(run["run_id"], EventType.BOOK_RETURNED)

        async def cancel_twice_before_failed_settlement():
            publisher.started = asyncio.Event()
            task = asyncio.create_task(
                client.app.state.experiment_runner.publish_event(event, lambda: True)
            )
            await publisher.started.wait()
            task.cancel()
            assert await asyncio.to_thread(settlement_started.wait, 3)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            allow_settlement.set()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_twice_before_failed_settlement())
        repository.mark_run_event_publish_unknown = original_mark
        caplog.set_level(logging.ERROR, logger="app.main")
        completion = client.post(
            f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}/complete"
        )
        with repository._reader() as connection:
            row = connection.execute(
                "SELECT publish_unknown, published, settled FROM experiment_run_events "
                "WHERE event_id = ?", (event.event_id,),
            ).fetchone()

    assert len(publisher.events) == 1
    assert dict(row) == {"publish_unknown": 0, "published": 0, "settled": 0}
    assert repository.unresolved_publish_count(run["run_id"]) == 0
    assert completion.status_code == 409
    assert "Could not confirm unknown publish settlement" in caplog.text


def test_uncertain_unknown_settlement_keeps_reservation_and_never_republishes(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []
            self.started = None

        async def publish(self, event):
            self.events.append(event)
            self.started.set()
            await asyncio.Future()

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "cancel-unsettled.sqlite3",
    )) as client:
        repository = client.app.state.experiment_store
        _, run = start_run(repository, planned=1)
        original_mark = repository.mark_run_event_publish_unknown

        def fail_unknown(*_args):
            raise OSError("unknown settlement unavailable")

        repository.mark_run_event_publish_unknown = fail_unknown
        event = ExperimentWorkloadRunner.build_event(run["run_id"], EventType.BOOK_RETURNED)

        async def cancel_publish():
            publisher.started = asyncio.Event()
            task = asyncio.create_task(
                client.app.state.experiment_runner.publish_event(event, lambda: True)
            )
            await publisher.started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_publish())
        repository.mark_run_event_publish_unknown = original_mark
        with repository._reader() as connection:
            reservation = connection.execute(
                "SELECT publish_unknown, published FROM experiment_run_events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()

    assert len(publisher.events) == 1
    assert dict(reservation) == {"publish_unknown": 0, "published": 0}
    assert repository.unresolved_publish_count(run["run_id"]) == 0
    assert repository.run_event_accounting(run["run_id"])["accepted_events"] == 0


def test_cancellation_while_reservation_is_pending_never_calls_publisher(tmp_path):
    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, event):
            self.events.append(event)

    publisher = Publisher()
    with TestClient(create_app(
        publisher=publisher, experiment_db_path=tmp_path / "cancel-reservation.sqlite3",
    )) as client:
        repository = client.app.state.experiment_store
        _, run = start_run(repository, planned=1)
        original_reserve = repository.reserve_run_event
        started = ThreadEvent()
        allow_reservation = ThreadEvent()

        def block_reservation(run_id, event_id):
            started.set()
            assert allow_reservation.wait(timeout=3)
            return original_reserve(run_id, event_id)

        repository.reserve_run_event = block_reservation
        event = ExperimentWorkloadRunner.build_event(run["run_id"], EventType.BOOK_RESERVED)

        async def cancel_before_invocation():
            task = asyncio.create_task(
                client.app.state.experiment_runner.publish_event(event, lambda: True)
            )
            assert await asyncio.to_thread(started.wait, 3)
            task.cancel()
            allow_reservation.set()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(cancel_before_invocation())
        repository.reserve_run_event = original_reserve
        with repository._reader() as connection:
            reservation_count = connection.execute(
                "SELECT COUNT(*) FROM experiment_run_events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()[0]

    assert publisher.events == []
    assert reservation_count == 0


@pytest.mark.asyncio
async def test_stop_observed_before_publish_accounts_generated_event_and_releases_claim(tmp_path):
    repository = ExperimentRepository(tmp_path / "stop-before-publish.sqlite3")
    repository.initialize()
    experiment, run = start_run(repository, planned=4)
    claim = repository.claim_run_execution(run["run_id"])
    original_update = repository.update_run_execution_progress
    published = []

    def update_and_request_stop(run_id, owner_token, **kwargs):
        result = original_update(run_id, owner_token, **kwargs)
        if kwargs.get("generated") == 1:
            repository.request_run_stop(
                experiment["experiment_id"], run_id, RunStatus.CANCELLED,
            )
        return result

    repository.update_run_execution_progress = update_and_request_stop

    async def should_not_publish(event, _lease_guard):
        published.append(event)
        return EventPublishResult("confirmed")

    runner = ExperimentWorkloadRunner(repository, should_not_publish, renewal_interval_seconds=5)
    report = await runner.execute(run, claim)
    repository.update_run_execution_progress = original_update
    state = repository.get_run(experiment["experiment_id"], run["run_id"])

    assert published == []
    assert state["status"] == RunStatus.RUNNING.value
    assert state["stop_intent"]["target"] == RunStatus.CANCELLED.value
    assert state["progress"]["generated"] == 1
    assert state["progress"]["publish_failed"] == 1
    assert report["claim_released"] is True
    assert state["execution"]["state"] == "RELEASED"


@pytest.mark.asyncio
async def test_stop_while_waiting_for_publish_lock_prevents_reservation_or_publish(tmp_path):
    repository = ExperimentRepository(tmp_path / "stop-publish-lock.sqlite3")
    repository.initialize()
    experiment, run = start_run(repository, planned=2)
    claim = repository.claim_run_execution(run["run_id"])
    publish_lock = asyncio.Lock()
    await publish_lock.acquire()
    published = []

    async def publish(event, _lease_guard):
        published.append(event)
        return EventPublishResult("confirmed")

    runner = ExperimentWorkloadRunner(
        repository, publish, publish_lock=publish_lock, renewal_interval_seconds=5,
    )
    task = asyncio.create_task(runner.execute(run, claim))
    # The runner first records generation, then blocks on the held publish lock.
    deadline = asyncio.get_running_loop().time() + 3
    while repository.get_run(experiment["experiment_id"], run["run_id"])["progress"]["generated"] == 0:
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0)

    await asyncio.to_thread(
        repository.request_run_stop,
        experiment["experiment_id"], run["run_id"], RunStatus.CANCELLED,
    )
    publish_lock.release()
    report = await task
    state = repository.get_run(experiment["experiment_id"], run["run_id"])

    assert published == []
    assert state["progress"]["generated"] == 1
    assert state["progress"]["publish_failed"] == 1
    assert report["claim_released"] is True
    assert state["execution"]["state"] == "RELEASED"
    with repository._reader() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM experiment_run_events WHERE run_id = ?",
            (run["run_id"],),
        ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_stop_during_pacing_prevents_next_generated_event(tmp_path):
    repository = ExperimentRepository(tmp_path / "stop-during-pacing.sqlite3")
    repository.initialize()
    experiment, run = start_run(repository, planned=3, interval_ms=100)
    claim = repository.claim_run_execution(run["run_id"])
    published = []
    pacing_started = asyncio.Event()
    resume_pacing = asyncio.Event()
    original_release = repository.release_run_execution_claim
    release_progress = []

    def inspect_progress_before_release(run_id, owner_token):
        state = repository.get_run(experiment["experiment_id"], run_id)
        release_progress.append(state["progress"].copy())
        return original_release(run_id, owner_token)

    repository.release_run_execution_claim = inspect_progress_before_release

    async def publish(event, _lease_guard):
        published.append(event)
        return EventPublishResult("confirmed")

    runner = ExperimentWorkloadRunner(repository, publish, renewal_interval_seconds=5)

    async def controlled_wait(delay_seconds, lost):
        if delay_seconds > 0:
            pacing_started.set()
            await resume_pacing.wait()
        return lost.is_set()

    runner._wait_or_lease_loss = controlled_wait
    task = asyncio.create_task(runner.execute(run, claim))
    await pacing_started.wait()
    request = await asyncio.to_thread(
        repository.request_run_stop,
        experiment["experiment_id"], run["run_id"], RunStatus.CANCELLED,
    )
    assert request["run"]["event_ingestion_closed"] is True
    resume_pacing.set()
    report = await task
    state = repository.get_run(experiment["experiment_id"], run["run_id"])

    assert len(published) == 1
    assert state["progress"]["generated"] == 1
    assert state["progress"]["publish_confirmed"] == 1
    assert report["claim_released"] is True
    assert release_progress == [state["progress"]]


@pytest.mark.asyncio
async def test_stop_during_inflight_ambiguous_publish_records_outcome_without_republish(tmp_path):
    repository = ExperimentRepository(tmp_path / "stop-inflight-unknown.sqlite3")
    repository.initialize()
    experiment, run = start_run(repository, planned=5)
    claim = repository.claim_run_execution(run["run_id"])
    publish_started = asyncio.Event()
    allow_outcome = asyncio.Event()
    published = []

    async def publish(event, _lease_guard):
        published.append(event)
        publish_started.set()
        await allow_outcome.wait()
        return EventPublishResult("unknown")

    runner = ExperimentWorkloadRunner(repository, publish, renewal_interval_seconds=5)
    task = asyncio.create_task(runner.execute(run, claim))
    await publish_started.wait()
    stop_request = await asyncio.to_thread(
        repository.request_run_stop,
        experiment["experiment_id"], run["run_id"], RunStatus.FAILED,
        reason="controlled stop during publish",
    )
    assert stop_request["run"]["status"] == RunStatus.RUNNING.value
    assert repository.get_run(experiment["experiment_id"], run["run_id"])["execution"]["state"] == "CLAIMED"
    allow_outcome.set()
    report = await task
    state = repository.get_run(experiment["experiment_id"], run["run_id"])

    assert len(published) == 1
    assert state["progress"]["generated"] == 1
    assert state["progress"]["publish_unknown"] == 1
    assert state["stop_intent"]["target"] == RunStatus.FAILED.value
    assert report["claim_released"] is True
    assert state["execution"]["state"] == "RELEASED"


def test_unexpected_runner_exception_is_logged_and_claim_stays_held(tmp_path, caplog):
    class Publisher:
        async def publish(self, event):
            raise AssertionError("replacement callback should be used")

    with TestClient(create_app(
        publisher=Publisher(), experiment_db_path=tmp_path / "runner-exception.sqlite3",
    )) as client:
        experiment = client.post("/api/experiments", json={
            "name": "runner exception", "default_workload_configuration": {
                "target_event_count": 1, "publish_interval_ms": 10,
                "event_type_distribution": {"BOOK_ISSUED": 1},
            },
        }).json()
        run = client.post(f"/api/experiments/{experiment['experiment_id']}/runs", json={}).json()
        run_url = f"/api/experiments/{experiment['experiment_id']}/runs/{run['run_id']}"
        client.post(f"{run_url}/start")

        async def fail_unexpectedly(_event, _lease_guard):
            raise RuntimeError("unexpected runner failure")

        client.app.state.experiment_runner.publish_event = fail_unexpectedly
        caplog.set_level(logging.ERROR, logger="app.main")
        assert client.post(f"{run_url}/execute").status_code == 202
        deadline = time.monotonic() + 3
        while str(run["run_id"]) in client.app.state.experiment_runner_tasks:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        state = client.app.state.experiment_store.get_run(
            experiment["experiment_id"], run["run_id"],
        )

    assert state["execution"]["state"] == "CLAIMED"
    assert "Experiment runner task failed" in caplog.text
    assert run["run_id"] in caplog.text
    assert "RuntimeError" in caplog.text
    assert "owner_token" not in caplog.text

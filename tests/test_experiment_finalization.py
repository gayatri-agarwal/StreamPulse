import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.experiments.schemas import RunStatus
from app.experiments.store import (
    ExecutionClaimConflict, ExperimentRepository, InvalidRunTransition, utc_now,
)
from app.main import create_app
from app.processing.measurements import MeasurementSummary


def make_run(repository, *, workload_count=2, started=True):
    repository.initialize()
    experiment = repository.create_experiment({
        "name": "Finalization", "experiment_type": "window_performance",
        "default_configuration": {"tumbling_size": 5, "sliding_size": 5, "sliding_interval": 1},
    })
    run = repository.create_run(
        experiment["experiment_id"],
        workload_configuration={"target_event_count": workload_count},
    )
    if started:
        repository.transition_run(experiment["experiment_id"], run["run_id"], RunStatus.RUNNING)
    return experiment, run


def complete_workload(repository, experiment, run, *, release=True):
    state = repository.get_run(experiment["experiment_id"], run["run_id"])
    if state["progress"]["generated"] >= state["progress"]["planned"]:
        return
    claim = repository.claim_run_execution(run["run_id"])
    for _ in range(state["progress"]["planned"]):
        event_id = str(uuid4())
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"],
            operation_id=str(uuid4()), generated=1,
        )
        repository.reserve_run_event(run["run_id"], event_id)
        repository.mark_run_event_published(run["run_id"], event_id)
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"],
            operation_id=str(uuid4()), publish_confirmed=1,
        )
        repository.claim_run_event(run["run_id"], event_id)
        repository.settle_run_event(run["run_id"], event_id, succeeded=True)
    if release:
        repository.release_run_execution_claim(run["run_id"], claim["owner_token"])
    return claim


def request_stop(repository, experiment, run, target=RunStatus.COMPLETED):
    if target == RunStatus.COMPLETED:
        complete_workload(repository, experiment, run)
    return repository.request_run_stop(
        experiment["experiment_id"], run["run_id"], target,
        reason="requested by test" if target != RunStatus.COMPLETED else None,
    )


def finalize(repository, experiment, run, result=None):
    target = repository.get_run_stop_intent(run["run_id"])["target"]
    result = dict(result or {"status": target, "result_complete": False})
    result.setdefault("event_accounting", repository.run_event_accounting(run["run_id"]))
    return repository.finalize_run_after_drain(
        experiment["experiment_id"], run["run_id"], ended_at=utc_now(),
        results=result,
    )


@pytest.mark.parametrize("target", [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED])
def test_finalization_honors_persisted_stop_target(tmp_path, target):
    repository = ExperimentRepository(tmp_path / f"{target.value}.sqlite3")
    experiment, run = make_run(repository, started=(target != RunStatus.CANCELLED))
    request_stop(repository, experiment, run, target)

    result = finalize(repository, experiment, run)

    assert result["status"] == target.value
    assert result["failure_reason"] == ("requested by test" if target != RunStatus.COMPLETED else None)
    assert result["result_available"] is True


def test_created_run_can_only_finalize_persisted_cancellation(tmp_path):
    repository = ExperimentRepository(tmp_path / "created-cancel.sqlite3")
    experiment, run = make_run(repository, started=False)
    request_stop(repository, experiment, run, RunStatus.CANCELLED)

    assert finalize(repository, experiment, run)["status"] == RunStatus.CANCELLED.value


@pytest.mark.parametrize("target", [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED])
def test_transition_run_cannot_commit_terminal_state(tmp_path, target):
    repository = ExperimentRepository(tmp_path / f"direct-{target.value}.sqlite3")
    experiment, run = make_run(repository, started=(target != RunStatus.CANCELLED))

    with pytest.raises(InvalidRunTransition, match="finalize_run_after_drain"):
        repository.transition_run(
            experiment["experiment_id"], run["run_id"], target,
            ended_at=utc_now(), results={},
        )

    saved = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert saved["status"] == (RunStatus.RUNNING.value if target != RunStatus.CANCELLED else RunStatus.CREATED.value)
    assert saved["result_available"] is False


def test_running_run_without_stop_intent_cannot_terminalize(tmp_path):
    repository = ExperimentRepository(tmp_path / "missing-stop-intent.sqlite3")
    experiment, run = make_run(repository)

    with pytest.raises(InvalidRunTransition, match="stop intent"):
        repository.finalize_run_after_drain(
            experiment["experiment_id"], run["run_id"], ended_at=utc_now(),
            results={"event_accounting": repository.run_event_accounting(run["run_id"])},
        )
    with pytest.raises(InvalidRunTransition, match="finalize_run_after_drain"):
        repository.transition_run(
            experiment["experiment_id"], run["run_id"], RunStatus.COMPLETED,
            ended_at=utc_now(), results={},
        )
    assert repository.get_run(experiment["experiment_id"], run["run_id"])["status"] == "RUNNING"


def test_incomplete_workload_cannot_persist_completed_intent(tmp_path):
    repository = ExperimentRepository(tmp_path / "incomplete-intent.sqlite3")
    experiment, run = make_run(repository, workload_count=2)
    claim = repository.claim_run_execution(run["run_id"])
    event_id = str(uuid4())
    repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"], operation_id=str(uuid4()), generated=1,
    )
    repository.reserve_run_event(run["run_id"], event_id)
    repository.mark_run_event_published(run["run_id"], event_id)
    repository.update_run_execution_progress(
        run["run_id"], claim["owner_token"],
        operation_id=str(uuid4()), publish_confirmed=1,
    )
    repository.release_run_execution_claim(run["run_id"], claim["owner_token"])

    with pytest.raises(InvalidRunTransition, match="full planned workload"):
        repository.request_run_stop(
            experiment["experiment_id"], run["run_id"], RunStatus.COMPLETED,
        )
    saved = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert saved["stop_intent"] is None
    assert saved["event_ingestion_closed"] is False


def test_cancelled_target_remains_authoritative_over_completion(tmp_path):
    repository = ExperimentRepository(tmp_path / "cancel-precedence.sqlite3")
    experiment, run = make_run(repository, workload_count=1)
    complete_workload(repository, experiment, run)
    request_stop(repository, experiment, run, RunStatus.CANCELLED)

    with pytest.raises(InvalidRunTransition, match="already has a CANCELLED"):
        request_stop(repository, experiment, run, RunStatus.COMPLETED)
    assert finalize(repository, experiment, run)["status"] == RunStatus.CANCELLED.value


def test_interrupted_execution_cannot_finalize_completed_intent(tmp_path):
    repository = ExperimentRepository(tmp_path / "interrupted-completion.sqlite3")
    experiment, run = make_run(repository, workload_count=1)
    claim = complete_workload(repository, experiment, run, release=False)
    request_stop(repository, experiment, run)
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
            ("2000-01-01T00:00:00+00:00", run["run_id"]),
        )

    with pytest.raises(ExecutionClaimConflict):
        finalize(repository, experiment, run)
    interrupted = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert interrupted["execution"]["interruption_recorded"] is True
    with pytest.raises(InvalidRunTransition, match="interrupted execution"):
        finalize(repository, experiment, run)
    assert repository.get_run(experiment["experiment_id"], run["run_id"])["status"] == "RUNNING"


def test_direct_conflicting_target_cannot_override_persisted_intent(tmp_path):
    repository = ExperimentRepository(tmp_path / "direct-conflicting-target.sqlite3")
    experiment, run = make_run(repository)
    request_stop(repository, experiment, run, RunStatus.COMPLETED)

    with pytest.raises(InvalidRunTransition, match="finalize_run_after_drain"):
        repository.transition_run(
            experiment["experiment_id"], run["run_id"], RunStatus.FAILED,
            ended_at=utc_now(), failure_reason="wrong target", results={},
        )

    assert repository.get_run(experiment["experiment_id"], run["run_id"])["status"] == "RUNNING"
    assert finalize(repository, experiment, run)["status"] == RunStatus.COMPLETED.value


def test_created_to_running_transition_remains_supported(tmp_path):
    repository = ExperimentRepository(tmp_path / "nonterminal-transition.sqlite3")
    experiment, run = make_run(repository, started=False)

    transitioned = repository.transition_run(
        experiment["experiment_id"], run["run_id"], RunStatus.RUNNING,
    )

    assert transitioned["status"] == RunStatus.RUNNING.value
    assert transitioned["started_at"] is not None


def test_conflicting_stop_target_cannot_change_finalization_target(tmp_path):
    repository = ExperimentRepository(tmp_path / "target-conflict.sqlite3")
    experiment, run = make_run(repository)
    request_stop(repository, experiment, run, RunStatus.COMPLETED)

    with pytest.raises(InvalidRunTransition):
        request_stop(repository, experiment, run, RunStatus.FAILED)
    assert finalize(repository, experiment, run)["status"] == RunStatus.COMPLETED.value


def test_active_execution_claim_blocks_finalization(tmp_path):
    repository = ExperimentRepository(tmp_path / "active-claim.sqlite3")
    experiment, run = make_run(repository)
    claim = complete_workload(repository, experiment, run, release=False)
    request_stop(repository, experiment, run)

    with pytest.raises(ExecutionClaimConflict):
        finalize(repository, experiment, run)
    assert repository.get_run(experiment["experiment_id"], run["run_id"])["status"] == "RUNNING"


def test_open_ingestion_gate_blocks_finalization(tmp_path):
    repository = ExperimentRepository(tmp_path / "open-gate.sqlite3")
    experiment, run = make_run(repository)
    request_stop(repository, experiment, run)
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET event_ingestion_closed = 0 WHERE run_id = ?",
            (run["run_id"],),
        )

    with pytest.raises(InvalidRunTransition, match="ingestion"):
        finalize(repository, experiment, run)


@pytest.mark.parametrize("pending_state", ["reserved", "unsettled", "in_flight"])
def test_each_pending_predicate_condition_blocks_finalization(tmp_path, pending_state):
    repository = ExperimentRepository(tmp_path / f"pending-{pending_state}.sqlite3")
    experiment, run = make_run(repository)
    request_stop(repository, experiment, run)
    with repository._reader() as connection:
        event_id = connection.execute(
            "SELECT event_id FROM experiment_run_events WHERE run_id = ? LIMIT 1",
            (run["run_id"],),
        ).fetchone()[0]
    if pending_state == "reserved":
        with repository._transaction() as connection:
            connection.execute(
                "UPDATE experiment_run_events SET published = 0 WHERE event_id = ?",
                (event_id,),
            )
    elif pending_state == "unsettled":
        with repository._transaction() as connection:
            connection.execute(
                "UPDATE experiment_run_events SET settled = 0, outcome = NULL WHERE event_id = ?",
                (event_id,),
            )
    else:
        with repository._transaction() as connection:
            connection.execute(
                "UPDATE experiment_run_events SET in_flight_deliveries = 1 WHERE event_id = ?",
                (event_id,),
            )

    with pytest.raises(InvalidRunTransition, match="awaiting"):
        finalize(repository, experiment, run)
    assert repository.get_run_pending_event_inventory(
        experiment["experiment_id"], run["run_id"],
    )["summary"]["pending_count"] == 1


def test_running_without_stop_intent_and_expired_claim_cannot_finalize(tmp_path):
    repository = ExperimentRepository(tmp_path / "no-intent.sqlite3")
    experiment, run = make_run(repository)
    with pytest.raises(InvalidRunTransition, match="stop intent"):
        repository.finalize_run_after_drain(
            experiment["experiment_id"], run["run_id"], ended_at=utc_now(), results={},
        )

    claim = repository.claim_run_execution(run["run_id"], lease_seconds=5)
    with repository._transaction() as connection:
        connection.execute(
            "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
            ("2000-01-01T00:00:00+00:00", run["run_id"]),
        )
    with pytest.raises(ExecutionClaimConflict):
        repository.renew_run_execution_claim(run["run_id"], claim["owner_token"])
    with pytest.raises(InvalidRunTransition, match="stop intent"):
        repository.finalize_run_after_drain(
            experiment["experiment_id"], run["run_id"], ended_at=utc_now(), results={},
        )
    assert repository.get_run(experiment["experiment_id"], run["run_id"])["status"] == "RUNNING"


def test_concurrent_finalizers_persist_one_immutable_result(tmp_path):
    repository = ExperimentRepository(tmp_path / "concurrent-finalize.sqlite3")
    experiment, run = make_run(repository)
    request_stop(repository, experiment, run)

    def commit(value):
        return repository.finalize_run_after_drain(
            experiment["experiment_id"], run["run_id"], ended_at=utc_now(),
            results={
                "winner": value, "result_complete": False,
                "event_accounting": repository.run_event_accounting(run["run_id"]),
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(commit, ("A", "B")))
    assert results[0]["results"] == results[1]["results"]
    assert results[0]["results"]["winner"] in {"A", "B"}


def test_terminal_update_failure_rolls_back_status_and_result(tmp_path):
    repository = ExperimentRepository(tmp_path / "rollback.sqlite3")
    experiment, run = make_run(repository)
    request_stop(repository, experiment, run)
    with repository._transaction() as connection:
        connection.execute(
            "CREATE TRIGGER reject_terminal_result BEFORE UPDATE OF results_json ON experiment_runs "
            "WHEN NEW.results_json IS NOT NULL BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError):
        finalize(repository, experiment, run)
    saved = repository.get_run(experiment["experiment_id"], run["run_id"])
    assert saved["status"] == "RUNNING"
    assert saved["result_available"] is False


def test_repeated_finalization_returns_persisted_snapshot(tmp_path):
    repository = ExperimentRepository(tmp_path / "idempotent.sqlite3")
    experiment, run = make_run(repository)
    request_stop(repository, experiment, run)
    first = finalize(repository, experiment, run, {"marker": "first"})
    second = finalize(repository, experiment, run, {"marker": "replacement"})
    assert first["results"] == second["results"]
    assert first["results"]["marker"] == "first"


def test_accounting_change_during_snapshot_build_requires_finalization_retry(tmp_path):
    repository = ExperimentRepository(tmp_path / "accounting-race.sqlite3")
    experiment, run = make_run(repository, workload_count=2)
    claim = repository.claim_run_execution(run["run_id"])
    event_ids = []
    for _ in range(2):
        event_id = str(uuid4())
        event_ids.append(event_id)
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"], operation_id=str(uuid4()), generated=1,
        )
        repository.reserve_run_event(run["run_id"], event_id)
        repository.mark_run_event_published(run["run_id"], event_id)
        repository.update_run_execution_progress(
            run["run_id"], claim["owner_token"],
            operation_id=str(uuid4()), publish_confirmed=1,
        )
    repository.release_run_execution_claim(run["run_id"], claim["owner_token"])
    repository.claim_run_event(run["run_id"], event_ids[0])
    repository.settle_run_event(run["run_id"], event_ids[0], succeeded=True)
    stale_accounting = repository.run_event_accounting(run["run_id"])
    request_stop(repository, experiment, run)
    repository.claim_run_event(run["run_id"], event_ids[1])
    repository.settle_run_event(run["run_id"], event_ids[1], succeeded=True)

    with pytest.raises(InvalidRunTransition, match="accounting changed"):
        repository.finalize_run_after_drain(
            experiment["experiment_id"], run["run_id"], ended_at=utc_now(),
            results={"event_accounting": stale_accounting},
        )
    assert repository.get_run(experiment["experiment_id"], run["run_id"])["status"] == "RUNNING"
    fresh = finalize(repository, experiment, run)
    assert fresh["results"]["event_accounting"]["accepted_events"] == 2


def test_finalization_uses_durable_accounting_without_flushing_partial_window():
    from app.processing.event_processor import LibraryEventProcessor
    from app.processing.window_config import WindowConfiguration
    from app.processing.window_manager import SlidingWindow, TumblingWindow
    from app.schemas import LibraryEvent

    class Store:
        def __init__(self):
            self.saved = []

        async def save_metrics(self, kind, metrics):
            self.saved.append(kind)

    metrics_store = Store()
    configuration = WindowConfiguration(tumbling_size=5, sliding_size=5, sliding_interval=1)
    processor = LibraryEventProcessor(
        TumblingWindow(5), SlidingWindow(5, 1), metrics_store,
        active_configuration=configuration,
    )
    event = LibraryEvent.from_mapping({
        "event_id": str(uuid4()), "event_type": "BOOK_ISSUED", "student_id": "s",
        "book_id": "b", "timestamp": "2026-01-01T00:00:00+00:00", "value": 1.0,
    })
    import asyncio
    asyncio.run(processor.process(event))
    assert processor.tumbling.pending_count == 1
    assert "tumbling" not in metrics_store.saved


def test_workload_shortfall_makes_results_incomplete_without_affecting_latency(tmp_path):
    class Measurements:
        async def get_measurement_summary(self, run_id=None):
            return MeasurementSummary(
                publish_attempts=0, publish_successes=0, publish_failures=0,
                publish_failure_rate=None, consumer_attempts=0, consumer_successes=0,
                consumer_failures=0, consumer_failure_rate=None,
                throughput_events_per_second=None,
                sample_count={"consumer_processing_latency_ms": 1},
                average_latency_ms={"consumer_processing_latency_ms": 1.0},
                p95_latency_ms={"consumer_processing_latency_ms": 1.0},
                consumer_unique_successes=0, inter_arrival_rate_events_per_second=None,
                measurements_present=True,
            )

    with TestClient(create_app(
        metrics_store=Measurements(), experiment_db_path=tmp_path / "shortfall.sqlite3",
    )) as client:
        experiment_response = client.post("/api/experiments", json={"name": "shortfall"})
        experiment_id = experiment_response.json()["experiment_id"]
        run = client.post(
            f"/api/experiments/{experiment_id}/runs",
            json={"workload_configuration": {"target_event_count": 2}},
        ).json()
        run_id = run["run_id"]
        run_url = f"/api/experiments/{experiment_id}/runs/{run_id}"
        client.post(f"{run_url}/start")
        repository = client.app.state.experiment_store
        claim = repository.claim_run_execution(run_id)
        repository.update_run_execution_progress(
            run_id, claim["owner_token"], operation_id=str(uuid4()), generated=1,
        )
        repository.release_run_execution_claim(run_id, claim["owner_token"])
        rejected = client.post(f"{run_url}/complete")
        assert rejected.status_code == 409
        assert client.post(f"{run_url}/fail", json={"reason": "short workload"}).status_code == 202
        import asyncio
        result = asyncio.run(client.app.state.finalize_experiment_run(experiment_id, run_id))["results"]

    assert result["planned_event_count"] == 2
    assert result["generated_event_count"] == 1
    assert result["remaining_workload_count"] == result["workload_shortfall"] == 1
    assert result["planned_workload_fully_generated"] is False
    assert result["result_complete"] is False
    assert result["latency_complete"] is True

"""FastAPI application for publishing library events and reading metrics."""
from dotenv import load_dotenv

load_dotenv()
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import asyncio
import inspect
import logging
import math
import random
import time
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Optional
from uuid import UUID, uuid4

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, field_validator

from app.experiments.schemas import (
    ExperimentCreate, ExperimentUpdate, RunCreate, RunFailure, RunReason, RunStatus,
)
from app.experiments.store import (
    ExecutionClaimConflict, ExecutionProgressConflict, ExperimentConflict,
    ExperimentNotFound, ExperimentRepository, InvalidRunWorkloadConfiguration,
    InvalidRunTransition, RunNotFound, utc_now,
)
from app.experiments.runner import EventPublishResult, ExperimentWorkloadRunner
from app.messaging.rabbitmq import RabbitMQError, RabbitMQPublisher, RabbitMQPublishError
from app.processing.window_config import WindowConfiguration
from app.schemas import EventType, LibraryEvent
from app.storage.redis_metrics import (
    RedisMetricsStore,
    RedisConfigurationDataError,
    RedisConfigurationError,
    RedisMonitoringDataError,
    RedisMonitoringError,
    RedisStorageError,
    MEASUREMENT_STORAGE_OPERATION_TIMEOUT_SECONDS,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIR = PROJECT_ROOT / "frontend"
logger = logging.getLogger(__name__)


async def _await_task_to_completion(task: asyncio.Task) -> Optional[BaseException]:
    """Observe a shielded cleanup task despite repeated cancellation of its caller."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # A new shield is needed after each cancellation: shield protects the
            # child task, while this loop keeps waiting for its actual completion.
            continue
        except BaseException:
            # The task is normally done when its exception reaches this await;
            # task.result() below observes and returns that exception.
            break
    try:
        task.result()
    except BaseException as exc:
        return exc
    return None


def load_project_environment() -> None:
    """Load project settings without replacing values supplied by the process."""
    load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)


load_project_environment()


class EventSubmission(BaseModel):
    """Event input; numeric latency is assigned by the consumer."""

    model_config = ConfigDict(extra="forbid", strict=True)

    event_type: str
    student_id: str
    book_id: str
    value: Optional[float] = None  # Accepted for older clients; never used as the metric value.
    run_id: Optional[str] = None

    @field_validator("run_id")
    @classmethod
    def validate_run_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        try:
            return str(UUID(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("run_id must be a valid UUID") from exc


class SampleBatchSubmission(BaseModel):
    """A finite request for a supported sample batch size."""

    model_config = ConfigDict(extra="forbid", strict=True)

    batch_size: int = 8
    run_id: Optional[str] = None

    @field_validator("run_id")
    @classmethod
    def validate_run_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        try:
            return str(UUID(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("run_id must be a valid UUID") from exc


BATCH_EVENT_INTERVAL_SECONDS = 0.1


def _monotonic_time() -> float:
    return time.monotonic()


async def _wait_for_batch_interval(delay: float) -> None:
    await asyncio.sleep(delay)


def _random_library_event(run_id: Optional[str] = None) -> LibraryEvent:
    return LibraryEvent.from_mapping({
        "event_id": str(uuid4()),
        "event_type": random.choice(tuple(EventType)).value,
        "student_id": f"STU-{random.randint(1000, 9999)}",
        "book_id": f"BK-{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc),
        **({"run_id": run_id} if run_id is not None else {}),
    })



def _event_data(event: LibraryEvent) -> Dict[str, Any]:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "student_id": event.student_id,
        "book_id": event.book_id,
        "timestamp": event.timestamp.isoformat(),
        **({"run_id": event.run_id} if event.run_id is not None else {}),
        **({"value": event.value} if event.value is not None else {}),
    }


def _metrics_data(metrics: Any) -> Optional[Dict[str, Any]]:
    if metrics is None:
        return None
    return {
        "count": metrics.count,
        "sum": metrics.total,
        "average": metrics.average,
        "minimum": metrics.minimum,
        "maximum": metrics.maximum,
    }


def create_app(*, publisher: Any = None, metrics_store: Any = None,
               recent_event_limit: int = 100,
               experiment_db_path: Optional[Path | str] = None) -> FastAPI:
    """Create an app; dependencies can be injected for tests without live services."""
    if isinstance(recent_event_limit, bool) or not isinstance(recent_event_limit, int) or recent_event_limit <= 0:
        raise ValueError("recent_event_limit must be a positive integer")

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        await asyncio.to_thread(application.state.experiment_store.initialize)
        if application.state.publisher is None:
            try:
                application.state.publisher = RabbitMQPublisher()
                application.state.rabbitmq_status = "not_verified"
            except ValueError:
                application.state.rabbitmq_status = "not_configured"
        if application.state.metrics_store is None:
            try:
                application.state.metrics_store = RedisMetricsStore()
                application.state.redis_status = "not_verified"
            except ValueError:
                application.state.redis_status = "not_configured"
        # Recreate drain retry coordinators from durable stop intents after restart.
        try:
            experiments = await database_call(application.state.experiment_store.list_experiments)
            for experiment in experiments:
                runs = await database_call(
                    application.state.experiment_store.list_runs, experiment["experiment_id"],
                )
                for run in runs:
                    if (run["stop_intent"] is not None and run["results"] is None
                            and run["status"] not in {
                                RunStatus.COMPLETED.value, RunStatus.FAILED.value,
                                RunStatus.CANCELLED.value,
                            }):
                        schedule_run_finalization(experiment["experiment_id"], run["run_id"])
        except Exception as exc:
            logger.error(
                "Could not recover persisted run finalization tasks (exception_type=%s)",
                type(exc).__name__,
            )
        try:
            yield
        finally:
            runner_tasks = list(application.state.experiment_runner_tasks.values())
            application.state.shutting_down = True
            # Close admission cooperatively and let in-flight publish outcomes settle.
            for run_id in list(application.state.experiment_runner_tasks):
                try:
                    run = await database_call(
                        application.state.experiment_store.get_run_with_workload_configuration_snapshot,
                        run_id,
                    )
                    await database_call(
                        application.state.experiment_store.request_run_stop,
                        run["experiment_id"], run_id, RunStatus.CANCELLED,
                        reason="Application shutdown",
                    )
                except InvalidRunTransition:
                    # Existing stop intent is immutable and remains authoritative.
                    pass
                except Exception as exc:
                    logger.warning(
                        "Could not persist shutdown stop intent (run_id=%s exception_type=%s)",
                        run_id, type(exc).__name__,
                    )
            if runner_tasks:
                _, pending_runners = await asyncio.wait(runner_tasks, timeout=5.0)
                # Cancellation uses the publisher's durable unknown-outcome path and
                # leaves the execution claim held if settlement cannot be confirmed.
                for task in pending_runners:
                    task.cancel()
                await asyncio.gather(*runner_tasks, return_exceptions=True)
            finalization_tasks = list(application.state.experiment_finalization_tasks.values())
            for task in finalization_tasks:
                task.cancel()
            if finalization_tasks:
                await asyncio.gather(*finalization_tasks, return_exceptions=True)
            for resource in (application.state.publisher, application.state.metrics_store):
                close = getattr(resource, "close", None)
                if close is not None:
                    try:
                        result = close()
                        if inspect.isawaitable(result):
                            await result
                    except Exception:
                        # Shutdown remains clean; adapters expose no secret-bearing details.
                        pass

    application = FastAPI(title="StreamPulse API", version="0.4.0", lifespan=lifespan)
    @application.get("/", include_in_schema=False)
    @application.get("/experiments", include_in_schema=False)
    @application.get("/experiments/new", include_in_schema=False)
    @application.get("/failure-recovery", include_in_schema=False)
    async def frontend_index() -> FileResponse:
        return FileResponse(FRONTEND_DIR / "index.html")

    @application.get("/experiments/{experiment_id:uuid}", include_in_schema=False)
    async def frontend_experiment_detail(experiment_id: UUID) -> FileResponse:
        return FileResponse(FRONTEND_DIR / "index.html")

    @application.get("/experiments/{experiment_id:uuid}/runs/{run_id:uuid}", include_in_schema=False)
    async def frontend_experiment_run(experiment_id: UUID, run_id: UUID) -> FileResponse:
        return FileResponse(FRONTEND_DIR / "index.html")

    @application.get("/experiments/{experiment_id:uuid}/runs/{run_id:uuid}/results", include_in_schema=False)
    async def frontend_experiment_results(experiment_id: UUID, run_id: UUID) -> FileResponse:
        return FileResponse(FRONTEND_DIR / "index.html")

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5500", "http://localhost:5500"],
        allow_methods=["GET", "POST", "PUT", "PATCH"],
        allow_headers=["Content-Type"],
    )
    application.state.publisher = publisher
    application.state.metrics_store = metrics_store
    application.state.experiment_store = ExperimentRepository(experiment_db_path)
    application.state.rabbitmq_status = "not_verified" if publisher is not None else "not_configured"
    application.state.redis_status = "not_verified" if metrics_store is not None else "not_configured"
    application.state.recent_events = deque(maxlen=recent_event_limit)
    application.state.experiment_runner_tasks = {}
    application.state.experiment_finalization_tasks = {}
    application.state.shutting_down = False
    application.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
    batch_publish_lock = asyncio.Lock()
    experiment_publish_lock = asyncio.Lock()
    batch_next_publish_at = 0.0

    async def record_publish_measurement(event: LibraryEvent, elapsed_ms: Optional[float], succeeded: bool) -> None:
        recorder = getattr(application.state.metrics_store, "record_measurements", None)
        if not callable(recorder):
            return
        try:
            await asyncio.wait_for(
                recorder(
                    run_id=event.run_id,
                    counters={"publish_attempts": 1,
                              "publish_successes": int(succeeded),
                              "publish_failures": int(not succeeded)},
                    samples={"publish_latency_ms": elapsed_ms},
                ),
                timeout=MEASUREMENT_STORAGE_OPERATION_TIMEOUT_SECONDS,
            )
        except Exception:
            # Instrumentation must not change whether a broker publish succeeded.
            pass

    @application.get("/api/health")
    async def health() -> Dict[str, Any]:
        """Report API availability separately from checked external services."""
        return {
            "status": "available",
            "services": {
                "rabbitmq": application.state.rabbitmq_status,
                "redis": application.state.redis_status,
            },
        }

    def configuration_response(active: WindowConfiguration, pending: Optional[WindowConfiguration]) -> Dict[str, Any]:
        return {
            "active": asdict(active),
            "pending": asdict(pending) if pending is not None else None,
            "status": "pending" if pending is not None else "active",
        }

    async def active_configuration_or_default(store: Any) -> WindowConfiguration:
        active = await store.read_active_configuration()
        if active is None:
            active = await store.initialize_active_configuration()
        return active

    def raise_configuration_storage_error(error: Exception) -> None:
        if isinstance(error, RedisConfigurationDataError):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Stored window configuration is invalid",
            ) from None
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Configuration storage is unavailable",
        ) from None

    @application.get("/api/config")
    async def get_configuration() -> Dict[str, Any]:
        """Return persisted active and pending window configurations."""
        store = application.state.metrics_store
        if store is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Configuration storage is not configured",
            )
        try:
            active = await active_configuration_or_default(store)
            pending = await store.read_pending_configuration()
        except (RedisConfigurationDataError, RedisConfigurationError) as exc:
            raise_configuration_storage_error(exc)
        except Exception:
            raise_configuration_storage_error(RuntimeError())
        return configuration_response(active, pending)

    @application.put("/api/config")
    async def put_configuration(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a candidate and persist it as pending for boundary activation."""
        try:
            configuration = WindowConfiguration(**payload)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

        store = application.state.metrics_store
        if store is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Configuration storage is not configured",
            )
        try:
            active = await active_configuration_or_default(store)
            await store.save_pending_configuration(configuration)
            pending = await store.read_pending_configuration()
        except (RedisConfigurationDataError, RedisConfigurationError) as exc:
            raise_configuration_storage_error(exc)
        except Exception:
            raise_configuration_storage_error(RuntimeError())

        response = configuration_response(active, pending)
        response["message"] = "Configuration will activate after the current tumbling window completes."
        return response

    @application.get("/api/monitoring")
    async def monitoring() -> Dict[str, Any]:
        """Return best-effort component state without failing the whole response."""
        api_redis_status = application.state.redis_status
        store = application.state.metrics_store
        snapshot = None
        consumer_status = "unavailable"
        consumer_reason = "monitoring_not_configured" if store is None else "not_initialized"

        if store is not None:
            try:
                snapshot = await store.read_monitoring_state()
                api_redis_status = "available"
                application.state.redis_status = "available"
            except RedisMonitoringDataError:
                # Redis returned data successfully; the stored monitoring payload is invalid.
                api_redis_status = "available"
                application.state.redis_status = "available"
                consumer_reason = "invalid_snapshot"
            except RedisMonitoringError:
                api_redis_status = "unavailable"
                application.state.redis_status = "unavailable"
                consumer_reason = "redis_unavailable"
            except Exception:
                api_redis_status = "unavailable"
                application.state.redis_status = "unavailable"
                consumer_reason = "redis_unavailable"
            else:
                if snapshot is not None:
                    try:
                        heartbeat_fresh = await store.is_monitoring_heartbeat_fresh()
                        api_redis_status = "available"
                        application.state.redis_status = "available"
                    except RedisMonitoringError:
                        heartbeat_fresh = None
                        api_redis_status = "unavailable"
                        application.state.redis_status = "unavailable"
                        consumer_reason = "redis_unavailable"
                    except Exception:
                        heartbeat_fresh = None
                        api_redis_status = "unavailable"
                        application.state.redis_status = "unavailable"
                        consumer_reason = "redis_unavailable"

                    if api_redis_status != "unavailable":
                        if snapshot.consumer_state == "stopped":
                            consumer_status = "stopped"
                            consumer_reason = None
                        elif snapshot.consumer_state == "running" and heartbeat_fresh is True:
                            consumer_status = "running"
                            consumer_reason = None
                        elif snapshot.consumer_state == "running" and heartbeat_fresh is False:
                            consumer_status = "stale"
                            consumer_reason = "heartbeat_expired"
                        elif heartbeat_fresh is None:
                            consumer_reason = "not_initialized"
                        else:
                            consumer_reason = "invalid_snapshot"

        publisher_adapter = application.state.publisher
        publisher_settings = getattr(publisher_adapter, "settings", None)
        queue_name = getattr(publisher_settings, "queue", None)
        rabbitmq_status = application.state.rabbitmq_status
        messages_ready = None
        consumer_count = None
        counts_observed_at = None
        inspect_queue = getattr(publisher_adapter, "inspect_queue", None)
        if callable(inspect_queue):
            try:
                queue_data = await inspect_queue()
                if queue_data is not None:
                    rabbitmq_status = "available"
                    application.state.rabbitmq_status = "available"
                    messages_ready = queue_data.get("messages_ready")
                    consumer_count = queue_data.get("consumer_count")
                    counts_observed_at = datetime.now(timezone.utc).isoformat()
            except Exception:
                # Do not expose transport exception details.
                rabbitmq_status = "unavailable"
                application.state.rabbitmq_status = "unavailable"

        consumer_redis_status = "not_verified"
        if (
            consumer_status == "running"
            and snapshot is not None
            and snapshot.consumer_redis_status in {"available", "unavailable"}
        ):
            consumer_redis_status = snapshot.consumer_redis_status

        measurement_summary = None
        read_measurements = getattr(store, "get_measurement_summary", None)
        if callable(read_measurements):
            try:
                measurement_summary = asdict(await read_measurements())
            except Exception:
                measurement_summary = {"status": "unavailable"}

        return {
            "api": {
                "status": "available",
                "redis_status": api_redis_status,
            },
            "rabbitmq": {
                "status": rabbitmq_status,
                "queue_name": queue_name,
                "messages_ready": messages_ready,
                "unacknowledged_messages": None,
                "unacknowledged_status": "unavailable",
                "consumer_count": consumer_count,
                "counts_observed_at": counts_observed_at,
            },
            "consumer": {
                "status": consumer_status,
                "reason": consumer_reason,
                "last_heartbeat": snapshot.last_heartbeat_at if snapshot is not None else None,
                "processed_since_start": snapshot.processed_events_since_start if snapshot is not None else None,
                "processing_failures_since_start": snapshot.processing_failures_since_start if snapshot is not None else None,
                "redis_status": consumer_redis_status,
                "last_successful_aggregation_write": snapshot.last_successful_aggregation_write_at if snapshot is not None else None,
                "windows": {
                    "tumbling": {
                        "size": snapshot.tumbling_window_size if snapshot is not None else None,
                        "progress": snapshot.tumbling_window_progress if snapshot is not None else None,
                        "last_completed_at": snapshot.last_completed_tumbling_aggregation_at if snapshot is not None else None,
                    },
                    "sliding": {
                        "size": snapshot.sliding_window_size if snapshot is not None else None,
                        "progress": snapshot.sliding_window_progress if snapshot is not None else None,
                        "interval": snapshot.sliding_interval if snapshot is not None else None,
                    },
                },
            },
            "measurements": measurement_summary,
        }

    @application.get("/api/measurements")
    async def current_measurements(run_id: Optional[str] = None) -> Dict[str, Any]:
        """Read overall measurements, or isolate them to one UUID run."""
        if run_id is not None:
            try:
                run_id = str(UUID(run_id))
            except (TypeError, ValueError, AttributeError):
                raise HTTPException(status_code=422, detail="run_id must be a valid UUID") from None
        store = application.state.metrics_store
        read_measurements = getattr(store, "get_measurement_summary", None)
        if not callable(read_measurements):
            raise HTTPException(status_code=503, detail="Measurement storage is not configured")
        try:
            return {"scope": "run" if run_id else "overall", "run_id": run_id,
                    **asdict(await read_measurements(run_id))}
        except Exception:
            raise HTTPException(status_code=503, detail="Measurement storage is unavailable") from None

    async def database_call(method: Any, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(method, *args, **kwargs)

    def raise_experiment_error(error: Exception) -> None:
        if isinstance(error, (ExperimentNotFound, RunNotFound)):
            raise HTTPException(status_code=404, detail=str(error)) from None
        if isinstance(error, (ExperimentConflict, ExecutionClaimConflict,
                              ExecutionProgressConflict, InvalidRunTransition,
                              InvalidRunWorkloadConfiguration)):
            raise HTTPException(status_code=409, detail=str(error)) from None
        if isinstance(error, ValueError):
            raise HTTPException(status_code=422, detail=str(error)) from None
        raise HTTPException(status_code=500, detail="Experiment storage operation failed") from None

    @application.post("/api/experiments", status_code=status.HTTP_201_CREATED)
    async def create_experiment(payload: ExperimentCreate) -> Dict[str, Any]:
        try:
            return await database_call(
                application.state.experiment_store.create_experiment,
                payload.model_dump(mode="json"),
            )
        except (ExperimentConflict, ValueError) as exc:
            raise_experiment_error(exc)

    @application.get("/api/experiments")
    async def list_experiments() -> Dict[str, Any]:
        experiments = await database_call(application.state.experiment_store.list_experiments)
        return {"count": len(experiments), "experiments": experiments}

    @application.get("/api/experiments/{experiment_id}")
    async def get_experiment(experiment_id: UUID) -> Dict[str, Any]:
        try:
            return await database_call(
                application.state.experiment_store.get_experiment, str(experiment_id)
            )
        except ExperimentNotFound as exc:
            raise_experiment_error(exc)

    @application.patch("/api/experiments/{experiment_id}")
    async def update_experiment(experiment_id: UUID, payload: ExperimentUpdate) -> Dict[str, Any]:
        updates = payload.model_dump(mode="json", exclude_unset=True)
        try:
            return await database_call(
                application.state.experiment_store.update_experiment,
                str(experiment_id), updates,
            )
        except (ExperimentNotFound, ExperimentConflict, ValueError) as exc:
            raise_experiment_error(exc)

    @application.post(
        "/api/experiments/{experiment_id}/runs",
        status_code=status.HTTP_201_CREATED,
    )
    async def create_experiment_run(
        experiment_id: UUID, payload: RunCreate = RunCreate()
    ) -> Dict[str, Any]:
        configuration = (
            payload.configuration.model_dump()
            if payload.configuration is not None else None
        )
        workload_configuration = (
            payload.workload_configuration.model_dump(mode="json")
            if payload.workload_configuration is not None else None
        )
        try:
            return await database_call(
                application.state.experiment_store.create_run,
                str(experiment_id), payload.run_id, configuration,
                workload_configuration,
            )
        except (ExperimentNotFound, ExperimentConflict, ValueError) as exc:
            raise_experiment_error(exc)

    @application.get("/api/experiments/{experiment_id}/runs")
    async def list_experiment_runs(experiment_id: UUID) -> Dict[str, Any]:
        try:
            runs = await database_call(
                application.state.experiment_store.list_runs, str(experiment_id)
            )
        except ExperimentNotFound as exc:
            raise_experiment_error(exc)
        return {"count": len(runs), "runs": runs}

    @application.get("/api/experiments/{experiment_id}/runs/{run_id}")
    async def get_experiment_run(experiment_id: UUID, run_id: UUID) -> Dict[str, Any]:
        try:
            run = await database_call(
                application.state.experiment_store.get_run,
                str(experiment_id), str(run_id),
            )
            run["event_accounting"] = await database_call(
                application.state.experiment_store.run_event_accounting, str(run_id)
            )
            return run
        except (ExperimentNotFound, RunNotFound) as exc:
            raise_experiment_error(exc)

    @application.get("/api/experiments/{experiment_id}/runs/{run_id}/pending-events")
    async def get_run_pending_event_inventory(
        experiment_id: UUID, run_id: UUID,
    ) -> Dict[str, Any]:
        """Expose all persisted reservations that currently block terminal transition."""
        try:
            return await database_call(
                application.state.experiment_store.get_run_pending_event_inventory,
                str(experiment_id), str(run_id),
            )
        except (ExperimentNotFound, RunNotFound) as exc:
            raise_experiment_error(exc)

    @application.get("/api/experiments/{experiment_id}/runs/{run_id}/results")
    async def get_experiment_run_results(experiment_id: UUID, run_id: UUID) -> Dict[str, Any]:
        try:
            run = await database_call(
                application.state.experiment_store.get_run,
                str(experiment_id), str(run_id),
            )
        except (ExperimentNotFound, RunNotFound) as exc:
            raise_experiment_error(exc)
        if not run["result_available"]:
            raise HTTPException(status_code=409, detail="Run results are not available until the run is terminal")
        return {"run_id": run["run_id"], "results": run["results"]}

    @application.get("/api/experiments/{experiment_id}/runs/{run_id}/execution")
    async def get_run_execution_progress(experiment_id: UUID, run_id: UUID) -> Dict[str, Any]:
        try:
            return await database_call(
                application.state.experiment_store.get_run_execution_progress,
                str(experiment_id), str(run_id),
            )
        except (ExperimentNotFound, RunNotFound) as exc:
            raise_experiment_error(exc)

    async def build_final_run_result(
        run: Dict[str, Any], terminal_status: RunStatus, ended_at: str,
        event_accounting: Dict[str, int],
    ) -> Dict[str, Any]:
        summary = None
        measurement_error = None
        read_measurements = getattr(application.state.metrics_store, "get_measurement_summary", None)
        if not callable(read_measurements):
            measurement_error = "Run-scoped measurement storage is not configured."
        else:
            try:
                summary = await asyncio.wait_for(
                    read_measurements(run["run_id"]),
                    timeout=MEASUREMENT_STORAGE_OPERATION_TIMEOUT_SECONDS,
                )
                if not getattr(summary, "measurements_present", False):
                    summary = None
                    measurement_error = "No run-scoped measurements were recorded."
            except Exception:
                measurement_error = "Run-scoped measurements are unavailable."

        duration_seconds = None
        if run["started_at"] is not None:
            try:
                started = datetime.fromisoformat(run["started_at"])
                ended = datetime.fromisoformat(ended_at)
                measured_duration = (ended - started).total_seconds()
                if measured_duration > 0:
                    duration_seconds = measured_duration
            except (TypeError, ValueError, OverflowError):
                duration_seconds = None

        reasons = []
        workload_snapshot = run["workload_snapshot"]
        progress = run["progress"]
        planned_event_count = int(workload_snapshot["target_event_count"])
        generated_event_count = int(progress["generated"])
        remaining_workload_count = max(0, planned_event_count - generated_event_count)
        planned_workload_fully_generated = generated_event_count == planned_event_count
        if terminal_status != RunStatus.COMPLETED:
            reasons.append(f"Run ended with status {terminal_status.value}; benchmark throughput is unavailable.")
        if not planned_workload_fully_generated:
            reasons.append(
                "Generated event progress is below the planned workload count; the planned workload was not fully generated."
            )
        if summary is None:
            reasons.append(measurement_error or "Run-scoped measurements are unavailable.")
        if duration_seconds is None:
            reasons.append("A positive run duration could not be established.")

        throughput = None
        unique_successes = None
        measurement_data = None
        latency_incomplete_reasons = []
        if summary is None:
            latency_incomplete_reasons.append(
                "Run-scoped measurements are unavailable; latency completeness cannot be established."
            )
        if summary is not None:
            measurement_data = asdict(summary)
            unique_successes = summary.consumer_unique_successes
            processing_samples = summary.sample_count.get("consumer_processing_latency_ms", 0)
            processing_average = summary.average_latency_ms.get("consumer_processing_latency_ms")
            processing_p95 = summary.p95_latency_ms.get("consumer_processing_latency_ms")
            if not processing_samples:
                latency_incomplete_reasons.append(
                    "No consumer processing latency samples were recorded."
                )
            if processing_average is None:
                latency_incomplete_reasons.append(
                    "Average consumer processing latency is unavailable."
                )
            if processing_p95 is None:
                latency_incomplete_reasons.append(
                    "P95 consumer processing latency is unavailable."
                )
            if summary.publish_attempts != summary.publish_successes + summary.publish_failures:
                reasons.append("Publish counters do not reconcile; measurement data may be incomplete.")
            if summary.consumer_attempts != summary.consumer_successes + summary.consumer_failures:
                reasons.append("Consumer counters do not reconcile; measurement data may be incomplete.")
            if summary.consumer_attempts < summary.publish_successes:
                reasons.append("Some successful publishes have no recorded consumer delivery attempt.")
            if summary.consumer_unique_successes > summary.consumer_successes:
                reasons.append("Unique consumer successes exceed successful delivery attempts.")
            if summary.publish_successes != event_accounting["accepted_events"]:
                reasons.append("Published event measurements do not match the durable accepted-event count.")
            if summary.consumer_unique_successes != event_accounting["consumer_processed_successes"]:
                reasons.append("Unique consumer success measurements do not match durable processing receipts.")
            if summary.consumer_failures != event_accounting["consumer_processed_failures"]:
                reasons.append("Consumer failure measurements do not match durable processing receipts.")
            if (
                terminal_status == RunStatus.COMPLETED
                and duration_seconds is not None
            ):
                throughput = unique_successes / duration_seconds

        return {
            "run_id": run["run_id"],
            "experiment_id": run["experiment_id"],
            "status": terminal_status.value,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds,
            "measurements_available": summary is not None,
            "unique_successful_events": unique_successes,
            "benchmark_throughput_events_per_second": throughput,
            "result_complete": not reasons,
            "incomplete_reasons": reasons,
            "latency_complete": not latency_incomplete_reasons,
            "latency_incomplete_reasons": latency_incomplete_reasons,
            "event_accounting": event_accounting,
            "unresolved_publish_count": event_accounting["unresolved_publish_count"],
            "planned_event_count": planned_event_count,
            "generated_event_count": generated_event_count,
            "remaining_workload_count": remaining_workload_count,
            "workload_shortfall": remaining_workload_count,
            "planned_workload_fully_generated": planned_workload_fully_generated,
            "measurements": measurement_data,
        }

    async def finalize_experiment_run(experiment_id: str, run_id: str) -> Optional[Dict[str, Any]]:
        """Build and atomically persist the result for a drained durable stop intent."""
        repository = application.state.experiment_store
        run = await database_call(repository.get_run, experiment_id, run_id)
        stop_intent = run["stop_intent"]
        if stop_intent is None:
            raise InvalidRunTransition("Run has no persisted stop intent")
        target = RunStatus(stop_intent["target"])
        if run["status"] in {
            RunStatus.COMPLETED.value, RunStatus.FAILED.value, RunStatus.CANCELLED.value,
        }:
            # The repository makes this idempotent and returns the committed snapshot.
            return await database_call(
                repository.finalize_run_after_drain, experiment_id, run_id,
                ended_at=run["ended_at"], results=run["results"],
            )
        accounting = await database_call(repository.run_event_accounting, run_id)
        ended_at = utc_now()
        results = await build_final_run_result(run, target, ended_at, accounting)
        return await database_call(
            repository.finalize_run_after_drain, experiment_id, run_id,
            ended_at=ended_at, results=results,
        )

    def schedule_run_finalization(experiment_id: str, run_id: str) -> None:
        tasks = application.state.experiment_finalization_tasks
        if application.state.shutting_down or run_id in tasks:
            return

        async def retry_until_settled() -> None:
            delay = 0.1
            logged_failure = False
            while not application.state.shutting_down:
                try:
                    await finalize_experiment_run(experiment_id, run_id)
                    return
                except (ExperimentNotFound, RunNotFound):
                    logger.warning("Run finalization could not find its run (run_id=%s)", run_id)
                    return
                except Exception as exc:
                    try:
                        run = await database_call(
                            application.state.experiment_store.get_run,
                            experiment_id, run_id,
                        )
                    except (ExperimentNotFound, RunNotFound):
                        return
                    except Exception:
                        run = None
                    if run is not None:
                        if run["results"] is not None or run["stop_intent"] is None:
                            return
                        target = run["stop_intent"]["target"]
                        if (target == RunStatus.COMPLETED.value
                                and run["execution"]["interruption_recorded"]):
                            return
                    if not logged_failure:
                        logger.warning(
                            "Run finalization is waiting to retry (run_id=%s exception_type=%s)",
                            run_id, type(exc).__name__,
                        )
                        logged_failure = True
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 2.0)

        task = asyncio.create_task(
            retry_until_settled(), name=f"experiment-finalize-{run_id}",
        )
        tasks[run_id] = task

        def forget_finished_task(finished: asyncio.Task, key: str = run_id) -> None:
            if tasks.get(key) is finished:
                tasks.pop(key, None)
            if not finished.cancelled():
                try:
                    finished.result()
                except Exception as exc:
                    logger.error(
                        "Run finalization watcher failed (run_id=%s exception_type=%s)",
                        key, type(exc).__name__,
                    )

        task.add_done_callback(forget_finished_task)

    async def request_terminal_stop(
        experiment_id: UUID,
        run_id: UUID,
        target: RunStatus,
        failure_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            request = await database_call(
                application.state.experiment_store.request_run_stop,
                str(experiment_id), str(run_id), target,
                reason=failure_reason,
            )
        except (ExperimentNotFound, RunNotFound, InvalidRunTransition,
                ExecutionClaimConflict, ValueError) as exc:
            raise_experiment_error(exc)
        run = request["run"]
        schedule_run_finalization(str(experiment_id), str(run_id))
        return {
            "run_id": run["run_id"],
            "status": run["status"],
            "stop_intent": run["stop_intent"],
            "event_ingestion_closed": run["event_ingestion_closed"],
            "execution_state": run["execution"]["state"],
            "drain_state": "ALREADY_REQUESTED" if request["already_requested"] else "REQUESTED",
            "already_requested": request["already_requested"],
            "pending_events": request["pending_events"],
        }

    @application.post("/api/experiments/{experiment_id}/runs/{run_id}/start")
    async def start_experiment_run(experiment_id: UUID, run_id: UUID) -> Dict[str, Any]:
        try:
            return await database_call(
                application.state.experiment_store.transition_run,
                str(experiment_id), str(run_id), RunStatus.RUNNING,
                started_at=utc_now(),
            )
        except (ExperimentNotFound, RunNotFound, InvalidRunTransition) as exc:
            raise_experiment_error(exc)

    @application.post(
        "/api/experiments/{experiment_id}/runs/{run_id}/complete",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def complete_experiment_run(experiment_id: UUID, run_id: UUID) -> Dict[str, Any]:
        return await request_terminal_stop(experiment_id, run_id, RunStatus.COMPLETED)

    @application.post(
        "/api/experiments/{experiment_id}/runs/{run_id}/fail",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def fail_experiment_run(
        experiment_id: UUID, run_id: UUID, payload: RunFailure
    ) -> Dict[str, Any]:
        return await request_terminal_stop(
            experiment_id, run_id, RunStatus.FAILED, payload.reason
        )

    @application.post(
        "/api/experiments/{experiment_id}/runs/{run_id}/cancel",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def cancel_experiment_run(
        experiment_id: UUID, run_id: UUID, payload: RunReason = RunReason()
    ) -> Dict[str, Any]:
        return await request_terminal_stop(
            experiment_id, run_id, RunStatus.CANCELLED, payload.reason
        )

    async def reserve_scoped_event(event: LibraryEvent) -> None:
        if event.run_id is None:
            return
        try:
            await database_call(
                application.state.experiment_store.reserve_run_event,
                event.run_id, event.event_id,
            )
        except (ExperimentNotFound, RunNotFound) as exc:
            raise HTTPException(status_code=404, detail="Experiment run was not found") from exc
        except InvalidRunTransition as exc:
            raise HTTPException(status_code=409, detail="Experiment run is not accepting events") from exc
        except Exception:
            raise HTTPException(status_code=503, detail="Run event accounting is unavailable") from None

    async def release_unpublished_event(event: LibraryEvent) -> None:
        if event.run_id is None:
            return
        try:
            await database_call(
                application.state.experiment_store.release_unpublished_run_event,
                event.run_id, event.event_id,
            )
        except Exception:
            raise HTTPException(
                status_code=503,
                detail="Event publishing failed and its run reservation could not be released",
            ) from None

    async def mark_scoped_event_published(event: LibraryEvent) -> None:
        if event.run_id is None:
            return
        try:
            await database_call(
                application.state.experiment_store.mark_run_event_published,
                event.run_id, event.event_id,
            )
        except Exception:
            raise HTTPException(
                status_code=503,
                detail="Event was published but run event accounting could not be confirmed",
            ) from None

    async def mark_scoped_event_publish_unknown(event: LibraryEvent) -> None:
        if event.run_id is None:
            return
        try:
            marked = await database_call(
                application.state.experiment_store.mark_run_event_publish_unknown,
                event.run_id, event.event_id,
            )
            if not marked:
                raise RuntimeError("The run event reservation was not updated")
        except Exception:
            raise HTTPException(
                status_code=503,
                detail="Publish outcome is unknown and its run reservation could not be retained",
            ) from None

    async def publish_event_through_existing_path(
        event: LibraryEvent,
        lease_guard: Optional[Callable[[], bool]] = None,
    ) -> EventPublishResult:
        """Share run reservation, broker confirmation, and outcome accounting with all publishers."""
        reservation_task = asyncio.create_task(reserve_scoped_event(event))
        try:
            await asyncio.shield(reservation_task)
        except asyncio.CancelledError:
            # The SQLite call runs in a worker thread and cannot be cancelled.
            # Wait for it to settle before releasing, or release could race ahead
            # of the reservation and leave an unpublished row behind.
            if event.run_id is not None:
                try:
                    await asyncio.shield(reservation_task)
                except Exception:
                    # Reservation did not complete, so there is nothing to release.
                    raise asyncio.CancelledError
                try:
                    await release_unpublished_event(event)
                except Exception:
                    logger.warning(
                        "Could not release pre-publish reservation during cancellation "
                        "(run_id=%s event_id=%s)", event.run_id, event.event_id,
                    )
            raise
        except HTTPException as exc:
            return EventPublishResult(
                "failed", str(exc.detail), exc.status_code, stop_execution=True,
                accounting_settled=exc.status_code != 503,
            )

        if lease_guard is not None and not lease_guard():
            accounting_settled = True
            try:
                await release_unpublished_event(event)
                detail = "Execution lease was lost before broker publishing began"
            except HTTPException as exc:
                detail = str(exc.detail)
                accounting_settled = False
            return EventPublishResult(
                "failed", detail, 409, stop_execution=True, lease_lost=True,
                accounting_settled=accounting_settled,
            )

        publisher_adapter = application.state.publisher
        if publisher_adapter is None:
            await record_publish_measurement(event, None, False)
            try:
                await release_unpublished_event(event)
            except HTTPException as exc:
                return EventPublishResult(
                    "failed", str(exc.detail), exc.status_code, stop_execution=True,
                    accounting_settled=False,
                )
            return EventPublishResult(
                "failed", "RabbitMQ publisher is not configured", 503,
                stop_execution=True,
            )

        started_at = time.perf_counter()
        publisher_invocation_started = False
        try:
            # This is the invocation boundary. There is no await between the
            # synchronous lease guard above and calling the publisher.
            publisher_invocation_started = True
            publish_operation = publisher_adapter.publish(event)
            await publish_operation
        except asyncio.CancelledError as cancellation:
            if publisher_invocation_started and event.run_id is not None:
                try:
                    settlement_task = asyncio.create_task(
                        mark_scoped_event_publish_unknown(event),
                        name=f"unknown-publish-settlement-{event.event_id}",
                    )
                except Exception as settlement_error:
                    logger.error(
                        "Could not start unknown publish settlement; reservation remains "
                        "pending (run_id=%s event_id=%s exception_type=%s)",
                        event.run_id, event.event_id, type(settlement_error).__name__,
                    )
                else:
                    settlement_error = await _await_task_to_completion(settlement_task)
                    if settlement_error is not None:
                        logger.error(
                            "Could not confirm unknown publish settlement; reservation "
                            "remains pending (run_id=%s event_id=%s exception_type=%s)",
                            event.run_id, event.event_id, type(settlement_error).__name__,
                        )
                # Settlement must finish before optional metrics and before this
                # cancellation can escape. SQLite lock waits are bounded by the
                # repository's 10-second busy timeout; the task is never abandoned.
                try:
                    measurement_task = asyncio.create_task(
                        record_publish_measurement(
                            event, (time.perf_counter() - started_at) * 1000.0, False,
                        ),
                        name=f"cancelled-publish-measurement-{event.event_id}",
                    )
                    measurement_error = await _await_task_to_completion(measurement_task)
                    if measurement_error is not None:
                        logger.warning(
                            "Could not record cancelled publish measurement "
                            "(run_id=%s event_id=%s exception_type=%s)",
                            event.run_id, event.event_id, type(measurement_error).__name__,
                        )
                except Exception as measurement_error:
                    logger.warning(
                        "Could not start cancelled publish measurement "
                        "(run_id=%s event_id=%s exception_type=%s)",
                        event.run_id, event.event_id, type(measurement_error).__name__,
                    )
            elif event.run_id is not None:
                try:
                    await release_unpublished_event(event)
                except Exception:
                    logger.warning(
                        "Could not release pre-publish reservation during cancellation "
                        "(run_id=%s event_id=%s)", event.run_id, event.event_id,
                    )
            raise cancellation
        except Exception as exc:
            await record_publish_measurement(
                event, (time.perf_counter() - started_at) * 1000.0, False,
            )
            ambiguous = not isinstance(exc, RabbitMQPublishError) or exc.ambiguous
            disposition = "unknown" if ambiguous else "failed"
            try:
                if ambiguous:
                    await mark_scoped_event_publish_unknown(event)
                else:
                    await release_unpublished_event(event)
            except HTTPException as accounting_error:
                application.state.rabbitmq_status = "unavailable"
                return EventPublishResult(
                    disposition, str(accounting_error.detail), accounting_error.status_code,
                    stop_execution=True, accounting_settled=False,
                )
            application.state.rabbitmq_status = "unavailable"
            return EventPublishResult(
                disposition,
                "Publish outcome is unknown; reservation retained" if ambiguous
                else "Could not publish event",
            )

        await record_publish_measurement(
            event, (time.perf_counter() - started_at) * 1000.0, True,
        )
        try:
            await mark_scoped_event_published(event)
        except HTTPException as accounting_error:
            return EventPublishResult(
                "confirmed", str(accounting_error.detail), accounting_error.status_code,
                stop_execution=True, accounting_settled=False,
            )
        application.state.rabbitmq_status = "available"
        recent_events: Deque[Dict[str, Any]] = application.state.recent_events
        recent_events.appendleft(_event_data(event))
        return EventPublishResult("confirmed")

    application.state.experiment_runner = ExperimentWorkloadRunner(
        application.state.experiment_store,
        publish_event_through_existing_path,
        publish_lock=experiment_publish_lock,
    )
    application.state.finalize_experiment_run = finalize_experiment_run

    @application.post("/api/events", status_code=status.HTTP_202_ACCEPTED)
    async def create_library_event(submission: EventSubmission) -> Dict[str, Any]:
        """Validate and publish an action; acknowledge success only after broker publish."""
        if submission.value is not None and not math.isfinite(submission.value):
            raise HTTPException(status_code=422, detail="value must be finite")
        timestamp = datetime.now(timezone.utc)
        try:
            event = LibraryEvent.from_mapping({
                "event_id": str(uuid4()),
                "event_type": submission.event_type,
                "student_id": submission.student_id,
                "book_id": submission.book_id,
                "timestamp": timestamp,
                "value": None,
                **({"run_id": submission.run_id} if submission.run_id is not None else {}),
            })
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

        result = await publish_event_through_existing_path(event)
        if result.detail is not None or result.disposition != "confirmed":
            raise HTTPException(
                status_code=result.http_status,
                detail=result.detail or "Could not publish event",
            ) from None
        return {"status": "published", "event": _event_data(event)}

    @application.post(
        "/api/experiments/{experiment_id}/runs/{run_id}/execute",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def execute_experiment_run(experiment_id: UUID, run_id: UUID) -> Dict[str, Any]:
        repository = application.state.experiment_store
        try:
            run = await database_call(
                repository.get_run_with_workload_configuration_snapshot, str(run_id),
            )
            if run["experiment_id"] != str(experiment_id):
                raise RunNotFound("Run was not found for this experiment")
            if run["status"] != RunStatus.RUNNING.value:
                raise InvalidRunTransition("Only a RUNNING run can execute a workload")
            claim = await database_call(repository.claim_run_execution, str(run_id))
        except (ExperimentNotFound, RunNotFound, InvalidRunWorkloadConfiguration,
                InvalidRunTransition, ExecutionClaimConflict) as exc:
            raise_experiment_error(exc)

        try:
            task = asyncio.create_task(
                application.state.experiment_runner.execute(run, claim),
                name=f"experiment-run-{run_id}",
            )
        except Exception:
            try:
                await database_call(
                    repository.release_run_execution_claim,
                    str(run_id), claim["owner_token"],
                )
            except Exception:
                pass
            raise HTTPException(
                status_code=503, detail="Could not start experiment workload execution",
            ) from None

        application.state.experiment_runner_tasks[str(run_id)] = task

        def forget_finished_task(finished: asyncio.Task, key: str = str(run_id)) -> None:
            if application.state.experiment_runner_tasks.get(key) is finished:
                application.state.experiment_runner_tasks.pop(key, None)
            try:
                result = finished.result()
            except asyncio.CancelledError:
                return
            except Exception as exception:
                logger.error(
                    "Experiment runner task failed (run_id=%s exception_type=%s)",
                    key, type(exception).__name__,
                )
            else:
                if result.get("claim_released"):
                    schedule_run_finalization(str(experiment_id), key)

        task.add_done_callback(forget_finished_task)
        return {
            "status": "accepted",
            "run_id": str(run_id),
            "execution_state": "CLAIMED",
            "progress_url": f"/api/experiments/{experiment_id}/runs/{run_id}/execution",
        }

    @application.post("/api/events/batch")
    async def create_sample_event_batch(submission: SampleBatchSubmission) -> Dict[str, Any]:
        """Publish a finite batch through the existing publisher at no more than 10/s."""
        nonlocal batch_next_publish_at
        if submission.batch_size not in (5, 8, 10):
            raise HTTPException(status_code=422, detail="batch_size must be 5, 8, or 10")

        publisher_adapter = application.state.publisher
        published = 0
        failures = []
        recent_events: Deque[Dict[str, Any]] = application.state.recent_events

        async with batch_publish_lock:
            for event_number in range(1, submission.batch_size + 1):
                while True:
                    delay = batch_next_publish_at - _monotonic_time()
                    if delay <= 0:
                        break
                    await _wait_for_batch_interval(delay)
                batch_next_publish_at = _monotonic_time() + BATCH_EVENT_INTERVAL_SECONDS
                event = _random_library_event(submission.run_id)

                try:
                    await reserve_scoped_event(event)
                except HTTPException as exc:
                    if published == 0 and event_number == 1:
                        raise
                    failures.append({
                        "event_number": event_number,
                        "detail": str(exc.detail),
                    })
                    continue

                if publisher_adapter is None:
                    await record_publish_measurement(event, None, False)
                    await release_unpublished_event(event)
                    failures.append({
                        "event_number": event_number,
                        "detail": "RabbitMQ publisher is not configured",
                    })
                    continue
                started_at = time.perf_counter()
                try:
                    await publisher_adapter.publish(event)
                except Exception as exc:
                    await record_publish_measurement(event, (time.perf_counter() - started_at) * 1000.0, False)
                    ambiguous = not isinstance(exc, RabbitMQPublishError) or exc.ambiguous
                    if ambiguous:
                        await mark_scoped_event_publish_unknown(event)
                    else:
                        await release_unpublished_event(event)
                    application.state.rabbitmq_status = "unavailable"
                    failures.append({
                        "event_number": event_number,
                        "detail": "Publish outcome is unknown; reservation retained" if ambiguous
                        else "Could not publish event",
                    })
                    continue

                await record_publish_measurement(event, (time.perf_counter() - started_at) * 1000.0, True)
                await mark_scoped_event_published(event)

                application.state.rabbitmq_status = "available"
                recent_events.appendleft(_event_data(event))
                published += 1

        failed = len(failures)
        result_status = "completed" if failed == 0 else "failed" if published == 0 else "partial"
        return {
            "status": result_status,
            "requested": submission.batch_size,
            "published": published,
            "failed": failed,
            "failures": failures,
        }

    @application.get("/api/events")
    async def recent_events() -> Dict[str, Any]:
        """Return this API process's newest successfully published events."""
        events = list(application.state.recent_events)
        return {"source": "in_memory", "count": len(events), "events": events}

    @application.get("/api/metrics")
    async def current_metrics() -> Dict[str, Any]:
        """Read latest summaries from Redis; missing values remain null."""
        store = application.state.metrics_store
        if store is None:
            raise HTTPException(status_code=503, detail="Metrics storage is not configured")
        try:
            tumbling = await store.get_metrics("tumbling")
            sliding = await store.get_metrics("sliding")
        except RedisStorageError:
            application.state.redis_status = "unavailable"
            raise HTTPException(status_code=503, detail="Metrics storage is unavailable") from None
        except Exception:
            application.state.redis_status = "unavailable"
            raise HTTPException(status_code=503, detail="Metrics storage is unavailable") from None

        application.state.redis_status = "available"
        return {
            "tumbling": _metrics_data(tumbling),
            "sliding": _metrics_data(sliding),
        }

    return application


app = create_app()

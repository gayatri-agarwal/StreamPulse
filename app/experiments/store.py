"""SQLite persistence for experiment definitions, runs, and final results."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import sqlite3
from datetime import timedelta
from typing import Any, Dict, Iterator, Optional
from uuid import uuid4

from app.processing.window_config import WindowConfiguration
from app.experiments.schemas import ExperimentWorkloadConfiguration, RunStatus


class ExperimentNotFound(LookupError):
    """Requested experiment does not exist."""


class RunNotFound(LookupError):
    """Requested run does not exist for the selected experiment."""


class InvalidRunConfiguration(ValueError):
    """A persisted run configuration snapshot is malformed or invalid."""


class InvalidRunWorkloadConfiguration(ValueError):
    """A persisted workload snapshot is malformed or violates current bounds."""


class ExperimentStorageError(RuntimeError):
    """SQLite could not retrieve experiment or run data."""


class ExperimentConflict(RuntimeError):
    """An identifier is already in use or a concurrent update conflicted."""


class InvalidRunTransition(ValueError):
    """A run cannot make the requested lifecycle transition."""


class ExecutionClaimConflict(RuntimeError):
    """A run execution is already claimed or cannot be claimed again."""


class ExecutionProgressConflict(RuntimeError):
    """A progress operation ID was reused with a different owner or payload."""


class RunEventNotFound(LookupError):
    """A reservation was not found within the selected run."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_experiment_database_path() -> Path:
    """Resolve a durable user-data path, overridable for deployments and tests."""
    configured = os.getenv("STREAMPULSE_EXPERIMENT_DB_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    if os.name == "nt":
        base = Path(os.getenv("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
    else:
        base = Path(os.getenv("XDG_DATA_HOME") or (Path.home() / ".local" / "share"))
    return base / "StreamPulse" / "experiments.sqlite3"


class ExperimentRepository:
    """Short-lived SQLite connections with explicit write transactions."""

    def __init__(self, database_path: Optional[Path | str] = None) -> None:
        self.database_path = Path(database_path or default_experiment_database_path()).expanduser()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.database_path), timeout=10, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    @contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._reader() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS experiments (
                    experiment_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    experiment_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    default_configuration TEXT NOT NULL,
                    default_workload_configuration TEXT NOT NULL DEFAULT
                        '{"target_event_count":100,"publish_interval_ms":100,"event_type_distribution":{"BOOK_ISSUED":1,"BOOK_RETURNED":1,"BOOK_RESERVED":1}}'
                );
                CREATE TABLE IF NOT EXISTS experiment_runs (
                    run_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
                    status TEXT NOT NULL CHECK (status IN (
                        'CREATED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'
                    )),
                    configuration_snapshot TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    ended_at TEXT,
                    failure_reason TEXT,
                    results_json TEXT,
                    event_ingestion_closed INTEGER NOT NULL DEFAULT 0
                        CHECK (event_ingestion_closed IN (0, 1)),
                    stop_target TEXT CHECK (stop_target IS NULL OR stop_target IN (
                        'COMPLETED', 'FAILED', 'CANCELLED'
                    )),
                    stop_requested_at TEXT,
                    stop_reason TEXT,
                    workload_snapshot TEXT NOT NULL DEFAULT
                        '{"target_event_count":100,"publish_interval_ms":100,"event_type_distribution":{"BOOK_ISSUED":1,"BOOK_RETURNED":1,"BOOK_RESERVED":1}}',
                    execution_state TEXT NOT NULL DEFAULT 'NOT_STARTED'
                        CHECK (execution_state IN ('NOT_STARTED','CLAIMED','EXPIRED','RELEASED')),
                    execution_owner_token TEXT,
                    execution_claimed_at TEXT,
                    execution_lease_expires_at TEXT,
                    execution_interrupted_at TEXT,
                    execution_interruption_reason TEXT,
                    progress_planned INTEGER NOT NULL DEFAULT 100 CHECK (progress_planned >= 0),
                    progress_generated INTEGER NOT NULL DEFAULT 0 CHECK (progress_generated >= 0),
                    progress_publish_confirmed INTEGER NOT NULL DEFAULT 0 CHECK (progress_publish_confirmed >= 0),
                    progress_publish_failed INTEGER NOT NULL DEFAULT 0 CHECK (progress_publish_failed >= 0),
                    progress_publish_unknown INTEGER NOT NULL DEFAULT 0 CHECK (progress_publish_unknown >= 0)
                );
                CREATE INDEX IF NOT EXISTS idx_experiment_runs_experiment_created
                    ON experiment_runs(experiment_id, created_at DESC);
            """)
            # Upgrade databases created before lifecycle-bound event ingestion.
            self._ensure_column(
                connection,
                "experiment_runs",
                "event_ingestion_closed",
                "INTEGER NOT NULL DEFAULT 0 CHECK (event_ingestion_closed IN (0, 1))",
            )
            for column, definition in (
                ("stop_target", "TEXT CHECK (stop_target IS NULL OR stop_target IN ('COMPLETED','FAILED','CANCELLED'))"),
                ("stop_requested_at", "TEXT"),
                ("stop_reason", "TEXT"),
            ):
                self._ensure_column(connection, "experiment_runs", column, definition)
            workload_default = (
                "'{\"target_event_count\":100,\"publish_interval_ms\":100,"
                "\"event_type_distribution\":{\"BOOK_ISSUED\":1,"
                "\"BOOK_RETURNED\":1,\"BOOK_RESERVED\":1}}'"
            )
            for column, definition in (
                ("default_workload_configuration", f"TEXT NOT NULL DEFAULT {workload_default}"),
            ):
                self._ensure_column(connection, "experiments", column, definition)
            for column, definition in (
                ("workload_snapshot", f"TEXT NOT NULL DEFAULT {workload_default}"),
                ("execution_state", "TEXT NOT NULL DEFAULT 'NOT_STARTED' CHECK (execution_state IN ('NOT_STARTED','CLAIMED','EXPIRED','RELEASED'))"),
                ("execution_owner_token", "TEXT"),
                ("execution_claimed_at", "TEXT"),
                ("execution_lease_expires_at", "TEXT"),
                ("execution_interrupted_at", "TEXT"),
                ("execution_interruption_reason", "TEXT"),
                ("progress_planned", "INTEGER NOT NULL DEFAULT 100 CHECK (progress_planned >= 0)"),
                ("progress_generated", "INTEGER NOT NULL DEFAULT 0 CHECK (progress_generated >= 0)"),
                ("progress_publish_confirmed", "INTEGER NOT NULL DEFAULT 0 CHECK (progress_publish_confirmed >= 0)"),
                ("progress_publish_failed", "INTEGER NOT NULL DEFAULT 0 CHECK (progress_publish_failed >= 0)"),
                ("progress_publish_unknown", "INTEGER NOT NULL DEFAULT 0 CHECK (progress_publish_unknown >= 0)"),
            ):
                self._ensure_column(connection, "experiment_runs", column, definition)
            # Older versions recorded EXPIRED without durable interruption details
            # or closing admission. Upgrade those records atomically per row.
            legacy_expired_at = utc_now()
            connection.execute(
                "UPDATE experiment_runs SET event_ingestion_closed = 1, "
                "execution_owner_token = NULL, "
                "execution_interrupted_at = COALESCE(execution_interrupted_at, ?), "
                "execution_interruption_reason = COALESCE(execution_interruption_reason, ?) "
                "WHERE execution_state = 'EXPIRED'",
                (legacy_expired_at,
                 "Execution lease expired; workload will not be resumed automatically"),
            )
            connection.commit()
            connection.execute("""
                CREATE TABLE IF NOT EXISTS experiment_run_events (
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES experiment_runs(run_id),
                    state TEXT NOT NULL CHECK (state IN ('RESERVED', 'PROCESSING')),
                    created_at TEXT NOT NULL,
                    published INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0, 1)),
                    settled INTEGER NOT NULL DEFAULT 0 CHECK (settled IN (0, 1)),
                    outcome TEXT CHECK (outcome IN ('SUCCESS', 'FAILURE') OR outcome IS NULL),
                    in_flight_deliveries INTEGER NOT NULL DEFAULT 0 CHECK (in_flight_deliveries >= 0),
                    publish_unknown INTEGER NOT NULL DEFAULT 0 CHECK (publish_unknown IN (0, 1))
                )
            """)
            connection.execute("""
                CREATE TABLE IF NOT EXISTS experiment_run_event_reconciliation_audit (
                    reconciliation_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES experiment_runs(run_id),
                    event_id TEXT NOT NULL,
                    evidence_source TEXT NOT NULL,
                    evidence_reference TEXT NOT NULL,
                    evidence_details TEXT NOT NULL,
                    observed_event_state_json TEXT NOT NULL,
                    disposition TEXT NOT NULL CHECK (disposition = 'NOT_RECONCILED'),
                    decision_reason TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                )
            """)
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_event_reconciliation_audit_event "
                "ON experiment_run_event_reconciliation_audit(run_id, event_id, recorded_at)"
            )
            connection.execute("""
                CREATE TABLE IF NOT EXISTS experiment_run_progress_operations (
                    run_id TEXT NOT NULL REFERENCES experiment_runs(run_id),
                    operation_id TEXT NOT NULL,
                    owner_token TEXT NOT NULL,
                    generated INTEGER NOT NULL CHECK (generated >= 0),
                    publish_confirmed INTEGER NOT NULL CHECK (publish_confirmed >= 0),
                    publish_failed INTEGER NOT NULL CHECK (publish_failed >= 0),
                    publish_unknown INTEGER NOT NULL CHECK (publish_unknown >= 0),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, operation_id)
                )
            """)
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_experiment_run_events_run "
                "ON experiment_run_events(run_id)"
            )
            for column, definition in (
                ("published", "INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0, 1))"),
                ("settled", "INTEGER NOT NULL DEFAULT 0 CHECK (settled IN (0, 1))"),
                ("outcome", "TEXT CHECK (outcome IN ('SUCCESS', 'FAILURE') OR outcome IS NULL)"),
                ("in_flight_deliveries", "INTEGER NOT NULL DEFAULT 0 CHECK (in_flight_deliveries >= 0)"),
                ("publish_unknown", "INTEGER NOT NULL DEFAULT 0 CHECK (publish_unknown IN (0, 1))"),
            ):
                self._ensure_column(
                    connection, "experiment_run_events", column, definition
                )

    @staticmethod
    def _ensure_column(
        connection: sqlite3.Connection, table: str, column: str, definition: str
    ) -> None:
        columns = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column in columns:
            return
        try:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        except sqlite3.OperationalError:
            # API and consumer processes can initialize the same new database
            # concurrently. Treat a competing successful ALTER as idempotent.
            columns = {
                row["name"]
                for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if column not in columns:
                raise

    @staticmethod
    def _configuration_json(configuration: Any) -> str:
        if isinstance(configuration, WindowConfiguration):
            values = configuration.__dict__
        elif isinstance(configuration, dict):
            values = configuration
        else:
            raise ValueError("configuration must contain window settings")
        validated = WindowConfiguration(**values)
        return json.dumps(validated.__dict__, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _workload_json(configuration: Any) -> str:
        if isinstance(configuration, ExperimentWorkloadConfiguration):
            values = configuration.model_dump(mode="json")
        elif isinstance(configuration, dict):
            values = configuration
        else:
            raise ValueError("workload configuration must be an object")
        validated = ExperimentWorkloadConfiguration.model_validate(values)
        return json.dumps(validated.model_dump(mode="json"), separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _validated_workload_snapshot(snapshot: Any) -> Dict[str, Any]:
        try:
            raw = json.loads(snapshot) if isinstance(snapshot, str) else snapshot
            validated = ExperimentWorkloadConfiguration.model_validate(raw)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise InvalidRunWorkloadConfiguration(
                "Stored run workload snapshot is invalid"
            ) from exc
        return validated.model_dump(mode="json")

    @staticmethod
    def _experiment(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "experiment_id": row["experiment_id"],
            "name": row["name"],
            "description": row["description"],
            "experiment_type": row["experiment_type"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "default_configuration": json.loads(row["default_configuration"]),
            "default_workload_configuration": json.loads(row["default_workload_configuration"]),
        }

    @staticmethod
    def _run(row: sqlite3.Row) -> Dict[str, Any]:
        results = json.loads(row["results_json"]) if row["results_json"] is not None else None
        execution_state = row["execution_state"]
        lease_expires_at = row["execution_lease_expires_at"]
        if (execution_state == "CLAIMED" and
                (not lease_expires_at or lease_expires_at <= utc_now())):
            execution_state = "EXPIRED"
        return {
            "run_id": row["run_id"],
            "experiment_id": row["experiment_id"],
            "status": row["status"],
            "configuration_snapshot": json.loads(row["configuration_snapshot"]),
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "failure_reason": row["failure_reason"],
            "event_ingestion_closed": bool(row["event_ingestion_closed"]),
            "stop_intent": (
                None if row["stop_target"] is None else {
                    "target": row["stop_target"],
                    "requested_at": row["stop_requested_at"],
                    "reason": row["stop_reason"],
                }
            ),
            "result_available": results is not None,
            "result_complete": bool(results and results.get("result_complete")),
            "results": results,
            "workload_snapshot": json.loads(row["workload_snapshot"]),
            "execution": {
                "state": execution_state,
                "claimed_at": row["execution_claimed_at"],
                "lease_expires_at": row["execution_lease_expires_at"],
                "interruption_recorded": row["execution_interrupted_at"] is not None,
                "interrupted_at": row["execution_interrupted_at"],
                "interruption_reason": row["execution_interruption_reason"],
            },
            "progress": {
                "planned": row["progress_planned"],
                "generated": row["progress_generated"],
                "publish_confirmed": row["progress_publish_confirmed"],
                "publish_failed": row["progress_publish_failed"],
                "publish_unknown": row["progress_publish_unknown"],
            },
        }

    def create_experiment(self, data: Dict[str, Any]) -> Dict[str, Any]:
        experiment_id = str(uuid4())
        now = utc_now()
        configuration = self._configuration_json(data["default_configuration"])
        workload = self._workload_json(data.get("default_workload_configuration", {}))
        try:
            with self._transaction() as connection:
                connection.execute(
                    """INSERT INTO experiments
                       (experiment_id, name, description, experiment_type, created_at,
                       updated_at, default_configuration, default_workload_configuration)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (experiment_id, data["name"], data.get("description"),
                     data["experiment_type"], now, now, configuration, workload),
                )
                row = connection.execute(
                    "SELECT * FROM experiments WHERE experiment_id = ?", (experiment_id,)
                ).fetchone()
        except sqlite3.IntegrityError as exc:
            raise ExperimentConflict("Experiment could not be created") from exc
        return self._experiment(row)

    def list_experiments(self) -> list[Dict[str, Any]]:
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT * FROM experiments ORDER BY created_at DESC, experiment_id"
            ).fetchall()
        return [self._experiment(row) for row in rows]

    def get_experiment(self, experiment_id: str) -> Dict[str, Any]:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM experiments WHERE experiment_id = ?", (experiment_id,)
            ).fetchone()
        if row is None:
            raise ExperimentNotFound("Experiment was not found")
        return self._experiment(row)

    def update_experiment(self, experiment_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        columns = {
            "name": "name",
            "description": "description",
            "experiment_type": "experiment_type",
            "default_workload_configuration": "default_workload_configuration",
        }
        values = {column: updates[key] for key, column in columns.items() if key in updates}
        if "default_configuration" in updates and updates["default_configuration"] is not None:
            values["default_configuration"] = self._configuration_json(updates["default_configuration"])
        if "default_workload_configuration" in updates and updates["default_workload_configuration"] is not None:
            values["default_workload_configuration"] = self._workload_json(updates["default_workload_configuration"])
        if not values:
            raise ValueError("at least one non-null experiment field must be supplied")
        values["updated_at"] = utc_now()
        assignments = ", ".join(f"{column} = ?" for column in values)
        try:
            with self._transaction() as connection:
                cursor = connection.execute(
                    f"UPDATE experiments SET {assignments} WHERE experiment_id = ?",
                    (*values.values(), experiment_id),
                )
                if cursor.rowcount == 0:
                    raise ExperimentNotFound("Experiment was not found")
                row = connection.execute(
                    "SELECT * FROM experiments WHERE experiment_id = ?", (experiment_id,)
                ).fetchone()
        except sqlite3.IntegrityError as exc:
            raise ExperimentConflict("Experiment update conflicts with stored data") from exc
        return self._experiment(row)

    def create_run(
        self,
        experiment_id: str,
        run_id: Optional[str] = None,
        configuration: Optional[Dict[str, Any]] = None,
        workload_configuration: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        run_id = run_id or str(uuid4())
        created_at = utc_now()
        try:
            with self._transaction() as connection:
                experiment = connection.execute(
                    "SELECT default_configuration, default_workload_configuration FROM experiments WHERE experiment_id = ?",
                    (experiment_id,),
                ).fetchone()
                if experiment is None:
                    raise ExperimentNotFound("Experiment was not found")
                snapshot = self._configuration_json(
                    configuration if configuration is not None
                    else json.loads(experiment["default_configuration"])
                )
                workload_snapshot = self._workload_json(
                    workload_configuration if workload_configuration is not None
                    else json.loads(experiment["default_workload_configuration"])
                )
                connection.execute(
                    """INSERT INTO experiment_runs
                       (run_id, experiment_id, status, configuration_snapshot, created_at,
                        workload_snapshot, progress_planned)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (run_id, experiment_id, RunStatus.CREATED.value, snapshot, created_at,
                     workload_snapshot,
                     json.loads(workload_snapshot)["target_event_count"]),
                )
                row = connection.execute(
                    "SELECT * FROM experiment_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
        except sqlite3.IntegrityError as exc:
            raise ExperimentConflict("Run ID is already in use") from exc
        return self._run(row)

    def list_runs(self, experiment_id: str) -> list[Dict[str, Any]]:
        with self._reader() as connection:
            exists = connection.execute(
                "SELECT 1 FROM experiments WHERE experiment_id = ?", (experiment_id,)
            ).fetchone()
            if exists is None:
                raise ExperimentNotFound("Experiment was not found")
            rows = connection.execute(
                "SELECT * FROM experiment_runs WHERE experiment_id = ? ORDER BY created_at DESC, run_id",
                (experiment_id,),
            ).fetchall()
        return [self._run(row) for row in rows]

    def get_run(self, experiment_id: str, run_id: str) -> Dict[str, Any]:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM experiment_runs WHERE run_id = ? AND experiment_id = ?",
                (run_id, experiment_id),
            ).fetchone()
        if row is None:
            raise RunNotFound("Run was not found for this experiment")
        return self._run(row)

    def request_run_stop(
        self,
        experiment_id: str,
        run_id: str,
        target: RunStatus | str,
        *,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist an idempotent stop intent and close event admission atomically."""
        try:
            target = RunStatus(target)
        except (TypeError, ValueError) as exc:
            raise InvalidRunTransition("Stop target must be COMPLETED, FAILED, or CANCELLED") from exc
        if target not in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}:
            raise InvalidRunTransition("Stop target must be COMPLETED, FAILED, or CANCELLED")
        if reason is not None and (not isinstance(reason, str) or len(reason) > 2000):
            raise ValueError("stop reason must be at most 2000 characters")

        expired_completion_claim = False
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM experiment_runs WHERE run_id = ? AND experiment_id = ?",
                (run_id, experiment_id),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found for this experiment")

            existing_target = row["stop_target"]
            already_requested = existing_target is not None
            if already_requested:
                if existing_target != target.value:
                    raise InvalidRunTransition(
                        f"Run already has a {existing_target} stop intent"
                    )
            else:
                current = RunStatus(row["status"])
                if current not in {RunStatus.CREATED, RunStatus.RUNNING}:
                    raise InvalidRunTransition(
                        f"Run cannot accept a stop intent from {current.value}"
                    )
                if target in {RunStatus.COMPLETED, RunStatus.FAILED} and current != RunStatus.RUNNING:
                    raise InvalidRunTransition(
                        f"Only a running run can request {target.value.lower()}"
                    )
            now = utc_now()
            if self._claim_is_expired(row, now):
                self._record_expired_execution(connection, run_id, now)
                if target == RunStatus.COMPLETED:
                    expired_completion_claim = True
            if target == RunStatus.COMPLETED and not already_requested:
                if row["execution_interrupted_at"] is not None or row["execution_state"] == "EXPIRED":
                    raise InvalidRunTransition("An interrupted execution cannot request COMPLETED")
                if not expired_completion_claim:
                    workload = ExperimentWorkloadConfiguration.model_validate(
                        json.loads(row["workload_snapshot"])
                    )
                    planned = workload.target_event_count
                    if (row["progress_planned"] != planned
                            or row["progress_generated"] != planned
                            or row["progress_publish_confirmed"] != planned
                            or row["progress_publish_failed"] != 0
                            or row["progress_publish_unknown"] != 0):
                        raise InvalidRunTransition(
                            "COMPLETED requires the full planned workload and confirmed publishes"
                        )
                    evidence = connection.execute(
                        "SELECT COUNT(*) AS total, "
                        "SUM(CASE WHEN published = 1 AND publish_unknown = 0 THEN 1 ELSE 0 END) AS confirmed, "
                        "SUM(CASE WHEN publish_unknown = 1 AND settled = 0 THEN 1 ELSE 0 END) AS unresolved "
                        "FROM experiment_run_events WHERE run_id = ?",
                        (run_id,),
                    ).fetchone()
                    if (int(evidence["total"] or 0) != planned
                            or int(evidence["confirmed"] or 0) != planned
                            or int(evidence["unresolved"] or 0) != 0):
                        raise InvalidRunTransition(
                            "COMPLETED requires durable confirmation for every planned publish"
                        )
            if not already_requested and not expired_completion_claim:
                connection.execute(
                    "UPDATE experiment_runs SET stop_target = ?, stop_requested_at = ?, "
                    "stop_reason = ?, event_ingestion_closed = 1 "
                    "WHERE run_id = ? AND experiment_id = ?",
                    (target.value, utc_now(), reason, run_id, experiment_id),
                )

            row = connection.execute(
                "SELECT * FROM experiment_runs WHERE run_id = ? AND experiment_id = ?",
                (run_id, experiment_id),
            ).fetchone()

            pending_events = int(connection.execute(
                "SELECT COUNT(*) FROM experiment_run_events "
                "WHERE run_id = ? AND (published = 0 OR settled = 0 "
                "OR in_flight_deliveries > 0)",
                (run_id,),
            ).fetchone()[0])
        if expired_completion_claim:
            raise ExecutionClaimConflict(
                "Execution lease expired; interruption was recorded and COMPLETED was not requested"
            )
        return {
            "run": self._run(row),
            "already_requested": already_requested,
            "pending_events": pending_events,
        }

    def get_run_stop_intent(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Read the durable cooperative stop intent for a runner."""
        with self._reader() as connection:
            row = connection.execute(
                "SELECT stop_target, stop_requested_at, stop_reason "
                "FROM experiment_runs WHERE run_id = ?", (run_id,),
            ).fetchone()
        if row is None:
            raise RunNotFound("Run was not found")
        if row["stop_target"] is None:
            return None
        return {
            "target": row["stop_target"],
            "requested_at": row["stop_requested_at"],
            "reason": row["stop_reason"],
        }

    def get_run_with_configuration_snapshot(self, run_id: str) -> Dict[str, Any]:
        """Retrieve a globally identified run and validate its stored snapshot."""
        try:
            with self._reader() as connection:
                row = connection.execute(
                    "SELECT * FROM experiment_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
        except sqlite3.Error as exc:
            raise ExperimentStorageError("Could not retrieve run configuration") from exc
        if row is None:
            raise RunNotFound("Run was not found")
        try:
            raw_snapshot = json.loads(row["configuration_snapshot"])
            configuration = WindowConfiguration(**raw_snapshot)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise InvalidRunConfiguration("Stored run configuration snapshot is invalid") from exc
        run = self._run(row)
        run["configuration_snapshot"] = configuration.__dict__.copy()
        return run

    def get_run_with_workload_configuration_snapshot(self, run_id: str) -> Dict[str, Any]:
        """Return a runner-facing run after validating both immutable snapshots."""
        try:
            with self._reader() as connection:
                row = connection.execute(
                    "SELECT * FROM experiment_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
        except sqlite3.Error as exc:
            raise ExperimentStorageError("Could not retrieve run workload configuration") from exc
        if row is None:
            raise RunNotFound("Run was not found")
        try:
            configuration = WindowConfiguration(**json.loads(row["configuration_snapshot"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise InvalidRunConfiguration("Stored run configuration snapshot is invalid") from exc
        workload = self._validated_workload_snapshot(row["workload_snapshot"])
        run = self._run(row)
        run["configuration_snapshot"] = configuration.__dict__.copy()
        run["workload_snapshot"] = workload
        return run

    @staticmethod
    def _lease_expiry(lease_seconds: int) -> str:
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or not 1 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be an integer from 1 to 3600")
        return (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat()

    @staticmethod
    def _claim_is_expired(row: sqlite3.Row, now: str) -> bool:
        return row["execution_state"] == "CLAIMED" and (
            not row["execution_lease_expires_at"]
            or row["execution_lease_expires_at"] <= now
        )

    @staticmethod
    def _record_expired_execution(
        connection: sqlite3.Connection, run_id: str, observed_at: str,
    ) -> None:
        """Persist interruption and close admission atomically for an expired claim."""
        reason = "Execution lease expired; workload will not be resumed automatically"
        connection.execute(
            "UPDATE experiment_runs SET execution_state = 'EXPIRED', "
            "execution_owner_token = NULL, event_ingestion_closed = 1, "
            "execution_interrupted_at = COALESCE(execution_interrupted_at, ?), "
            "execution_interruption_reason = COALESCE(execution_interruption_reason, ?) "
            "WHERE run_id = ?",
            (observed_at, reason[:200], run_id),
        )

    def claim_run_execution(self, run_id: str, *, lease_seconds: int = 60) -> Dict[str, str]:
        """Claim a RUNNING run once; expired or released claims are never reacquired."""
        expiry = self._lease_expiry(lease_seconds)
        now = utc_now()
        token = secrets.token_urlsafe(32)
        expired_claim = False
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, event_ingestion_closed, execution_state, "
                "execution_lease_expires_at, workload_snapshot "
                "FROM experiment_runs WHERE run_id = ?", (run_id,),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found")
            if self._claim_is_expired(row, now):
                self._record_expired_execution(connection, run_id, now)
                expired_claim = True
            else:
                self._validated_workload_snapshot(row["workload_snapshot"])
                if row["status"] != RunStatus.RUNNING.value:
                    raise InvalidRunTransition("Only a RUNNING run can be claimed for execution")
                if row["execution_state"] not in {"NOT_STARTED", "CLAIMED"}:
                    raise ExecutionClaimConflict("Run execution claim cannot be reacquired")
                if row["event_ingestion_closed"]:
                    raise InvalidRunTransition("Run event ingestion is closed")
                if row["execution_state"] == "CLAIMED":
                    raise ExecutionClaimConflict("Run execution already has an active claim")
                if row["execution_state"] != "NOT_STARTED":
                    raise ExecutionClaimConflict("Run execution claim cannot be reacquired")
                connection.execute(
                    "UPDATE experiment_runs SET execution_state = 'CLAIMED', "
                    "execution_owner_token = ?, execution_claimed_at = ?, "
                    "execution_lease_expires_at = ? WHERE run_id = ?",
                    (token, now, expiry, run_id),
                )
        if expired_claim:
            raise ExecutionClaimConflict("Run execution lease expired; execution is not restarted automatically")
        return {"run_id": run_id, "owner_token": token, "lease_expires_at": expiry}

    def renew_run_execution_claim(
        self, run_id: str, owner_token: str, *, lease_seconds: int = 60
    ) -> str:
        expiry = self._lease_expiry(lease_seconds)
        now = utc_now()
        expired_claim = False
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT execution_state, execution_owner_token, execution_lease_expires_at "
                "FROM experiment_runs WHERE run_id = ?", (run_id,),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found")
            if row["execution_state"] != "CLAIMED" or row["execution_owner_token"] != owner_token:
                raise ExecutionClaimConflict("Execution claim is not owned by this caller")
            if self._claim_is_expired(row, now):
                self._record_expired_execution(connection, run_id, now)
                expired_claim = True
            else:
                connection.execute(
                    "UPDATE experiment_runs SET execution_lease_expires_at = ? WHERE run_id = ?",
                    (expiry, run_id),
                )
        if expired_claim:
            raise ExecutionClaimConflict("Execution lease expired and cannot be renewed")
        return expiry

    def release_run_execution_claim(self, run_id: str, owner_token: str) -> bool:
        """Retire a claim without making the run eligible for duplicate execution."""
        expired_claim = False
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT execution_state, execution_owner_token, execution_lease_expires_at "
                "FROM experiment_runs WHERE run_id = ?", (run_id,),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found")
            if row["execution_state"] != "CLAIMED" or row["execution_owner_token"] != owner_token:
                raise ExecutionClaimConflict("Active execution claim was not found for this owner")
            now = utc_now()
            if self._claim_is_expired(row, now):
                self._record_expired_execution(connection, run_id, now)
                expired_claim = True
            else:
                connection.execute(
                    "UPDATE experiment_runs SET execution_state = 'RELEASED', "
                    "execution_owner_token = NULL, execution_lease_expires_at = NULL "
                    "WHERE run_id = ?", (run_id,),
                )
        if expired_claim:
            raise ExecutionClaimConflict("Execution lease expired and cannot be released")
        return True

    def update_run_execution_progress(
        self, run_id: str, owner_token: str, *, operation_id: str, generated: int = 0,
        publish_confirmed: int = 0, publish_failed: int = 0,
        publish_unknown: int = 0,
    ) -> Dict[str, int]:
        if not isinstance(operation_id, str) or not operation_id.strip() or len(operation_id) > 200:
            raise ValueError("operation_id must be a non-empty string of at most 200 characters")
        increments = {
            "generated": generated,
            "publish_confirmed": publish_confirmed,
            "publish_failed": publish_failed,
            "publish_unknown": publish_unknown,
        }
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0
               for value in increments.values()):
            raise ValueError("progress increments must be non-negative integers")
        now = utc_now()
        expired_claim = False
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT execution_state, execution_owner_token, execution_lease_expires_at, "
                "progress_planned, progress_generated, progress_publish_confirmed, "
                "progress_publish_failed, progress_publish_unknown "
                "FROM experiment_runs WHERE run_id = ?", (run_id,),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found")
            previous = connection.execute(
                "SELECT owner_token, generated, publish_confirmed, publish_failed, publish_unknown "
                "FROM experiment_run_progress_operations WHERE run_id = ? AND operation_id = ?",
                (run_id, operation_id),
            ).fetchone()
            payload = (generated, publish_confirmed, publish_failed, publish_unknown)
            if previous is not None:
                prior_payload = tuple(previous[key] for key in (
                    "generated", "publish_confirmed", "publish_failed", "publish_unknown",
                ))
                if previous["owner_token"] != owner_token or prior_payload != payload:
                    raise ExecutionProgressConflict(
                        "Progress operation ID was already used with a different owner or payload"
                    )
                if self._claim_is_expired(row, now):
                    self._record_expired_execution(connection, run_id, now)
                return {
                    "planned": row["progress_planned"], "generated": row["progress_generated"],
                    "publish_confirmed": row["progress_publish_confirmed"],
                    "publish_failed": row["progress_publish_failed"],
                    "publish_unknown": row["progress_publish_unknown"],
                }
            if row["execution_state"] != "CLAIMED" or row["execution_owner_token"] != owner_token:
                raise ExecutionClaimConflict("Execution claim is not owned by this caller")
            if self._claim_is_expired(row, now):
                self._record_expired_execution(connection, run_id, now)
                expired_claim = True
            else:
                next_generated = row["progress_generated"] + generated
                next_confirmed = row["progress_publish_confirmed"] + publish_confirmed
                next_failed = row["progress_publish_failed"] + publish_failed
                next_unknown = row["progress_publish_unknown"] + publish_unknown
                if next_generated > row["progress_planned"]:
                    raise ValueError("generated progress cannot exceed planned events")
                if next_confirmed + next_failed + next_unknown > next_generated:
                    raise ValueError("publish outcomes cannot exceed generated events")
                connection.execute(
                    "INSERT INTO experiment_run_progress_operations "
                    "(run_id, operation_id, owner_token, generated, publish_confirmed, "
                    "publish_failed, publish_unknown, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (run_id, operation_id, owner_token, generated, publish_confirmed,
                     publish_failed, publish_unknown, now),
                )
                connection.execute(
                    "UPDATE experiment_runs SET progress_generated = ?, "
                    "progress_publish_confirmed = ?, progress_publish_failed = ?, "
                    "progress_publish_unknown = ? WHERE run_id = ?",
                    (next_generated, next_confirmed, next_failed, next_unknown, run_id),
                )
                return {
                    "planned": row["progress_planned"], "generated": next_generated,
                    "publish_confirmed": next_confirmed, "publish_failed": next_failed,
                    "publish_unknown": next_unknown,
                }
        if expired_claim:
            raise ExecutionClaimConflict("Execution lease expired; progress update rejected")
        raise ExecutionClaimConflict("Execution claim could not be updated")

    def get_run_execution_progress(self, experiment_id: str, run_id: str) -> Dict[str, Any]:
        run = self.get_run(experiment_id, run_id)
        return {
            "run_id": run_id,
            "execution": run["execution"],
            "workload": run["workload_snapshot"],
            "progress": run["progress"],
            "stop_intent": run["stop_intent"],
            "event_ingestion_closed": run["event_ingestion_closed"],
        }

    def reserve_run_event(self, run_id: str, event_id: str) -> None:
        """Atomically reserve an event only while its run accepts submissions."""
        interrupted = False
        admission_error: Optional[InvalidRunTransition] = None
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, event_ingestion_closed, execution_state, "
                "execution_lease_expires_at FROM experiment_runs "
                "WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found for this experiment")
            now = utc_now()
            if self._claim_is_expired(row, now):
                self._record_expired_execution(connection, run_id, now)
                interrupted = True
            elif row["status"] != RunStatus.RUNNING.value or row["event_ingestion_closed"]:
                admission_error = InvalidRunTransition("Run is not accepting events")
            if not interrupted and admission_error is None:
                try:
                    connection.execute(
                        "INSERT INTO experiment_run_events (event_id, run_id, state, created_at) "
                        "VALUES (?, ?, 'RESERVED', ?)",
                        (event_id, run_id, now),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ExperimentConflict("Event reservation already exists") from exc
        if interrupted:
            raise InvalidRunTransition("Run execution expired; event ingestion is closed")
        if admission_error is not None:
            raise admission_error

    def claim_run_event(self, run_id: str, event_id: str) -> bool:
        """Mark an accepted event as processing; PROCESSING permits broker redelivery."""
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE experiment_run_events SET state = 'PROCESSING', "
                "published = CASE WHEN publish_unknown = 1 THEN 1 ELSE published END, "
                "in_flight_deliveries = in_flight_deliveries + 1 "
                "WHERE event_id = ? AND run_id = ? AND state IN ('RESERVED', 'PROCESSING') "
                "AND (settled = 0 OR EXISTS (SELECT 1 FROM experiment_runs r "
                "WHERE r.run_id = experiment_run_events.run_id AND r.status = 'RUNNING' "
                "AND r.event_ingestion_closed = 0))",
                (event_id, run_id),
            )
            return cursor.rowcount == 1

    def settle_run_event(self, run_id: str, event_id: str, succeeded: bool) -> bool:
        """Persist the first consumer outcome; later broker redeliveries remain valid."""
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT settled FROM experiment_run_events WHERE event_id = ? AND run_id = ?",
                (event_id, run_id),
            ).fetchone()
            if row is None:
                return False
            cursor = connection.execute(
                "UPDATE experiment_run_events SET published = 1, "
                "settled = 1, publish_unknown = 0, outcome = CASE WHEN settled = 0 THEN ? ELSE outcome END, "
                "in_flight_deliveries = CASE WHEN in_flight_deliveries > 0 "
                "THEN in_flight_deliveries - 1 ELSE 0 END "
                "WHERE event_id = ? AND run_id = ? AND settled = 0",
                ("SUCCESS" if succeeded else "FAILURE", event_id, run_id),
            )
            if cursor.rowcount == 1:
                return True
            # A previous delivery may already have settled this event. A new
            # claim still increments the in-flight count, which must be drained.
            cursor = connection.execute(
                "UPDATE experiment_run_events SET in_flight_deliveries = "
                "CASE WHEN in_flight_deliveries > 0 THEN in_flight_deliveries - 1 ELSE 0 END "
                "WHERE event_id = ? AND run_id = ? AND settled = 1",
                (event_id, run_id),
            )
            return cursor.rowcount == 1

    def mark_run_event_published(self, run_id: str, event_id: str) -> bool:
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE experiment_run_events SET published = 1 "
                "WHERE event_id = ? AND run_id = ?",
                (event_id, run_id),
            )
            return cursor.rowcount == 1

    def mark_run_event_publish_unknown(self, run_id: str, event_id: str) -> bool:
        """Retain a reservation when broker acceptance cannot be determined."""
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE experiment_run_events SET publish_unknown = 1, "
                "published = CASE WHEN state = 'PROCESSING' THEN 1 ELSE published END "
                "WHERE event_id = ? AND run_id = ? AND settled = 0",
                (event_id, run_id),
            )
            return cursor.rowcount == 1

    def unresolved_publish_count(self, run_id: str) -> int:
        with self._reader() as connection:
            return int(connection.execute(
                "SELECT COUNT(*) FROM experiment_run_events "
                "WHERE run_id = ? AND publish_unknown = 1 AND settled = 0",
                (run_id,),
            ).fetchone()[0])

    def release_unpublished_run_event(self, run_id: str, event_id: str) -> bool:
        """Release only a reservation not yet claimed by the consumer."""
        with self._transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM experiment_run_events "
                "WHERE event_id = ? AND run_id = ? AND state = 'RESERVED' "
                "AND published = 0 AND publish_unknown = 0",
                (event_id, run_id),
            )
            if cursor.rowcount:
                return True
            # A broker error can be ambiguous. If the consumer already claimed
            # this event, retain it as accepted and wait for its processing receipt.
            cursor = connection.execute(
                "UPDATE experiment_run_events SET published = 1 "
                "WHERE event_id = ? AND run_id = ? AND state = 'PROCESSING'",
                (event_id, run_id),
            )
            return cursor.rowcount == 1

    def close_run_event_ingestion(self, experiment_id: str, run_id: str) -> int:
        """Close the submission gate and return accepted events still in flight."""
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, execution_state, execution_owner_token, "
                "execution_lease_expires_at FROM experiment_runs "
                "WHERE run_id = ? AND experiment_id = ?",
                (run_id, experiment_id),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found for this experiment")
            status = RunStatus(row["status"])
            if status not in {RunStatus.CREATED, RunStatus.RUNNING}:
                raise InvalidRunTransition(f"Run cannot close from {status.value}")
            if row["execution_state"] == "CLAIMED":
                now = utc_now()
                if not self._claim_is_expired(row, now):
                    raise ExecutionClaimConflict("Run has an active execution claim")
                self._record_expired_execution(connection, run_id, now)
            connection.execute(
                "UPDATE experiment_runs SET event_ingestion_closed = 1 "
                "WHERE run_id = ? AND experiment_id = ?",
                (run_id, experiment_id),
            )
            return int(connection.execute(
                "SELECT COUNT(*) FROM experiment_run_events "
                "WHERE run_id = ? AND (published = 0 OR settled = 0 OR in_flight_deliveries > 0)",
                (run_id,),
            ).fetchone()[0])

    def run_event_accounting(self, run_id: str) -> Dict[str, int]:
        with self._reader() as connection:
            row = connection.execute(
                """SELECT
                    SUM(CASE WHEN published = 1 OR settled = 1 THEN 1 ELSE 0 END) AS accepted,
                    SUM(CASE WHEN settled = 1 AND outcome = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
                    SUM(CASE WHEN settled = 1 AND outcome = 'FAILURE' THEN 1 ELSE 0 END) AS failures,
                    SUM(CASE WHEN publish_unknown = 1 AND settled = 0 THEN 1 ELSE 0 END) AS unresolved
                   FROM experiment_run_events WHERE run_id = ?""",
                (run_id,),
            ).fetchone()
        return {
            "accepted_events": int(row["accepted"] or 0),
            "consumer_processed_successes": int(row["successes"] or 0),
            "consumer_processed_failures": int(row["failures"] or 0),
            "unresolved_publish_count": int(row["unresolved"] or 0),
        }

    @staticmethod
    def _required_reconciliation_text(name: str, value: Any, max_length: int) -> str:
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a non-empty string")
        value = value.strip()
        if not value or len(value) > max_length:
            raise ValueError(f"{name} must contain 1 to {max_length} characters")
        return value

    def get_run_pending_event_inventory(
        self, experiment_id: str, run_id: str,
    ) -> Dict[str, Any]:
        """Return one consistent snapshot of rows that block terminal transition."""
        pending_predicate = (
            "(e.published = 0 OR e.settled = 0 OR e.in_flight_deliveries > 0)"
        )
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT r.run_id, r.experiment_id, r.status, r.event_ingestion_closed, "
                "r.stop_target, r.stop_requested_at, r.stop_reason, "
                "e.event_id, e.state, e.created_at, e.published, e.settled, e.outcome, "
                "e.in_flight_deliveries, e.publish_unknown "
                "FROM experiment_runs r LEFT JOIN experiment_run_events e "
                "ON e.run_id = r.run_id AND " + pending_predicate + " "
                "WHERE r.experiment_id = ? AND r.run_id = ? "
                "ORDER BY e.created_at, e.event_id",
                (experiment_id, run_id),
            ).fetchall()
        if not rows:
            raise RunNotFound("Run was not found for this experiment")

        run_row = rows[0]
        events = []
        for row in rows:
            if row["event_id"] is None:
                continue
            event = {
                "event_id": row["event_id"],
                "run_id": row["run_id"],
                "state": row["state"],
                "created_at": row["created_at"],
                "published": row["published"],
                "settled": row["settled"],
                "outcome": row["outcome"],
                "in_flight_deliveries": row["in_flight_deliveries"],
                "publish_unknown": row["publish_unknown"],
            }
            if event["publish_unknown"]:
                event["publish_classification"] = "PUBLISH_UNKNOWN_MARKER_PERSISTED"
            elif event["published"]:
                event["publish_classification"] = "PUBLISHED_FLAG_SET"
            else:
                event["publish_classification"] = "PUBLISH_OUTCOME_NOT_ESTABLISHED"
            event["consumer_claim_activity_recorded"] = bool(
                event["state"] == "PROCESSING" or event["in_flight_deliveries"] > 0
            )
            event["consumer_receipt_persisted"] = bool(event["settled"])
            events.append(event)

        summary = {
            "pending_count": len(events),
            # Preserve the existing unresolved-publish meaning exactly.
            "unresolved_publish_count": sum(
                event["publish_unknown"] == 1 and event["settled"] == 0
                for event in events
            ),
            "published_flag_unsettled_count": sum(
                event["published"] == 1 and event["settled"] == 0
                for event in events
            ),
            "publish_outcome_not_established_count": sum(
                event["published"] == 0 and event["publish_unknown"] == 0
                and event["settled"] == 0
                for event in events
            ),
            "consumer_claim_activity_count": sum(
                event["consumer_claim_activity_recorded"] for event in events
            ),
            "consumer_receipt_persisted_count": sum(
                event["consumer_receipt_persisted"] for event in events
            ),
        }
        return {
            "run_id": run_row["run_id"],
            "experiment_id": run_row["experiment_id"],
            "status": run_row["status"],
            "event_ingestion_closed": bool(run_row["event_ingestion_closed"]),
            "stop_intent": (
                None if run_row["stop_target"] is None else {
                    "target": run_row["stop_target"],
                    "requested_at": run_row["stop_requested_at"],
                    "reason": run_row["stop_reason"],
                }
            ),
            "summary": summary,
            "events": events,
        }

    def record_run_event_reconciliation_evidence(
        self,
        experiment_id: str,
        run_id: str,
        event_id: str,
        *,
        evidence_source: str,
        evidence_reference: str,
        evidence_details: str,
    ) -> Dict[str, Any]:
        """Durably record an assessment without changing reservation state.

        The application has no independently verifiable per-event broker
        evidence. Operator-supplied material is retained for review but never
        authorizes deleting or settling a reservation here.
        """
        evidence_source = self._required_reconciliation_text(
            "evidence_source", evidence_source, 200,
        )
        evidence_reference = self._required_reconciliation_text(
            "evidence_reference", evidence_reference, 1000,
        )
        evidence_details = self._required_reconciliation_text(
            "evidence_details", evidence_details, 8000,
        )
        reconciliation_id = str(uuid4())
        recorded_at = utc_now()
        with self._transaction() as connection:
            run = connection.execute(
                "SELECT 1 FROM experiment_runs WHERE experiment_id = ? AND run_id = ?",
                (experiment_id, run_id),
            ).fetchone()
            if run is None:
                raise RunNotFound("Run was not found for this experiment")
            row = connection.execute(
                "SELECT event_id, run_id, state, created_at, published, settled, outcome, "
                "in_flight_deliveries, publish_unknown FROM experiment_run_events "
                "WHERE run_id = ? AND event_id = ?",
                (run_id, event_id),
            ).fetchone()
            if row is None:
                raise RunEventNotFound("Reservation was not found for this run")
            observed_state = dict(row)
            if observed_state["settled"]:
                reason = "A consumer receipt is already persisted; reconciliation will not alter it."
            elif (observed_state["state"] == "PROCESSING"
                  or observed_state["in_flight_deliveries"] > 0):
                reason = (
                    "Consumer claim activity or in-flight deliveries are recorded; "
                    "the reservation cannot be released."
                )
            elif observed_state["published"]:
                reason = (
                    "The published flag is set; the message may still be delivered "
                    "or processed."
                )
            else:
                reason = (
                    "Evidence is recorded but not independently verifiable by "
                    "StreamPulse; the reservation was left unchanged. Queue "
                    "snapshots, database flags, and operator assertions alone "
                    "cannot establish that delivery or processing is impossible."
                )
            connection.execute(
                "INSERT INTO experiment_run_event_reconciliation_audit "
                "(reconciliation_id, run_id, event_id, evidence_source, "
                "evidence_reference, evidence_details, observed_event_state_json, "
                "disposition, decision_reason, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'NOT_RECONCILED', ?, ?)",
                (
                    reconciliation_id, run_id, event_id, evidence_source,
                    evidence_reference, evidence_details,
                    json.dumps(observed_state, separators=(",", ":"), sort_keys=True),
                    reason, recorded_at,
                ),
            )
        return {
            "reconciliation_id": reconciliation_id,
            "run_id": run_id,
            "event_id": event_id,
            "evidence_source": evidence_source,
            "evidence_reference": evidence_reference,
            "disposition": "NOT_RECONCILED",
            "decision_reason": reason,
            "reservation_changed": False,
            "recorded_at": recorded_at,
        }

    def finalize_run_after_drain(
        self,
        experiment_id: str,
        run_id: str,
        *,
        ended_at: str,
        results: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Atomically commit a result for the durable stop target after drain."""
        if not isinstance(results, dict):
            raise ValueError("terminal result snapshot must be an object")
        serialized_results = json.dumps(
            results, separators=(",", ":"), allow_nan=False,
        )
        expired_claim = False
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM experiment_runs WHERE run_id = ? AND experiment_id = ?",
                (run_id, experiment_id),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found for this experiment")

            target_value = row["stop_target"]
            if target_value is None:
                raise InvalidRunTransition("Run has no persisted stop intent")
            target = RunStatus(target_value)
            current = RunStatus(row["status"])
            if current in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}:
                if current != target or row["results_json"] is None:
                    raise InvalidRunTransition("Run is already terminal with a different outcome")
                terminal_row = row
            else:
                now = utc_now()
                if target == RunStatus.COMPLETED and (
                    row["execution_interrupted_at"] is not None
                    or row["execution_state"] == "EXPIRED"
                ):
                    raise InvalidRunTransition(
                        "An interrupted execution cannot be finalized as COMPLETED"
                    )
                if self._claim_is_expired(row, now):
                    self._record_expired_execution(connection, run_id, now)
                    expired_claim = True
                elif row["execution_state"] == "CLAIMED":
                    raise ExecutionClaimConflict("Run has an active execution claim")

                if not expired_claim:
                    if current == RunStatus.CREATED:
                        if target != RunStatus.CANCELLED:
                            raise InvalidRunTransition(
                                "Only an unstarted run cancellation can be finalized"
                            )
                    elif current != RunStatus.RUNNING:
                        raise InvalidRunTransition(
                            f"Run cannot be finalized from {current.value}"
                        )

                    if target == RunStatus.COMPLETED:
                        workload = ExperimentWorkloadConfiguration.model_validate(
                            json.loads(row["workload_snapshot"])
                        )
                        planned = workload.target_event_count
                        if (row["progress_planned"] != planned
                                or row["progress_generated"] != planned
                                or row["progress_publish_confirmed"] != planned
                                or row["progress_publish_failed"] != 0
                                or row["progress_publish_unknown"] != 0):
                            raise InvalidRunTransition(
                                "COMPLETED requires the full planned workload and confirmed publishes"
                            )

                    if not row["event_ingestion_closed"]:
                        raise InvalidRunTransition("Run event ingestion must be closed before finalization")
                    pending = int(connection.execute(
                        "SELECT COUNT(*) FROM experiment_run_events "
                        "WHERE run_id = ? AND (published = 0 OR settled = 0 "
                        "OR in_flight_deliveries > 0)",
                        (run_id,),
                    ).fetchone()[0])
                    if pending:
                        raise InvalidRunTransition(
                            "Run still has accepted events awaiting consumer processing"
                        )
                    accounting_row = connection.execute(
                        """SELECT
                            SUM(CASE WHEN published = 1 OR settled = 1 THEN 1 ELSE 0 END) AS accepted,
                            SUM(CASE WHEN settled = 1 AND outcome = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
                            SUM(CASE WHEN settled = 1 AND outcome = 'FAILURE' THEN 1 ELSE 0 END) AS failures,
                            SUM(CASE WHEN publish_unknown = 1 AND settled = 0 THEN 1 ELSE 0 END) AS unresolved
                           FROM experiment_run_events WHERE run_id = ?""",
                        (run_id,),
                    ).fetchone()
                    current_accounting = {
                        "accepted_events": int(accounting_row["accepted"] or 0),
                        "consumer_processed_successes": int(accounting_row["successes"] or 0),
                        "consumer_processed_failures": int(accounting_row["failures"] or 0),
                        "unresolved_publish_count": int(accounting_row["unresolved"] or 0),
                    }
                    if target == RunStatus.COMPLETED:
                        publish_evidence = connection.execute(
                            "SELECT COUNT(*) AS total, "
                            "SUM(CASE WHEN published = 1 AND publish_unknown = 0 THEN 1 ELSE 0 END) AS confirmed, "
                            "SUM(CASE WHEN publish_unknown = 1 AND settled = 0 THEN 1 ELSE 0 END) AS unresolved "
                            "FROM experiment_run_events WHERE run_id = ?",
                            (run_id,),
                        ).fetchone()
                        if (int(publish_evidence["total"] or 0) != row["progress_planned"]
                                or int(publish_evidence["confirmed"] or 0) != row["progress_planned"]
                                or int(publish_evidence["unresolved"] or 0) != 0):
                            raise InvalidRunTransition(
                                "COMPLETED requires durable confirmation for every planned publish"
                            )
                    if results.get("event_accounting") != current_accounting:
                        raise InvalidRunTransition(
                            "Run accounting changed while the result was being built; finalization must be retried"
                        )

                    connection.execute(
                        "UPDATE experiment_runs SET status = ?, ended_at = ?, "
                        "failure_reason = ?, results_json = ? "
                        "WHERE run_id = ? AND experiment_id = ?",
                        (target.value, ended_at, row["stop_reason"], serialized_results,
                         run_id, experiment_id),
                    )
                    connection.execute(
                        "DELETE FROM experiment_run_events WHERE run_id = ?", (run_id,),
                    )
                    terminal_row = connection.execute(
                        "SELECT * FROM experiment_runs WHERE run_id = ? AND experiment_id = ?",
                        (run_id, experiment_id),
                    ).fetchone()
                else:
                    terminal_row = connection.execute(
                        "SELECT * FROM experiment_runs WHERE run_id = ? AND experiment_id = ?",
                        (run_id, experiment_id),
                    ).fetchone()

        if expired_claim:
            raise ExecutionClaimConflict(
                "Execution lease expired; interruption was recorded and finalization must be retried"
            )
        return self._run(terminal_row)

    def transition_run(
        self,
        experiment_id: str,
        run_id: str,
        target: RunStatus,
        *,
        started_at: Optional[str] = None,
        ended_at: Optional[str] = None,
        failure_reason: Optional[str] = None,
        results: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if target != RunStatus.RUNNING:
            raise InvalidRunTransition(
                "Terminal run transitions must use finalize_run_after_drain"
            )
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM experiment_runs WHERE run_id = ? AND experiment_id = ?",
                (run_id, experiment_id),
            ).fetchone()
            if row is None:
                raise RunNotFound("Run was not found for this experiment")
            current = RunStatus(row["status"])
            if current != RunStatus.CREATED:
                raise InvalidRunTransition(
                    f"Run cannot transition from {current.value} to {target.value}"
                )
            if row["event_ingestion_closed"]:
                raise InvalidRunTransition("Run event ingestion is already closed")
            connection.execute(
                "UPDATE experiment_runs SET status = ?, started_at = ? WHERE run_id = ?",
                (target.value, started_at or utc_now(), run_id),
            )
            updated = connection.execute(
                "SELECT * FROM experiment_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._run(updated)

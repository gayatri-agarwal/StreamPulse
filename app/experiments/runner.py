"""Bounded asynchronous workload execution for experiment runs."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import random
import sqlite3
from typing import Any, Awaitable, Callable, Dict, Optional
from uuid import uuid4

from app.experiments.schemas import ExperimentWorkloadConfiguration, RunStatus
from app.experiments.store import (
    ExecutionClaimConflict,
    ExecutionProgressConflict,
    ExperimentRepository,
    InvalidRunTransition,
    RunNotFound,
)
from app.schemas import EventType, LibraryEvent


GLOBAL_EXPERIMENT_PUBLISH_INTERVAL_SECONDS = 0.01


@dataclass(frozen=True)
class EventPublishResult:
    """Outcome returned by StreamPulse's shared reservation/publish path."""

    disposition: str  # confirmed, failed, or unknown
    detail: Optional[str] = None
    http_status: int = 503
    stop_execution: bool = False
    lease_lost: bool = False
    accounting_settled: bool = True


PublishEvent = Callable[[LibraryEvent, Callable[[], bool]], Awaitable[EventPublishResult]]


class ExperimentWorkloadRunner:
    """Executes one finite run while renewing its persistent owner lease."""

    def __init__(
        self,
        repository: ExperimentRepository,
        publish_event: PublishEvent,
        *,
        publish_lock: Optional[asyncio.Lock] = None,
        lease_seconds: int = 60,
        renewal_interval_seconds: float = 15.0,
        progress_retry_attempts: int = 3,
    ) -> None:
        self.repository = repository
        self.publish_event = publish_event
        self.publish_lock = publish_lock or asyncio.Lock()
        self.lease_seconds = lease_seconds
        self.renewal_interval_seconds = renewal_interval_seconds
        self.progress_retry_attempts = progress_retry_attempts
        self._next_global_publish_at = 0.0

    @staticmethod
    def build_event(run_id: str, event_type: EventType) -> LibraryEvent:
        return LibraryEvent.from_mapping({
            "event_id": str(uuid4()),
            "event_type": event_type.value,
            "student_id": f"STU-{uuid4().hex[:12].upper()}",
            "book_id": f"BK-{uuid4().hex[:12].upper()}",
            "timestamp": datetime.now(timezone.utc),
            "run_id": run_id,
        })

    @staticmethod
    def choose_event_type(distribution: Dict[str, int]) -> EventType:
        types = [EventType(value) for value in distribution]
        weights = [distribution[event_type.value] for event_type in types]
        return random.choices(types, weights=weights, k=1)[0]

    async def _record_progress(
        self,
        run_id: str,
        owner_token: str,
        *,
        operation_id: str,
        generated: int = 0,
        publish_confirmed: int = 0,
        publish_failed: int = 0,
        publish_unknown: int = 0,
    ) -> bool:
        arguments = {
            "operation_id": operation_id,
            "generated": generated,
            "publish_confirmed": publish_confirmed,
            "publish_failed": publish_failed,
            "publish_unknown": publish_unknown,
        }
        for attempt in range(self.progress_retry_attempts):
            try:
                await asyncio.to_thread(
                    self.repository.update_run_execution_progress,
                    run_id, owner_token, **arguments,
                )
                return True
            except (ExecutionClaimConflict, ExecutionProgressConflict, RunNotFound, ValueError):
                return False
            except Exception:
                if attempt + 1 >= self.progress_retry_attempts:
                    return False
                await asyncio.sleep(0.05 * (attempt + 1))
        return False

    async def _renew_lease(self, run_id: str, owner_token: str, lost: asyncio.Event) -> None:
        while not lost.is_set():
            try:
                await asyncio.sleep(self.renewal_interval_seconds)
                if lost.is_set():
                    return
                await asyncio.to_thread(
                    self.repository.renew_run_execution_claim,
                    run_id, owner_token, lease_seconds=self.lease_seconds,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                lost.set()
                return

    async def _stop_requested(self, run_id: str) -> bool:
        return await asyncio.to_thread(
            self.repository.get_run_stop_intent, run_id,
        ) is not None

    async def _wait_or_stop(
        self, delay_seconds: float, lost: asyncio.Event, run_id: str,
    ) -> bool:
        deadline = asyncio.get_running_loop().time() + max(0.0, delay_seconds)
        while not lost.is_set():
            if await self._stop_requested(run_id):
                return True
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return lost.is_set()
            if await self._wait_or_lease_loss(min(remaining, 0.2), lost):
                return True
        return True

    @staticmethod
    async def _wait_or_lease_loss(delay_seconds: float, lost: asyncio.Event) -> bool:
        if lost.is_set():
            return True
        try:
            await asyncio.wait_for(lost.wait(), timeout=max(0.0, delay_seconds))
            return True
        except asyncio.TimeoutError:
            return lost.is_set()

    async def _publish_sequentially(self, event: LibraryEvent, lost: asyncio.Event):
        async with self.publish_lock:
            loop = asyncio.get_running_loop()
            delay = self._next_global_publish_at - loop.time()
            if await self._wait_or_stop(delay, lost, event.run_id or ""):
                if not lost.is_set() and event.run_id is not None:
                    return EventPublishResult(
                        "failed",
                        "Run stop intent was observed before reservation",
                        http_status=409,
                        stop_execution=True,
                    )
                return None
            if lost.is_set():
                return None
            if event.run_id is not None and await self._stop_requested(event.run_id):
                return EventPublishResult(
                    "failed",
                    "Run stop intent was observed before reservation",
                    http_status=409,
                    stop_execution=True,
                )
            self._next_global_publish_at = loop.time() + GLOBAL_EXPERIMENT_PUBLISH_INTERVAL_SECONDS
            return await self.publish_event(event, lambda: not lost.is_set())

    async def execute(self, run: Dict[str, Any], claim: Dict[str, str]) -> Dict[str, Any]:
        run_id = run["run_id"]
        owner_token = claim["owner_token"]
        workload = ExperimentWorkloadConfiguration.model_validate(run["workload_snapshot"])
        planned = workload.target_event_count
        lease_lost = asyncio.Event()
        renewer = asyncio.create_task(self._renew_lease(run_id, owner_token, lease_lost))
        safe_to_release = False
        generated = 0
        completed_loop = True
        progress_confirmed = True
        publish_accounting_confirmed = True
        publish_confirmed = 0
        try:
            for event_number in range(planned):
                if lease_lost.is_set():
                    completed_loop = False
                    break
                if await self._stop_requested(run_id):
                    completed_loop = False
                    break
                event_type = self.choose_event_type(workload.event_type_distribution)
                event = self.build_event(run_id, event_type)
                generated_operation_id = str(uuid4())
                if not await self._record_progress(
                    run_id, owner_token, operation_id=generated_operation_id, generated=1,
                ):
                    completed_loop = False
                    progress_confirmed = False
                    break
                generated += 1
                if lease_lost.is_set():
                    completed_loop = False
                    break
                if await self._stop_requested(run_id):
                    outcome_recorded = await self._record_progress(
                        run_id, owner_token, operation_id=str(uuid4()),
                        publish_failed=1,
                    )
                    if not outcome_recorded:
                        progress_confirmed = False
                    completed_loop = False
                    break

                result = await self._publish_sequentially(event, lease_lost)
                if result is None:
                    completed_loop = False
                    break
                if result.lease_lost:
                    lease_lost.set()

                outcome_increment = {
                    "publish_confirmed": int(result.disposition == "confirmed"),
                    "publish_failed": int(result.disposition == "failed"),
                    "publish_unknown": int(result.disposition == "unknown"),
                }
                outcome_recorded = await self._record_progress(
                    run_id, owner_token, operation_id=str(uuid4()), **outcome_increment,
                )
                if result.disposition == "confirmed":
                    publish_confirmed += 1
                if not result.accounting_settled:
                    publish_accounting_confirmed = False
                if not outcome_recorded or lease_lost.is_set():
                    completed_loop = False
                    if not outcome_recorded:
                        progress_confirmed = False
                    break
                if result.stop_execution:
                    completed_loop = False
                    break

                if await self._stop_requested(run_id):
                    completed_loop = False
                    break

                if event_number + 1 < planned:
                    if await self._wait_or_stop(
                        workload.publish_interval_ms / 1000.0, lease_lost, run_id,
                    ):
                        completed_loop = False
                        break
                    if await self._stop_requested(run_id):
                        completed_loop = False
                        break

            safe_to_release = (
                progress_confirmed
                and publish_accounting_confirmed
                and not lease_lost.is_set()
            )
            if (safe_to_release and completed_loop and generated == planned
                    and publish_confirmed == planned):
                try:
                    await asyncio.to_thread(
                        self.repository.request_run_stop,
                        run["experiment_id"], run_id, RunStatus.COMPLETED,
                        reason="Planned workload published successfully",
                    )
                except InvalidRunTransition:
                    # A concurrently persisted stop target wins the race.
                    try:
                        existing_intent = await asyncio.to_thread(
                            self.repository.get_run_stop_intent, run_id,
                        )
                    except Exception:
                        safe_to_release = False
                    else:
                        safe_to_release = existing_intent is not None
                except Exception:
                    # Stop intent must be durable before the claim is released.
                    safe_to_release = False
                else:
                    safe_to_release = True
        finally:
            renewer.cancel()
            try:
                await renewer
            except asyncio.CancelledError:
                pass

        claim_released = False
        if safe_to_release and not lease_lost.is_set():
            try:
                claim_released = await asyncio.to_thread(
                    self.repository.release_run_execution_claim, run_id, owner_token,
                )
            except Exception:
                # Leave an unconfirmed release to expire; never reacquire it.
                pass

        return {
            "run_id": run_id,
            "generated_in_this_process": generated,
            "stopped_for_lease_loss": lease_lost.is_set(),
            "claim_released": claim_released,
        }

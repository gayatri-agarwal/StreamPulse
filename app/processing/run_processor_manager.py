"""Lazily manage isolated in-memory processors for experiment runs."""

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict

from app.experiments.schemas import RunStatus
from app.experiments.store import ExperimentRepository
from app.processing.event_processor import LibraryEventProcessor
from app.processing.window_config import WindowConfiguration
from app.processing.window_manager import SlidingWindow, TumblingWindow


logger = logging.getLogger(__name__)


@dataclass
class _RunProcessorContext:
    experiment_id: str
    configuration: WindowConfiguration
    processor: LibraryEventProcessor
    users: int = 0


class RunProcessorManager:
    """Keep one snapshot-configured processor and window pair per active run."""

    def __init__(self, repository: ExperimentRepository, metrics_store: object) -> None:
        self._repository = repository
        self._metrics_store = metrics_store
        self._contexts: Dict[str, _RunProcessorContext] = {}
        self._lock = asyncio.Lock()

    async def configuration_for(self, run_id: str) -> tuple[str, WindowConfiguration]:
        """Reuse a cached immutable snapshot or load and validate it from SQLite."""
        async with self._lock:
            context = self._contexts.get(run_id)
            if context is not None:
                return context.experiment_id, context.configuration
        run = await asyncio.to_thread(
            self._repository.get_run_with_configuration_snapshot, run_id
        )
        configuration = WindowConfiguration(**run["configuration_snapshot"])
        return run["experiment_id"], configuration

    async def acquire(
        self, run_id: str, experiment_id: str, configuration: WindowConfiguration
    ) -> LibraryEventProcessor:
        """Return the run processor while protecting it from terminal cleanup."""
        async with self._lock:
            context = self._contexts.get(run_id)
            if context is None:
                processor = LibraryEventProcessor(
                    TumblingWindow(configuration.tumbling_size),
                    SlidingWindow(configuration.sliding_size, configuration.sliding_interval),
                    self._metrics_store,
                    configuration,
                    run_id=run_id,
                )
                context = _RunProcessorContext(experiment_id, configuration, processor)
                self._contexts[run_id] = context
            elif (
                context.experiment_id != experiment_id
                or context.configuration != configuration
            ):
                raise ValueError("Cached run processor does not match its stored configuration")
            context.users += 1
            return context.processor

    async def release(self, run_id: str) -> None:
        async with self._lock:
            context = self._contexts.get(run_id)
            if context is not None and context.users > 0:
                context.users -= 1

    async def cleanup_terminal(self) -> int:
        """Drop only terminal contexts whose accepted processing leases drained."""
        async with self._lock:
            candidates = list(self._contexts.items())
        terminal_ids = set()
        for run_id, context in candidates:
            try:
                run = await asyncio.to_thread(
                    self._repository.get_run, context.experiment_id, run_id
                )
            except Exception:
                # A failed or missing status read is not grounds to discard state.
                logger.warning("Could not check run state for processor cleanup; retaining context.")
                continue
            if run["status"] in {
                RunStatus.COMPLETED.value,
                RunStatus.FAILED.value,
                RunStatus.CANCELLED.value,
            }:
                terminal_ids.add(run_id)
        removed = 0
        async with self._lock:
            for run_id in terminal_ids:
                context = self._contexts.get(run_id)
                if context is not None and context.users == 0:
                    del self._contexts[run_id]
                    removed += 1
        return removed

    async def context_count(self) -> int:
        async with self._lock:
            return len(self._contexts)
